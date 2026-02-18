//! Direct-to-columnar Arrow batch building.
//!
//! Eliminates the `Vec<Vec<CellValue>>` intermediate by pushing decoded values
//! directly from decompressed slots into pre-allocated Arrow column builders.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Date32Builder, DurationMicrosecondBuilder, Float64Builder, StringViewBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use encoding_rs::Encoding;
use rayon::prelude::*;

use crate::arrow_convert;
use crate::compression::bytecode::SlotValue;
use crate::constants::{
    is_sysmis, TemporalKind, VarType, MICROS_PER_SECOND, SECONDS_PER_DAY,
    SPSS_EPOCH_OFFSET_DAYS, SPSS_EPOCH_OFFSET_SECONDS,
};
use crate::dictionary::ResolvedDictionary;
use crate::encoding;
use crate::error::Result;
use crate::io_utils;
use crate::variable::VariableRecord;

/// Pre-computed info for one VLS segment (how many 8-byte slots to read).
struct VlsSegmentInfo {
    useful_slots: usize,
}

/// Pre-computed mapping from a visible variable to its slot position and type.
struct ColumnMapping {
    /// Starting slot index in the raw slot array.
    slot_index: usize,
    /// Variable type (determines which builder to use).
    var_type: VarType,
    /// Number of segments for very long strings (1 for normal vars).
    n_segments: usize,
    /// Pre-computed VLS segment layout (empty for n_segments <= 1).
    vls_layout: Vec<VlsSegmentInfo>,
    /// Temporal kind for numeric columns (None = plain Float64).
    temporal_kind: Option<TemporalKind>,
}

/// Arrow column builder (typed).
enum ColBuilder {
    Float64(Float64Builder),
    Str(StringViewBuilder),
    Date32(Date32Builder),
    TimestampMicro(TimestampMicrosecondBuilder),
    DurationMicro(DurationMicrosecondBuilder),
}

/// Builds an Arrow RecordBatch by pushing values directly from decompressed
/// slots into columnar builders, skipping the `CellValue` intermediate.
pub struct ColumnarBatchBuilder {
    mappings: Vec<ColumnMapping>,
    builders: Vec<ColBuilder>,
    file_encoding: &'static Encoding,
    schema: Arc<Schema>,
    rows_appended: usize,
    /// Reusable byte buffer for string assembly (avoids per-string allocation).
    string_buf: Vec<u8>,
}

impl ColumnarBatchBuilder {
    /// Create a new builder from a resolved dictionary.
    ///
    /// If `projection` is Some, only the specified variable indices are built.
    /// `capacity` is the expected number of rows (for pre-sizing builders).
    pub fn new(
        dict: &ResolvedDictionary,
        projection: Option<&[usize]>,
        capacity: usize,
    ) -> Self {
        let vars: Vec<&VariableRecord> = match projection {
            Some(proj) => proj.iter().map(|&i| &dict.variables[i]).collect(),
            None => dict.variables.iter().collect(),
        };

        let mut mappings = Vec::with_capacity(vars.len());
        let mut builders = Vec::with_capacity(vars.len());
        let mut fields = Vec::with_capacity(vars.len());

        for var in &vars {
            // Pre-compute VLS segment layout
            let vls_layout = if var.n_segments > 1 {
                let width = match &var.var_type {
                    VarType::String(w) => *w,
                    _ => 0,
                };
                (0..var.n_segments)
                    .map(|seg| {
                        let seg_useful = if seg < var.n_segments - 1 {
                            252
                        } else {
                            width - (var.n_segments - 1) * 252
                        };
                        VlsSegmentInfo {
                            useful_slots: (seg_useful + 7) / 8,
                        }
                    })
                    .collect()
            } else {
                Vec::new()
            };

            let temporal_kind = match &var.var_type {
                VarType::Numeric => var
                    .print_format
                    .as_ref()
                    .and_then(|f| f.format_type.temporal_kind()),
                VarType::String(_) => None,
            };

            mappings.push(ColumnMapping {
                slot_index: var.slot_index,
                var_type: var.var_type.clone(),
                n_segments: var.n_segments,
                vls_layout,
                temporal_kind,
            });

            let data_type = arrow_convert::var_to_arrow_type(var);

            match &var.var_type {
                VarType::Numeric => {
                    match temporal_kind {
                        None => {
                            builders.push(ColBuilder::Float64(
                                Float64Builder::with_capacity(capacity),
                            ));
                        }
                        Some(TemporalKind::Date) => {
                            builders.push(ColBuilder::Date32(
                                Date32Builder::with_capacity(capacity),
                            ));
                        }
                        Some(TemporalKind::Timestamp) => {
                            builders.push(ColBuilder::TimestampMicro(
                                TimestampMicrosecondBuilder::with_capacity(capacity),
                            ));
                        }
                        Some(TemporalKind::Duration) => {
                            builders.push(ColBuilder::DurationMicro(
                                DurationMicrosecondBuilder::with_capacity(capacity),
                            ));
                        }
                    }
                    fields.push(Field::new(&var.long_name, data_type, true));
                }
                VarType::String(_) => {
                    // Only enable dedup for categorical columns (those with value labels).
                    // High-cardinality columns (IDs, free-text) pay hash overhead with no benefit.
                    let has_value_labels = dict
                        .metadata
                        .variable_value_labels
                        .contains_key(&var.long_name);
                    let sb = if has_value_labels {
                        StringViewBuilder::new().with_deduplicate_strings()
                    } else {
                        StringViewBuilder::new()
                    };
                    builders.push(ColBuilder::Str(sb));
                    fields.push(Field::new(&var.long_name, data_type, true));
                }
            }
        }

        ColumnarBatchBuilder {
            mappings,
            builders,
            file_encoding: dict.file_encoding,
            schema: Arc::new(Schema::new(fields)),
            rows_appended: 0,
            string_buf: Vec::with_capacity(1024),
        }
    }

    /// Push one row of SlotValues directly into the column builders.
    /// This is the hot-path method for compressed data.
    pub fn push_slot_row(&mut self, slots: &[SlotValue]) {
        for (i, mapping) in self.mappings.iter().enumerate() {
            match &mapping.var_type {
                VarType::Numeric => match mapping.temporal_kind {
                    None => {
                        let b = match &mut self.builders[i] {
                            ColBuilder::Float64(b) => b,
                            _ => unreachable!(),
                        };
                        push_numeric_from_slot(b, slots, mapping.slot_index);
                    }
                    Some(TemporalKind::Date) => {
                        let b = match &mut self.builders[i] {
                            ColBuilder::Date32(b) => b,
                            _ => unreachable!(),
                        };
                        push_date32_from_slot(b, slots, mapping.slot_index);
                    }
                    Some(TemporalKind::Timestamp) => {
                        let b = match &mut self.builders[i] {
                            ColBuilder::TimestampMicro(b) => b,
                            _ => unreachable!(),
                        };
                        push_timestamp_from_slot(b, slots, mapping.slot_index);
                    }
                    Some(TemporalKind::Duration) => {
                        let b = match &mut self.builders[i] {
                            ColBuilder::DurationMicro(b) => b,
                            _ => unreachable!(),
                        };
                        push_duration_from_slot(b, slots, mapping.slot_index);
                    }
                },
                VarType::String(width) => {
                    let builder = match &mut self.builders[i] {
                        ColBuilder::Str(b) => b,
                        _ => unreachable!(),
                    };
                    push_string_from_slot_values(
                        builder,
                        &mut self.string_buf,
                        slots,
                        mapping.slot_index,
                        *width,
                        mapping.n_segments,
                        &mapping.vls_layout,
                        self.file_encoding,
                    );
                }
            }
        }
        self.rows_appended += 1;
    }

    /// Push a chunk of raw bytes column-at-a-time for better cache locality.
    /// `chunk` is a contiguous buffer of `num_rows * slots_per_row * 8` bytes.
    /// Each row occupies `slots_per_row * 8` bytes.
    ///
    /// For large chunks (>= 10,000 rows), columns are processed in parallel
    /// using rayon. Each thread fills its own builder independently.
    pub fn push_raw_chunk(&mut self, chunk: &[u8], num_rows: usize, slots_per_row: usize) {
        let row_bytes = slots_per_row * 8;

        // Borrow fields separately to allow parallel access:
        // mappings (read-only) + builders (each thread gets exclusive &mut to one)
        let mappings = &self.mappings;
        let file_encoding = self.file_encoding;

        if num_rows >= 10_000 {
            // Parallel: each column processed by a separate rayon thread
            self.builders
                .par_iter_mut()
                .enumerate()
                .for_each(|(i, builder)| {
                    let mapping = &mappings[i];
                    match (&mapping.var_type, &mapping.temporal_kind, builder) {
                        (VarType::Numeric, None, ColBuilder::Float64(b)) => {
                            let slot_offset = mapping.slot_index * 8;
                            for row in 0..num_rows {
                                let offset = row * row_bytes + slot_offset;
                                let val = f64::from_le_bytes(
                                    chunk[offset..offset + 8].try_into().unwrap(),
                                );
                                if is_sysmis(val) {
                                    b.append_null();
                                } else {
                                    b.append_value(val);
                                }
                            }
                        }
                        (VarType::Numeric, Some(TemporalKind::Date), ColBuilder::Date32(b)) => {
                            let slot_offset = mapping.slot_index * 8;
                            for row in 0..num_rows {
                                let offset = row * row_bytes + slot_offset;
                                let val = f64::from_le_bytes(
                                    chunk[offset..offset + 8].try_into().unwrap(),
                                );
                                if is_sysmis(val) {
                                    b.append_null();
                                } else {
                                    match spss_to_date32(val) {
                                        Some(d) => b.append_value(d),
                                        None => b.append_null(),
                                    }
                                }
                            }
                        }
                        (
                            VarType::Numeric,
                            Some(TemporalKind::Timestamp),
                            ColBuilder::TimestampMicro(b),
                        ) => {
                            let slot_offset = mapping.slot_index * 8;
                            for row in 0..num_rows {
                                let offset = row * row_bytes + slot_offset;
                                let val = f64::from_le_bytes(
                                    chunk[offset..offset + 8].try_into().unwrap(),
                                );
                                if is_sysmis(val) {
                                    b.append_null();
                                } else {
                                    match spss_to_timestamp_micros(val) {
                                        Some(ts) => b.append_value(ts),
                                        None => b.append_null(),
                                    }
                                }
                            }
                        }
                        (
                            VarType::Numeric,
                            Some(TemporalKind::Duration),
                            ColBuilder::DurationMicro(b),
                        ) => {
                            let slot_offset = mapping.slot_index * 8;
                            for row in 0..num_rows {
                                let offset = row * row_bytes + slot_offset;
                                let val = f64::from_le_bytes(
                                    chunk[offset..offset + 8].try_into().unwrap(),
                                );
                                if is_sysmis(val) {
                                    b.append_null();
                                } else {
                                    match spss_to_duration_micros(val) {
                                        Some(d) => b.append_value(d),
                                        None => b.append_null(),
                                    }
                                }
                            }
                        }
                        (VarType::String(width), _, ColBuilder::Str(b)) => {
                            // Each thread gets its own string buffer
                            let mut local_string_buf = Vec::with_capacity(1024);
                            for row in 0..num_rows {
                                let row_start = row * row_bytes;
                                let raw_slots: &[[u8; 8]] = unsafe {
                                    std::slice::from_raw_parts(
                                        chunk[row_start..].as_ptr() as *const [u8; 8],
                                        slots_per_row,
                                    )
                                };
                                push_string_from_raw_slots(
                                    b,
                                    &mut local_string_buf,
                                    raw_slots,
                                    mapping.slot_index,
                                    *width,
                                    mapping.n_segments,
                                    &mapping.vls_layout,
                                    file_encoding,
                                );
                            }
                        }
                        _ => unreachable!(),
                    }
                });
        } else {
            // Sequential: small chunks (lazy head, small files)
            for (i, mapping) in mappings.iter().enumerate() {
                match (&mapping.var_type, &mapping.temporal_kind) {
                    (VarType::Numeric, None) => {
                        let builder = match &mut self.builders[i] {
                            ColBuilder::Float64(b) => b,
                            _ => unreachable!(),
                        };
                        let slot_offset = mapping.slot_index * 8;
                        for row in 0..num_rows {
                            let offset = row * row_bytes + slot_offset;
                            let val = f64::from_le_bytes(
                                chunk[offset..offset + 8].try_into().unwrap(),
                            );
                            if is_sysmis(val) {
                                builder.append_null();
                            } else {
                                builder.append_value(val);
                            }
                        }
                    }
                    (VarType::Numeric, Some(TemporalKind::Date)) => {
                        let builder = match &mut self.builders[i] {
                            ColBuilder::Date32(b) => b,
                            _ => unreachable!(),
                        };
                        let slot_offset = mapping.slot_index * 8;
                        for row in 0..num_rows {
                            let offset = row * row_bytes + slot_offset;
                            let val = f64::from_le_bytes(
                                chunk[offset..offset + 8].try_into().unwrap(),
                            );
                            if is_sysmis(val) {
                                builder.append_null();
                            } else {
                                match spss_to_date32(val) {
                                    Some(d) => builder.append_value(d),
                                    None => builder.append_null(),
                                }
                            }
                        }
                    }
                    (VarType::Numeric, Some(TemporalKind::Timestamp)) => {
                        let builder = match &mut self.builders[i] {
                            ColBuilder::TimestampMicro(b) => b,
                            _ => unreachable!(),
                        };
                        let slot_offset = mapping.slot_index * 8;
                        for row in 0..num_rows {
                            let offset = row * row_bytes + slot_offset;
                            let val = f64::from_le_bytes(
                                chunk[offset..offset + 8].try_into().unwrap(),
                            );
                            if is_sysmis(val) {
                                builder.append_null();
                            } else {
                                match spss_to_timestamp_micros(val) {
                                    Some(ts) => builder.append_value(ts),
                                    None => builder.append_null(),
                                }
                            }
                        }
                    }
                    (VarType::Numeric, Some(TemporalKind::Duration)) => {
                        let builder = match &mut self.builders[i] {
                            ColBuilder::DurationMicro(b) => b,
                            _ => unreachable!(),
                        };
                        let slot_offset = mapping.slot_index * 8;
                        for row in 0..num_rows {
                            let offset = row * row_bytes + slot_offset;
                            let val = f64::from_le_bytes(
                                chunk[offset..offset + 8].try_into().unwrap(),
                            );
                            if is_sysmis(val) {
                                builder.append_null();
                            } else {
                                match spss_to_duration_micros(val) {
                                    Some(d) => builder.append_value(d),
                                    None => builder.append_null(),
                                }
                            }
                        }
                    }
                    (VarType::String(width), _) => {
                        let builder = match &mut self.builders[i] {
                            ColBuilder::Str(b) => b,
                            _ => unreachable!(),
                        };
                        for row in 0..num_rows {
                            let row_start = row * row_bytes;
                            let raw_slots: &[[u8; 8]] = unsafe {
                                std::slice::from_raw_parts(
                                    chunk[row_start..].as_ptr() as *const [u8; 8],
                                    slots_per_row,
                                )
                            };
                            push_string_from_raw_slots(
                                builder,
                                &mut self.string_buf,
                                raw_slots,
                                mapping.slot_index,
                                *width,
                                mapping.n_segments,
                                &mapping.vls_layout,
                                self.file_encoding,
                            );
                        }
                    }
                }
            }
        }

        self.rows_appended += num_rows;
    }

    /// Finish building and return the RecordBatch.
    pub fn finish(self) -> Result<RecordBatch> {
        let columns: Vec<ArrayRef> = self
            .builders
            .into_iter()
            .map(|b| -> ArrayRef {
                match b {
                    ColBuilder::Float64(mut b) => Arc::new(b.finish()),
                    ColBuilder::Str(mut b) => Arc::new(b.finish()),
                    ColBuilder::Date32(mut b) => Arc::new(b.finish()),
                    ColBuilder::TimestampMicro(mut b) => Arc::new(b.finish()),
                    ColBuilder::DurationMicro(mut b) => Arc::new(b.finish()),
                }
            })
            .collect();

        let batch = RecordBatch::try_new(self.schema, columns)?;
        Ok(batch)
    }

    /// Number of rows appended so far.
    pub fn len(&self) -> usize {
        self.rows_appended
    }
}

// ---------------------------------------------------------------------------
// Temporal conversion helpers
// ---------------------------------------------------------------------------

/// Convert SPSS seconds-since-1582 to Date32 (days since Unix epoch).
/// Returns None on non-finite values (fallback to null).
#[inline]
fn spss_to_date32(spss_seconds: f64) -> Option<i32> {
    if !spss_seconds.is_finite() {
        return None;
    }
    let days = spss_seconds / SECONDS_PER_DAY - SPSS_EPOCH_OFFSET_DAYS as f64;
    Some(days as i32)
}

/// Convert SPSS seconds-since-1582 to Timestamp microseconds since Unix epoch.
/// Returns None on non-finite values (fallback to null).
#[inline]
fn spss_to_timestamp_micros(spss_seconds: f64) -> Option<i64> {
    if !spss_seconds.is_finite() {
        return None;
    }
    let unix_seconds = spss_seconds - SPSS_EPOCH_OFFSET_SECONDS;
    Some((unix_seconds * MICROS_PER_SECOND) as i64)
}

/// Convert SPSS elapsed seconds to Duration microseconds.
/// Returns None on non-finite values (fallback to null).
#[inline]
fn spss_to_duration_micros(spss_seconds: f64) -> Option<i64> {
    if !spss_seconds.is_finite() {
        return None;
    }
    Some((spss_seconds * MICROS_PER_SECOND) as i64)
}

// ---------------------------------------------------------------------------
// Numeric push helpers
// ---------------------------------------------------------------------------

/// Push a numeric value from SlotValues directly into a Float64Builder.
#[inline]
fn push_numeric_from_slot(builder: &mut Float64Builder, slots: &[SlotValue], slot_idx: usize) {
    if slot_idx >= slots.len() {
        builder.append_null();
        return;
    }
    match &slots[slot_idx] {
        SlotValue::Numeric(v) => {
            if is_sysmis(*v) {
                builder.append_null();
            } else {
                builder.append_value(*v);
            }
        }
        SlotValue::Sysmis | SlotValue::EndOfFile | SlotValue::Spaces => {
            builder.append_null();
        }
        SlotValue::Raw(bytes) => {
            let val = f64::from_le_bytes(*bytes);
            if is_sysmis(val) {
                builder.append_null();
            } else {
                builder.append_value(val);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Temporal push helpers (compressed path)
// ---------------------------------------------------------------------------

/// Extract an f64 from a slot, returning None for SYSMIS/EOF/Spaces.
#[inline]
fn extract_f64_from_slot(slots: &[SlotValue], slot_idx: usize) -> Option<f64> {
    if slot_idx >= slots.len() {
        return None;
    }
    match &slots[slot_idx] {
        SlotValue::Numeric(v) => {
            if is_sysmis(*v) {
                None
            } else {
                Some(*v)
            }
        }
        SlotValue::Sysmis | SlotValue::EndOfFile | SlotValue::Spaces => None,
        SlotValue::Raw(bytes) => {
            let val = f64::from_le_bytes(*bytes);
            if is_sysmis(val) {
                None
            } else {
                Some(val)
            }
        }
    }
}

#[inline]
fn push_date32_from_slot(builder: &mut Date32Builder, slots: &[SlotValue], slot_idx: usize) {
    match extract_f64_from_slot(slots, slot_idx).and_then(spss_to_date32) {
        Some(d) => builder.append_value(d),
        None => builder.append_null(),
    }
}

#[inline]
fn push_timestamp_from_slot(
    builder: &mut TimestampMicrosecondBuilder,
    slots: &[SlotValue],
    slot_idx: usize,
) {
    match extract_f64_from_slot(slots, slot_idx).and_then(spss_to_timestamp_micros) {
        Some(ts) => builder.append_value(ts),
        None => builder.append_null(),
    }
}

#[inline]
fn push_duration_from_slot(
    builder: &mut DurationMicrosecondBuilder,
    slots: &[SlotValue],
    slot_idx: usize,
) {
    match extract_f64_from_slot(slots, slot_idx).and_then(spss_to_duration_micros) {
        Some(d) => builder.append_value(d),
        None => builder.append_null(),
    }
}

// ---------------------------------------------------------------------------
// String push helpers
// ---------------------------------------------------------------------------

/// Assemble a string from SlotValues and push directly into a StringViewBuilder.
/// Uses `string_buf` as a reusable byte buffer to avoid per-string allocation.
#[inline]
fn push_string_from_slot_values(
    builder: &mut StringViewBuilder,
    string_buf: &mut Vec<u8>,
    slots: &[SlotValue],
    start_slot: usize,
    width: usize,
    n_segments: usize,
    vls_layout: &[VlsSegmentInfo],
    file_encoding: &'static Encoding,
) {
    string_buf.clear();

    if n_segments <= 1 {
        // Simple string: read ceil(width/8) slots
        let n_slots = (width + 7) / 8;
        for i in 0..n_slots {
            let idx = start_slot + i;
            if idx < slots.len() {
                push_slot_bytes(string_buf, &slots[idx]);
            }
        }
    } else {
        // Very long string: use pre-computed segment layout
        let mut slot = start_slot;
        for seg_info in vls_layout {
            for i in 0..seg_info.useful_slots {
                if slot + i < slots.len() {
                    push_slot_bytes(string_buf, &slots[slot + i]);
                }
            }
            slot += 32; // seg_slots is always 32 (ceil(255/8))
        }
    }

    string_buf.truncate(width);
    let trimmed = io_utils::trim_trailing_padding(string_buf);
    let decoded = encoding::decode_str_lossy(trimmed, file_encoding);
    builder.append_value(&*decoded);
}

/// Assemble a string from raw 8-byte slots and push directly into a StringViewBuilder.
#[inline]
fn push_string_from_raw_slots(
    builder: &mut StringViewBuilder,
    string_buf: &mut Vec<u8>,
    raw_slots: &[[u8; 8]],
    start_slot: usize,
    width: usize,
    n_segments: usize,
    vls_layout: &[VlsSegmentInfo],
    file_encoding: &'static Encoding,
) {
    string_buf.clear();

    if n_segments <= 1 {
        let n_slots = (width + 7) / 8;
        for i in 0..n_slots {
            let idx = start_slot + i;
            if idx < raw_slots.len() {
                string_buf.extend_from_slice(&raw_slots[idx]);
            }
        }
    } else {
        // Very long string: use pre-computed segment layout
        let mut slot = start_slot;
        for seg_info in vls_layout {
            for i in 0..seg_info.useful_slots {
                if slot + i < raw_slots.len() {
                    string_buf.extend_from_slice(&raw_slots[slot + i]);
                }
            }
            slot += 32;
        }
    }

    string_buf.truncate(width);
    let trimmed = io_utils::trim_trailing_padding(string_buf);
    let decoded = encoding::decode_str_lossy(trimmed, file_encoding);
    builder.append_value(&*decoded);
}

/// Extract 8 bytes from a SlotValue into a byte buffer.
#[inline]
fn push_slot_bytes(buf: &mut Vec<u8>, sv: &SlotValue) {
    match sv {
        SlotValue::Raw(b) => buf.extend_from_slice(b),
        SlotValue::Spaces => buf.extend_from_slice(&[b' '; 8]),
        SlotValue::Numeric(v) => buf.extend_from_slice(&v.to_le_bytes()),
        SlotValue::Sysmis => buf.extend_from_slice(&[0u8; 8]),
        SlotValue::EndOfFile => buf.extend_from_slice(&[0u8; 8]),
    }
}
