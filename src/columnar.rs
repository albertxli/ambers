//! Direct-to-columnar Arrow batch building.
//!
//! Eliminates the `Vec<Vec<CellValue>>` intermediate by pushing decoded values
//! directly from decompressed slots into pre-allocated Arrow column builders.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Builder, StringViewBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use encoding_rs::Encoding;

use crate::compression::bytecode::SlotValue;
use crate::constants::{is_sysmis, VarType};
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
}

/// Arrow column builder (typed).
enum ColBuilder {
    Float64(Float64Builder),
    Str(StringViewBuilder),
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

            mappings.push(ColumnMapping {
                slot_index: var.slot_index,
                var_type: var.var_type.clone(),
                n_segments: var.n_segments,
                vls_layout,
            });

            match &var.var_type {
                VarType::Numeric => {
                    builders.push(ColBuilder::Float64(Float64Builder::with_capacity(capacity)));
                    fields.push(Field::new(&var.long_name, DataType::Float64, true));
                }
                VarType::String(_) => {
                    // StringViewBuilder with deduplication for categorical SPSS data
                    let sb = StringViewBuilder::new()
                        .with_deduplicate_strings();
                    builders.push(ColBuilder::Str(sb));
                    fields.push(Field::new(&var.long_name, DataType::Utf8View, true));
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
                VarType::Numeric => {
                    let builder = match &mut self.builders[i] {
                        ColBuilder::Float64(b) => b,
                        _ => unreachable!(),
                    };
                    push_numeric_from_slot(builder, slots, mapping.slot_index);
                }
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

    /// Push one row of raw 8-byte slots directly into the column builders.
    /// This is the hot-path method for uncompressed data.
    pub fn push_raw_row(&mut self, raw_slots: &[[u8; 8]]) {
        for (i, mapping) in self.mappings.iter().enumerate() {
            match &mapping.var_type {
                VarType::Numeric => {
                    let builder = match &mut self.builders[i] {
                        ColBuilder::Float64(b) => b,
                        _ => unreachable!(),
                    };
                    push_numeric_from_raw(builder, raw_slots, mapping.slot_index);
                }
                VarType::String(width) => {
                    let builder = match &mut self.builders[i] {
                        ColBuilder::Str(b) => b,
                        _ => unreachable!(),
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
        self.rows_appended += 1;
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

/// Push a numeric value from raw 8-byte slots directly into a Float64Builder.
#[inline]
fn push_numeric_from_raw(builder: &mut Float64Builder, raw_slots: &[[u8; 8]], slot_idx: usize) {
    if slot_idx >= raw_slots.len() {
        builder.append_null();
        return;
    }
    let val = f64::from_le_bytes(raw_slots[slot_idx]);
    if is_sysmis(val) {
        builder.append_null();
    } else {
        builder.append_value(val);
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
