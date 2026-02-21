//! Direct-to-columnar Arrow batch building.
//!
//! Eliminates the `Vec<Vec<CellValue>>` intermediate by pushing decoded values
//! directly from decompressed slots into pre-allocated Arrow column builders.
//!
//! **Performance rule:** The hot paths (`push_raw_chunk`) must stay minimal —
//! only Float64 + String. Temporal conversion happens in `finish()` as a
//! post-processing step. Never add new ColBuilder variants or match arms to
//! the hot loops; it causes icache pressure that slows ALL columns.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Date32Array, DurationMicrosecondArray, Float64Array, Float64Builder,
    StringViewBuilder, TimestampMicrosecondArray,
};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use encoding_rs::Encoding;
use rayon::prelude::*;

use crate::arrow_convert;
use crate::constants::{
    is_sysmis, TemporalKind, VarType, MICROS_PER_SECOND, SECONDS_PER_DAY,
    SPSS_EPOCH_OFFSET_DAYS, SPSS_EPOCH_OFFSET_SECONDS,
};
use crate::dictionary::ResolvedDictionary;
use crate::encoding;
use crate::error::Result;
use crate::io_utils;
use crate::variable::VariableRecord;

/// Row byte threshold for switching to tiled parallel column processing.
/// When row_bytes exceeds this, the full-chunk column-at-a-time pattern
/// thrashes L3 cache because each column pass scans the entire 256 MB chunk
/// with only 8 useful bytes per large stride. Tiling into L3-sized row chunks
/// keeps the working set cache-hot for all rayon threads.
///
/// At 12,288 bytes (~1,536 slots), files with ~915 visible columns (which may
/// have ~1,736 total slots due to multi-slot string variables) correctly hit
/// the tiled path.
const WIDE_ROW_THRESHOLD: usize = 12_288;

/// Target tile size in bytes for wide-file row tiling.
/// Each tile should comfortably fit in L3 cache (~12–36 MB on modern CPUs)
/// so all rayon threads access cache-hot data during parallel column processing.
/// 4 MB leaves headroom for builder state and other cache residents.
const L3_TILE_BYTES: usize = 4 * 1024 * 1024;

/// Pre-computed info for one VLS (very long string) segment.
/// VLS variables (width > 255) are stored across multiple 32-slot segments.
/// The primary variable (segment 0) has width 255, ghost segments have width 252.
/// Each segment occupies 32 slots (256 bytes), but only `useful_bytes` contain data.
struct VlsSegmentInfo {
    /// Number of useful DATA bytes in this segment (255 for primary, 252 for ghost,
    /// remainder for the last segment).
    useful_bytes: usize,
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

/// Arrow column builder (typed). Only two variants — keeps hot path icache small.
enum ColBuilder {
    Float64(Float64Builder),
    Str(StringViewBuilder),
}

/// Builds an Arrow RecordBatch by pushing values directly from decompressed
/// slots into columnar builders, skipping the `CellValue` intermediate.
///
/// Temporal columns (DATE, DATETIME, TIME) are read as Float64 in the hot path,
/// then converted to proper Arrow temporal types in `finish()`.
pub struct ColumnarBatchBuilder {
    mappings: Vec<ColumnMapping>,
    builders: Vec<ColBuilder>,
    file_encoding: &'static Encoding,
    /// The output schema (with temporal types for date/time columns).
    schema: Arc<Schema>,
    rows_appended: usize,
    /// Reusable byte buffer for string assembly (avoids per-string allocation).
    /// Used only in the sequential path; parallel paths use thread-local buffers.
    string_buf: Vec<u8>,
    /// Column indices that need temporal conversion in finish().
    /// Empty for files with no date/time columns — zero overhead.
    temporal_columns: Vec<(usize, TemporalKind)>,
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
        let mut temporal_columns = Vec::new();

        for (col_idx, var) in vars.iter().enumerate() {
            // Pre-compute VLS segment layout
            let vls_layout = if var.n_segments > 1 {
                let width = match &var.var_type {
                    VarType::String(w) => *w,
                    _ => 0,
                };
                (0..var.n_segments)
                    .map(|seg| {
                        // Each segment stores up to 255 bytes of content in
                        // the data section. The last segment stores only the
                        // remaining bytes after all prior segments.
                        let seg_useful = if seg < var.n_segments - 1 {
                            255
                        } else {
                            width - (var.n_segments - 1) * 255
                        };
                        VlsSegmentInfo {
                            useful_bytes: seg_useful,
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

            // Output schema uses temporal types; builders always use Float64.
            let output_type = arrow_convert::var_to_arrow_type(var);
            fields.push(Field::new(&var.long_name, output_type, true));

            match &var.var_type {
                VarType::Numeric => {
                    // Record temporal columns for post-processing in finish()
                    if let Some(kind) = var
                        .print_format
                        .as_ref()
                        .and_then(|f| f.format_type.temporal_kind())
                    {
                        temporal_columns.push((col_idx, kind));
                    }
                    // ALL numerics use Float64Builder — keeps hot path minimal
                    builders.push(ColBuilder::Float64(Float64Builder::with_capacity(capacity)));
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
            temporal_columns,
        }
    }

    /// Push a chunk of raw bytes into columnar builders.
    /// `chunk` is a contiguous buffer of `num_rows * slots_per_row * 8` bytes.
    /// Each row occupies `slots_per_row * 8` bytes.
    ///
    /// Dispatch strategy:
    /// - **Wide files** (row_bytes > 12,288): tiled parallel — processes L3-sized
    ///   row tiles with rayon, avoiding cache thrashing from large column strides.
    /// - **Narrow files, large chunks** (>= 10,000 rows): column-at-a-time with
    ///   rayon parallelism. Each thread fills its own builder independently.
    /// - **Narrow files, small chunks**: sequential column-at-a-time.
    pub fn push_raw_chunk(&mut self, chunk: &[u8], num_rows: usize, slots_per_row: usize) {
        let row_bytes = slots_per_row * 8;

        // Wide files: tiled parallel avoids L3 cache thrashing.
        // Column-at-a-time with large stride (e.g. 14,656 bytes for 1832 slots)
        // causes each column pass to touch the entire 256 MB chunk with only 8
        // useful bytes per stride, thrashing L3 cache. Tiling processes small
        // row windows that fit in L3, keeping data cache-hot for all rayon threads.
        if row_bytes > WIDE_ROW_THRESHOLD {
            self.push_raw_chunk_tiled(chunk, num_rows, slots_per_row);
            return;
        }

        let mappings = &self.mappings;
        let file_encoding = self.file_encoding;

        if num_rows >= 10_000 {
            // Parallel: each column processed by a separate rayon thread.
            // rayon splits builders into ~24 contiguous groups (one per core).
            self.builders
                .par_iter_mut()
                .enumerate()
                .for_each(|(i, builder)| {
                    let mapping = &mappings[i];
                    match (&mapping.var_type, builder) {
                        (VarType::Numeric, ColBuilder::Float64(b)) => {
                            process_numeric_rows(b, chunk, 0, num_rows, row_bytes, mapping.slot_index);
                        }
                        (VarType::String(_), ColBuilder::Str(b)) => {
                            let mut local_buf = Vec::with_capacity(256);
                            process_string_rows(
                                b, &mut local_buf, chunk, 0, num_rows,
                                row_bytes, slots_per_row, mapping, file_encoding,
                            );
                        }
                        _ => unreachable!(),
                    }
                });
        } else {
            // Sequential: small chunks (lazy head, small files).
            for (i, mapping) in mappings.iter().enumerate() {
                match (&mapping.var_type, &mut self.builders[i]) {
                    (VarType::Numeric, ColBuilder::Float64(b)) => {
                        process_numeric_rows(b, chunk, 0, num_rows, row_bytes, mapping.slot_index);
                    }
                    (VarType::String(_), ColBuilder::Str(b)) => {
                        process_string_rows(
                            b, &mut self.string_buf, chunk, 0, num_rows,
                            row_bytes, slots_per_row, mapping, self.file_encoding,
                        );
                    }
                    _ => unreachable!(),
                }
            }
        }

        self.rows_appended += num_rows;
    }

    /// Tiled parallel column processing for wide files.
    ///
    /// Processes the chunk in small row tiles that fit in L3 cache, with each
    /// tile using rayon `par_iter_mut` over columns — same parallel pattern as
    /// the untiled path but on L3-sized data windows.
    ///
    /// Without tiling, column-at-a-time on a 256 MB chunk with 14,656-byte
    /// stride causes all 24 threads to thrash L3 cache. With 4 MB tiles,
    /// the entire working set stays cache-hot.
    fn push_raw_chunk_tiled(
        &mut self,
        chunk: &[u8],
        num_rows: usize,
        slots_per_row: usize,
    ) {
        let row_bytes = slots_per_row * 8;
        let tile_rows = (L3_TILE_BYTES / row_bytes).max(64);

        let mappings = &self.mappings;
        let file_encoding = self.file_encoding;

        let mut row_offset = 0;
        while row_offset < num_rows {
            let n = (num_rows - row_offset).min(tile_rows);
            let tile_start = row_offset * row_bytes;

            // Parallel column processing within this L3-sized tile.
            self.builders
                .par_iter_mut()
                .enumerate()
                .for_each(|(i, builder)| {
                    let mapping = &mappings[i];
                    match (&mapping.var_type, builder) {
                        (VarType::Numeric, ColBuilder::Float64(b)) => {
                            process_numeric_rows(b, chunk, tile_start, n, row_bytes, mapping.slot_index);
                        }
                        (VarType::String(_), ColBuilder::Str(b)) => {
                            let mut local_buf = Vec::with_capacity(256);
                            process_string_rows(
                                b, &mut local_buf, chunk, tile_start, n,
                                row_bytes, slots_per_row, mapping, file_encoding,
                            );
                        }
                        _ => unreachable!(),
                    }
                });

            row_offset += n;
        }

        self.rows_appended += num_rows;
    }

    /// Finish building and return the RecordBatch.
    ///
    /// Temporal columns are converted from Float64 to their proper Arrow types
    /// here, outside the hot path. This keeps the read loops fast for all columns.
    pub fn finish(self) -> Result<RecordBatch> {
        let mut columns: Vec<ArrayRef> = self
            .builders
            .into_iter()
            .map(|b| -> ArrayRef {
                match b {
                    ColBuilder::Float64(mut b) => Arc::new(b.finish()),
                    ColBuilder::Str(mut b) => Arc::new(b.finish()),
                }
            })
            .collect();

        // Post-process: convert temporal Float64 columns to proper Arrow types.
        // This is O(n) per temporal column, typically 0-5 columns out of hundreds.
        for &(col_idx, kind) in &self.temporal_columns {
            let float_arr = columns[col_idx]
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("temporal column should be Float64Array");
            columns[col_idx] = convert_float64_to_temporal(float_arr, kind);
        }

        let batch = RecordBatch::try_new(self.schema, columns)?;
        Ok(batch)
    }

    /// Number of rows appended so far.
    pub fn len(&self) -> usize {
        self.rows_appended
    }
}

// ---------------------------------------------------------------------------
// Column processing helpers (shared by parallel, sequential, and tiled paths)
// ---------------------------------------------------------------------------

/// Process numeric rows from a chunk into a Float64Builder.
///
/// Reads `num_rows` f64 values starting at `base_offset` in the chunk,
/// with `row_bytes` stride and `slot_index` column offset.
#[inline(always)]
fn process_numeric_rows(
    builder: &mut Float64Builder,
    chunk: &[u8],
    base_offset: usize,
    num_rows: usize,
    row_bytes: usize,
    slot_index: usize,
) {
    let slot_offset = slot_index * 8;
    for row in 0..num_rows {
        let offset = base_offset + row * row_bytes + slot_offset;
        // SAFETY: offset + 8 <= chunk.len() because the caller guarantees
        // (base_offset + num_rows * row_bytes) <= chunk.len() and
        // slot_offset + 8 <= row_bytes. [u8; 8] has 1-byte alignment,
        // so any byte-aligned pointer from chunk is valid.
        let val = f64::from_le_bytes(unsafe {
            *(chunk.as_ptr().add(offset) as *const [u8; 8])
        });
        if is_sysmis(val) {
            builder.append_null();
        } else {
            builder.append_value(val);
        }
    }
}

/// Process string rows from a chunk into a StringViewBuilder.
///
/// Reads `num_rows` string values starting at `base_offset` in the chunk,
/// assembling bytes from the appropriate slots per the column mapping.
#[inline(always)]
fn process_string_rows(
    builder: &mut StringViewBuilder,
    string_buf: &mut Vec<u8>,
    chunk: &[u8],
    base_offset: usize,
    num_rows: usize,
    row_bytes: usize,
    slots_per_row: usize,
    mapping: &ColumnMapping,
    file_encoding: &'static Encoding,
) {
    let width = match &mapping.var_type {
        VarType::String(w) => *w,
        _ => unreachable!(),
    };
    for row in 0..num_rows {
        let row_start = base_offset + row * row_bytes;
        // SAFETY: row_start + slots_per_row * 8 <= chunk.len() (same caller
        // invariant as process_numeric_rows). [u8; 8] has 1-byte alignment.
        let raw_slots: &[[u8; 8]] = unsafe {
            std::slice::from_raw_parts(
                chunk[row_start..].as_ptr() as *const [u8; 8],
                slots_per_row,
            )
        };
        push_string_from_raw_slots(
            builder,
            string_buf,
            raw_slots,
            mapping.slot_index,
            width,
            mapping.n_segments,
            &mapping.vls_layout,
            file_encoding,
        );
    }
}

// ---------------------------------------------------------------------------
// Temporal post-processing (runs in finish(), NOT in hot path)
// ---------------------------------------------------------------------------

/// Convert a Float64Array of SPSS numeric values to the appropriate Arrow
/// temporal type. Reuses the null bitmap from the source array directly.
#[inline(never)]
fn convert_float64_to_temporal(arr: &Float64Array, kind: TemporalKind) -> ArrayRef {
    let nulls = arr.nulls().cloned();
    let values = arr.values();

    match kind {
        TemporalKind::Date => {
            let converted: Vec<i32> = values
                .iter()
                .map(|&v| {
                    (v / SECONDS_PER_DAY - SPSS_EPOCH_OFFSET_DAYS as f64) as i32
                })
                .collect();
            Arc::new(Date32Array::new(converted.into(), nulls))
        }
        TemporalKind::Timestamp => {
            let converted: Vec<i64> = values
                .iter()
                .map(|&v| {
                    ((v - SPSS_EPOCH_OFFSET_SECONDS) * MICROS_PER_SECOND) as i64
                })
                .collect();
            Arc::new(TimestampMicrosecondArray::new(converted.into(), nulls))
        }
        TemporalKind::Duration => {
            let converted: Vec<i64> = values
                .iter()
                .map(|&v| (v * MICROS_PER_SECOND) as i64)
                .collect();
            Arc::new(DurationMicrosecondArray::new(converted.into(), nulls))
        }
    }
}

// ---------------------------------------------------------------------------
// String push helpers
// ---------------------------------------------------------------------------

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
        // Very long string: copy slots per segment, truncating each segment
        // to its useful byte count to strip slot-alignment padding.
        let mut slot = start_slot;
        let mut cumulative = 0;
        for seg_info in vls_layout {
            cumulative += seg_info.useful_bytes;
            let slots_to_read = (seg_info.useful_bytes + 7) / 8;
            for i in 0..slots_to_read {
                if slot + i < raw_slots.len() {
                    string_buf.extend_from_slice(&raw_slots[slot + i]);
                }
            }
            // Strip padding bytes at end of this segment (e.g. byte 255 of
            // a 255-byte primary segment stored in 32 slots = 256 bytes).
            string_buf.truncate(cumulative);
            slot += 32;
        }
    }

    string_buf.truncate(width);
    let trimmed = io_utils::trim_trailing_padding(string_buf);
    let decoded = encoding::decode_str_lossy(trimmed, file_encoding);
    builder.append_value(&*decoded);
}
