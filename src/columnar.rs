//! Direct-to-columnar Arrow batch building.
//!
//! Eliminates the `Vec<Vec<CellValue>>` intermediate by pushing decoded values
//! directly from decompressed slots into pre-allocated Arrow column builders.
//!
//! **Performance rule:** The hot paths (`push_slot_row`, `push_raw_chunk`) must
//! stay minimal — only Float64 + String. Temporal conversion happens in `finish()`
//! as a post-processing step. Never add new ColBuilder variants or match arms to
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
                    match (&mapping.var_type, builder) {
                        (VarType::Numeric, ColBuilder::Float64(b)) => {
                            let slot_offset = mapping.slot_index * 8;
                            for row in 0..num_rows {
                                let offset = row * row_bytes + slot_offset;
                                // SAFETY: offset + 8 <= chunk.len() because
                                // num_rows * row_bytes <= chunk.len() and
                                // offset = row * row_bytes + slot_offset where slot_offset + 8 <= row_bytes.
                                let val = f64::from_le_bytes(unsafe {
                                    *(chunk.as_ptr().add(offset) as *const [u8; 8])
                                });
                                if is_sysmis(val) {
                                    b.append_null();
                                } else {
                                    b.append_value(val);
                                }
                            }
                        }
                        (VarType::String(width), ColBuilder::Str(b)) => {
                            // Each thread gets its own string buffer, sized to column width
                            let mut local_string_buf = Vec::with_capacity((*width).min(1024));
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
                match &mapping.var_type {
                    VarType::Numeric => {
                        let builder = match &mut self.builders[i] {
                            ColBuilder::Float64(b) => b,
                            _ => unreachable!(),
                        };
                        let slot_offset = mapping.slot_index * 8;
                        for row in 0..num_rows {
                            let offset = row * row_bytes + slot_offset;
                            // SAFETY: same invariant as parallel path above.
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
                    VarType::String(width) => {
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

