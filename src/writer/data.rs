//! Data section writers for SAV/ZSAV files.

use std::io::{Seek, SeekFrom, Write};

use arrow::array::{
    Array, BooleanArray, Date32Array, DurationMicrosecondArray, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeStringArray, StringViewArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use super::layout::CaseLayout;
use crate::compression::bytecode_encoder::BytecodeEncoder;
use crate::constants::*;
use crate::error::{Result, SpssError};
use crate::io_utils::SavWriteExt;

// ---------------------------------------------------------------------------
// Data helpers
// ---------------------------------------------------------------------------

/// Extract a string value from any Arrow string array type.
/// Handles StringViewArray (Utf8View), StringArray (Utf8), and LargeStringArray (LargeUtf8).
/// Polars exports LargeUtf8 via PyCapsule, so all three must be supported.
#[inline]
fn get_string_value(col: &dyn Array, row: usize) -> &str {
    col.as_any()
        .downcast_ref::<StringViewArray>()
        .map(|a| a.value(row))
        .or_else(|| {
            col.as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .map(|a| a.value(row))
        })
        .or_else(|| {
            col.as_any()
                .downcast_ref::<LargeStringArray>()
                .map(|a| a.value(row))
        })
        .unwrap_or("")
}

/// Convert temporal Arrow arrays back to SPSS float values.
/// Returns a new Float64Array with SPSS epoch-based seconds.
fn temporal_to_spss_float(arr: &dyn Array, kind: TemporalKind) -> Float64Array {
    match kind {
        TemporalKind::Date => {
            let date_arr = arr.as_any().downcast_ref::<Date32Array>().unwrap();
            let null_buf = date_arr.nulls().cloned();
            let values: Vec<f64> = date_arr
                .values()
                .iter()
                .map(|&d| (d as f64 + SPSS_EPOCH_OFFSET_DAYS as f64) * SECONDS_PER_DAY)
                .collect();
            Float64Array::new(values.into(), null_buf)
        }
        TemporalKind::Timestamp => {
            let ts_arr = arr
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let null_buf = ts_arr.nulls().cloned();
            let values: Vec<f64> = ts_arr
                .values()
                .iter()
                .map(|&us| (us as f64 / MICROS_PER_SECOND) + SPSS_EPOCH_OFFSET_SECONDS)
                .collect();
            Float64Array::new(values.into(), null_buf)
        }
        TemporalKind::Duration => {
            let dur_arr = arr
                .as_any()
                .downcast_ref::<DurationMicrosecondArray>()
                .unwrap();
            let null_buf = dur_arr.nulls().cloned();
            let values: Vec<f64> = dur_arr
                .values()
                .iter()
                .map(|&us| us as f64 / MICROS_PER_SECOND)
                .collect();
            Float64Array::new(values.into(), null_buf)
        }
    }
}

/// Get a numeric f64 value from various Arrow numeric types.
fn get_numeric_f64(arr: &dyn Array, row: usize) -> Option<f64> {
    if arr.is_null(row) {
        return None;
    }
    let dt = arr.data_type();
    match dt {
        DataType::Float64 => {
            let a = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            Some(a.value(row))
        }
        DataType::Int64 => {
            let a = arr.as_any().downcast_ref::<Int64Array>().unwrap();
            Some(a.value(row) as f64)
        }
        DataType::Int32 => {
            let a = arr.as_any().downcast_ref::<Int32Array>().unwrap();
            Some(a.value(row) as f64)
        }
        DataType::Int16 => {
            let a = arr.as_any().downcast_ref::<Int16Array>().unwrap();
            Some(a.value(row) as f64)
        }
        DataType::Int8 => {
            let a = arr.as_any().downcast_ref::<Int8Array>().unwrap();
            Some(a.value(row) as f64)
        }
        DataType::Boolean => {
            let a = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            Some(if a.value(row) { 1.0 } else { 0.0 })
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Shared row-building helpers (used by all three data writers)
// ---------------------------------------------------------------------------

/// Pre-convert temporal Arrow columns to SPSS Float64 values.
fn preconvert_temporal_columns(
    batch: &RecordBatch,
    layout: &CaseLayout,
) -> Vec<Option<Float64Array>> {
    let mut temporal_arrays = Vec::with_capacity(layout.write_vars.len());
    for var in &layout.write_vars {
        let col = batch.column(var.col_index);
        if let Some(kind) = var.format.format_type.temporal_kind() {
            temporal_arrays.push(Some(temporal_to_spss_float(col.as_ref(), kind)));
        } else {
            temporal_arrays.push(None);
        }
    }
    temporal_arrays
}

/// Fill a pre-allocated row buffer with data from one row of the RecordBatch.
/// `row_buf` must be pre-filled with spaces (0x20) before calling.
fn fill_row_buffer(
    row_buf: &mut [u8],
    row: usize,
    batch: &RecordBatch,
    layout: &CaseLayout,
    temporal_arrays: &[Option<Float64Array>],
) {
    let mut slot_offset = 0;
    for (var_idx, var) in layout.write_vars.iter().enumerate() {
        let col: &dyn Array = if let Some(ref arr) = temporal_arrays[var_idx] {
            arr
        } else {
            batch.column(var.col_index).as_ref()
        };

        match &var.var_type {
            VarType::Numeric => {
                let val = if col.is_null(row) {
                    f64::from_bits(SYSMIS_BITS)
                } else {
                    get_numeric_f64(col, row).unwrap_or(f64::from_bits(SYSMIS_BITS))
                };
                row_buf[slot_offset * 8..(slot_offset + 1) * 8].copy_from_slice(&val.to_le_bytes());
                slot_offset += 1;
            }
            VarType::String(width) => {
                let str_val = if col.is_null(row) {
                    ""
                } else {
                    get_string_value(col, row)
                };

                let str_bytes = str_val.as_bytes();
                let total_slots = var.total_slots();

                if var.n_segments == 1 {
                    let total_slot_bytes = total_slots * 8;
                    let start = slot_offset * 8;
                    let copy_len = str_bytes.len().min(total_slot_bytes);
                    row_buf[start..start + copy_len].copy_from_slice(&str_bytes[..copy_len]);
                } else {
                    // VLS: distribute across segments
                    let mut str_pos = 0;
                    for seg in 0..var.n_segments {
                        let seg_start = (slot_offset + seg * 32) * 8;
                        let useful = if seg < var.n_segments - 1 {
                            255
                        } else {
                            width.saturating_sub((var.n_segments - 1) * 255)
                        };
                        let copy_len = str_bytes.len().saturating_sub(str_pos).min(useful);
                        if copy_len > 0 {
                            row_buf[seg_start..seg_start + copy_len]
                                .copy_from_slice(&str_bytes[str_pos..str_pos + copy_len]);
                        }
                        str_pos += useful;
                    }
                }
                slot_offset += total_slots;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Data writers
// ---------------------------------------------------------------------------

/// Maximum rows to fill in one parallel chunk (~256 MB for 1000 slots).
const CHUNK_ROWS: usize = 32_768;

/// Minimum rows to justify rayon parallel overhead.
const MIN_PARALLEL_ROWS: usize = 5_000;

/// Fill row buffers in parallel for a chunk of rows.
/// `big_buf` must have length `nrows * row_bytes` and will be filled with
/// space-padded row data. Each row is independent -- no data races.
fn fill_rows_parallel(
    big_buf: &mut [u8],
    start_row: usize,
    nrows: usize,
    row_bytes: usize,
    batch: &RecordBatch,
    layout: &CaseLayout,
    temporal_arrays: &[Option<Float64Array>],
) {
    use rayon::prelude::*;

    big_buf[..nrows * row_bytes]
        .par_chunks_mut(row_bytes)
        .enumerate()
        .for_each(|(chunk_idx, row_buf)| {
            row_buf.fill(b' ');
            fill_row_buffer(
                row_buf,
                start_row + chunk_idx,
                batch,
                layout,
                temporal_arrays,
            );
        });
}

/// Fill row buffers sequentially (for small files where rayon overhead exceeds benefit).
fn fill_rows_sequential(
    big_buf: &mut [u8],
    start_row: usize,
    nrows: usize,
    row_bytes: usize,
    batch: &RecordBatch,
    layout: &CaseLayout,
    temporal_arrays: &[Option<Float64Array>],
) {
    for i in 0..nrows {
        let offset = i * row_bytes;
        let row_buf = &mut big_buf[offset..offset + row_bytes];
        row_buf.fill(b' ');
        fill_row_buffer(row_buf, start_row + i, batch, layout, temporal_arrays);
    }
}

/// Fill row buffers, choosing parallel or sequential based on row count.
#[inline]
fn fill_rows(
    big_buf: &mut [u8],
    start_row: usize,
    nrows: usize,
    row_bytes: usize,
    batch: &RecordBatch,
    layout: &CaseLayout,
    temporal_arrays: &[Option<Float64Array>],
) {
    if nrows >= MIN_PARALLEL_ROWS {
        fill_rows_parallel(
            big_buf,
            start_row,
            nrows,
            row_bytes,
            batch,
            layout,
            temporal_arrays,
        );
    } else {
        fill_rows_sequential(
            big_buf,
            start_row,
            nrows,
            row_bytes,
            batch,
            layout,
            temporal_arrays,
        );
    }
}

pub(super) fn write_data_uncompressed<W: Write>(
    w: &mut W,
    batch: &RecordBatch,
    layout: &CaseLayout,
) -> Result<()> {
    let nrows = batch.num_rows();
    let row_bytes = layout.slots_per_row * 8;
    let temporal_arrays = preconvert_temporal_columns(batch, layout);

    let chunk_rows = CHUNK_ROWS.min(nrows);
    let mut big_buf = vec![0u8; chunk_rows * row_bytes];

    let mut row = 0;
    while row < nrows {
        let this_chunk = (nrows - row).min(chunk_rows);
        let buf_slice = &mut big_buf[..this_chunk * row_bytes];
        fill_rows(
            buf_slice,
            row,
            this_chunk,
            row_bytes,
            batch,
            layout,
            &temporal_arrays,
        );
        w.write_all(buf_slice)?;
        row += this_chunk;
    }

    Ok(())
}

/// Drain threshold: flush encoder to disk when accumulated output exceeds ~1MB.
const BYTECODE_DRAIN_THRESHOLD: usize = 1 << 20;

pub(super) fn write_data_bytecode<W: Write>(
    w: &mut W,
    batch: &RecordBatch,
    layout: &CaseLayout,
) -> Result<()> {
    let nrows = batch.num_rows();
    let row_bytes = layout.slots_per_row * 8;
    let temporal_arrays = preconvert_temporal_columns(batch, layout);
    let mut encoder = BytecodeEncoder::new(DEFAULT_BIAS);

    let chunk_rows = CHUNK_ROWS.min(nrows);
    let mut big_buf = vec![0u8; chunk_rows * row_bytes];

    let mut row = 0;
    while row < nrows {
        let this_chunk = (nrows - row).min(chunk_rows);
        let buf_slice = &mut big_buf[..this_chunk * row_bytes];
        fill_rows(
            buf_slice,
            row,
            this_chunk,
            row_bytes,
            batch,
            layout,
            &temporal_arrays,
        );

        // Encode rows sequentially from pre-filled buffer
        for r in 0..this_chunk {
            let row_slice = &buf_slice[r * row_bytes..(r + 1) * row_bytes];
            encoder.encode_row(row_slice, layout.slots_per_row);
        }

        // Periodically drain to disk to keep memory bounded
        if encoder.output_len() >= BYTECODE_DRAIN_THRESHOLD {
            let chunk = encoder.drain_output();
            w.write_all(&chunk)?;
        }

        row += this_chunk;
    }

    // Finalize: write EOF and flush remaining
    encoder.write_eof();
    let remaining = encoder.drain_output();
    w.write_all(&remaining)?;

    Ok(())
}

/// Default ZSAV block size (0x3FF000 = ~4MB uncompressed bytecode per block).
const ZSAV_BLOCK_SIZE: usize = 0x3FF000;

struct ZsavBlockInfo {
    uncompressed_offset: i64,
    compressed_offset: i64,
    uncompressed_size: i32,
    compressed_size: i32,
}

/// Compress a byte slice with zlib at the given compression level.
fn zlib_compress(data: &[u8], level: flate2::Compression) -> Result<Vec<u8>> {
    use flate2::write::ZlibEncoder;
    let mut zlib_enc = ZlibEncoder::new(Vec::new(), level);
    zlib_enc
        .write_all(data)
        .map_err(|e| SpssError::Zlib(format!("zlib compression error: {e}")))?;
    zlib_enc
        .finish()
        .map_err(|e| SpssError::Zlib(format!("zlib finish error: {e}")))
}

pub(super) fn write_data_zsav<W: Write + Seek>(
    w: &mut W,
    batch: &RecordBatch,
    layout: &CaseLayout,
    level: flate2::Compression,
) -> Result<()> {
    use rayon::prelude::*;

    let nrows = batch.num_rows();
    let row_bytes = layout.slots_per_row * 8;
    let temporal_arrays = preconvert_temporal_columns(batch, layout);

    // Write zheader placeholder (will backpatch later)
    let zheader_offset = w.stream_position().map_err(SpssError::Io)? as i64;
    w.write_all(&[0u8; 24])?;

    // Phase 1: Generate ALL bytecode into a single contiguous buffer
    let mut encoder = BytecodeEncoder::with_capacity(DEFAULT_BIAS, nrows * row_bytes);
    let chunk_rows = CHUNK_ROWS.min(nrows);
    let mut big_buf = vec![0u8; chunk_rows * row_bytes];

    let mut row = 0;
    while row < nrows {
        let this_chunk = (nrows - row).min(chunk_rows);
        let buf_slice = &mut big_buf[..this_chunk * row_bytes];
        fill_rows(
            buf_slice,
            row,
            this_chunk,
            row_bytes,
            batch,
            layout,
            &temporal_arrays,
        );

        for r in 0..this_chunk {
            let row_slice = &buf_slice[r * row_bytes..(r + 1) * row_bytes];
            encoder.encode_row(row_slice, layout.slots_per_row);
        }
        row += this_chunk;
    }
    encoder.write_eof();
    let all_bytecode = encoder.drain_output();

    // Phase 2: Split bytecode into blocks, compress all in parallel
    let n_full_blocks = all_bytecode.len() / ZSAV_BLOCK_SIZE;
    let has_remainder = !all_bytecode.len().is_multiple_of(ZSAV_BLOCK_SIZE);
    let n_blocks = n_full_blocks + if has_remainder { 1 } else { 0 };

    let chunk_ranges: Vec<(usize, usize)> = (0..n_blocks)
        .map(|i| {
            let start = i * ZSAV_BLOCK_SIZE;
            let len = (all_bytecode.len() - start).min(ZSAV_BLOCK_SIZE);
            (start, len)
        })
        .collect();

    let compressed_blocks: Vec<Vec<u8>> = chunk_ranges
        .par_iter()
        .map(|&(start, len)| zlib_compress(&all_bytecode[start..start + len], level))
        .collect::<Result<Vec<_>>>()?;

    // Phase 3: Write compressed blocks sequentially, build trailer
    let mut blocks: Vec<ZsavBlockInfo> = Vec::with_capacity(n_blocks);
    let mut bytecode_offset: i64 = zheader_offset; // per PSPP spec: starts at zheader file position

    for (i, compressed) in compressed_blocks.iter().enumerate() {
        let (_, uncompressed_len) = chunk_ranges[i];
        let compressed_offset = w.stream_position().map_err(SpssError::Io)? as i64;
        w.write_all(compressed)?;

        blocks.push(ZsavBlockInfo {
            uncompressed_offset: bytecode_offset,
            compressed_offset,
            uncompressed_size: uncompressed_len as i32,
            compressed_size: compressed.len() as i32,
        });

        bytecode_offset += uncompressed_len as i64;
    }

    // Write ztrailer
    let ztrailer_offset = w.stream_position().map_err(SpssError::Io)? as i64;
    w.write_all(&(-100_i64).to_le_bytes())?; // ztrailer bias is negative per PSPP spec
    w.write_all(&0_i64.to_le_bytes())?;
    w.write_i32_le(ZSAV_BLOCK_SIZE as i32)?;
    w.write_i32_le(blocks.len() as i32)?;

    for block in &blocks {
        w.write_all(&block.uncompressed_offset.to_le_bytes())?;
        w.write_all(&block.compressed_offset.to_le_bytes())?;
        w.write_i32_le(block.uncompressed_size)?;
        w.write_i32_le(block.compressed_size)?;
    }

    let ztrailer_end = w.stream_position().map_err(SpssError::Io)? as i64;
    let ztrailer_length = ztrailer_end - ztrailer_offset;

    // Backpatch zheader
    w.seek(SeekFrom::Start(zheader_offset as u64))
        .map_err(SpssError::Io)?;
    w.write_all(&zheader_offset.to_le_bytes())?;
    w.write_all(&ztrailer_offset.to_le_bytes())?;
    w.write_all(&ztrailer_length.to_le_bytes())?;

    // Seek back to end
    w.seek(SeekFrom::Start(ztrailer_end as u64))
        .map_err(SpssError::Io)?;

    Ok(())
}
