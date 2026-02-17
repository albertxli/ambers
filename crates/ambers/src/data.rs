use std::io::{Read, Seek};

use encoding_rs::Encoding;
use rayon::prelude::*;

use crate::compression::bytecode::{BytecodeDecompressor, SlotValue};
use crate::compression::zlib;
use crate::constants::*;
use crate::dictionary::ResolvedDictionary;
use crate::encoding;
use crate::error::{Result, SpssError};
use crate::io_utils::{self, SavReader};
use crate::variable::VariableRecord;

/// A typed cell value from one variable in one case.
#[derive(Debug, Clone)]
pub enum CellValue {
    Numeric(f64),
    Missing,
    Text(String),
}

/// Read all data from an uncompressed SAV file.
pub fn read_uncompressed<R: Read>(
    reader: &mut SavReader<R>,
    dict: &ResolvedDictionary,
) -> Result<Vec<Vec<CellValue>>> {
    let slots_per_row = dict.header.nominal_case_size as usize;
    let ncases = if dict.header.ncases >= 0 {
        Some(dict.header.ncases as usize)
    } else {
        None
    };

    let mut all_rows = Vec::with_capacity(ncases.unwrap_or(1000));

    loop {
        // Read one row of raw 8-byte slots
        let mut raw_slots = Vec::with_capacity(slots_per_row);
        for _ in 0..slots_per_row {
            match reader.read_8_bytes() {
                Ok(bytes) => raw_slots.push(bytes),
                Err(_) => {
                    // EOF or short read -- we're done
                    if raw_slots.is_empty() {
                        return Ok(all_rows);
                    } else {
                        return Err(SpssError::TruncatedFile {
                            expected: slots_per_row * 8,
                            actual: raw_slots.len() * 8,
                        });
                    }
                }
            }
        }

        let row = slots_to_row(&raw_slots, &dict.all_slots, &dict.variables, dict.file_encoding)?;
        all_rows.push(row);
    }
}

/// Read all data from a bytecode-compressed SAV file.
pub fn read_bytecode_compressed<R: Read>(
    reader: &mut SavReader<R>,
    dict: &ResolvedDictionary,
) -> Result<Vec<Vec<CellValue>>> {
    let slots_per_row = dict.header.nominal_case_size as usize;
    let bias = dict.header.bias;

    let ncases = if dict.header.ncases >= 0 {
        Some(dict.header.ncases as usize)
    } else {
        None
    };

    // Read all remaining data into memory for bytecode decompression
    let estimated_size = ncases.unwrap_or(1000) * slots_per_row * 8;
    let mut compressed_data = Vec::with_capacity(estimated_size);
    reader.inner_mut().read_to_end(&mut compressed_data)?;

    // Phase 1: Sequential bytecode decompression (stateful — cannot parallelize)
    let mut decompressor = BytecodeDecompressor::new(bias);
    let mut slot_rows = Vec::with_capacity(ncases.unwrap_or(1000));

    loop {
        let slots = decompressor.decompress_row(&compressed_data, slots_per_row)?;

        if slots.is_empty() || slots.len() < slots_per_row {
            break;
        }

        slot_rows.push(slots);
    }

    // Phase 2: Parallel SlotValue -> CellValue conversion
    let all_rows: Vec<Vec<CellValue>> = slot_rows
        .par_iter()
        .map(|slots| {
            slot_values_to_row(
                slots,
                &dict.all_slots,
                &dict.variables,
                dict.file_encoding,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(all_rows)
}

/// Read all data from a ZSAV (zlib-compressed) file.
pub fn read_zlib_compressed<R: Read + Seek>(
    reader: &mut SavReader<R>,
    dict: &ResolvedDictionary,
) -> Result<Vec<Vec<CellValue>>> {
    // Read the ZSAV header
    let zheader = zlib::read_zheader(reader)?;

    // Read the trailer (requires seeking)
    let ztrailer = zlib::read_ztrailer(reader, &zheader)?;

    // Decompress all blocks -- this yields bytecode-compressed data
    let bytecode_data = zlib::decompress_zsav_blocks(reader, &ztrailer)?;

    // Now decompress the bytecodes using stateful decompressor
    let slots_per_row = dict.header.nominal_case_size as usize;
    let bias = dict.header.bias;
    let ncases = if dict.header.ncases >= 0 {
        Some(dict.header.ncases as usize)
    } else {
        None
    };

    // Phase 1: Sequential bytecode decompression (stateful)
    let mut decompressor = BytecodeDecompressor::new(bias);
    let mut slot_rows = Vec::with_capacity(ncases.unwrap_or(1000));

    loop {
        let slots = decompressor.decompress_row(&bytecode_data, slots_per_row)?;

        if slots.is_empty() || slots.len() < slots_per_row {
            break;
        }

        slot_rows.push(slots);
    }

    // Phase 2: Parallel SlotValue -> CellValue conversion
    let all_rows: Vec<Vec<CellValue>> = slot_rows
        .par_iter()
        .map(|slots| {
            slot_values_to_row(
                slots,
                &dict.all_slots,
                &dict.variables,
                dict.file_encoding,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(all_rows)
}

/// Convert raw 8-byte slots into typed cell values for one row.
pub(crate) fn slots_to_row(
    raw_slots: &[[u8; 8]],
    all_slots: &[VariableRecord],
    visible_vars: &[VariableRecord],
    file_encoding: &'static Encoding,
) -> Result<Vec<CellValue>> {
    let mut row = Vec::with_capacity(visible_vars.len());

    for var in visible_vars {
        let slot_idx = var.slot_index;

        match &var.var_type {
            VarType::Numeric => {
                if slot_idx < raw_slots.len() {
                    let val = f64::from_le_bytes(raw_slots[slot_idx]);
                    if is_sysmis(val) {
                        row.push(CellValue::Missing);
                    } else {
                        row.push(CellValue::Numeric(val));
                    }
                } else {
                    row.push(CellValue::Missing);
                }
            }
            VarType::String(width) => {
                let text = read_string_from_slots(
                    raw_slots,
                    slot_idx,
                    *width,
                    var.n_segments,
                    all_slots,
                    file_encoding,
                );
                row.push(CellValue::Text(text));
            }
        }
    }

    Ok(row)
}

/// Convert SlotValues from bytecode decompression into typed cell values.
/// Direct conversion without intermediate Vec<[u8;8]>.
pub(crate) fn slot_values_to_row(
    slot_values: &[SlotValue],
    all_slots: &[VariableRecord],
    visible_vars: &[VariableRecord],
    file_encoding: &'static Encoding,
) -> Result<Vec<CellValue>> {
    let mut row = Vec::with_capacity(visible_vars.len());

    for var in visible_vars {
        let slot_idx = var.slot_index;

        match &var.var_type {
            VarType::Numeric => {
                if slot_idx < slot_values.len() {
                    match &slot_values[slot_idx] {
                        SlotValue::Numeric(v) => {
                            if is_sysmis(*v) {
                                row.push(CellValue::Missing);
                            } else {
                                row.push(CellValue::Numeric(*v));
                            }
                        }
                        SlotValue::Sysmis => row.push(CellValue::Missing),
                        SlotValue::Raw(bytes) => {
                            let val = f64::from_le_bytes(*bytes);
                            if is_sysmis(val) {
                                row.push(CellValue::Missing);
                            } else {
                                row.push(CellValue::Numeric(val));
                            }
                        }
                        SlotValue::EndOfFile => row.push(CellValue::Missing),
                        SlotValue::Spaces => row.push(CellValue::Missing),
                    }
                } else {
                    row.push(CellValue::Missing);
                }
            }
            VarType::String(width) => {
                let text = read_string_from_slot_values(
                    slot_values,
                    slot_idx,
                    *width,
                    var.n_segments,
                    all_slots,
                    file_encoding,
                );
                row.push(CellValue::Text(text));
            }
        }
    }

    Ok(row)
}

/// Read a string value from raw 8-byte slots, handling multi-segment very long strings.
fn read_string_from_slots(
    raw_slots: &[[u8; 8]],
    start_slot: usize,
    width: usize,
    n_segments: usize,
    _all_slots: &[VariableRecord],
    file_encoding: &'static Encoding,
) -> String {
    let mut bytes = Vec::with_capacity(width);

    if n_segments <= 1 {
        // Simple string: read ceil(width/8) slots
        let n_slots = (width + 7) / 8;
        for i in 0..n_slots {
            let idx = start_slot + i;
            if idx < raw_slots.len() {
                bytes.extend_from_slice(&raw_slots[idx]);
            }
        }
    } else {
        // Very long string: read across segments.
        // Each segment contributes 252 useful bytes from 255-byte (32 slots) of storage.
        // The segment variable uses ceil(255/8) = 32 slots per segment.
        let mut slot = start_slot;
        for seg in 0..n_segments {
            let seg_useful = if seg < n_segments - 1 {
                252
            } else {
                // Last segment: remainder
                width - (n_segments - 1) * 252
            };

            let seg_slots = 32; // ceil(255/8) = 32 slots per segment (255 byte string width)
            let useful_slots = (seg_useful + 7) / 8;

            for i in 0..useful_slots {
                if slot + i < raw_slots.len() {
                    bytes.extend_from_slice(&raw_slots[slot + i]);
                }
            }

            slot += seg_slots;
        }
    }

    // Truncate to actual width and trim trailing spaces
    bytes.truncate(width);
    let trimmed = io_utils::trim_trailing_padding(&bytes);
    encoding::decode_str_lossy(trimmed, file_encoding)
}

/// Read a string value from SlotValues, handling multi-segment very long strings.
/// Same logic as read_string_from_slots but operates on &[SlotValue] directly.
fn read_string_from_slot_values(
    slot_values: &[SlotValue],
    start_slot: usize,
    width: usize,
    n_segments: usize,
    _all_slots: &[VariableRecord],
    file_encoding: &'static Encoding,
) -> String {
    let mut bytes = Vec::with_capacity(width);

    /// Extract 8 bytes from a SlotValue into the output buffer.
    #[inline]
    fn push_slot_bytes(bytes: &mut Vec<u8>, sv: &SlotValue) {
        match sv {
            SlotValue::Raw(b) => bytes.extend_from_slice(b),
            SlotValue::Spaces => bytes.extend_from_slice(&[b' '; 8]),
            SlotValue::Numeric(v) => bytes.extend_from_slice(&v.to_le_bytes()),
            SlotValue::Sysmis => bytes.extend_from_slice(&[0u8; 8]),
            SlotValue::EndOfFile => bytes.extend_from_slice(&[0u8; 8]),
        }
    }

    if n_segments <= 1 {
        let n_slots = (width + 7) / 8;
        for i in 0..n_slots {
            let idx = start_slot + i;
            if idx < slot_values.len() {
                push_slot_bytes(&mut bytes, &slot_values[idx]);
            }
        }
    } else {
        let mut slot = start_slot;
        for seg in 0..n_segments {
            let seg_useful = if seg < n_segments - 1 {
                252
            } else {
                width - (n_segments - 1) * 252
            };

            let seg_slots = 32;
            let useful_slots = (seg_useful + 7) / 8;

            for i in 0..useful_slots {
                if slot + i < slot_values.len() {
                    push_slot_bytes(&mut bytes, &slot_values[slot + i]);
                }
            }

            slot += seg_slots;
        }
    }

    bytes.truncate(width);
    let trimmed = io_utils::trim_trailing_padding(&bytes);
    encoding::decode_str_lossy(trimmed, file_encoding)
}

/// Convert SlotValues to CellValues for a projected subset of variables.
/// `projected_vars` is a slice of indices into `visible_vars`.
/// Returns Vec<CellValue> with length = projected_vars.len().
pub(crate) fn slot_values_to_row_projected(
    slot_values: &[SlotValue],
    all_slots: &[VariableRecord],
    visible_vars: &[VariableRecord],
    projected_vars: &[usize],
    file_encoding: &'static Encoding,
) -> Result<Vec<CellValue>> {
    let mut row = Vec::with_capacity(projected_vars.len());

    for &var_idx in projected_vars {
        let var = &visible_vars[var_idx];
        let slot_idx = var.slot_index;

        match &var.var_type {
            VarType::Numeric => {
                if slot_idx < slot_values.len() {
                    match &slot_values[slot_idx] {
                        SlotValue::Numeric(v) => {
                            if is_sysmis(*v) {
                                row.push(CellValue::Missing);
                            } else {
                                row.push(CellValue::Numeric(*v));
                            }
                        }
                        SlotValue::Sysmis => row.push(CellValue::Missing),
                        SlotValue::Raw(bytes) => {
                            let val = f64::from_le_bytes(*bytes);
                            if is_sysmis(val) {
                                row.push(CellValue::Missing);
                            } else {
                                row.push(CellValue::Numeric(val));
                            }
                        }
                        SlotValue::EndOfFile => row.push(CellValue::Missing),
                        SlotValue::Spaces => row.push(CellValue::Missing),
                    }
                } else {
                    row.push(CellValue::Missing);
                }
            }
            VarType::String(width) => {
                let text = read_string_from_slot_values(
                    slot_values,
                    slot_idx,
                    *width,
                    var.n_segments,
                    all_slots,
                    file_encoding,
                );
                row.push(CellValue::Text(text));
            }
        }
    }

    Ok(row)
}

/// Convert raw 8-byte slots to CellValues for a projected subset of variables.
pub(crate) fn slots_to_row_projected(
    raw_slots: &[[u8; 8]],
    all_slots: &[VariableRecord],
    visible_vars: &[VariableRecord],
    projected_vars: &[usize],
    file_encoding: &'static Encoding,
) -> Result<Vec<CellValue>> {
    let mut row = Vec::with_capacity(projected_vars.len());

    for &var_idx in projected_vars {
        let var = &visible_vars[var_idx];
        let slot_idx = var.slot_index;

        match &var.var_type {
            VarType::Numeric => {
                if slot_idx < raw_slots.len() {
                    let val = f64::from_le_bytes(raw_slots[slot_idx]);
                    if is_sysmis(val) {
                        row.push(CellValue::Missing);
                    } else {
                        row.push(CellValue::Numeric(val));
                    }
                } else {
                    row.push(CellValue::Missing);
                }
            }
            VarType::String(width) => {
                let text = read_string_from_slots(
                    raw_slots,
                    slot_idx,
                    *width,
                    var.n_segments,
                    all_slots,
                    file_encoding,
                );
                row.push(CellValue::Text(text));
            }
        }
    }

    Ok(row)
}
