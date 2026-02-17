use std::io::Read;

use crate::constants::{Alignment, Measure, SpssFormat, VarType};
use crate::error::Result;
use crate::io_utils::{self, SavReader};

/// Missing value specification for a variable.
#[derive(Debug, Clone)]
pub enum MissingValues {
    None,
    /// Up to 3 discrete numeric values.
    DiscreteNumeric(Vec<f64>),
    /// A range of numeric values [low, high].
    Range { low: f64, high: f64 },
    /// A range plus one discrete value.
    RangeAndValue {
        low: f64,
        high: f64,
        value: f64,
    },
    /// Up to 3 discrete string values (8 bytes each, space-padded).
    DiscreteString(Vec<Vec<u8>>),
}

/// Internal representation of a parsed variable record.
#[derive(Debug, Clone)]
pub struct VariableRecord {
    /// Position in the case (0-based slot index).
    pub slot_index: usize,
    /// SPSS variable type: 0 = numeric, -1 = ghost (continuation), >0 = string width.
    pub raw_type: i32,
    /// Short variable name (up to 8 characters).
    pub short_name: String,
    /// Long variable name (set later from subtype 13; initially same as short_name).
    pub long_name: String,
    /// Variable label text (if present).
    pub label: Option<Vec<u8>>,
    /// Print format.
    pub print_format: Option<SpssFormat>,
    /// Write format.
    pub write_format: Option<SpssFormat>,
    /// Missing value specifications.
    pub missing_values: MissingValues,
    /// Parsed variable type.
    pub var_type: VarType,
    /// Whether this is a "ghost" (continuation) record for long strings.
    pub is_ghost: bool,
    /// Measurement level (set later from subtype 11).
    pub measure: Measure,
    /// Display width (set later from subtype 11).
    pub display_width: u32,
    /// Alignment (set later from subtype 11).
    pub alignment: Alignment,
    /// Number of segments for very long strings (set later from subtype 14).
    pub n_segments: usize,
}

impl VariableRecord {
    /// Parse a type 2 (variable) record. The record type i32 has already been read.
    pub fn parse<R: Read>(reader: &mut SavReader<R>, slot_index: usize) -> Result<VariableRecord> {
        let raw_type = reader.read_i32()?;
        let has_var_label = reader.read_i32()?;
        let n_missing_values = reader.read_i32()?;
        let print_packed = reader.read_i32()?;
        let write_packed = reader.read_i32()?;

        // Short name: 8 bytes
        let name_bytes = reader.read_bytes(8)?;
        let short_name = io_utils::bytes_to_string_lossy(io_utils::trim_trailing_padding(&name_bytes));

        // Determine variable type
        let (var_type, is_ghost) = match raw_type {
            0 => (VarType::Numeric, false),
            t if t > 0 => (VarType::String(t as usize), false),
            -1 => (VarType::Numeric, true), // ghost/continuation record
            _ => (VarType::Numeric, true),   // treat other negatives as ghost
        };

        // Variable label
        let label = if has_var_label == 1 {
            let label_len = reader.read_i32()? as usize;
            let padded_len = io_utils::round_up(label_len, 4);
            let label_bytes = reader.read_bytes(padded_len)?;
            Some(label_bytes[..label_len].to_vec())
        } else {
            None
        };

        // Missing values
        let missing_values = parse_missing_values(reader, n_missing_values, &var_type)?;

        // Formats
        let print_format = SpssFormat::from_packed(print_packed);
        let write_format = SpssFormat::from_packed(write_packed);

        // Default display width from format
        let display_width = print_format.as_ref().map_or(8, |f| f.width as u32);

        Ok(VariableRecord {
            slot_index,
            raw_type,
            short_name: short_name.to_uppercase(),
            long_name: short_name.to_uppercase(), // will be overridden by subtype 13
            label,
            print_format,
            write_format,
            missing_values,
            var_type,
            is_ghost,
            measure: Measure::Unknown,
            display_width,
            alignment: Alignment::Left,
            n_segments: 1,
        })
    }

    /// Get the number of 8-byte slots this variable occupies.
    pub fn n_slots(&self) -> usize {
        match &self.var_type {
            VarType::Numeric => 1,
            VarType::String(width) => {
                // Each 8-byte slot holds 8 bytes of string data.
                // String width rounded up to multiple of 8.
                (width + 7) / 8
            }
        }
    }
}

fn parse_missing_values<R: Read>(
    reader: &mut SavReader<R>,
    n_missing: i32,
    var_type: &VarType,
) -> Result<MissingValues> {
    if n_missing == 0 {
        return Ok(MissingValues::None);
    }

    let abs_n = n_missing.unsigned_abs() as usize;
    let is_range = n_missing < 0;

    match var_type {
        VarType::Numeric => {
            let mut values = Vec::with_capacity(abs_n);
            for _ in 0..abs_n {
                values.push(reader.read_f64()?);
            }

            if is_range {
                match abs_n {
                    2 => Ok(MissingValues::Range {
                        low: values[0],
                        high: values[1],
                    }),
                    3 => Ok(MissingValues::RangeAndValue {
                        low: values[0],
                        high: values[1],
                        value: values[2],
                    }),
                    _ => Ok(MissingValues::DiscreteNumeric(values)),
                }
            } else {
                Ok(MissingValues::DiscreteNumeric(values))
            }
        }
        VarType::String(_) => {
            let mut values = Vec::with_capacity(abs_n);
            for _ in 0..abs_n {
                let bytes = reader.read_8_bytes()?;
                values.push(bytes.to_vec());
            }
            Ok(MissingValues::DiscreteString(values))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_variable_bytes(var_type: i32, name: &[u8; 8], has_label: bool) -> Vec<u8> {
        let mut buf = Vec::new();
        // raw_type
        buf.extend_from_slice(&var_type.to_le_bytes());
        // has_var_label
        buf.extend_from_slice(&(if has_label { 1_i32 } else { 0_i32 }).to_le_bytes());
        // n_missing_values
        buf.extend_from_slice(&0_i32.to_le_bytes());
        // print format: F8.2 = (5 << 16) | (8 << 8) | 2
        let print_fmt: i32 = (5 << 16) | (8 << 8) | 2;
        buf.extend_from_slice(&print_fmt.to_le_bytes());
        // write format: same
        buf.extend_from_slice(&print_fmt.to_le_bytes());
        // name
        buf.extend_from_slice(name);

        if has_label {
            let label = b"Test label";
            let label_len = label.len() as i32;
            buf.extend_from_slice(&label_len.to_le_bytes());
            buf.extend_from_slice(label);
            // pad to multiple of 4
            let padding = io_utils::round_up(label.len(), 4) - label.len();
            buf.extend_from_slice(&vec![0u8; padding]);
        }

        buf
    }

    #[test]
    fn test_parse_numeric_variable() {
        let data = make_variable_bytes(0, b"AGE     ", false);
        let mut reader = SavReader::new(&data[..]);
        let var = VariableRecord::parse(&mut reader, 0).unwrap();

        assert_eq!(var.short_name, "AGE");
        assert_eq!(var.var_type, VarType::Numeric);
        assert!(!var.is_ghost);
        assert!(var.label.is_none());
        assert_eq!(var.print_format.as_ref().unwrap().to_spss_string(), "F8.2");
    }

    #[test]
    fn test_parse_string_variable() {
        let data = make_variable_bytes(20, b"NAME    ", false);
        let mut reader = SavReader::new(&data[..]);
        let var = VariableRecord::parse(&mut reader, 0).unwrap();

        assert_eq!(var.short_name, "NAME");
        assert_eq!(var.var_type, VarType::String(20));
        assert!(!var.is_ghost);
    }

    #[test]
    fn test_parse_variable_with_label() {
        let data = make_variable_bytes(0, b"SCORE   ", true);
        let mut reader = SavReader::new(&data[..]);
        let var = VariableRecord::parse(&mut reader, 0).unwrap();

        assert!(var.label.is_some());
        assert_eq!(var.label.as_ref().unwrap(), b"Test label");
    }

    #[test]
    fn test_ghost_variable() {
        let data = make_variable_bytes(-1, b"        ", false);
        let mut reader = SavReader::new(&data[..]);
        let var = VariableRecord::parse(&mut reader, 5).unwrap();

        assert!(var.is_ghost);
    }
}
