use std::io::Read;

use crate::error::{Result, SpssError};
use crate::io_utils::{self, SavReader};

/// A raw value from a value label record (always 8 bytes).
#[derive(Debug, Clone)]
pub enum RawValue {
    Numeric(f64),
    String(Vec<u8>),
}

/// A set of value labels parsed from type 3 + type 4 records.
#[derive(Debug, Clone)]
pub struct ValueLabelSet {
    /// (raw_value, label_bytes) pairs.
    pub labels: Vec<(RawValue, Vec<u8>)>,
    /// 0-based variable slot indices that use this label set (from type 4).
    pub variable_indices: Vec<usize>,
}

/// Parse a type 3 (value label) record. The record type i32 has already been read.
///
/// Returns the value-label pairs. The caller should immediately read the
/// following type 4 record to get the variable indices.
pub fn parse_value_labels<R: Read>(reader: &mut SavReader<R>) -> Result<Vec<(RawValue, Vec<u8>)>> {
    let count = reader.read_i32()? as usize;
    let mut labels = Vec::with_capacity(count);

    for _ in 0..count {
        // Value: 8 bytes (could be numeric f64 or string bytes)
        let value_bytes = reader.read_8_bytes()?;

        // Label length: 1 byte
        let mut label_len_buf = [0u8; 1];
        reader.read_exact(&mut label_len_buf)?;
        let label_len = label_len_buf[0] as usize;

        // Label text + padding to align to 8 bytes
        // Total consumed so far for this label entry: 8 (value) + 1 (len byte) + label_len + padding
        // The label + length byte together must be padded to a multiple of 8
        let padded_label_len = io_utils::round_up(label_len + 1, 8) - 1;
        let label_data = reader.read_bytes(padded_label_len)?;
        let label_bytes = label_data[..label_len].to_vec();

        // We store as numeric by default; the dictionary resolution step will
        // determine if this should be string based on the linked variable types.
        let value = RawValue::Numeric(f64::from_le_bytes(value_bytes));

        labels.push((value, label_bytes));
    }

    Ok(labels)
}

/// Parse a type 4 (value label variables) record. The record type i32 has already been read.
///
/// Returns 0-based variable slot indices.
pub fn parse_value_label_variables<R: Read>(reader: &mut SavReader<R>) -> Result<Vec<usize>> {
    let count = reader.read_i32()? as usize;

    if count == 0 {
        return Err(SpssError::InvalidValueLabel(
            "type 4 record with 0 variables".to_string(),
        ));
    }

    let mut indices = Vec::with_capacity(count);
    for _ in 0..count {
        let index = reader.read_i32()?;
        // Convert from 1-based to 0-based
        if index < 1 {
            return Err(SpssError::InvalidValueLabel(format!(
                "invalid variable index {index} in type 4 record"
            )));
        }
        indices.push((index - 1) as usize);
    }

    Ok(indices)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_value_labels() {
        let mut buf = Vec::new();

        // Count: 2 labels
        buf.extend_from_slice(&2_i32.to_le_bytes());

        // Label 1: value=1.0, label="Male"
        buf.extend_from_slice(&1.0_f64.to_le_bytes());
        buf.push(4); // label length
        buf.extend_from_slice(b"Male");
        buf.extend_from_slice(&[0u8; 3]); // pad to 8 - 1 = 7 total, already have 4, need 3

        // Label 2: value=2.0, label="Female"
        buf.extend_from_slice(&2.0_f64.to_le_bytes());
        buf.push(6); // label length
        buf.extend_from_slice(b"Female");
        buf.push(0); // pad to 8 - 1 = 7 total, already have 6, need 1

        let mut reader = SavReader::new(&buf[..]);
        let labels = parse_value_labels(&mut reader).unwrap();

        assert_eq!(labels.len(), 2);
        assert_eq!(labels[0].1, b"Male");
        assert_eq!(labels[1].1, b"Female");
    }

    #[test]
    fn test_parse_value_label_variables() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&3_i32.to_le_bytes()); // 3 variables
        buf.extend_from_slice(&1_i32.to_le_bytes()); // var 1 (0-based: 0)
        buf.extend_from_slice(&5_i32.to_le_bytes()); // var 5 (0-based: 4)
        buf.extend_from_slice(&10_i32.to_le_bytes()); // var 10 (0-based: 9)

        let mut reader = SavReader::new(&buf[..]);
        let indices = parse_value_label_variables(&mut reader).unwrap();

        assert_eq!(indices, vec![0, 4, 9]);
    }
}
