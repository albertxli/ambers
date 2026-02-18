use crate::error::{Result, SpssError};

/// Missing value specification for a long string variable.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct LongStringMissingEntry {
    /// Variable name.
    pub var_name: String,
    /// Number of missing values.
    pub n_values: u8,
    /// Missing value byte arrays.
    pub values: Vec<Vec<u8>>,
}

/// Parse subtype 22: long string missing values.
///
/// Format (for each variable):
///   4-byte var_name_length
///   var_name bytes
///   1-byte n_missing_values
///   4-byte value_length (per SPSS spec: the width)
///   For each missing value:
///     value_length bytes of value
pub fn parse_long_string_missing(data: &[u8]) -> Result<Vec<LongStringMissingEntry>> {
    let mut result = Vec::new();
    let mut pos = 0;

    while pos + 4 <= data.len() {
        // Variable name
        let name_len = read_i32_le(data, pos)? as usize;
        pos += 4;
        if pos + name_len > data.len() {
            break;
        }
        let var_name = String::from_utf8_lossy(&data[pos..pos + name_len])
            .trim()
            .to_string();
        pos += name_len;

        // Number of missing values
        if pos >= data.len() {
            break;
        }
        let n_values = data[pos];
        pos += 1;

        // Value length
        if pos + 4 > data.len() {
            break;
        }
        let value_len = read_i32_le(data, pos)? as usize;
        pos += 4;

        let mut values = Vec::with_capacity(n_values as usize);
        for _ in 0..n_values {
            if pos + value_len > data.len() {
                break;
            }
            values.push(data[pos..pos + value_len].to_vec());
            pos += value_len;
        }

        result.push(LongStringMissingEntry {
            var_name,
            n_values,
            values,
        });
    }

    Ok(result)
}

fn read_i32_le(data: &[u8], pos: usize) -> Result<i32> {
    if pos + 4 > data.len() {
        return Err(SpssError::TruncatedFile {
            expected: pos + 4,
            actual: data.len(),
        });
    }
    let bytes: [u8; 4] = data[pos..pos + 4].try_into().unwrap();
    Ok(i32::from_le_bytes(bytes))
}
