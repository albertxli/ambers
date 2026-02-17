use crate::error::{Result, SpssError};

/// A set of value labels for a long string variable.
#[derive(Debug, Clone)]
pub struct LongStringLabelSet {
    /// Variable name.
    pub var_name: String,
    /// (value, label) pairs.
    pub labels: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Parse subtype 21: long string value labels.
///
/// Format (for each variable):
///   4-byte var_name_length
///   var_name bytes
///   4-byte label_count
///   For each label:
///     4-byte value_length
///     value bytes
///     4-byte label_length
///     label bytes
pub fn parse_long_string_labels(data: &[u8]) -> Result<Vec<LongStringLabelSet>> {
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

        // Label count
        if pos + 4 > data.len() {
            break;
        }
        let label_count = read_i32_le(data, pos)? as usize;
        pos += 4;

        let mut labels = Vec::with_capacity(label_count);
        for _ in 0..label_count {
            // Value length + value
            if pos + 4 > data.len() {
                break;
            }
            let value_len = read_i32_le(data, pos)? as usize;
            pos += 4;
            if pos + value_len > data.len() {
                break;
            }
            let value = data[pos..pos + value_len].to_vec();
            pos += value_len;

            // Label length + label
            if pos + 4 > data.len() {
                break;
            }
            let label_len = read_i32_le(data, pos)? as usize;
            pos += 4;
            if pos + label_len > data.len() {
                break;
            }
            let label = data[pos..pos + label_len].to_vec();
            pos += label_len;

            labels.push((value, label));
        }

        result.push(LongStringLabelSet { var_name, labels });
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
