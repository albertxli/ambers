use crate::io_utils;

/// Parse subtype 14: very long string widths.
///
/// Format: `VARNAME=WIDTH\0\tVARNAME2=WIDTH2\0\t...`
///
/// Returns a vector of (variable_name, true_width) pairs.
pub fn parse_very_long_strings(data: &[u8]) -> Vec<(String, usize)> {
    let text = io_utils::bytes_to_string_lossy(data);
    let mut result = Vec::new();

    // Split by \0 or \t
    for entry in text.split(|c| c == '\0' || c == '\t') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        if let Some((name, width_str)) = entry.split_once('=') {
            if let Ok(width) = width_str.trim().parse::<usize>() {
                result.push((name.trim().to_uppercase(), width));
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_very_long_strings() {
        let data = b"LONGVAR1=500\0\tLONGVAR2=1000\0\t";
        let entries = parse_very_long_strings(data);

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], ("LONGVAR1".to_string(), 500));
        assert_eq!(entries[1], ("LONGVAR2".to_string(), 1000));
    }
}
