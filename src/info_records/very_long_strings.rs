use crate::io_utils::{self, RawText};

/// Parse subtype 14: very long string widths.
///
/// Format: `VARNAME=WIDTH\0\tVARNAME2=WIDTH2\0\t...`
///
/// Returns (variable_name_bytes, true_width) pairs. The name is kept
/// undecoded (ASCII-uppercased) so it matches variable records byte for byte
/// regardless of the file encoding.
pub fn parse_very_long_strings(data: &[u8]) -> Vec<(RawText, usize)> {
    let mut result = Vec::new();

    // Split by \0 or \t
    for entry in data.split(|&b| b == 0 || b == b'\t') {
        let entry = io_utils::trim_ascii_nul(entry);
        if entry.is_empty() {
            continue;
        }
        if let Some(eq) = entry.iter().position(|&b| b == b'=')
            && let Ok(width_str) = std::str::from_utf8(&entry[eq + 1..])
            && let Ok(width) = width_str.trim().parse::<usize>()
        {
            let mut name = io_utils::trim_ascii_nul(&entry[..eq]).to_vec();
            name.make_ascii_uppercase();
            result.push((name, width));
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
        assert_eq!(entries[0], (b"LONGVAR1".to_vec(), 500));
        assert_eq!(entries[1], (b"LONGVAR2".to_vec(), 1000));
    }
}
