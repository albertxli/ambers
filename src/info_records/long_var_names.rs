use crate::io_utils::{self, RawText};

/// Parse subtype 13: long variable names.
///
/// Format: `SHORT_NAME=LongVariableName\tSHORT2=LongName2\t...`
///
/// Returns (short_name, long_name) byte pairs. Names are kept undecoded
/// (only the short side is ASCII-uppercased, matching how variable records
/// are normalised) and decoded with the file encoding in `resolve_dictionary`.
pub fn parse_long_var_names(data: &[u8]) -> Vec<(RawText, RawText)> {
    let mut result = Vec::new();

    for pair in data.split(|&b| b == b'\t') {
        let pair = io_utils::trim_ascii_nul(pair);
        if pair.is_empty() {
            continue;
        }
        if let Some(eq) = pair.iter().position(|&b| b == b'=') {
            let mut short = io_utils::trim_ascii_nul(&pair[..eq]).to_vec();
            short.make_ascii_uppercase();
            let long = io_utils::trim_ascii_nul(&pair[eq + 1..]).to_vec();
            result.push((short, long));
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_long_var_names() {
        let data = b"Q1=Question1\tQ2=Question_Two\tAGE=RespondentAge\t";
        let names = parse_long_var_names(data);

        assert_eq!(names.len(), 3);
        assert_eq!(names[0], (b"Q1".to_vec(), b"Question1".to_vec()));
        assert_eq!(names[1], (b"Q2".to_vec(), b"Question_Two".to_vec()));
        assert_eq!(names[2], (b"AGE".to_vec(), b"RespondentAge".to_vec()));
    }

    #[test]
    fn test_non_ascii_names_are_preserved_as_bytes() {
        // windows-1250 bytes: KORKVÓTA (0xD3 = Ó). Only ASCII letters are
        // case-folded; the accented byte must pass through untouched.
        let data = b"korkv\xd3ta=KORKV\xd3TA\tS0=S0";
        let names = parse_long_var_names(data);
        assert_eq!(names[0], (b"KORKV\xd3TA".to_vec(), b"KORKV\xd3TA".to_vec()));
        assert_eq!(names[1], (b"S0".to_vec(), b"S0".to_vec()));
    }
}
