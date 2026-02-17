use crate::io_utils;

/// Parse subtype 20: character encoding declaration.
///
/// Returns the encoding name string (e.g., "UTF-8", "windows-1252").
pub fn parse_encoding_record(data: &[u8]) -> String {
    io_utils::bytes_to_string_lossy(io_utils::trim_trailing_padding(data))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_encoding() {
        assert_eq!(parse_encoding_record(b"UTF-8"), "UTF-8");
        assert_eq!(parse_encoding_record(b"windows-1252   "), "windows-1252");
    }
}
