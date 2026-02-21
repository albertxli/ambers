use std::borrow::Cow;

use encoding_rs::Encoding;

use crate::error::{Result, SpssError};

/// Map an SPSS IANA code page number to an encoding_rs Encoding.
pub fn encoding_from_code_page(code_page: i32) -> &'static Encoding {
    match code_page {
        437 => encoding_rs::IBM866,       // approximate: DOS US
        850 => encoding_rs::WINDOWS_1252, // approximate: DOS Western European
        874 => encoding_rs::WINDOWS_874,
        932 => encoding_rs::SHIFT_JIS,
        936 => encoding_rs::GBK,
        949 => encoding_rs::EUC_KR,
        950 => encoding_rs::BIG5,
        1200 => encoding_rs::UTF_16LE,
        1201 => encoding_rs::UTF_16BE,
        1250 => encoding_rs::WINDOWS_1250,
        1251 => encoding_rs::WINDOWS_1251,
        1252 => encoding_rs::WINDOWS_1252,
        1253 => encoding_rs::WINDOWS_1253,
        1254 => encoding_rs::WINDOWS_1254,
        1255 => encoding_rs::WINDOWS_1255,
        1256 => encoding_rs::WINDOWS_1256,
        1257 => encoding_rs::WINDOWS_1257,
        1258 => encoding_rs::WINDOWS_1258,
        20127 => encoding_rs::WINDOWS_1252, // US-ASCII -> treat as 1252
        20936 => encoding_rs::GBK,          // simplified Chinese
        28591 => encoding_rs::WINDOWS_1252, // ISO-8859-1 mapped to windows-1252 per WHATWG
        28592 => encoding_rs::ISO_8859_2,
        28593 => encoding_rs::ISO_8859_3,
        28594 => encoding_rs::ISO_8859_4,
        28595 => encoding_rs::ISO_8859_5,
        28596 => encoding_rs::ISO_8859_6,
        28597 => encoding_rs::ISO_8859_7,
        28598 => encoding_rs::ISO_8859_8,
        28599 => encoding_rs::WINDOWS_1254, // ISO-8859-9 ≈ windows-1254
        28603 => encoding_rs::ISO_8859_13,
        28605 => encoding_rs::ISO_8859_15,
        50220 => encoding_rs::ISO_2022_JP,
        51932 => encoding_rs::EUC_JP,
        51949 => encoding_rs::EUC_KR,
        52936 => encoding_rs::GBK,          // HZ-GB-2312
        54936 => encoding_rs::GB18030,
        65001 => encoding_rs::UTF_8,
        _ => encoding_rs::WINDOWS_1252,     // safe default
    }
}

/// Map an encoding name string (e.g., "UTF-8", "windows-1252") to an encoding_rs Encoding.
pub fn encoding_from_name(name: &str) -> &'static Encoding {
    let normalized = name.trim().to_ascii_lowercase();
    Encoding::for_label(normalized.as_bytes()).unwrap_or(encoding_rs::WINDOWS_1252)
}

/// Decode a byte slice using the given encoding, returning a UTF-8 String.
#[allow(dead_code)]
pub fn decode_str(bytes: &[u8], encoding: &'static Encoding) -> Result<String> {
    if encoding == encoding_rs::UTF_8 {
        // Fast path: check if it's valid UTF-8
        match std::str::from_utf8(bytes) {
            Ok(s) => Ok(s.to_string()),
            Err(_) => {
                let (decoded, _, had_errors) = encoding.decode(bytes);
                if had_errors {
                    // Still return the lossy result but note it
                    Ok(decoded.into_owned())
                } else {
                    Ok(decoded.into_owned())
                }
            }
        }
    } else {
        let (decoded, _, had_errors) = encoding.decode(bytes);
        if had_errors {
            Err(SpssError::Encoding(format!(
                "failed to decode bytes with encoding {}",
                encoding.name()
            )))
        } else {
            Ok(decoded.into_owned())
        }
    }
}

/// Decode a byte slice using the given encoding, never failing (lossy).
/// Returns `Cow::Borrowed` for valid UTF-8, avoiding heap allocation.
#[inline]
pub fn decode_str_lossy<'a>(bytes: &'a [u8], encoding: &'static Encoding) -> Cow<'a, str> {
    if encoding == encoding_rs::UTF_8 {
        // Fast path: just validate UTF-8, zero-copy borrow
        match std::str::from_utf8(bytes) {
            Ok(s) => return Cow::Borrowed(s),
            Err(_) => {} // fall through to encoding_rs for lossy decode
        }
    }
    let (decoded, _, _) = encoding.decode(bytes);
    decoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_utf8_passthrough() {
        let s = "Hello, world!";
        let result = decode_str(s.as_bytes(), encoding_rs::UTF_8).unwrap();
        assert_eq!(result, "Hello, world!");
    }

    #[test]
    fn test_windows_1252_decode() {
        // "café" in windows-1252: 63 61 66 e9
        let bytes = [0x63, 0x61, 0x66, 0xe9];
        let result = decode_str(&bytes, encoding_rs::WINDOWS_1252).unwrap();
        assert_eq!(result, "café");
    }

    #[test]
    fn test_code_page_mapping() {
        assert_eq!(encoding_from_code_page(65001), encoding_rs::UTF_8);
        assert_eq!(encoding_from_code_page(1252), encoding_rs::WINDOWS_1252);
        assert_eq!(encoding_from_code_page(932), encoding_rs::SHIFT_JIS);
        // Unknown code page defaults to windows-1252
        assert_eq!(encoding_from_code_page(99999), encoding_rs::WINDOWS_1252);
    }

    #[test]
    fn test_encoding_from_name() {
        assert_eq!(encoding_from_name("UTF-8"), encoding_rs::UTF_8);
        assert_eq!(encoding_from_name("utf-8"), encoding_rs::UTF_8);
        assert_eq!(encoding_from_name("windows-1252"), encoding_rs::WINDOWS_1252);
        assert_eq!(encoding_from_name("ISO-8859-1"), encoding_rs::WINDOWS_1252); // encoding_rs maps this
    }
}
