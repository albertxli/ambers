/// Subtype 18: Variable Attributes Record.
///
/// Text-based format (NOT pascal-string binary like subtypes 21/22).
/// Grammar per PSPP Developer Guide:
///
///   record     = var_set ("/" var_set)*
///   var_set    = var_name ":" attribute+
///   attribute  = attr_name "(" value+ ")"
///   value      = "'" text "'" "\n"
///
/// Example: `age:$@Role('0'\n)/income:$@Role('1'\n)`
///
/// All text is kept as the file's bytes and decoded with the file encoding in
/// `resolve_dictionary`; every delimiter above is ASCII, so the parser never
/// needs to know the encoding.
use crate::io_utils::{self, RawText};

/// A parsed variable attribute set.
#[derive(Debug, Clone)]
pub struct VarAttributeSet {
    /// Variable name bytes (may be a short name — resolved in dictionary.rs).
    pub var_name: RawText,
    /// (attribute_name, [values]) pairs, undecoded.
    pub attributes: Vec<(RawText, Vec<RawText>)>,
}

/// Parse a subtype 18 text blob into variable attribute sets.
pub fn parse_var_attributes(data: &[u8]) -> Vec<VarAttributeSet> {
    let end = data.len() - data.iter().rev().take_while(|&&b| b == 0).count();
    let text = &data[..end];

    let mut result = Vec::new();

    // Split on '/' to get individual variable attribute sets.
    // Be careful: '/' can appear inside quoted values, so we need to
    // track whether we're inside quotes.
    for var_chunk in split_var_sets(text) {
        let var_chunk = io_utils::trim_ascii_nul(var_chunk);
        if var_chunk.is_empty() {
            continue;
        }

        // Split on first ':' to get var_name and attribute text
        let colon_pos = match var_chunk.iter().position(|&b| b == b':') {
            Some(p) => p,
            None => continue,
        };

        let var_name = io_utils::trim_ascii_nul(&var_chunk[..colon_pos]).to_vec();
        let attr_text = &var_chunk[colon_pos + 1..];

        let attributes = parse_attributes(attr_text);
        if !attributes.is_empty() {
            result.push(VarAttributeSet {
                var_name,
                attributes,
            });
        }
    }

    result
}

/// Split the record on '/' delimiters, respecting single-quoted values.
fn split_var_sets(text: &[u8]) -> Vec<&[u8]> {
    let mut result = Vec::new();
    let mut start = 0;
    let mut in_quote = false;

    for (i, &b) in text.iter().enumerate() {
        match b {
            b'\'' => in_quote = !in_quote,
            b'/' if !in_quote => {
                result.push(&text[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    // Last segment
    if start < text.len() {
        result.push(&text[start..]);
    }

    result
}

/// Parse the attribute portion: `AttrName1('val1'\n)AttrName2('val2'\n)`
fn parse_attributes(bytes: &[u8]) -> Vec<(RawText, Vec<RawText>)> {
    let mut result = Vec::new();
    let mut pos = 0;

    while pos < bytes.len() {
        // Skip whitespace
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos >= bytes.len() {
            break;
        }

        // Read attribute name (up to '(')
        let name_start = pos;
        while pos < bytes.len() && bytes[pos] != b'(' {
            pos += 1;
        }
        if pos >= bytes.len() {
            break;
        }
        let attr_name = io_utils::trim_ascii_nul(&bytes[name_start..pos]).to_vec();
        pos += 1; // skip '('

        // Read values until closing ')'
        let mut values = Vec::new();
        while pos < bytes.len() && bytes[pos] != b')' {
            // Skip whitespace/newlines between values
            while pos < bytes.len()
                && (bytes[pos] == b'\n' || bytes[pos] == b'\r' || bytes[pos] == b' ')
            {
                pos += 1;
            }
            if pos >= bytes.len() || bytes[pos] == b')' {
                break;
            }

            // Expect opening quote
            if bytes[pos] == b'\'' {
                pos += 1;
                // Read until closing quote
                let val_start = pos;
                while pos < bytes.len() && bytes[pos] != b'\'' {
                    pos += 1;
                }
                values.push(bytes[val_start..pos].to_vec());
                if pos < bytes.len() {
                    pos += 1; // skip closing quote
                }
                // Skip trailing newline (part of the format spec)
                if pos < bytes.len() && bytes[pos] == b'\n' {
                    pos += 1;
                }
            } else {
                // Unexpected character — skip to avoid infinite loop
                pos += 1;
            }
        }
        if pos < bytes.len() {
            pos += 1; // skip ')'
        }

        if !attr_name.is_empty() {
            result.push((attr_name, values));
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vals(v: &[&str]) -> Vec<RawText> {
        v.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    #[test]
    fn test_parse_single_role() {
        let data = b"age:$@Role('0'\n)";
        let sets = parse_var_attributes(data);
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].var_name, b"age");
        assert_eq!(sets[0].attributes.len(), 1);
        assert_eq!(sets[0].attributes[0].0, b"$@Role");
        assert_eq!(sets[0].attributes[0].1, vals(&["0"]));
    }

    #[test]
    fn test_parse_multiple_vars() {
        let data = b"age:$@Role('0'\n)/income:$@Role('1'\n)/region:$@Role('4'\n)";
        let sets = parse_var_attributes(data);
        assert_eq!(sets.len(), 3);
        assert_eq!(sets[0].var_name, b"age");
        assert_eq!(sets[0].attributes[0].1, vals(&["0"]));
        assert_eq!(sets[1].var_name, b"income");
        assert_eq!(sets[1].attributes[0].1, vals(&["1"]));
        assert_eq!(sets[2].var_name, b"region");
        assert_eq!(sets[2].attributes[0].1, vals(&["4"]));
    }

    #[test]
    fn test_parse_multiple_attrs() {
        let data = b"age:$@Role('0'\n)CustomNote('hello'\n)";
        let sets = parse_var_attributes(data);
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].attributes.len(), 2);
        assert_eq!(sets[0].attributes[0].0, b"$@Role");
        assert_eq!(sets[0].attributes[0].1, vals(&["0"]));
        assert_eq!(sets[0].attributes[1].0, b"CustomNote");
        assert_eq!(sets[0].attributes[1].1, vals(&["hello"]));
    }

    #[test]
    fn test_parse_empty() {
        let sets = parse_var_attributes(b"");
        assert!(sets.is_empty());
    }

    #[test]
    fn test_parse_non_ascii_value_kept_as_bytes() {
        let data = b"kv\xf3ta:Note('\xe9rt\xe9k'\n)";
        let sets = parse_var_attributes(data);
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].var_name, b"kv\xf3ta");
        assert_eq!(sets[0].attributes[0].1, vec![b"\xe9rt\xe9k".to_vec()]);
    }

    #[test]
    fn test_parse_all_roles() {
        let data = b"v1:$@Role('0'\n)/v2:$@Role('1'\n)/v3:$@Role('2'\n)/v4:$@Role('3'\n)/v5:$@Role('4'\n)/v6:$@Role('5'\n)";
        let sets = parse_var_attributes(data);
        assert_eq!(sets.len(), 6);
        for (i, set) in sets.iter().enumerate() {
            assert_eq!(set.attributes[0].1[0], i.to_string().as_bytes());
        }
    }
}
