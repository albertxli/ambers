use crate::metadata::MrType;

/// Raw multiple response set parsed from subtype 7.
/// Variable names are SHORT names — must be resolved to long names later.
#[derive(Debug, Clone)]
pub struct RawMrSet {
    pub name: String,
    pub mr_type: MrType,
    pub counted_value: Option<String>,
    pub label: String,
    pub var_names: Vec<String>,
}

/// Parse subtype 7 multiple response sets.
///
/// Format: newline-separated set definitions. Each set is one line:
///   $NAME=Dn counted_value label_len label var1 var2 ...\n   (dichotomy)
///   $NAME=C label_len label var1 var2 ...\n                   (category)
///
/// Where n is the ASCII length of counted_value (can be multi-digit),
/// and label_len is the ASCII length of the label string that follows.
pub fn parse_mr_sets(data: &[u8]) -> Vec<RawMrSet> {
    let text = String::from_utf8_lossy(data);
    let mut sets = Vec::new();

    // Sets are newline-separated (may also have NUL terminators)
    for line in text.split('\n') {
        let line = line.trim_matches('\0').trim();
        if line.is_empty() || !line.starts_with('$') {
            continue;
        }
        if let Some(mr_set) = parse_one_mr_set(line) {
            sets.push(mr_set);
        }
    }

    sets
}

fn parse_one_mr_set(text: &str) -> Option<RawMrSet> {
    // Must start with $
    let text = text.strip_prefix('$')?;

    // Find '=' to split name and rest
    let eq_pos = text.find('=')?;
    let name = text[..eq_pos].to_string();
    let rest = &text[eq_pos + 1..];

    if rest.is_empty() {
        return None;
    }

    let type_char = rest.as_bytes()[0] as char;
    let rest = &rest[1..];

    let (mr_type, counted_value, after_cv) = match type_char {
        'D' | 'E' => {
            // Dichotomy: Dn counted_value ...
            // n is ASCII digits = length of counted value
            let (cv_len, after_len) = parse_number(rest)?;
            // Skip one space after the number
            let after_space = after_len.strip_prefix(' ').unwrap_or(after_len);
            // Read cv_len characters as counted_value
            if after_space.len() < cv_len {
                return None;
            }
            let counted_value = after_space[..cv_len].to_string();
            let remainder = &after_space[cv_len..];
            (MrType::MultipleDichotomy, Some(counted_value), remainder)
        }
        'C' => (MrType::MultipleCategory, None, rest),
        _ => return None,
    };

    // Next: skip space(s), then parse label_len and label
    let trimmed = after_cv.trim_start();

    // Parse label_len
    let (label_len, after_label_len) = parse_number(trimmed)?;

    // Skip one space after label_len
    let after_space = after_label_len.strip_prefix(' ').unwrap_or(after_label_len);

    // Read label_len characters as the label
    if after_space.len() < label_len {
        // Label extends to end of available text
        let label = after_space.trim().to_string();
        return Some(RawMrSet {
            name,
            mr_type,
            counted_value,
            label,
            var_names: Vec::new(),
        });
    }

    let label = after_space[..label_len].to_string();
    let remainder = &after_space[label_len..];

    // Remaining text is space-separated variable names
    let var_names: Vec<String> = remainder
        .split_whitespace()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    Some(RawMrSet {
        name,
        mr_type,
        counted_value,
        label,
        var_names,
    })
}

/// Parse an ASCII integer from the start of a string.
/// Returns (value, remaining_str).
fn parse_number(s: &str) -> Option<(usize, &str)> {
    let end = s
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(s.len());
    if end == 0 {
        return None;
    }
    let n: usize = s[..end].parse().ok()?;
    Some((n, &s[end..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_dichotomy_set() {
        // Real format: $AD6=D1 1 16 AD6. QC Autofill ad6r1 ad6r2 ad6r3
        let data = b"$AD6=D1 1 16 AD6. QC Autofill ad6r1 ad6r2 ad6r3\n";
        let sets = parse_mr_sets(data);
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].name, "AD6");
        assert_eq!(sets[0].mr_type, MrType::MultipleDichotomy);
        assert_eq!(sets[0].counted_value, Some("1".to_string()));
        assert_eq!(sets[0].label, "AD6. QC Autofill");
        assert_eq!(sets[0].var_names, vec!["ad6r1", "ad6r2", "ad6r3"]);
    }

    #[test]
    fn test_parse_category_set() {
        let data = b"$colors=C 15 Favorite Colors RED GREEN BLUE\n";
        let sets = parse_mr_sets(data);
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].name, "colors");
        assert_eq!(sets[0].mr_type, MrType::MultipleCategory);
        assert_eq!(sets[0].counted_value, None);
        assert_eq!(sets[0].label, "Favorite Colors");
        assert_eq!(sets[0].var_names, vec!["RED", "GREEN", "BLUE"]);
    }

    #[test]
    fn test_parse_multiple_sets() {
        let data = b"$set1=D1 1 9 Label One V1 V2\n$set2=C 9 Label Two V3 V4\n";
        let sets = parse_mr_sets(data);
        assert_eq!(sets.len(), 2);
        assert_eq!(sets[0].name, "set1");
        assert_eq!(sets[0].label, "Label One");
        assert_eq!(sets[0].var_names, vec!["V1", "V2"]);
        assert_eq!(sets[1].name, "set2");
        assert_eq!(sets[1].label, "Label Two");
        assert_eq!(sets[1].var_names, vec!["V3", "V4"]);
    }

    #[test]
    fn test_parse_number() {
        assert_eq!(parse_number("123abc"), Some((123, "abc")));
        assert_eq!(parse_number("1 rest"), Some((1, " rest")));
        assert_eq!(parse_number("abc"), None);
    }

    #[test]
    fn test_parse_multidigit_counted_value() {
        // Counted value "10" has length 2
        let data = b"$test=D2 10 5 Label V1 V2\n";
        let sets = parse_mr_sets(data);
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].counted_value, Some("10".to_string()));
        assert_eq!(sets[0].label, "Label");
        assert_eq!(sets[0].var_names, vec!["V1", "V2"]);
    }
}
