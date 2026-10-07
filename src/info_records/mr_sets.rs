use crate::io_utils::{self, RawText};
use crate::metadata::MrType;

/// Raw multiple response set parsed from subtype 7 or 19.
///
/// All text is kept as the file's bytes and decoded with the file encoding in
/// `resolve_dictionary`. The record is parsed on bytes because its length
/// prefixes count *bytes* in the file's encoding; decoding first (as this
/// parser once did, with UTF-8) breaks those offsets for non-ASCII labels and
/// made the parser panic on a char boundary (GitHub issue #5).
///
/// Subtype 7: variable names are SHORT names — must be resolved to long names.
/// Subtype 19: variable names are already LONG names.
#[derive(Debug, Clone)]
pub struct RawMrSet {
    pub name: RawText,
    pub mr_type: MrType,
    pub counted_value: Option<RawText>,
    pub label: RawText,
    pub var_names: Vec<RawText>,
    /// If true, var_names are already long names (subtype 19).
    pub uses_long_names: bool,
}

/// Parse subtype 7 multiple response sets (SHORT variable names).
///
/// Format: newline-separated set definitions. Each set is one line:
///   $NAME=Dn counted_value label_len label var1 var2 ...\n   (dichotomy)
///   $NAME=C label_len label var1 var2 ...\n                   (category)
///
/// Where n is the byte length of counted_value (can be multi-digit),
/// and label_len is the byte length of the label that follows.
pub fn parse_mr_sets(data: &[u8]) -> Vec<RawMrSet> {
    parse_mr_sets_inner(data, false)
}

/// Parse subtype 19 multiple response sets (LONG variable names).
///
/// Same text format as subtype 7, but variable names are already long names
/// and an additional `E` type (extended dichotomy) may appear:
///   $NAME=E counting_type cv_len counted_value label_len label var1 var2 ...
///
/// counting_type: 1 = CATEGORYLABELS=COUNTEDVALUES, 11 = LABELSOURCE=VARLABEL
pub fn parse_mr_sets_v2(data: &[u8]) -> Vec<RawMrSet> {
    parse_mr_sets_inner(data, true)
}

fn parse_mr_sets_inner(data: &[u8], uses_long_names: bool) -> Vec<RawMrSet> {
    let mut sets = Vec::new();

    // Sets are newline-separated (may also have NUL terminators)
    for line in data.split(|&b| b == b'\n') {
        let line = io_utils::trim_ascii_nul(line);
        if line.is_empty() || line[0] != b'$' {
            continue;
        }
        if let Some(mr_set) = parse_one_mr_set(line, uses_long_names) {
            sets.push(mr_set);
        }
    }

    sets
}

fn strip_one_space(s: &[u8]) -> &[u8] {
    s.strip_prefix(b" ").unwrap_or(s)
}

fn trim_start_ascii(s: &[u8]) -> &[u8] {
    let n = s.iter().take_while(|b| b.is_ascii_whitespace()).count();
    &s[n..]
}

fn parse_one_mr_set(text: &[u8], uses_long_names: bool) -> Option<RawMrSet> {
    // Must start with $
    let text = text.strip_prefix(b"$")?;

    // Find '=' to split name and rest
    let eq_pos = text.iter().position(|&b| b == b'=')?;
    let name = text[..eq_pos].to_vec();
    let rest = &text[eq_pos + 1..];

    if rest.is_empty() {
        return None;
    }

    let type_char = rest[0];
    let rest = &rest[1..];

    let (mr_type, counted_value, after_cv) = match type_char {
        b'D' => {
            // Dichotomy: Dn counted_value ...
            // n is ASCII digits = byte length of counted value
            let (cv_len, after_len) = parse_number(rest)?;
            let after_space = strip_one_space(after_len);
            if after_space.len() < cv_len {
                return None;
            }
            let counted_value = after_space[..cv_len].to_vec();
            let remainder = &after_space[cv_len..];
            (MrType::MultipleDichotomy, Some(counted_value), remainder)
        }
        b'E' => {
            // Extended dichotomy (subtype 19 only):
            //   E counting_type cv_len counted_value ...
            // counting_type: 1 or 11, followed by space
            // Then same as D: cv_len counted_value ...
            let rest = trim_start_ascii(rest);
            let (_, after_ct) = parse_number(rest)?;
            let after_ct = strip_one_space(after_ct);
            let (cv_len, after_len) = parse_number(after_ct)?;
            let after_space = strip_one_space(after_len);
            if after_space.len() < cv_len {
                return None;
            }
            let counted_value = after_space[..cv_len].to_vec();
            let remainder = &after_space[cv_len..];
            (MrType::MultipleDichotomy, Some(counted_value), remainder)
        }
        b'C' => (MrType::MultipleCategory, None, rest),
        _ => return None,
    };

    // Next: skip space(s), then parse label_len and label
    let trimmed = trim_start_ascii(after_cv);

    // Parse label_len
    let (label_len, after_label_len) = parse_number(trimmed)?;

    // Skip one space after label_len
    let after_space = strip_one_space(after_label_len);

    // Read label_len bytes as the label
    if after_space.len() < label_len {
        // Label extends to end of available text
        let label = io_utils::trim_ascii_nul(after_space).to_vec();
        return Some(RawMrSet {
            name,
            mr_type,
            counted_value,
            label,
            var_names: Vec::new(),
            uses_long_names,
        });
    }

    let label = after_space[..label_len].to_vec();
    let remainder = &after_space[label_len..];

    // Remaining text is whitespace-separated variable names
    let var_names: Vec<RawText> = remainder
        .split(|b| b.is_ascii_whitespace())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_vec())
        .collect();

    Some(RawMrSet {
        name,
        mr_type,
        counted_value,
        label,
        var_names,
        uses_long_names,
    })
}

/// Parse an ASCII integer from the start of a byte string.
/// Returns (value, remaining_bytes).
fn parse_number(s: &[u8]) -> Option<(usize, &[u8])> {
    let end = s
        .iter()
        .position(|b| !b.is_ascii_digit())
        .unwrap_or(s.len());
    if end == 0 {
        return None;
    }
    let n: usize = std::str::from_utf8(&s[..end]).ok()?.parse().ok()?;
    Some((n, &s[end..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(v: &[&str]) -> Vec<RawText> {
        v.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    #[test]
    fn test_parse_dichotomy_set() {
        // Real format: $AD6=D1 1 16 AD6. QC Autofill ad6r1 ad6r2 ad6r3
        let data = b"$AD6=D1 1 16 AD6. QC Autofill ad6r1 ad6r2 ad6r3\n";
        let sets = parse_mr_sets(data);
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].name, b"AD6");
        assert_eq!(sets[0].mr_type, MrType::MultipleDichotomy);
        assert_eq!(sets[0].counted_value, Some(b"1".to_vec()));
        assert_eq!(sets[0].label, b"AD6. QC Autofill");
        assert_eq!(sets[0].var_names, names(&["ad6r1", "ad6r2", "ad6r3"]));
    }

    #[test]
    fn test_parse_category_set() {
        let data = b"$colors=C 15 Favorite Colors RED GREEN BLUE\n";
        let sets = parse_mr_sets(data);
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].name, b"colors");
        assert_eq!(sets[0].mr_type, MrType::MultipleCategory);
        assert_eq!(sets[0].counted_value, None);
        assert_eq!(sets[0].label, b"Favorite Colors");
        assert_eq!(sets[0].var_names, names(&["RED", "GREEN", "BLUE"]));
    }

    #[test]
    fn test_parse_multiple_sets() {
        let data = b"$set1=D1 1 9 Label One V1 V2\n$set2=C 9 Label Two V3 V4\n";
        let sets = parse_mr_sets(data);
        assert_eq!(sets.len(), 2);
        assert_eq!(sets[0].name, b"set1");
        assert_eq!(sets[0].label, b"Label One");
        assert_eq!(sets[0].var_names, names(&["V1", "V2"]));
        assert_eq!(sets[1].name, b"set2");
        assert_eq!(sets[1].label, b"Label Two");
        assert_eq!(sets[1].var_names, names(&["V3", "V4"]));
    }

    #[test]
    fn test_parse_number() {
        assert_eq!(parse_number(b"123abc"), Some((123, &b"abc"[..])));
        assert_eq!(parse_number(b"1 rest"), Some((1, &b" rest"[..])));
        assert_eq!(parse_number(b"abc"), None);
    }

    #[test]
    fn test_parse_multidigit_counted_value() {
        // Counted value "10" has length 2
        let data = b"$test=D2 10 5 Label V1 V2\n";
        let sets = parse_mr_sets(data);
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].counted_value, Some(b"10".to_vec()));
        assert_eq!(sets[0].label, b"Label");
        assert_eq!(sets[0].var_names, names(&["V1", "V2"]));
        assert!(!sets[0].uses_long_names);
    }

    #[test]
    fn test_non_ascii_label_is_sliced_by_bytes() {
        // windows-1250 bytes for "Mik(őŐűŰ) ok": 12 bytes. Decoded as UTF-8
        // first (the old behaviour) each accented byte became a 3-byte U+FFFD,
        // so slicing 12 *bytes* cut the label short or landed mid-character.
        let data = b"$Q15M=C 12 Mik(\xf5\xd5\xfb\xdb) ok q15_1 q15_2\n";
        let sets = parse_mr_sets(data);
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].label, b"Mik(\xf5\xd5\xfb\xdb) ok");
        assert_eq!(sets[0].var_names, names(&["q15_1", "q15_2"]));
    }

    #[test]
    fn test_pure_non_ascii_label_and_counted_value() {
        let data = b"$s=D2 \xe9\xe1 4 \xf5\xd5\xfb\xdb v1\n";
        let sets = parse_mr_sets(data);
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].counted_value, Some(b"\xe9\xe1".to_vec()));
        assert_eq!(sets[0].label, b"\xf5\xd5\xfb\xdb");
        assert_eq!(sets[0].var_names, names(&["v1"]));
    }

    // --- Subtype 19 (v2) tests ---

    #[test]
    fn test_parse_v2_dichotomy() {
        // Subtype 19 D type — same format but long variable names
        let data = b"$q7all=D1 1 11 All of Q7's q7_1 q7_2 q7_3\n";
        let sets = parse_mr_sets_v2(data);
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].name, b"q7all");
        assert_eq!(sets[0].mr_type, MrType::MultipleDichotomy);
        assert_eq!(sets[0].counted_value, Some(b"1".to_vec()));
        assert_eq!(sets[0].label, b"All of Q7's");
        assert_eq!(sets[0].var_names, names(&["q7_1", "q7_2", "q7_3"]));
        assert!(sets[0].uses_long_names);
    }

    #[test]
    fn test_parse_v2_extended_dichotomy() {
        // E type: $d=E counting_type cv_len counted_value label_len label vars...
        // counting_type=1 (CATEGORYLABELS=COUNTEDVALUES)
        let data = b"$d=E 1 2 34 13 third mdgroup k l m\n";
        let sets = parse_mr_sets_v2(data);
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].name, b"d");
        assert_eq!(sets[0].mr_type, MrType::MultipleDichotomy);
        assert_eq!(sets[0].counted_value, Some(b"34".to_vec()));
        assert_eq!(sets[0].label, b"third mdgroup");
        assert_eq!(sets[0].var_names, names(&["k", "l", "m"]));
        assert!(sets[0].uses_long_names);
    }

    #[test]
    fn test_parse_v2_extended_labelsource_varlabel() {
        // E type with counting_type=11 (LABELSOURCE=VARLABEL)
        let data = b"$e=E 11 6 choice 0  n o p\n";
        let sets = parse_mr_sets_v2(data);
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].name, b"e");
        assert_eq!(sets[0].mr_type, MrType::MultipleDichotomy);
        assert_eq!(sets[0].counted_value, Some(b"choice".to_vec()));
        assert_eq!(sets[0].label, b"");
        assert_eq!(sets[0].var_names, names(&["n", "o", "p"]));
    }

    #[test]
    fn test_parse_v2_category() {
        let data = b"$colors=C 15 Favorite Colors RED GREEN BLUE\n";
        let sets = parse_mr_sets_v2(data);
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].name, b"colors");
        assert_eq!(sets[0].mr_type, MrType::MultipleCategory);
        assert!(sets[0].uses_long_names);
    }

    #[test]
    fn test_parse_v2_mixed() {
        let data =
            b"$set1=D1 1 5 Label q7_1 q7_2\n$set2=E 1 2 ab 4 Test x y\n$set3=C 4 Cats a b c\n";
        let sets = parse_mr_sets_v2(data);
        assert_eq!(sets.len(), 3);
        assert_eq!(sets[0].name, b"set1");
        assert_eq!(sets[0].mr_type, MrType::MultipleDichotomy);
        assert_eq!(sets[1].name, b"set2");
        assert_eq!(sets[1].mr_type, MrType::MultipleDichotomy);
        assert_eq!(sets[1].counted_value, Some(b"ab".to_vec()));
        assert_eq!(sets[2].name, b"set3");
        assert_eq!(sets[2].mr_type, MrType::MultipleCategory);
    }
}
