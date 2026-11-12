//! Variable types for a codebook: `text`, `date`, `numeric`, `categorical`.
//!
//! A port of Python's `ambers.codebook._types` (itself from ultrasav),
//! kept step for step so both give the same answer for every variable.
//! Internally a categorical variable is single-select or multi-select; the
//! codebook shows both as `categorical`.
//!
//! The type comes from the column's data type, refined by the metadata
//! (value labels, MR sets, sibling variables) and by one fact about the
//! data: whether an unlabelled or 0/1-labelled numeric column holds only
//! zeros and ones.

use std::collections::{HashMap, HashSet};

use ambers::{SpssMetadata, Value};
use arrow::datatypes::DataType;
use indexmap::IndexMap;

/// Label pairs (for 0 and 1) that mark a multi-select item.
const SELECTION_PAIRS: [(&str, &str); 7] = [
    ("not selected", "selected"),
    ("unchecked", "checked"),
    ("no", "yes"),
    ("0", "1"),
    ("not mentioned", "mentioned"),
    ("not chosen", "chosen"),
    ("exclude", "include"),
];

/// Label pairs of an ordinary two-way question, not a multi-select item.
const GENERIC_BINARY_LABELS: [(&str, &str); 6] = [
    ("no", "yes"),
    ("false", "true"),
    ("disagree", "agree"),
    ("male", "female"),
    ("off", "on"),
    ("absent", "present"),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VariableType {
    Text,
    Date,
    /// A number with no value labels: a measurement or a count.
    Numeric,
    /// Coded answers to one question.
    SingleSelect,
    /// One 0/1 item of a multiple-response question.
    MultiSelect,
}

impl VariableType {
    /// The name shown in a codebook: both select types are `categorical`.
    pub fn name(self) -> &'static str {
        match self {
            VariableType::Text => "text",
            VariableType::Date => "date",
            VariableType::Numeric => "numeric",
            VariableType::SingleSelect | VariableType::MultiSelect => "categorical",
        }
    }

    pub fn is_categorical(self) -> bool {
        matches!(self, VariableType::SingleSelect | VariableType::MultiSelect)
    }
}

/// What the data of a numeric column says about being a 0/1 indicator.
/// A column with no values at all, or with any other value, has no pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Binary {
    /// Only zeros and ones, and both occur.
    Strong,
    /// Only zeros, or only ones.
    Weak,
}

/// Classifies variables. Built once per file from the metadata.
#[derive(Debug, Clone)]
pub struct TypeDetector {
    /// Every variable that belongs to an MR set.
    mr_variables: HashSet<String>,
    /// Variables sharing a name stem (`Q4A`, `Q4B`, `Q4C`), each mapped to
    /// the others of its group, in file order.
    siblings: HashMap<String, Vec<String>>,
}

impl TypeDetector {
    pub fn new(meta: &SpssMetadata) -> TypeDetector {
        let mr_variables = (meta.mr_sets.values())
            .flat_map(|set| set.variables.iter().cloned())
            .collect();
        TypeDetector {
            mr_variables,
            siblings: sibling_map(&meta.variable_names),
        }
    }

    /// Whether `detect` needs the data's 0/1 pattern for this numeric
    /// variable. It does not for MR-set members, nor for variables whose
    /// labels already rule a 0/1 indicator out.
    pub fn wants_binary_pattern(&self, meta: &SpssMetadata, name: &str) -> bool {
        if self.mr_variables.contains(name) {
            return false;
        }
        match meta.variable_value_labels.get(name) {
            Some(labels) if !labels.is_empty() => labels.len() <= 2 && keys_are_01(labels),
            _ => true,
        }
    }

    /// The type of one variable. `binary` is the data's 0/1 pattern; it is
    /// only consulted where `wants_binary_pattern` is true.
    pub fn detect(
        &self,
        meta: &SpssMetadata,
        name: &str,
        data_type: &DataType,
        binary: Option<Binary>,
    ) -> VariableType {
        match data_type {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => VariableType::Text,
            DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(..)
            | DataType::Duration(_) => VariableType::Date,
            _ => self.detect_numeric(meta, name, binary),
        }
    }

    fn detect_numeric(
        &self,
        meta: &SpssMetadata,
        name: &str,
        binary: Option<Binary>,
    ) -> VariableType {
        let all_labels = &meta.variable_value_labels;
        let labels = all_labels.get(name);

        // Tier 1: SPSS says so.
        if self.mr_variables.contains(name) {
            return VariableType::MultiSelect;
        }

        // Tier 2: the data is 0/1, and something confirms it is an indicator.
        let binary = binary.filter(|_| self.wants_binary_pattern(meta, name));
        if let Some(pattern) = binary {
            let metadata_confirms = labels.is_some_and(keys_are_01);
            let series_confirms = !metadata_confirms && {
                let siblings = self.siblings_of(name);
                siblings.len() >= 2
                    && (siblings.iter().take(5))
                        .filter(|sibling| all_labels.get(*sibling).is_some_and(keys_are_01))
                        .count()
                        >= 2
            };
            let confirmed = metadata_confirms || series_confirms;
            let gated_ok = match pattern {
                // Both values present: no labels at all is good enough.
                Binary::Strong => confirmed || labels.is_none(),
                Binary::Weak => confirmed,
            };
            if gated_ok {
                // An ordinary yes/no question is left to the label tiers.
                let generic = labels
                    .filter(|labels| is_binary_value_dict(labels))
                    .is_some_and(|labels| {
                        let (label_0, label_1) = labels_lower_pair(labels);
                        is_generic_binary(&label_0, &label_1)
                    });
                if !generic {
                    return VariableType::MultiSelect;
                }
            }
        }

        // Tiers 3-7: what the value labels say.
        let Some(labels) = labels.filter(|labels| !labels.is_empty()) else {
            return VariableType::Numeric;
        };
        if is_binary_value_dict(labels) {
            let (label_0, label_1) = labels_lower_pair(labels);

            // Tier 3: nothing on 0, a description on 1.
            if matches!(label_0.as_str(), "" | "null" | "none" | "not selected")
                && !matches!(label_1.as_str(), "" | "yes" | "selected" | "true" | "1")
            {
                return VariableType::MultiSelect;
            }

            // Tier 4: a selected / not selected pair, on a name like `Q5_1`.
            let is_selection_pair = SELECTION_PAIRS
                .iter()
                .any(|pair| same_pair(&label_0, &label_1, pair));
            if is_selection_pair && matches_multi_name_pattern(name) {
                return VariableType::MultiSelect;
            }

            // Tier 5: its siblings are 0/1 items too. A sibling without
            // labels does not count against the series.
            let siblings = self.siblings_of(name);
            if siblings.len() >= 2
                && (siblings.iter().take(3))
                    .filter_map(|sibling| all_labels.get(sibling))
                    .all(is_binary_value_dict)
            {
                return VariableType::MultiSelect;
            }
        }
        // Tier 6 (generic yes/no) and tier 7 (any other labels).
        VariableType::SingleSelect
    }

    fn siblings_of(&self, name: &str) -> &[String] {
        self.siblings.get(name).map_or(&[], Vec::as_slice)
    }
}

/// Whether a label key is 0 or 1, as a number or as the text `"0"` / `"1"`.
fn is_01(key: &Value) -> bool {
    match key {
        Value::Numeric(number) => *number == 0.0 || *number == 1.0,
        Value::String(text) => text == "0" || text == "1",
    }
}

/// Every label key is 0 or 1. True for an empty map, as in Python, where
/// an empty set is a subset of `{0, 1}`.
fn keys_are_01(labels: &IndexMap<Value, String>) -> bool {
    labels.keys().all(is_01)
}

/// Exactly two labels, keyed 0 and 1.
fn is_binary_value_dict(labels: &IndexMap<Value, String>) -> bool {
    labels.len() == 2 && keys_are_01(labels)
}

/// The labels of 0 and 1, lower-cased and trimmed; empty when absent.
fn labels_lower_pair(labels: &IndexMap<Value, String>) -> (String, String) {
    let label_of = |number: f64, text: &str| {
        labels
            .get(&Value::Numeric(number))
            .or_else(|| labels.get(&Value::String(text.to_string())))
            .map_or(String::new(), |label| {
                label.to_lowercase().trim().to_string()
            })
    };
    (label_of(0.0, "0"), label_of(1.0, "1"))
}

/// The two labels are exactly the two words of `pair`, in either order.
fn same_pair(label_0: &str, label_1: &str, pair: &(&str, &str)) -> bool {
    (label_0 == pair.0 && label_1 == pair.1) || (label_0 == pair.1 && label_1 == pair.0)
}

fn is_generic_binary(label_0: &str, label_1: &str) -> bool {
    GENERIC_BINARY_LABELS
        .iter()
        .any(|pair| same_pair(label_0, label_1, pair))
}

/// Whether a name looks like one item of a multi-select question.
///
/// Python searches five patterns, ignoring case. Three of them only ask
/// for a trailing digit, so together they reduce to the tests below:
///
/// - `[_\-]?\d+$`, `r\d+$`, `[A-Z]\d+[A-Z]\d+$`: the name ends in a digit
/// - `_[A-Z]$`: it ends in an underscore and a letter
/// - `Q\d+[A-Z]$`: it ends in `q`, digits, and a letter
///
/// Digits and letters are ASCII here; Python's `\d` also accepts other
/// scripts' digits, which SPSS variable names do not use in practice.
fn matches_multi_name_pattern(name: &str) -> bool {
    let chars: Vec<char> = name.chars().collect();
    let Some((&last, before)) = chars.split_last() else {
        return false;
    };
    if last.is_ascii_digit() {
        return true;
    }
    if !last.is_ascii_alphabetic() {
        return false;
    }
    if before.last() == Some(&'_') {
        return true;
    }
    let digits = before
        .iter()
        .rev()
        .take_while(|c| c.is_ascii_digit())
        .count();
    digits > 0
        && before.len() > digits
        && before[before.len() - digits - 1].eq_ignore_ascii_case(&'q')
}

/// Group variables by name stem and map each to its group's other members.
///
/// The stem is what Python's `(.+?)([A-Z]|\d+)$` captures first: the name
/// without its final letter, or without its trailing run of digits. A stem
/// is never empty, so an all-digit name keeps its first digit.
fn sibling_map(variable_names: &[String]) -> HashMap<String, Vec<String>> {
    let mut groups: IndexMap<String, Vec<&String>> = IndexMap::new();
    for name in variable_names {
        if let Some(stem) = name_stem(name) {
            groups.entry(stem).or_default().push(name);
        }
    }
    let mut siblings = HashMap::new();
    for group in groups.values().filter(|group| group.len() >= 2) {
        for name in group {
            let others = group.iter().filter(|other| other != &name);
            siblings.insert(
                (*name).clone(),
                others.map(|other| (*other).clone()).collect(),
            );
        }
    }
    siblings
}

fn name_stem(name: &str) -> Option<String> {
    let chars: Vec<char> = name.chars().collect();
    let last = *chars.last()?;
    let stem_len = if last.is_ascii_digit() {
        let digits = chars
            .iter()
            .rev()
            .take_while(|c| c.is_ascii_digit())
            .count();
        (chars.len() - digits).max(1)
    } else if last.is_ascii_alphabetic() {
        chars.len() - 1
    } else {
        return None;
    };
    // The suffix must leave something for the stem and for itself.
    (stem_len >= 1 && stem_len < chars.len()).then(|| chars[..stem_len].iter().collect())
}

// The cases mirror python/tests/test_codebook_types.py.
#[cfg(test)]
mod tests {
    use super::*;
    use ambers::{MrSet, MrType};

    const NUMBER: DataType = DataType::Float64;

    /// Metadata for `names`, with value labels and MR sets as given.
    fn metadata(
        names: &[&str],
        labels: &[(&str, &[(f64, &str)])],
        mr_sets: &[(&str, &[&str])],
    ) -> SpssMetadata {
        let mut meta = SpssMetadata {
            variable_names: names.iter().map(|name| name.to_string()).collect(),
            ..SpssMetadata::default()
        };
        for (name, pairs) in labels {
            let map = (pairs.iter())
                .map(|(code, label)| (Value::Numeric(*code), label.to_string()))
                .collect();
            meta.variable_value_labels.insert(name.to_string(), map);
        }
        for (set, variables) in mr_sets {
            meta.mr_sets.insert(
                set.to_string(),
                MrSet {
                    name: set.to_string(),
                    label: String::new(),
                    mr_type: MrType::MultipleDichotomy,
                    counted_value: Some("1".to_string()),
                    variables: variables.iter().map(|v| v.to_string()).collect(),
                },
            );
        }
        meta
    }

    fn detect(meta: &SpssMetadata, name: &str, binary: Option<Binary>) -> VariableType {
        TypeDetector::new(meta).detect(meta, name, &NUMBER, binary)
    }

    #[test]
    fn data_types_decide_text_and_date() {
        let meta = metadata(&["x"], &[("x", &[(1.0, "A")])], &[]);
        let detector = TypeDetector::new(&meta);
        // Value labels do not make a string or a date categorical.
        assert_eq!(
            detector.detect(&meta, "x", &DataType::Utf8View, None),
            VariableType::Text
        );
        assert_eq!(
            detector.detect(&meta, "x", &DataType::Date32, None),
            VariableType::Date
        );
        let timestamp = DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None);
        assert_eq!(
            detector.detect(&meta, "x", &timestamp, None),
            VariableType::Date
        );
        let duration = DataType::Duration(arrow::datatypes::TimeUnit::Microsecond);
        assert_eq!(
            detector.detect(&meta, "x", &duration, None),
            VariableType::Date
        );
    }

    #[test]
    fn numbers_with_labels_are_single_select() {
        let meta = metadata(
            &["Q1"],
            &[("Q1", &[(1.0, "A"), (2.0, "B"), (3.0, "C")])],
            &[],
        );
        assert_eq!(detect(&meta, "Q1", None), VariableType::SingleSelect);
        assert_eq!(VariableType::SingleSelect.name(), "categorical");
    }

    #[test]
    fn numbers_without_labels_are_numeric() {
        let meta = metadata(&["age"], &[], &[]);
        assert_eq!(detect(&meta, "age", None), VariableType::Numeric);
    }

    #[test]
    fn generic_binary_labels_are_single_select() {
        for (zero, one) in [("No", "Yes"), ("Male", "Female")] {
            let meta = metadata(&["gender"], &[("gender", &[(0.0, zero), (1.0, one)])], &[]);
            assert_eq!(
                detect(&meta, "gender", Some(Binary::Strong)),
                VariableType::SingleSelect
            );
        }
    }

    #[test]
    fn tier_1_mr_set_members_are_multi_select() {
        let meta = metadata(
            &["Q1_1", "Q1_2"],
            &[
                ("Q1_1", &[(1.0, "Selected")]),
                ("Q1_2", &[(1.0, "Selected")]),
            ],
            &[("Q1", &["Q1_1", "Q1_2"])],
        );
        assert_eq!(
            detect(&meta, "Q1_1", Some(Binary::Strong)),
            VariableType::MultiSelect
        );
        assert_eq!(detect(&meta, "Q1_2", None), VariableType::MultiSelect);
        assert!(!TypeDetector::new(&meta).wants_binary_pattern(&meta, "Q1_1"));
    }

    #[test]
    fn tier_2_binary_data_with_01_labels() {
        let meta = metadata(
            &["Q1_1"],
            &[("Q1_1", &[(0.0, "Not selected"), (1.0, "Selected")])],
            &[],
        );
        assert_eq!(
            detect(&meta, "Q1_1", Some(Binary::Strong)),
            VariableType::MultiSelect
        );
    }

    #[test]
    fn tier_2_unlabelled_zeros_and_ones_are_multi_select() {
        let meta = metadata(&["Q1_1"], &[], &[]);
        assert_eq!(
            detect(&meta, "Q1_1", Some(Binary::Strong)),
            VariableType::MultiSelect
        );
    }

    #[test]
    fn tier_2_one_value_alone_is_not_enough() {
        let meta = metadata(&["Q1_1"], &[], &[]);
        assert_eq!(
            detect(&meta, "Q1_1", Some(Binary::Weak)),
            VariableType::Numeric
        );
    }

    #[test]
    fn tier_2_one_value_with_labelled_siblings() {
        // Q7_3 has no labels and only ones; three siblings are 0/1 items.
        let item: &[(f64, &str)] = &[(0.0, "No"), (1.0, "Yes")];
        let meta = metadata(
            &["Q7_1", "Q7_2", "Q7_3", "Q7_4"],
            &[("Q7_1", item), ("Q7_2", item), ("Q7_4", item)],
            &[],
        );
        assert_eq!(
            detect(&meta, "Q7_3", Some(Binary::Weak)),
            VariableType::MultiSelect
        );
    }

    #[test]
    fn sibling_series_makes_yes_no_items_multi_select() {
        // Q4A has siblings Q4B and Q4C, all labelled No / Yes.
        let item: &[(f64, &str)] = &[(0.0, "No"), (1.0, "Yes")];
        let meta = metadata(
            &["Q4A", "Q4B", "Q4C"],
            &[("Q4A", item), ("Q4B", item), ("Q4C", item)],
            &[],
        );
        assert_eq!(
            detect(&meta, "Q4A", Some(Binary::Strong)),
            VariableType::MultiSelect
        );

        let item: &[(f64, &str)] = &[(0.0, "Off"), (1.0, "On")];
        let meta = metadata(
            &["itemA", "itemB", "itemC"],
            &[("itemA", item), ("itemB", item), ("itemC", item)],
            &[],
        );
        assert_eq!(
            detect(&meta, "itemA", Some(Binary::Strong)),
            VariableType::MultiSelect
        );
    }

    #[test]
    fn descriptive_label_on_one_is_multi_select() {
        let meta = metadata(
            &["Q1_health"],
            &[(
                "Q1_health",
                &[(0.0, "Not selected"), (1.0, "Health insurance")],
            )],
            &[],
        );
        assert_eq!(
            detect(&meta, "Q1_health", Some(Binary::Strong)),
            VariableType::MultiSelect
        );
        // Without data to go on (an empty column), tier 3 decides.
        assert_eq!(detect(&meta, "Q1_health", None), VariableType::MultiSelect);
    }

    #[test]
    fn selection_pair_is_multi_select_with_or_without_the_name_pattern() {
        for name in ["Q5_1", "response"] {
            let meta = metadata(
                &[name],
                &[(name, &[(0.0, "Not selected"), (1.0, "Selected")])],
                &[],
            );
            assert_eq!(
                detect(&meta, name, Some(Binary::Strong)),
                VariableType::MultiSelect
            );
        }
        // With no data, the name pattern is what tier 4 needs.
        let numbered = metadata(&["Q5_1"], &[("Q5_1", &[(0.0, "No"), (1.0, "Yes")])], &[]);
        assert_eq!(detect(&numbered, "Q5_1", None), VariableType::MultiSelect);
        let plain = metadata(
            &["response"],
            &[("response", &[(0.0, "No"), (1.0, "Yes")])],
            &[],
        );
        assert_eq!(detect(&plain, "response", None), VariableType::SingleSelect);
    }

    #[test]
    fn labels_with_no_data_are_single_select() {
        // An empty or all-null column: no binary pattern.
        let meta = metadata(&["Q1"], &[("Q1", &[(1.0, "Yes"), (2.0, "No")])], &[]);
        assert_eq!(detect(&meta, "Q1", None), VariableType::SingleSelect);
        let one_label = metadata(&["Q1"], &[("Q1", &[(1.0, "Yes")])], &[]);
        assert_eq!(detect(&one_label, "Q1", None), VariableType::SingleSelect);
    }

    #[test]
    fn the_binary_pattern_is_only_wanted_where_it_can_matter() {
        let meta = metadata(
            &["plain", "item", "scale"],
            &[
                ("item", &[(0.0, "No"), (1.0, "Yes")]),
                ("scale", &[(1.0, "Low"), (2.0, "Mid"), (3.0, "High")]),
            ],
            &[],
        );
        let detector = TypeDetector::new(&meta);
        assert!(detector.wants_binary_pattern(&meta, "plain"));
        assert!(detector.wants_binary_pattern(&meta, "item"));
        assert!(!detector.wants_binary_pattern(&meta, "scale"));
        // A pattern passed for `scale` anyway is ignored.
        assert_eq!(
            detector.detect(&meta, "scale", &NUMBER, Some(Binary::Strong)),
            VariableType::SingleSelect
        );
    }

    #[test]
    fn name_patterns() {
        for name in [
            "Q1_1", "Q1-2", "q12", "r1", "A1B1", "Q1A", "q10b", "item_A", "x_b",
        ] {
            assert!(matches_multi_name_pattern(name), "{name}");
        }
        for name in ["response", "Q1AB", "gender", "", "A1x", "_"] {
            assert!(!matches_multi_name_pattern(name), "{name}");
        }
    }

    #[test]
    fn name_stems() {
        assert_eq!(name_stem("Q4A").as_deref(), Some("Q4"));
        assert_eq!(name_stem("Q1_12").as_deref(), Some("Q1_"));
        assert_eq!(name_stem("a1b2").as_deref(), Some("a1b"));
        assert_eq!(name_stem("itemA").as_deref(), Some("item"));
        // The stem keeps at least one character.
        assert_eq!(name_stem("123").as_deref(), Some("1"));
        assert_eq!(name_stem("ab").as_deref(), Some("a"));
        assert_eq!(name_stem("a"), None);
        assert_eq!(name_stem("7"), None);
        assert_eq!(name_stem("total_"), None);
        assert_eq!(name_stem(""), None);
    }

    #[test]
    fn siblings_share_a_stem_and_keep_file_order() {
        let names: Vec<String> = ["Q4A", "age", "Q4B", "Q4C", "Q5_1"]
            .iter()
            .map(|name| name.to_string())
            .collect();
        let map = sibling_map(&names);
        assert_eq!(map["Q4A"], ["Q4B", "Q4C"]);
        assert_eq!(map["Q4C"], ["Q4A", "Q4B"]);
        // `age` shares the stem `ag` with nothing; Q5_1 is alone in its group.
        assert!(!map.contains_key("age"));
        assert!(!map.contains_key("Q5_1"));
    }
}
