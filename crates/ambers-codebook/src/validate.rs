//! Validation.
//!
//! Today this holds the value-label quality checks: the same two checks as
//! Python's `ambers.validate()` (`python/ambers/_validate.py`), as pure
//! functions over a variable's data values and its value labels. The
//! terminal viewer runs them while it loads a file.
//!
//! Validating a `.sav` file against a codebook will also live here: it will
//! compare a file's metadata, and optionally its data, with a codebook and
//! return every difference in one pass as a list of problems with stable
//! codes, serialisable as a versioned report.

use ambers::Value;
use indexmap::IndexMap;

/// Data values of a variable that have no value label.
#[derive(Debug, Clone, PartialEq)]
pub struct Unlabeled {
    /// The values without a label, sorted.
    pub values: Vec<Value>,
    /// Distinct non-missing values in the data.
    pub unique_in_data: usize,
    /// How many of those have a label.
    pub labeled_in_data: usize,
}

/// A label shared by several codes of one variable.
#[derive(Debug, Clone, PartialEq)]
pub struct DuplicateLabel {
    pub label: String,
    /// The codes carrying that label, sorted. Always two or more.
    pub codes: Vec<Value>,
}

/// Check whether the data holds values without a label.
///
/// `data_values` are the variable's distinct values, system-missing
/// excluded; repeats are harmless. Returns `None` when every value has a
/// label. Labels that never occur in the data are not a finding.
pub fn unlabeled_values<'a>(
    data_values: impl IntoIterator<Item = &'a Value>,
    labels: &IndexMap<Value, String>,
) -> Option<Unlabeled> {
    let mut distinct: Vec<&Value> = data_values.into_iter().collect();
    distinct.sort();
    distinct.dedup();

    let values: Vec<Value> = distinct
        .iter()
        .filter(|value| !labels.contains_key(**value))
        .map(|value| (*value).clone())
        .collect();
    if values.is_empty() {
        return None;
    }
    Some(Unlabeled {
        unique_in_data: distinct.len(),
        labeled_in_data: distinct.len() - values.len(),
        values,
    })
}

/// Find labels that several codes share, in the order the labels first
/// appear. Empty when every label belongs to one code.
pub fn duplicate_labels(labels: &IndexMap<Value, String>) -> Vec<DuplicateLabel> {
    let mut by_label: IndexMap<&str, Vec<Value>> = IndexMap::new();
    for (code, label) in labels {
        by_label.entry(label).or_default().push(code.clone());
    }
    by_label
        .into_iter()
        .filter(|(_, codes)| codes.len() > 1)
        .map(|(label, mut codes)| {
            codes.sort();
            DuplicateLabel {
                label: label.to_string(),
                codes,
            }
        })
        .collect()
}

// The cases mirror python/tests/test_validate.py, so both implementations
// give the same answers.
#[cfg(test)]
mod tests {
    use super::*;

    fn num(v: f64) -> Value {
        Value::Numeric(v)
    }

    fn labels(pairs: &[(f64, &str)]) -> IndexMap<Value, String> {
        pairs
            .iter()
            .map(|(code, label)| (num(*code), label.to_string()))
            .collect()
    }

    #[test]
    fn all_labeled_is_no_finding() {
        let data = [num(1.0), num(2.0), num(3.0)];
        let labels = labels(&[(1.0, "A"), (2.0, "B"), (3.0, "C")]);
        assert_eq!(unlabeled_values(&data, &labels), None);
    }

    #[test]
    fn unlabeled_values_are_detected() {
        let data = [num(1.0), num(2.0), num(3.0), num(99.0)];
        let labels = labels(&[(1.0, "A"), (2.0, "B")]);
        let found = unlabeled_values(&data, &labels).unwrap();
        assert_eq!(found.values, [num(3.0), num(99.0)]);
        assert_eq!(found.unique_in_data, 4);
        assert_eq!(found.labeled_in_data, 2);
    }

    #[test]
    fn repeated_values_count_once() {
        let data = [num(9.0), num(1.0), num(9.0), num(1.0)];
        let found = unlabeled_values(&data, &labels(&[(1.0, "A")])).unwrap();
        assert_eq!(found.values, [num(9.0)]);
        assert_eq!(found.unique_in_data, 2);
    }

    #[test]
    fn no_data_is_no_finding() {
        // A column of nulls: the caller passes no values.
        let labels = labels(&[(1.0, "A"), (2.0, "B")]);
        assert_eq!(unlabeled_values(&[], &labels), None);
    }

    #[test]
    fn labels_not_in_data_are_fine() {
        let data = [num(1.0)];
        let labels = labels(&[(1.0, "A"), (2.0, "B"), (3.0, "C")]);
        assert_eq!(unlabeled_values(&data, &labels), None);
    }

    #[test]
    fn string_values_are_checked_the_same_way() {
        let labels: IndexMap<Value, String> =
            [(Value::String("NYC".into()), "New York".to_string())].into();
        let data = [Value::String("NYC".into()), Value::String("LA".into())];
        let found = unlabeled_values(&data, &labels).unwrap();
        assert_eq!(found.values, [Value::String("LA".into())]);
    }

    #[test]
    fn no_duplicates() {
        assert!(duplicate_labels(&labels(&[(1.0, "A"), (2.0, "B")])).is_empty());
    }

    #[test]
    fn duplicate_is_detected() {
        let found = duplicate_labels(&labels(&[(1.0, "Yes"), (2.0, "No"), (3.0, "Yes")]));
        assert_eq!(
            found,
            [DuplicateLabel {
                label: "Yes".into(),
                codes: vec![num(1.0), num(3.0)],
            }]
        );
    }

    #[test]
    fn multiple_duplicates_keep_label_order() {
        let found = duplicate_labels(&labels(&[
            (1.0, "A"),
            (2.0, "B"),
            (3.0, "A"),
            (4.0, "B"),
            (5.0, "C"),
        ]));
        let names: Vec<&str> = found.iter().map(|d| d.label.as_str()).collect();
        assert_eq!(names, ["A", "B"]);
        assert_eq!(found[1].codes, [num(2.0), num(4.0)]);
    }
}
