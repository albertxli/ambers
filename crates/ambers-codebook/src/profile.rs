//! Counting for a codebook: how many valid and missing cases each variable
//! has, and how often each value of a categorical variable occurs.
//!
//! A port of Python's `ambers.codebook` (`_engine.py` and the computed
//! columns in `__init__.py`), giving the same numbers. Where Python runs
//! Polars expressions over a whole DataFrame, the `Profiler` keeps running
//! tallies and is fed one Arrow batch at a time, so it works on a file as
//! it streams in and on files larger than memory.

use std::collections::HashMap;
use std::sync::Arc;

use ambers::{SpssMetadata, Value};
use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Float64Type, Schema};
use arrow::record_batch::RecordBatch;

use crate::types::{Binary, TypeDetector, VariableType};

/// The label of the row that counts missing cases.
pub const MISSING_LABEL: &str = "MISSING";

/// Distinct values counted per variable. Variables with value labels have
/// few; past this the counts of further new values are dropped and the
/// variable is reported as overflowed.
const DISTINCT_CAP: usize = 100_000;

/// One row of the codebook's variables view.
#[derive(Debug, Clone, PartialEq)]
pub struct VariableSummary {
    pub name: String,
    pub label: String,
    pub variable_type: VariableType,
    /// The labelled codes, in code order. Empty unless categorical.
    pub values: Vec<(Value, String)>,
    pub n_valid: u64,
    pub n_missing: u64,
    pub n_total: u64,
    /// Categorical: distinct codes in the labels and the data together.
    /// Otherwise equal to `n_valid`, as in Python.
    pub n_unique: u64,
    /// Codes that have a label. `None` unless categorical.
    pub n_labeled: Option<u64>,
    /// Codes in the data without a label. `None` unless categorical.
    pub n_unlabeled: Option<u64>,
}

/// One row of the codebook's values view.
#[derive(Debug, Clone, PartialEq)]
pub struct ValueRow {
    /// `None` on the `MISSING` row and on the summary row of a variable
    /// that is not categorical.
    pub code: Option<Value>,
    /// `MISSING_LABEL` on the missing row; `None` for a code without a
    /// label and on a summary row.
    pub label: Option<String>,
    pub n: u64,
    /// The variable's valid cases. On the `MISSING` row Python reports the
    /// row's own count here; that is kept.
    pub n_valid: u64,
    /// `n` as a share of the valid cases; `None` on the `MISSING` row and
    /// when there are no valid cases.
    pub pct_valid: Option<f64>,
    pub n_total: u64,
    pub pct_total: Option<f64>,
}

impl ValueRow {
    pub fn is_missing_row(&self) -> bool {
        self.code.is_none() && self.label.as_deref() == Some(MISSING_LABEL)
    }
}

/// What is counted for a column, by what it can turn out to be.
#[derive(Clone)]
enum Counts {
    /// Dates and times: nulls only.
    None,
    /// Strings: an empty string is a missing case.
    Text { empty: u64 },
    /// Numbers with value labels or in an MR set: every distinct value.
    Values {
        by_value: HashMap<u64, u64>,
        overflowed: bool,
    },
    /// Numbers without labels: only whether they are all 0 or 1, which
    /// decides between `categorical` and `numeric`.
    ZeroOne { zeros: u64, ones: u64, other: bool },
}

#[derive(Clone)]
struct Tally {
    nulls: u64,
    counts: Counts,
}

/// Cloning is cheap: the metadata and the rules are shared, only the
/// tallies are copied. A reader can so be handed a snapshot while another
/// thread keeps counting.
#[derive(Clone)]
pub struct Profiler {
    meta: Arc<SpssMetadata>,
    fields: Arc<[(String, DataType)]>,
    detector: Arc<TypeDetector>,
    tallies: Vec<Tally>,
    rows: u64,
}

impl Profiler {
    pub fn new(meta: &SpssMetadata, schema: &Schema) -> Profiler {
        let detector = TypeDetector::new(meta);
        let fields: Vec<(String, DataType)> = (schema.fields().iter())
            .map(|field| (field.name().clone(), field.data_type().clone()))
            .collect();
        let tallies = (fields.iter())
            .map(|(name, data_type)| {
                let counts = match data_type {
                    DataType::Float64 => {
                        let labelled = (meta.variable_value_labels.get(name))
                            .is_some_and(|labels| !labels.is_empty());
                        let in_mr_set = (meta.mr_sets.values())
                            .any(|set| set.variables.iter().any(|member| member == name));
                        if labelled || in_mr_set {
                            Counts::Values {
                                by_value: HashMap::new(),
                                overflowed: false,
                            }
                        } else {
                            Counts::ZeroOne {
                                zeros: 0,
                                ones: 0,
                                other: false,
                            }
                        }
                    }
                    DataType::Utf8View => Counts::Text { empty: 0 },
                    _ => Counts::None,
                };
                Tally { nulls: 0, counts }
            })
            .collect();
        Profiler {
            meta: Arc::new(meta.clone()),
            fields: fields.into(),
            detector: Arc::new(detector),
            tallies,
            rows: 0,
        }
    }

    /// Add a batch of rows. Its columns must be the schema's, in order.
    pub fn update(&mut self, batch: &RecordBatch) {
        self.rows += batch.num_rows() as u64;
        for (tally, column) in self.tallies.iter_mut().zip(batch.columns()) {
            tally.nulls += column.null_count() as u64;
            match &mut tally.counts {
                Counts::None => {}
                Counts::Text { empty } => {
                    let strings = column.as_string_view();
                    *empty += strings.iter().flatten().filter(|s| s.is_empty()).count() as u64;
                }
                Counts::Values {
                    by_value,
                    overflowed,
                } => {
                    let numbers = column.as_primitive::<Float64Type>();
                    // Survey columns repeat values in runs: add a whole run at once.
                    let mut run: Option<(u64, u64)> = None;
                    let mut add = |bits: u64, n: u64| {
                        if let Some(count) = by_value.get_mut(&bits) {
                            *count += n;
                        } else if by_value.len() < DISTINCT_CAP {
                            by_value.insert(bits, n);
                        } else {
                            *overflowed = true;
                        }
                    };
                    for number in numbers.iter().flatten() {
                        let bits = value_bits(number);
                        match &mut run {
                            Some((run_bits, n)) if *run_bits == bits => *n += 1,
                            _ => {
                                if let Some((run_bits, n)) = run.replace((bits, 1)) {
                                    add(run_bits, n);
                                }
                            }
                        }
                    }
                    if let Some((run_bits, n)) = run {
                        add(run_bits, n);
                    }
                }
                Counts::ZeroOne { zeros, ones, other } => {
                    if *other {
                        continue;
                    }
                    for number in column.as_primitive::<Float64Type>().iter().flatten() {
                        if number == 0.0 {
                            *zeros += 1;
                        } else if number == 1.0 {
                            *ones += 1;
                        } else {
                            // Not an indicator: nothing more to learn.
                            *other = true;
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Rows added so far.
    pub fn rows(&self) -> u64 {
        self.rows
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// The type of a variable, from the rows added so far. A column of
    /// zeros and ones can stop being `categorical` when another value
    /// arrives.
    pub fn variable_type(&self, index: usize) -> VariableType {
        let (name, data_type) = &self.fields[index];
        self.detector
            .detect(&self.meta, name, data_type, self.binary_pattern(index))
    }

    /// Whether a variable had more distinct values than are counted; its
    /// value counts are then incomplete.
    pub fn overflowed(&self, index: usize) -> bool {
        matches!(
            self.tallies[index].counts,
            Counts::Values {
                overflowed: true,
                ..
            }
        )
    }

    /// Python: every non-null value is 0 or 1, and there is at least one.
    fn binary_pattern(&self, index: usize) -> Option<Binary> {
        let (has_zero, has_one) = match &self.tallies[index].counts {
            Counts::ZeroOne { other: true, .. } => return None,
            Counts::ZeroOne { zeros, ones, .. } => (*zeros > 0, *ones > 0),
            Counts::Values {
                by_value,
                overflowed,
            } => {
                let zero = by_value.contains_key(&value_bits(0.0));
                let one = by_value.contains_key(&value_bits(1.0));
                if *overflowed || by_value.len() != usize::from(zero) + usize::from(one) {
                    return None;
                }
                (zero, one)
            }
            _ => return None,
        };
        match (has_zero, has_one) {
            (true, true) => Some(Binary::Strong),
            (false, false) => None,
            _ => Some(Binary::Weak),
        }
    }

    /// Missing cases: nulls, and for strings also empty strings.
    fn missing(&self, index: usize) -> u64 {
        let tally = &self.tallies[index];
        match tally.counts {
            Counts::Text { empty } => tally.nulls + empty,
            _ => tally.nulls,
        }
    }

    /// The codes of a categorical variable with their label and count: the
    /// labels and the data merged, in code order. A labelled code that
    /// never occurs has a count of 0; a code without a label has `None`.
    fn merged_codes(&self, index: usize) -> Vec<(Value, Option<String>, u64)> {
        let labels = self.meta.variable_value_labels.get(&self.fields[index].0);
        let mut counts: Vec<(Value, u64)> = match &self.tallies[index].counts {
            Counts::Values { by_value, .. } => (by_value.iter())
                .map(|(bits, n)| (Value::Numeric(f64::from_bits(*bits)), *n))
                .collect(),
            Counts::ZeroOne { zeros, ones, .. } => [(0.0, *zeros), (1.0, *ones)]
                .into_iter()
                .filter(|(_, n)| *n > 0)
                .map(|(code, n)| (Value::Numeric(code), n))
                .collect(),
            _ => Vec::new(),
        };
        for code in labels.into_iter().flat_map(|labels| labels.keys()) {
            if !counts.iter().any(|(counted, _)| counted == code) {
                counts.push((code.clone(), 0));
            }
        }
        counts.sort_by(|a, b| a.0.cmp(&b.0));
        counts
            .into_iter()
            .map(|(code, n)| {
                let label = labels.and_then(|labels| labels.get(&code)).cloned();
                (code, label, n)
            })
            .collect()
    }

    /// The variables-view row of one variable.
    pub fn summary(&self, index: usize) -> VariableSummary {
        let name = &self.fields[index].0;
        let variable_type = self.variable_type(index);
        let n_missing = self.missing(index);
        let n_valid = self.rows - n_missing;
        let mut summary = VariableSummary {
            name: name.clone(),
            label: self.meta.label(name).unwrap_or("").to_string(),
            variable_type,
            values: Vec::new(),
            n_valid,
            n_missing,
            n_total: self.rows,
            n_unique: n_valid,
            n_labeled: None,
            n_unlabeled: None,
        };
        if variable_type.is_categorical() {
            let codes = self.merged_codes(index);
            let labelled = codes.iter().filter(|(_, label, _)| label.is_some()).count() as u64;
            summary.n_unique = codes.len() as u64;
            summary.n_labeled = Some(labelled);
            summary.n_unlabeled = Some(codes.len() as u64 - labelled);
            summary.values = codes
                .into_iter()
                .filter_map(|(code, label, _)| label.map(|label| (code, label)))
                .collect();
        }
        summary
    }

    pub fn summaries(&self) -> Vec<VariableSummary> {
        (0..self.len()).map(|index| self.summary(index)).collect()
    }

    /// The values-view rows of one variable: the `MISSING` row when there
    /// are missing cases, then a row per code for a categorical variable,
    /// or one summary row otherwise.
    pub fn values(&self, index: usize) -> Vec<ValueRow> {
        let n_missing = self.missing(index);
        let n_total = self.rows;
        let n_valid = n_total - n_missing;
        let share = |n: u64, of: u64| (of > 0).then(|| n as f64 / of as f64);

        let mut rows = Vec::new();
        if n_missing > 0 {
            rows.push(ValueRow {
                code: None,
                label: Some(MISSING_LABEL.to_string()),
                n: n_missing,
                n_valid: n_missing,
                pct_valid: None,
                n_total,
                pct_total: share(n_missing, n_total),
            });
        }
        let mut push = |code: Option<Value>, label: Option<String>, n: u64| {
            rows.push(ValueRow {
                code,
                label,
                n,
                n_valid,
                pct_valid: share(n, n_valid),
                n_total,
                pct_total: share(n, n_total),
            });
        };
        if self.variable_type(index).is_categorical() {
            for (code, label, n) in self.merged_codes(index) {
                push(Some(code), label, n);
            }
        } else {
            push(None, None, n_valid);
        }
        rows
    }
}

/// The bits a value is counted under: `-0.0` counts as `0.0`, as it
/// compares equal in Python.
fn value_bits(number: f64) -> u64 {
    if number == 0.0 {
        0.0f64.to_bits()
    } else {
        number.to_bits()
    }
}

// The cases mirror python/tests/test_codebook_engine.py; the comparison
// with Python's full output is in tests/parity.rs.
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float64Array, StringViewArray};

    fn num(code: f64) -> Value {
        Value::Numeric(code)
    }

    /// A profiler over one numeric column `x` with the given labels, fed
    /// `batches` one after another.
    fn profile(labels: &[(f64, &str)], batches: &[&[Option<f64>]]) -> Profiler {
        let mut meta = SpssMetadata {
            variable_names: vec!["x".to_string()],
            ..SpssMetadata::default()
        };
        if !labels.is_empty() {
            let map = (labels.iter())
                .map(|(code, label)| (num(*code), label.to_string()))
                .collect();
            meta.variable_value_labels.insert("x".to_string(), map);
        }
        let mut profiler = None;
        for values in batches {
            let column: ArrayRef = Arc::new(Float64Array::from(values.to_vec()));
            let batch = RecordBatch::try_from_iter([("x", column)]).unwrap();
            profiler
                .get_or_insert_with(|| Profiler::new(&meta, &batch.schema()))
                .update(&batch);
        }
        profiler.unwrap()
    }

    #[test]
    fn labelled_values_are_counted_across_batches() {
        let profiler = profile(
            &[(1.0, "Low"), (2.0, "High")],
            &[&[Some(1.0), Some(1.0), Some(2.0)], &[Some(1.0), None]],
        );
        let rows = profiler.values(0);
        assert!(rows[0].is_missing_row());
        assert_eq!((rows[0].n, rows[0].pct_valid), (1, None));
        assert_eq!(rows[0].pct_total, Some(0.2));
        assert_eq!(
            (rows[1].code.clone(), rows[1].label.as_deref(), rows[1].n),
            (Some(num(1.0)), Some("Low"), 3)
        );
        assert_eq!(rows[1].pct_valid, Some(0.75));
        assert_eq!(rows[2].n, 1);

        let summary = profiler.summary(0);
        assert_eq!(summary.variable_type.name(), "categorical");
        assert_eq!(
            (summary.n_valid, summary.n_missing, summary.n_total),
            (4, 1, 5)
        );
        assert_eq!(
            (summary.n_unique, summary.n_labeled, summary.n_unlabeled),
            (2, Some(2), Some(0))
        );
    }

    #[test]
    fn labels_and_data_are_merged_in_code_order() {
        // 3 is labelled but never occurs; 9 occurs but has no label.
        let profiler = profile(
            &[(3.0, "Three"), (1.0, "One")],
            &[&[Some(9.0), Some(1.0), Some(9.0)]],
        );
        let rows = profiler.values(0);
        let codes: Vec<_> = rows
            .iter()
            .map(|row| (row.code.clone().unwrap(), row.n))
            .collect();
        assert_eq!(codes, [(num(1.0), 1), (num(3.0), 0), (num(9.0), 2)]);
        assert_eq!(rows[2].label, None);

        let summary = profiler.summary(0);
        assert_eq!(
            (summary.n_unique, summary.n_labeled, summary.n_unlabeled),
            (3, Some(2), Some(1))
        );
        assert_eq!(
            summary.values,
            [
                (num(1.0), "One".to_string()),
                (num(3.0), "Three".to_string())
            ]
        );
    }

    #[test]
    fn a_numeric_variable_has_one_summary_row() {
        let profiler = profile(&[], &[&[Some(18.0), Some(25.5), None, Some(40.0)]]);
        let summary = profiler.summary(0);
        assert_eq!(summary.variable_type, VariableType::Numeric);
        // Python reports n_unique = n_valid for a variable that is not categorical.
        assert_eq!(
            (summary.n_valid, summary.n_missing, summary.n_unique),
            (3, 1, 3)
        );
        assert_eq!((summary.n_labeled, summary.n_unlabeled), (None, None));
        assert!(summary.values.is_empty());

        let rows = profiler.values(0);
        assert_eq!(rows.len(), 2);
        assert_eq!(
            (rows[1].code.clone(), rows[1].label.clone(), rows[1].n),
            (None, None, 3)
        );
        assert_eq!(rows[1].pct_valid, Some(1.0));
    }

    #[test]
    fn unlabelled_zeros_and_ones_are_categorical_until_another_value_arrives() {
        let mut profiler = profile(&[], &[&[Some(0.0), Some(1.0), Some(1.0), None]]);
        assert_eq!(profiler.variable_type(0), VariableType::MultiSelect);
        let rows = profiler.values(0);
        let counts: Vec<_> = rows[1..]
            .iter()
            .map(|row| (row.code.clone().unwrap(), row.n))
            .collect();
        assert_eq!(counts, [(num(0.0), 1), (num(1.0), 2)]);
        assert_eq!(profiler.summary(0).n_unlabeled, Some(2));

        let column: ArrayRef = Arc::new(Float64Array::from(vec![2.0]));
        profiler.update(&RecordBatch::try_from_iter([("x", column)]).unwrap());
        assert_eq!(profiler.variable_type(0), VariableType::Numeric);
        assert_eq!(profiler.values(0).len(), 2);
    }

    #[test]
    fn only_ones_without_labels_stays_numeric() {
        let profiler = profile(&[], &[&[Some(1.0), Some(1.0)]]);
        assert_eq!(profiler.variable_type(0), VariableType::Numeric);
    }

    #[test]
    fn a_column_of_nulls_has_no_valid_cases() {
        let profiler = profile(&[(1.0, "Yes"), (2.0, "No")], &[&[None, None]]);
        let summary = profiler.summary(0);
        assert_eq!(
            (summary.n_valid, summary.n_missing, summary.n_unique),
            (0, 2, 2)
        );
        let rows = profiler.values(0);
        // No valid cases: there is nothing to take a share of.
        assert_eq!(rows[1].pct_valid, None);
        assert_eq!(rows[1].pct_total, Some(0.0));
    }

    #[test]
    fn negative_zero_counts_as_zero() {
        let profiler = profile(&[(0.0, "No"), (1.0, "Yes")], &[&[Some(-0.0), Some(0.0)]]);
        let rows = profiler.values(0);
        assert_eq!((rows[0].code.clone(), rows[0].n), (Some(num(0.0)), 2));
    }

    #[test]
    fn empty_strings_are_missing() {
        let meta = SpssMetadata {
            variable_names: vec!["city".to_string()],
            ..SpssMetadata::default()
        };
        let column: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("NYC"),
            Some(""),
            None,
            Some("LA"),
        ]));
        let batch = RecordBatch::try_from_iter([("city", column)]).unwrap();
        let mut profiler = Profiler::new(&meta, &batch.schema());
        profiler.update(&batch);
        let summary = profiler.summary(0);
        assert_eq!(summary.variable_type, VariableType::Text);
        assert_eq!(
            (summary.n_valid, summary.n_missing, summary.n_total),
            (2, 2, 4)
        );
    }

    #[test]
    fn no_rows_at_all() {
        let profiler = profile(&[(1.0, "Yes")], &[&[]]);
        let summary = profiler.summary(0);
        assert_eq!(
            (summary.n_valid, summary.n_missing, summary.n_total),
            (0, 0, 0)
        );
        assert_eq!(summary.variable_type, VariableType::SingleSelect);
        let rows = profiler.values(0);
        assert_eq!(rows.len(), 1);
        assert_eq!((rows[0].pct_valid, rows[0].pct_total), (None, None));
    }
}
