//! The value table of one variable, shown beside Variable View: every
//! distinct value with its count and share.
//!
//! For a categorical variable the rows come from the codebook profiler
//! (labels and data merged). For text, numeric and date variables the
//! codebook has only a summary, so the distinct values are counted here,
//! from the loaded rows of that one column, when the variable is opened.

use std::collections::HashMap;

use ambers::Value;
use ambers_codebook::profile::ValueRow;
use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    DataType, Date32Type, DurationMicrosecondType, Float64Type, TimestampMicrosecondType,
};
use arrow::record_batch::RecordBatch;

use super::format::{Column, Mode};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowKind {
    /// The count of missing cases.
    Missing,
    Plain,
    /// A code found in the data that has no value label.
    Unlabeled,
    /// A value the metadata declares user-missing.
    UserMissing,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DetailRow {
    /// The code or value as text; empty on a categorical variable's
    /// missing row.
    pub code: String,
    pub label: String,
    pub n: u64,
    pub pct_valid: Option<f64>,
    pub pct_total: Option<f64>,
    pub kind: RowKind,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Detail {
    /// The variable these rows describe, and how many rows were loaded
    /// when they were built: together they say when to rebuild.
    pub variable: usize,
    pub rows_loaded: usize,
    pub categorical: bool,
    pub rows: Vec<DetailRow>,
}

impl Detail {
    /// Whether any value row carries a label, so the table needs a label
    /// column. Always true for a categorical variable.
    pub fn has_labels(&self) -> bool {
        self.categorical || self.rows.iter().any(|row| !row.label.is_empty())
    }

    /// The table as tab-separated text with a header row, as it is shown:
    /// pasted into a spreadsheet it fills one column per field.
    pub fn tsv(&self) -> String {
        let labels = self.has_labels();
        let first = if self.categorical { "Code" } else { "Value" };
        let mut lines = vec![match labels {
            true => format!("{first}\tLabel\tN\t% valid\t% total"),
            false => format!("{first}\tN\t% valid\t% total"),
        }];
        for row in &self.rows {
            let shares = format!("{}\t{}", percent(row.pct_valid), percent(row.pct_total));
            lines.push(match labels {
                true => format!("{}\t{}\t{}\t{shares}", row.code, row.label, row.n),
                false => format!("{}\t{}\t{shares}", row.code, row.n),
            });
        }
        lines.join("\n")
    }
}

/// A share as shown in the table: `27.3%`, or nothing when there is none.
pub fn percent(share: Option<f64>) -> String {
    share.map_or(String::new(), |share| format!("{:.1}%", share * 100.0))
}

fn share(n: u64, of: u64) -> Option<f64> {
    (of > 0).then(|| n as f64 / of as f64)
}

/// The rows of a categorical variable, from the codebook's value rows.
pub fn categorical(column: &Column, values: Vec<ValueRow>) -> Vec<DetailRow> {
    values
        .into_iter()
        .map(|row| {
            let (code, label, kind) = match (&row.code, &row.label) {
                (None, _) => (
                    String::new(),
                    "MISSING (system-missing)".to_string(),
                    RowKind::Missing,
                ),
                (Some(code), label) => {
                    let user_missing = column.value_cell(code, Mode::Codes).user_missing;
                    let (label, kind) = match (label, user_missing) {
                        (Some(label), false) => (label.clone(), RowKind::Plain),
                        (Some(label), true) => {
                            (format!("{label} (user-missing)"), RowKind::UserMissing)
                        }
                        (None, false) => ("(no label)".to_string(), RowKind::Unlabeled),
                        (None, true) => {
                            ("(no label, user-missing)".to_string(), RowKind::Unlabeled)
                        }
                    };
                    (column.code_text(code), label, kind)
                }
            };
            DetailRow {
                code,
                label,
                n: row.n,
                pct_valid: row.pct_valid,
                pct_total: row.pct_total,
                kind,
            }
        })
        .collect()
}

/// The distinct values of a text, numeric or date variable in the loaded
/// rows, each with its count: the missing row first, then numbers and
/// dates in ascending order, text from A to Z (ignoring case, so special
/// characters and digits come before letters).
pub fn distinct(column: &Column, index: usize, batches: &[RecordBatch]) -> Vec<DetailRow> {
    let total: u64 = batches.iter().map(|batch| batch.num_rows() as u64).sum();
    let mut missing = 0u64;
    // The value as text, its label if it has one, user-missing or not, count.
    let mut found: Vec<(String, String, bool, u64)> = Vec::new();

    let data_type = batches
        .first()
        .map(|batch| batch.column(index).data_type().clone());
    match data_type {
        Some(DataType::Float64) => {
            let mut counts: HashMap<u64, u64> = HashMap::new();
            for batch in batches {
                let numbers = batch.column(index).as_primitive::<Float64Type>();
                missing += numbers.null_count() as u64;
                for number in numbers.iter().flatten() {
                    // -0.0 and 0.0 are one value.
                    let number = if number == 0.0 { 0.0 } else { number };
                    *counts.entry(number.to_bits()).or_default() += 1;
                }
            }
            let mut sorted: Vec<(f64, u64)> = (counts.into_iter())
                .map(|(bits, n)| (f64::from_bits(bits), n))
                .collect();
            sorted.sort_by(|a, b| a.0.total_cmp(&b.0));
            for (number, n) in sorted {
                let cell = column.value_cell(&Value::Numeric(number), Mode::Codes);
                found.push((cell.text, String::new(), cell.user_missing, n));
            }
        }
        Some(DataType::Utf8View) => {
            let mut counts: HashMap<&str, u64> = HashMap::new();
            for batch in batches {
                let strings = batch.column(index).as_string_view();
                missing += strings.null_count() as u64;
                for text in strings.iter().flatten() {
                    // The codebook counts an empty string as missing.
                    if text.is_empty() {
                        missing += 1;
                    } else {
                        *counts.entry(text).or_default() += 1;
                    }
                }
            }
            let mut sorted: Vec<(&str, u64)> = counts.into_iter().collect();
            sorted.sort_by_cached_key(|(text, _)| (text.to_lowercase(), text.to_string()));
            for (text, n) in sorted {
                let value = Value::String(text.to_string());
                let cell = column.value_cell(&value, Mode::Codes);
                let label = column.labels.get(&value).cloned().unwrap_or_default();
                found.push((cell.text, label, cell.user_missing, n));
            }
        }
        Some(data_type) => {
            // Dates and times: count by the stored number, remember one
            // place each occurs so it can be formatted for display.
            let mut counts: HashMap<i64, (u64, usize, usize)> = HashMap::new();
            for (batch_index, batch) in batches.iter().enumerate() {
                let array = batch.column(index);
                missing += array.null_count() as u64;
                let raw: Vec<Option<i64>> = match &data_type {
                    DataType::Date32 => (array.as_primitive::<Date32Type>().iter())
                        .map(|day| day.map(i64::from))
                        .collect(),
                    DataType::Timestamp(..) => array
                        .as_primitive::<TimestampMicrosecondType>()
                        .iter()
                        .collect(),
                    DataType::Duration(_) => array
                        .as_primitive::<DurationMicrosecondType>()
                        .iter()
                        .collect(),
                    _ => Vec::new(),
                };
                for (row, value) in raw.into_iter().enumerate() {
                    if let Some(value) = value {
                        counts.entry(value).or_insert((0, batch_index, row)).0 += 1;
                    }
                }
            }
            let mut sorted: Vec<(i64, (u64, usize, usize))> = counts.into_iter().collect();
            sorted.sort_by_key(|(value, _)| *value);
            for (_, (n, batch_index, row)) in sorted {
                let cell = column.cell(batches[batch_index].column(index), row, Mode::Codes);
                found.push((cell.text, String::new(), false, n));
            }
        }
        None => {}
    }

    let valid = total - missing;
    let mut rows = Vec::with_capacity(found.len() + 1);
    if missing > 0 {
        rows.push(DetailRow {
            code: "(missing)".to_string(),
            label: String::new(),
            n: missing,
            pct_valid: None,
            pct_total: share(missing, total),
            kind: RowKind::Missing,
        });
    }
    rows.extend(
        found
            .into_iter()
            .map(|(code, label, user_missing, n)| DetailRow {
                code,
                label,
                n,
                pct_valid: share(n, valid),
                pct_total: share(n, total),
                kind: if user_missing {
                    RowKind::UserMissing
                } else {
                    RowKind::Plain
                },
            }),
    );
    rows
}

#[cfg(test)]
mod tests {
    use super::super::format::tests::fixture;
    use super::*;

    /// The distinct-value rows of one variable of a fixture, as
    /// (value, label, n) with the kind checked separately.
    fn rows_of(file: &str, name: &str) -> Vec<DetailRow> {
        let (batch, columns) = fixture(file);
        let index = batch.schema().index_of(name).unwrap();
        // Split in two, so counts have to add up across batches.
        let half = batch.num_rows() / 2;
        let batches = [
            batch.slice(0, half),
            batch.slice(half, batch.num_rows() - half),
        ];
        distinct(&columns[index], index, &batches)
    }

    fn brief(rows: &[DetailRow]) -> Vec<(&str, u64)> {
        rows.iter().map(|row| (row.code.as_str(), row.n)).collect()
    }

    #[test]
    fn text_values_are_listed_from_a_to_z_after_missing() {
        // city: NYC, LA, "", Paris, NYC, LA, "", NYC.
        let rows = rows_of("variable_types.sav", "city");
        assert_eq!(
            brief(&rows),
            [("(missing)", 2), ("LA", 2), ("NYC", 3), ("Paris", 1)]
        );
        assert_eq!(rows[0].kind, RowKind::Missing);
        assert_eq!((rows[0].pct_valid, rows[0].pct_total), (None, Some(0.25)));
        // Shares of the valid cases: 3 of 6.
        assert_eq!(rows[2].pct_valid, Some(0.5));
        // A labelled string value shows its label.
        assert_eq!(rows[2].label, "New York");
        assert_eq!(rows[3].label, "");
    }

    #[test]
    fn sorting_ignores_case_and_puts_symbols_first() {
        let (_, columns) = fixture("variable_types.sav");
        let city = columns.iter().position(|c| c.name == "city").unwrap();
        let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(
            (0..=city)
                .map(|i| arrow::datatypes::Field::new(format!("c{i}"), DataType::Utf8View, true))
                .collect::<Vec<_>>(),
        ));
        let values = ["banana", "Apple", "#tag", "apple", "10", "Zebra", "2"];
        let arrays = (0..=city)
            .map(|_| {
                std::sync::Arc::new(arrow::array::StringViewArray::from(values.to_vec()))
                    as arrow::array::ArrayRef
            })
            .collect();
        let batch = RecordBatch::try_new(schema, arrays).unwrap();
        let rows = distinct(&columns[city], city, &[batch]);
        let order: Vec<&str> = rows.iter().map(|row| row.code.as_str()).collect();
        assert_eq!(
            order,
            ["#tag", "10", "2", "Apple", "apple", "banana", "Zebra"]
        );
    }

    #[test]
    fn numeric_values_are_listed_in_order_in_the_print_format() {
        // age (F8.1): 18, 25.5, 31, null, 52, 67, 23, 38.
        let rows = rows_of("variable_types.sav", "age");
        assert_eq!(
            brief(&rows),
            [
                ("(missing)", 1),
                ("18.0", 1),
                ("23.0", 1),
                ("25.5", 1),
                ("31.0", 1),
                ("38.0", 1),
                ("52.0", 1),
                ("67.0", 1)
            ]
        );
        // only1: seven ones and a null.
        assert_eq!(
            brief(&rows_of("variable_types.sav", "only1")),
            [("(missing)", 1), ("1", 7)]
        );
    }

    #[test]
    fn dates_are_listed_in_order() {
        let rows = rows_of("variable_types.sav", "when");
        assert_eq!(rows.len(), 8);
        assert_eq!(
            (rows[0].code.as_str(), rows[7].code.as_str()),
            ("2024-01-01", "2024-01-08")
        );
    }

    #[test]
    fn user_missing_values_are_marked() {
        // scale declares 90 thru 99 missing; 95 occurs once. It has labels,
        // so the viewer shows it as categorical, but the marking is the same.
        let rows = rows_of("imperfect_labels.sav", "scale");
        let last = rows.last().unwrap();
        assert_eq!(
            (last.code.as_str(), last.kind),
            ("95", RowKind::UserMissing)
        );
        assert!(
            rows[..rows.len() - 1]
                .iter()
                .all(|row| row.kind == RowKind::Plain)
        );
    }

    #[test]
    fn the_table_copies_as_tab_separated_columns() {
        let numeric = Detail {
            variable: 0,
            rows_loaded: 8,
            categorical: false,
            rows: rows_of("variable_types.sav", "only1"),
        };
        assert_eq!(
            numeric.tsv(),
            "Value\tN\t% valid\t% total\n(missing)\t1\t\t12.5%\n1\t7\t100.0%\t87.5%"
        );

        let text = Detail {
            rows: rows_of("variable_types.sav", "city"),
            ..numeric.clone()
        };
        // A labelled string variable gets a label column.
        assert!(
            text.tsv()
                .starts_with("Value\tLabel\tN\t% valid\t% total\n(missing)\t\t2\t\t25.0%\n")
        );
        assert!(text.tsv().contains("\nNYC\tNew York\t3\t50.0%\t37.5%\n"));
    }

    #[test]
    fn categorical_rows_name_what_is_wrong() {
        let (batch, columns) = fixture("imperfect_labels.sav");
        let index = batch.schema().index_of("q1").unwrap();
        let value = |code: Option<f64>, label: Option<&str>, n: u64| ValueRow {
            code: code.map(Value::Numeric),
            label: label.map(str::to_string),
            n,
            n_valid: 11,
            pct_valid: None,
            n_total: 12,
            pct_total: None,
        };
        let rows = categorical(
            &columns[index],
            vec![
                value(None, Some("MISSING"), 1),
                value(Some(1.0), Some("Agree"), 3),
                value(Some(7.0), None, 2),
                value(Some(99.0), Some("Refused"), 1),
            ],
        );
        let shown: Vec<(&str, &str, RowKind)> = (rows.iter())
            .map(|row| (row.code.as_str(), row.label.as_str(), row.kind))
            .collect();
        assert_eq!(
            shown,
            [
                ("", "MISSING (system-missing)", RowKind::Missing),
                ("1", "Agree", RowKind::Plain),
                ("7", "(no label)", RowKind::Unlabeled),
                ("99", "Refused (user-missing)", RowKind::UserMissing),
            ]
        );
        let detail = Detail {
            variable: index,
            rows_loaded: 12,
            categorical: true,
            rows,
        };
        assert!(
            detail.tsv().starts_with(
                "Code\tLabel\tN\t% valid\t% total\n\tMISSING (system-missing)\t1\t\t\n"
            )
        );
    }
}
