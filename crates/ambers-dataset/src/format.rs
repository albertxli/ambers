//! What a cell shows in each view mode, and how wide each column is.
//!
//! The rules follow Python's `apply_labels(output="string")`: nothing
//! fails and nothing is hidden. A value with a label shows the label; a
//! value without one shows itself, and is flagged when its variable does
//! have labels so the gap is visible. Numbers are written in the
//! variable's SPSS print format, as SPSS's Data View does.

use std::collections::HashSet;

use ambers::constants::SpssFormat;
use ambers::{MissingSpec, SpssMetadata, Value};
use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    DataType, Date32Type, DurationMicrosecondType, Float64Type, Schema, TimestampMicrosecondType,
};
use arrow::temporal_conversions::{date32_to_datetime, timestamp_us_to_datetime};
use indexmap::IndexMap;
use unicode_width::UnicodeWidthStr;

/// Widest a column's content may be; longer text is cut with `…`.
const MAX_WIDTH: usize = 30;
/// Same for Both mode, which shows a code and a label side by side.
const MAX_WIDTH_BOTH: usize = 40;

/// How cell values are shown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// The value label, or the value itself when it has none
    Labels,
    /// The stored value
    Codes,
    /// The stored value followed by its label
    Both,
}

impl Mode {
    pub fn next(self) -> Mode {
        match self {
            Mode::Labels => Mode::Codes,
            Mode::Codes => Mode::Both,
            Mode::Both => Mode::Labels,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Mode::Labels => "Labels",
            Mode::Codes => "Codes",
            Mode::Both => "Both",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// The text of one cell and what the viewer should know about it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Cell {
    pub text: String,
    /// The stored value as text, when `text` shows its label. `None` when
    /// the text is the stored value itself.
    pub code: Option<String>,
    /// A value with no label, in a numeric variable that has value labels.
    pub unlabeled: bool,
    /// A value the metadata declares as user-missing.
    pub user_missing: bool,
}

/// Everything needed to show one variable's cells.
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub label: String,
    /// Stored as a number (not a string or a date/time).
    pub numeric: bool,
    /// Stored as a string. Neither this nor `numeric`: a date or a time.
    pub string: bool,
    pub labels: IndexMap<Value, String>,
    pub missing: Vec<MissingSpec>,
    /// The SPSS print format as written in the file, e.g. `F8.2`.
    pub format: String,
    pub decimals: usize,
    pub measure: &'static str,
    pub role: &'static str,
    /// Labels carried by more than one code of this variable.
    shared_labels: HashSet<String>,
    /// Width of the widest labelled code, for aligning labels in Both mode.
    code_width: usize,
    /// Column width per mode, indexed by `Mode::index`.
    widths: [usize; 3],
}

impl Column {
    /// One `Column` per variable, in file order.
    pub fn from_metadata(meta: &SpssMetadata, schema: &Schema) -> Vec<Column> {
        schema
            .fields()
            .iter()
            .map(|field| Column::new(meta, field.name(), field.data_type()))
            .collect()
    }

    fn new(meta: &SpssMetadata, name: &str, data_type: &DataType) -> Column {
        let format = meta.format(name).unwrap_or("").to_string();
        let parsed = SpssFormat::from_string(&format);
        let numeric = matches!(data_type, DataType::Float64);
        let labels = meta.value_labels(name).cloned().unwrap_or_default();
        let shared_labels = ambers_codebook::validate::duplicate_labels(&labels)
            .into_iter()
            .map(|duplicate| duplicate.label)
            .collect();

        let mut column = Column {
            name: name.to_string(),
            label: one_line(meta.label(name).unwrap_or("")),
            numeric,
            string: matches!(
                data_type,
                DataType::Utf8View | DataType::Utf8 | DataType::LargeUtf8
            ),
            format,
            labels,
            missing: meta
                .variable_missing_values
                .get(name)
                .cloned()
                .unwrap_or_default(),
            decimals: parsed.as_ref().map_or(0, |f| f.decimals as usize),
            measure: meta.measure(name).map_or("", |measure| measure.as_str()),
            role: meta.role(name).map_or("", |role| role.as_str()),
            shared_labels,
            code_width: 0,
            widths: [0; 3],
        };
        column.code_width = column
            .labels
            .keys()
            .map(|code| column.code_text(code).width())
            .max()
            .unwrap_or(0);

        let stored = match data_type {
            DataType::Float64 => parsed.as_ref().map_or(8, |f| f.width as usize),
            DataType::Date32 => 10,
            DataType::Timestamp(..) => 19,
            DataType::Duration(_) => 9,
            // Strings: the declared width, which is the longest possible value.
            _ => parsed.as_ref().map_or(8, |f| f.width as usize),
        };
        let codes = stored.clamp(1, MAX_WIDTH);
        let (labels_width, both_width) = if column.labels.is_empty() {
            (codes, codes)
        } else {
            let longest = (column.labels.iter())
                .map(|(code, label)| column.label_text(code, label).width())
                .max()
                .unwrap_or(0);
            let longest_plain = (column.labels.values().map(|l| one_line(l).width()))
                .max()
                .unwrap_or(0);
            (
                longest.max(column.code_width).min(MAX_WIDTH),
                (column.code_width + 1 + longest_plain).min(MAX_WIDTH_BOTH),
            )
        };
        let name_width = column.name.width();
        column.widths = [labels_width, codes, both_width].map(|w| w.max(name_width));
        column
    }

    /// Display width of the column in `mode`. It never depends on the rows
    /// on screen, so columns do not move while scrolling.
    pub fn width(&self, mode: Mode) -> usize {
        self.widths[mode.index()]
    }

    /// Whether cells are right-aligned: numbers are, unless the column is
    /// showing labels.
    pub fn right_aligned(&self, mode: Mode) -> bool {
        self.numeric && (mode == Mode::Codes || self.labels.is_empty())
    }

    /// The cell at `row` of `array`, which must be this variable's column.
    pub fn cell(&self, array: &dyn Array, row: usize, mode: Mode) -> Cell {
        self.cell_as(array, row, mode, false)
    }

    /// The cell as its label with the stored value beside it (`code`), for
    /// a viewer that draws the two apart: the label is given plain, where
    /// Labels mode appends the code to a label several codes share.
    pub fn cell_apart(&self, array: &dyn Array, row: usize) -> Cell {
        self.cell_as(array, row, Mode::Labels, true)
    }

    fn cell_as(&self, array: &dyn Array, row: usize, mode: Mode, apart: bool) -> Cell {
        if array.is_null(row) {
            // System-missing, shown as SPSS shows it.
            return Cell {
                text: ".".to_string(),
                ..Cell::default()
            };
        }
        let value = match array.data_type() {
            DataType::Float64 => Value::Numeric(array.as_primitive::<Float64Type>().value(row)),
            DataType::Utf8View => Value::String(array.as_string_view().value(row).to_string()),
            other => {
                return Cell {
                    text: temporal_text(array, row, other),
                    ..Cell::default()
                };
            }
        };
        self.value_cell_as(&value, mode, apart)
    }

    /// The cell for a stored value.
    pub fn value_cell(&self, value: &Value, mode: Mode) -> Cell {
        self.value_cell_as(value, mode, false)
    }

    fn value_cell_as(&self, value: &Value, mode: Mode, apart: bool) -> Cell {
        let code = self.code_text(value);
        let label = self.labels.get(value);
        let (text, code) = match (mode, label) {
            (Mode::Codes, _) | (_, None) => (code, None),
            (Mode::Labels, Some(label)) if apart => (one_line(label), Some(code)),
            (Mode::Labels, Some(label)) => (self.label_text(value, label), Some(code)),
            (Mode::Both, Some(label)) => {
                let pad = self.code_width.saturating_sub(code.width());
                let text = format!("{code}{} {}", " ".repeat(pad), one_line(label));
                (text, Some(code))
            }
        };
        Cell {
            text,
            code,
            // Free text in a labelled string variable is legitimate data,
            // so only numeric variables are flagged (as `validate()` does).
            unlabeled: mode != Mode::Codes
                && label.is_none()
                && self.numeric
                && !self.labels.is_empty(),
            user_missing: self.is_user_missing(value),
        }
    }

    /// A stored value as text: numbers in the variable's print format.
    pub fn code_text(&self, value: &Value) -> String {
        match value {
            Value::Numeric(number) => format!("{number:.*}", self.decimals),
            Value::String(text) => one_line(text),
        }
    }

    /// A label as shown in Labels mode. A label shared by several codes
    /// gets its code appended, since the label alone cannot tell them apart.
    fn label_text(&self, code: &Value, label: &str) -> String {
        if self.shared_labels.contains(label) {
            format!("{} [{code}]", one_line(label))
        } else {
            one_line(label)
        }
    }

    fn is_user_missing(&self, value: &Value) -> bool {
        self.missing.iter().any(|spec| match (spec, value) {
            (MissingSpec::Value(missing), Value::Numeric(v)) => missing == v,
            (MissingSpec::Range { lo, hi }, Value::Numeric(v)) => lo <= v && v <= hi,
            (MissingSpec::StringValue(missing), Value::String(v)) => missing == v,
            _ => false,
        })
    }

    /// The missing-value definition as SPSS writes it, e.g. `99` or
    /// `90 thru 99`. Empty when the variable declares none.
    pub fn missing_text(&self) -> String {
        let parts: Vec<String> = (self.missing.iter())
            .map(|spec| match spec {
                MissingSpec::Value(v) => Value::Numeric(*v).to_string(),
                MissingSpec::Range { lo, hi } => {
                    format!("{} thru {}", Value::Numeric(*lo), Value::Numeric(*hi))
                }
                MissingSpec::StringValue(s) => format!("\"{s}\""),
            })
            .collect();
        parts.join(", ")
    }
}

/// Dates and times as text. SPSS stores them as numbers; the reader has
/// already converted them, so they are shown the same in every mode.
fn temporal_text(array: &dyn Array, row: usize, data_type: &DataType) -> String {
    let text = match data_type {
        DataType::Date32 => date32_to_datetime(array.as_primitive::<Date32Type>().value(row))
            .map(|datetime| datetime.date().to_string()),
        DataType::Timestamp(..) => {
            let micros = array.as_primitive::<TimestampMicrosecondType>().value(row);
            timestamp_us_to_datetime(micros)
                .map(|datetime| datetime.format("%Y-%m-%d %H:%M:%S").to_string())
        }
        DataType::Duration(_) => {
            let micros = array.as_primitive::<DurationMicrosecondType>().value(row);
            let seconds = micros.unsigned_abs() / 1_000_000;
            let sign = if micros < 0 { "-" } else { "" };
            Some(format!(
                "{sign}{:02}:{:02}:{:02}",
                seconds / 3600,
                seconds / 60 % 60,
                seconds % 60
            ))
        }
        _ => None,
    };
    text.unwrap_or_else(|| "?".to_string())
}

/// Replace line breaks and other control characters with spaces, so a
/// value cannot break the grid.
fn one_line(text: &str) -> String {
    text.chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::path::Path;

    /// A committed fixture from `tests/data/`.
    pub(crate) fn fixture(name: &str) -> (arrow::record_batch::RecordBatch, Vec<Column>) {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/data")
            .join(name);
        let (batch, meta) = ambers::read_sav(path).unwrap();
        let columns = Column::from_metadata(&meta, &batch.schema());
        (batch, columns)
    }

    fn texts(name: &str, column: &str, mode: Mode) -> Vec<String> {
        let (batch, columns) = fixture(name);
        let index = batch.schema().index_of(column).unwrap();
        (0..batch.num_rows())
            .map(|row| columns[index].cell(batch.column(index), row, mode).text)
            .collect()
    }

    fn cells(name: &str, column: &str, mode: Mode) -> Vec<Cell> {
        let (batch, columns) = fixture(name);
        let index = batch.schema().index_of(column).unwrap();
        (0..batch.num_rows())
            .map(|row| columns[index].cell(batch.column(index), row, mode))
            .collect()
    }

    #[test]
    fn numbers_use_the_print_format() {
        // id is F8.0, income is F10.2, weight is F8.2.
        assert_eq!(texts("survey.sav", "id", Mode::Codes)[..2], ["1", "2"]);
        assert_eq!(
            texts("survey.sav", "income", Mode::Codes)[..2],
            ["20000.00", "21500.50"]
        );
        assert_eq!(texts("survey.sav", "weight", Mode::Labels)[0], "0.50");
    }

    #[test]
    fn dates_and_times_are_text_in_every_mode() {
        for mode in [Mode::Labels, Mode::Codes, Mode::Both] {
            assert_eq!(texts("survey.sav", "interview_date", mode)[1], "2024-01-02");
            assert_eq!(
                texts("survey.sav", "interview_start", mode)[1],
                "2024-01-01 09:37:00"
            );
            assert_eq!(texts("survey.sav", "duration", mode)[1], "00:05:11");
        }
    }

    #[test]
    fn system_missing_is_a_dot() {
        // Every seventh satisfaction answer is system-missing.
        for mode in [Mode::Labels, Mode::Codes, Mode::Both] {
            let cell = &cells("survey.sav", "satisfaction", mode)[6];
            assert_eq!(cell.text, ".");
            assert!(!cell.unlabeled && !cell.user_missing);
        }
    }

    #[test]
    fn a_labelled_value_shows_per_mode() {
        assert_eq!(
            texts("survey.sav", "gender", Mode::Codes)[..2],
            ["1.00", "2.00"]
        );
        assert_eq!(
            texts("survey.sav", "gender", Mode::Labels)[..2],
            ["Male", "Female"]
        );
        assert_eq!(
            texts("survey.sav", "gender", Mode::Both)[..2],
            ["1.00 Male", "2.00 Female"]
        );
        // A cell that shows a label says which stored value it stands for.
        let code = |mode| cells("survey.sav", "gender", mode)[0].code.clone();
        assert_eq!(code(Mode::Labels).as_deref(), Some("1.00"));
        assert_eq!(code(Mode::Both).as_deref(), Some("1.00"));
        assert_eq!(code(Mode::Codes), None);
        // A value without a label is its own text.
        assert_eq!(
            cells("imperfect_labels.sav", "q1", Mode::Labels)[3].code,
            None
        );
    }

    #[test]
    fn a_label_drawn_apart_from_its_code_is_plain() {
        // brand: 97 and 98 share the label "Other".
        let (batch, columns) = fixture("imperfect_labels.sav");
        let index = batch.schema().index_of("brand").unwrap();
        let column = &columns[index];
        let array = batch.column(index);
        let row = (0..batch.num_rows())
            .find(|&row| column.cell(array.as_ref(), row, Mode::Labels).text == "Other [97]")
            .expect("a row holding 97");
        let apart = column.cell_apart(array.as_ref(), row);
        assert_eq!(apart.text, "Other");
        assert!(apart.code.is_some());
        // Everything but the label's text is as in Labels mode.
        for row in 0..batch.num_rows() {
            let labels = column.cell(array.as_ref(), row, Mode::Labels);
            let apart = column.cell_apart(array.as_ref(), row);
            if labels.code.is_none() {
                assert_eq!(apart, labels);
            }
            assert_eq!(apart.code, labels.code);
        }
    }

    #[test]
    fn an_unlabelled_value_shows_itself_and_is_flagged() {
        // q1 has labels for 1, 2, 3 and 99; row 4 holds 7.
        let labels = cells("imperfect_labels.sav", "q1", Mode::Labels);
        assert_eq!(labels[0].text, "Agree");
        assert!(!labels[0].unlabeled);
        assert_eq!(labels[3].text, "7");
        assert!(labels[3].unlabeled);
        assert!(cells("imperfect_labels.sav", "q1", Mode::Both)[3].unlabeled);
        // Codes mode shows codes for everything, so nothing is flagged.
        assert!(!cells("imperfect_labels.sav", "q1", Mode::Codes)[3].unlabeled);
    }

    #[test]
    fn a_variable_without_labels_looks_the_same_in_every_mode() {
        let codes = cells("imperfect_labels.sav", "age", Mode::Codes);
        assert_eq!(codes[1].text, "25.5");
        assert_eq!(codes, cells("imperfect_labels.sav", "age", Mode::Labels));
        assert_eq!(codes, cells("imperfect_labels.sav", "age", Mode::Both));
        assert!(codes.iter().all(|cell| !cell.unlabeled));
    }

    #[test]
    fn free_text_in_a_labelled_string_variable_is_not_flagged() {
        let labels = cells("imperfect_labels.sav", "city", Mode::Labels);
        assert_eq!(labels[0].text, "New York");
        assert_eq!(labels[2].text, "Springfield");
        assert!(labels.iter().all(|cell| !cell.unlabeled));
        assert_eq!(
            texts("imperfect_labels.sav", "city", Mode::Both)[0],
            "NYC New York"
        );
    }

    #[test]
    fn a_shared_label_gets_its_code_in_labels_mode() {
        // brand: 97 and 98 are both "Other".
        let labels = texts("imperfect_labels.sav", "brand", Mode::Labels);
        assert_eq!(labels[..3], ["Brand A", "Other [97]", "Other [98]"]);
        // Both mode already shows the code.
        assert_eq!(
            texts("imperfect_labels.sav", "brand", Mode::Both)[1],
            "97 Other"
        );
    }

    #[test]
    fn user_missing_values_are_flagged_and_kept() {
        // q1 declares 99 missing; scale declares 90 thru 99.
        let q1 = cells("imperfect_labels.sav", "q1", Mode::Labels);
        assert_eq!(q1[5].text, "Refused");
        assert!(q1[5].user_missing);
        assert!(!q1[0].user_missing);
        let scale = cells("imperfect_labels.sav", "scale", Mode::Codes);
        assert_eq!(scale[11].text, "95");
        assert!(scale[11].user_missing);
        assert!(!scale[10].user_missing);
    }

    #[test]
    fn missing_definitions_read_like_spss() {
        let (_, columns) = fixture("imperfect_labels.sav");
        let by_name = |name: &str| columns.iter().find(|c| c.name == name).unwrap();
        assert_eq!(by_name("q1").missing_text(), "99");
        assert_eq!(by_name("scale").missing_text(), "90 thru 99");
        assert_eq!(by_name("age").missing_text(), "");
    }

    #[test]
    fn widths_come_from_metadata_and_are_capped() {
        let (_, columns) = fixture("survey.sav");
        let by_name = |name: &str| columns.iter().find(|c| c.name == name).unwrap();
        // F8.2 with labels Male / Female: 8 as codes, 6 as labels.
        assert_eq!(by_name("gender").width(Mode::Codes), 8);
        assert_eq!(by_name("gender").width(Mode::Labels), 6);
        // Never narrower than the variable's name.
        assert_eq!(by_name("satisfaction").width(Mode::Codes), 12);
        assert_eq!(by_name("satisfaction").width(Mode::Labels), 17);

        let (_, columns) = fixture("long_strings.sav");
        // A1000 is capped.
        assert_eq!(columns[1].width(Mode::Codes), MAX_WIDTH);
    }

    #[test]
    fn alignment_follows_what_is_shown() {
        let (_, columns) = fixture("survey.sav");
        let by_name = |name: &str| columns.iter().find(|c| c.name == name).unwrap();
        assert!(by_name("age").right_aligned(Mode::Labels));
        assert!(by_name("gender").right_aligned(Mode::Codes));
        assert!(!by_name("gender").right_aligned(Mode::Labels));
        assert!(!by_name("city").right_aligned(Mode::Codes));
    }
}
