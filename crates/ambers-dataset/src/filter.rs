//! Row filters for Data View and the variable search for Variable View.
//!
//! A filter is what the user typed for one column. Its result is a list
//! of matching row numbers; no data is copied. Matching runs on the Arrow
//! column, not on formatted cells: for a numeric variable the typed text
//! is first resolved to the codes whose label matches, so the pass over
//! the column only compares numbers.
//!
//! What can be typed:
//!
//! | Typed            | Rows kept                                   |
//! |------------------|---------------------------------------------|
//! | `jap`            | the value or its label contains "jap"       |
//! | `=Japan`, `=3`   | the value or its label is exactly that      |
//! | `!=Japan`        | everything else                             |
//! | `>30` `>=` `<` `<=` | numeric variables only                   |
//! | `=.`             | system-missing (`!=.`: not missing)         |
//! | `japan|france`   | either: `|` separates alternatives          |
//!
//! Text is compared ignoring case. A label and a code both match, so the
//! result does not depend on the view mode.
//!
//! The browser's filter bar does not type: it sends a `Condition`, one of
//! a fixed set of operators and a value (`Matcher::from_condition`). The
//! matching is the same code, with one rule of its own: a missing value
//! passes only "is empty". "Not equal 99" there leaves out the rows with
//! no answer, as SPSS and SQL do, while the typed `!=99` keeps them.

use std::collections::{HashMap, HashSet};
use std::ops::Range;

use ambers::Value;
use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Float64Type};
use arrow::record_batch::RecordBatch;

use super::format::{Column, Mode};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Greater,
    GreaterOrEqual,
    Less,
    LessOrEqual,
}

/// What a typed filter asks for. Text is kept in lower case.
#[derive(Debug, Clone, PartialEq)]
pub enum Query {
    Contains(String),
    Equals(String),
    Compare(Op, f64),
    Missing,
}

/// A query, and whether the rows it does not match are the ones wanted.
#[derive(Debug, Clone, PartialEq)]
pub struct Parsed {
    pub query: Query,
    pub negate: bool,
}

/// What a condition of the browser's filter bar asks of a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operator {
    Equals,
    NotEquals,
    Greater,
    Less,
    GreaterOrEqual,
    LessOrEqual,
    Contains,
    NotContains,
    StartsWith,
    EndsWith,
    /// `%` stands for any text, `_` for one character; case counts.
    Like,
    /// `Like`, ignoring case.
    ILike,
    /// System-missing, or an empty string.
    IsEmpty,
    IsNotEmpty,
}

impl Operator {
    /// The operator as a sentence names it.
    pub fn name(self) -> &'static str {
        match self {
            Operator::Equals => "equals",
            Operator::NotEquals => "not equal",
            Operator::Greater => "greater than",
            Operator::Less => "less than",
            Operator::GreaterOrEqual => "greater or equal",
            Operator::LessOrEqual => "less or equal",
            Operator::Contains => "contains",
            Operator::NotContains => "does not contain",
            Operator::StartsWith => "starts with",
            Operator::EndsWith => "ends with",
            Operator::Like => "like",
            Operator::ILike => "ilike",
            Operator::IsEmpty => "is empty",
            Operator::IsNotEmpty => "is not empty",
        }
    }

    /// Whether the condition is incomplete without a value.
    pub fn needs_value(self) -> bool {
        !matches!(self, Operator::IsEmpty | Operator::IsNotEmpty)
    }
}

/// One condition on one variable.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Condition {
    pub operator: Operator,
    pub value: String,
}

/// What a variable is to a filter: it decides which operators apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Kind {
    /// A number without value labels.
    Number,
    /// A string.
    Text,
    /// A number with value labels: a condition may name a label or a code.
    Categorical,
    /// A date or a time, matched as it is written.
    Date,
}

impl Kind {
    pub const ALL: [Kind; 4] = [Kind::Number, Kind::Text, Kind::Categorical, Kind::Date];

    pub fn of(column: &Column) -> Kind {
        if column.numeric {
            if column.labels.is_empty() {
                Kind::Number
            } else {
                Kind::Categorical
            }
        } else if column.string {
            Kind::Text
        } else {
            Kind::Date
        }
    }

    /// The operators a variable of this kind offers, in the order a menu
    /// lists them.
    pub fn operators(self) -> &'static [Operator] {
        use Operator::*;
        match self {
            Kind::Number => &[
                Equals,
                NotEquals,
                Greater,
                Less,
                GreaterOrEqual,
                LessOrEqual,
                IsEmpty,
                IsNotEmpty,
            ],
            Kind::Text | Kind::Date => &[
                Equals,
                NotEquals,
                Contains,
                NotContains,
                StartsWith,
                EndsWith,
                Like,
                ILike,
                IsEmpty,
                IsNotEmpty,
            ],
            Kind::Categorical => &[
                Equals,
                NotEquals,
                Greater,
                Less,
                GreaterOrEqual,
                LessOrEqual,
                Contains,
                NotContains,
                StartsWith,
                EndsWith,
                Like,
                ILike,
                IsEmpty,
                IsNotEmpty,
            ],
        }
    }

    fn described(self) -> &'static str {
        match self {
            Kind::Number => "a number",
            Kind::Text => "text",
            Kind::Categorical => "a labelled number",
            Kind::Date => "a date or time",
        }
    }
}

/// Read what was typed. `Ok(None)` is an empty filter: every row.
///
/// `|` separates alternatives, each read on its own (`japan|france`,
/// `=1|=2`, `<18|>65`); a row is kept when any of them matches. An empty
/// alternative is skipped, so `japan|` half-typed is just `japan`.
pub fn parse(text: &str) -> Result<Option<Vec<Parsed>>, String> {
    let mut parts = Vec::new();
    for part in text.split('|') {
        if let Some(parsed) = parse_one(part)? {
            parts.push(parsed);
        }
    }
    Ok((!parts.is_empty()).then_some(parts))
}

/// One alternative of a filter.
fn parse_one(text: &str) -> Result<Option<Parsed>, String> {
    let text = text.trim();
    if text.is_empty() {
        return Ok(None);
    }
    let comparisons = [
        (">=", Op::GreaterOrEqual),
        ("<=", Op::LessOrEqual),
        (">", Op::Greater),
        ("<", Op::Less),
    ];
    for (sign, op) in comparisons {
        if let Some(rest) = text.strip_prefix(sign) {
            let number = rest.trim().parse::<f64>();
            return match number {
                Ok(number) if !number.is_nan() => Ok(Some(Parsed {
                    query: Query::Compare(op, number),
                    negate: false,
                })),
                _ => Err(format!("type a number after {sign}")),
            };
        }
    }
    let (rest, negate, sign) = if let Some(rest) = text.strip_prefix("!=") {
        (rest, true, "!=")
    } else if let Some(rest) = text.strip_prefix('=') {
        (rest, false, "=")
    } else {
        return Ok(Some(Parsed {
            query: Query::Contains(text.to_lowercase()),
            negate: false,
        }));
    };
    let query = match rest.trim() {
        "" => return Err(format!("type a value after {sign}")),
        "." => Query::Missing,
        value => Query::Equals(value.to_lowercase()),
    };
    Ok(Some(Parsed { query, negate }))
}

/// Whether `haystack` contains `needle`, ignoring case. `needle` must be
/// in lower case. An ASCII needle is compared byte by byte without
/// allocating, which is also right inside UTF-8 text.
fn contains(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    if needle.is_ascii() {
        let (haystack, needle) = (haystack.as_bytes(), needle.as_bytes());
        haystack.len() >= needle.len()
            && haystack
                .windows(needle.len())
                .any(|window| window.eq_ignore_ascii_case(needle))
    } else {
        haystack.to_lowercase().contains(needle)
    }
}

/// Whether `text` is `needle`, ignoring case and the spaces around it.
fn equals(text: &str, needle: &str) -> bool {
    let text = text.trim();
    if needle.is_ascii() {
        text.eq_ignore_ascii_case(needle)
    } else {
        text.to_lowercase() == needle
    }
}

/// Whether `text` starts with `needle` (in lower case), ignoring case.
fn starts_with(text: &str, needle: &str) -> bool {
    if needle.is_ascii() {
        let (text, needle) = (text.as_bytes(), needle.as_bytes());
        text.len() >= needle.len() && text[..needle.len()].eq_ignore_ascii_case(needle)
    } else {
        text.to_lowercase().starts_with(needle)
    }
}

/// Whether `text` ends with `needle` (in lower case), ignoring case and
/// the spaces after it.
fn ends_with(text: &str, needle: &str) -> bool {
    let text = text.trim_end();
    if needle.is_ascii() {
        let (text, needle) = (text.as_bytes(), needle.as_bytes());
        text.len() >= needle.len() && text[text.len() - needle.len()..].eq_ignore_ascii_case(needle)
    } else {
        text.to_lowercase().ends_with(needle)
    }
}

/// One piece of a LIKE pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Token {
    /// `%`: any text, also none.
    Any,
    /// `_`: exactly one character.
    One,
    Char(char),
}

/// A character without its case, for ILIKE. One character stays one.
fn fold(ch: char) -> char {
    if ch.is_ascii() {
        ch.to_ascii_lowercase()
    } else {
        ch.to_lowercase().next().unwrap_or(ch)
    }
}

/// A LIKE pattern cut into its pieces; without case when `folded`.
fn tokens(pattern: &str, folded: bool) -> Vec<Token> {
    (pattern.chars())
        .map(|ch| match ch {
            '%' => Token::Any,
            '_' => Token::One,
            ch if folded => Token::Char(fold(ch)),
            ch => Token::Char(ch),
        })
        .collect()
}

/// Whether the whole of `text` (without the spaces around it) fits the
/// pattern. When `folded`, the pattern must have been cut with `folded`.
fn like(pattern: &[Token], text: &str, folded: bool) -> bool {
    let text = text.trim();
    let (mut at, mut piece) = (0, 0);
    // Where to go back to: the piece after the last `%`, and how much of
    // the text that `%` has taken so far.
    let mut retry: Option<(usize, usize)> = None;
    loop {
        let next = text[at..].chars().next();
        match (pattern.get(piece), next) {
            (Some(Token::Any), _) => {
                piece += 1;
                retry = Some((piece, at));
            }
            (Some(Token::One), Some(ch)) => {
                piece += 1;
                at += ch.len_utf8();
            }
            (Some(Token::Char(wanted)), Some(ch))
                if *wanted == if folded { fold(ch) } else { ch } =>
            {
                piece += 1;
                at += ch.len_utf8();
            }
            (None, None) => return true,
            _ => {
                // Let the last `%` take one more character and try again.
                let Some((after, taken)) = retry else {
                    return false;
                };
                let Some(ch) = text[taken..].chars().next() else {
                    return false;
                };
                at = taken + ch.len_utf8();
                piece = after;
                retry = Some((after, at));
            }
        }
    }
}

/// How a text condition compares.
#[derive(Debug, Clone, PartialEq, Eq)]
enum How {
    Contains,
    Equals,
    StartsWith,
    EndsWith,
    Like { pattern: Vec<Token>, folded: bool },
}

impl How {
    /// Whether `text` passes; `needle` is in lower case (unused by LIKE,
    /// which carries its pattern).
    fn hit(&self, text: &str, needle: &str) -> bool {
        match self {
            How::Contains => contains(text, needle),
            How::Equals => equals(text, needle),
            How::StartsWith => starts_with(text, needle),
            How::EndsWith => ends_with(text, needle),
            How::Like { pattern, folded } => like(pattern, text, *folded),
        }
    }
}

/// `0.0` and `-0.0` are one value.
fn bits(number: f64) -> u64 {
    if number == 0.0 {
        0.0f64.to_bits()
    } else {
        number.to_bits()
    }
}

/// Which rows to test: a range of file rows, or a list of them in
/// ascending order (the rows another filter kept).
pub enum Candidates<'a> {
    Range(Range<usize>),
    Rows(&'a [u32]),
}

// A filter holds one to a few of these, and rows are tested against them in
// place: boxing the large variant would add a pointer to follow per row.
#[allow(clippy::large_enum_variant)]
enum Test {
    Missing,
    Compare(Op, f64),
    Text {
        needle: String,
        how: How,
        /// The needle as a number, for an exact match on a numeric column.
        number: Option<f64>,
        /// Whether the needle could be part of a written number; if not,
        /// numbers need not be written out to be compared.
        number_like: bool,
        /// Numeric codes whose label matches.
        codes: HashSet<u64>,
        /// String values whose label matches.
        labelled: HashSet<String>,
        /// Numbers already tested as text.
        seen: HashMap<u64, bool>,
    },
}

/// One alternative of a filter, made ready for one column.
struct Part {
    test: Test,
    negate: bool,
}

/// A filter made ready for one column: a value passes when any of its
/// alternatives does.
pub struct Matcher {
    parts: Vec<Part>,
    /// A missing value passes only a test for missing: the rule of the
    /// browser's conditions. Without it, a negated test keeps missing
    /// rows, as the typed `!=99` does.
    strict: bool,
}

impl Test {
    /// A text test for `column`: the labels that pass are looked up once,
    /// so a labelled value is found by its code. `needle` is in lower case.
    fn text(column: &Column, how: How, needle: &str) -> Test {
        let mut codes = HashSet::new();
        let mut labelled = HashSet::new();
        for (value, label) in &column.labels {
            if how.hit(label, needle) {
                match value {
                    Value::Numeric(number) => {
                        codes.insert(bits(*number));
                    }
                    Value::String(text) => {
                        labelled.insert(text.clone());
                    }
                }
            }
        }
        Test::Text {
            number: needle.parse::<f64>().ok().filter(|n| !n.is_nan()),
            // A pattern can stand for digits; any other needle with a
            // letter in it cannot be part of a written number.
            number_like: matches!(how, How::Like { .. })
                || needle
                    .chars()
                    .all(|ch| ch.is_ascii_digit() || ".-infa".contains(ch)),
            needle: needle.to_string(),
            how,
            codes,
            labelled,
            seen: HashMap::new(),
        }
    }
}

impl Part {
    fn new(column: &Column, parsed: &Parsed) -> Result<Part, String> {
        let test = match &parsed.query {
            Query::Missing => Test::Missing,
            Query::Compare(op, number) => {
                if !column.numeric {
                    return Err(format!(
                        "{} is not numeric: > and < need numbers",
                        column.name
                    ));
                }
                Test::Compare(*op, *number)
            }
            Query::Contains(needle) => Test::text(column, How::Contains, needle),
            Query::Equals(needle) => Test::text(column, How::Equals, needle),
        };
        Ok(Part {
            test,
            negate: parsed.negate,
        })
    }

    /// A condition of the filter bar, for `column`. It says what is wrong
    /// with a condition the column cannot take.
    fn from_condition(column: &Column, condition: &Condition) -> Result<Part, String> {
        let kind = Kind::of(column);
        let operator = condition.operator;
        if !kind.operators().contains(&operator) {
            return Err(format!(
                "{} is {}: \"{}\" does not apply",
                column.name,
                kind.described(),
                operator.name()
            ));
        }
        let value = condition.value.trim();
        if operator.needs_value() && value.is_empty() {
            return Err("enter a value".to_string());
        }
        let number = || {
            (value.parse::<f64>().ok())
                .filter(|number| number.is_finite())
                .ok_or_else(|| "enter a number".to_string())
        };
        let lower = value.to_lowercase();
        let (test, negate) = match operator {
            Operator::IsEmpty => (Test::Missing, false),
            Operator::IsNotEmpty => (Test::Missing, true),
            Operator::Greater => (Test::Compare(Op::Greater, number()?), false),
            Operator::Less => (Test::Compare(Op::Less, number()?), false),
            Operator::GreaterOrEqual => (Test::Compare(Op::GreaterOrEqual, number()?), false),
            Operator::LessOrEqual => (Test::Compare(Op::LessOrEqual, number()?), false),
            Operator::Equals | Operator::NotEquals => {
                // A number without labels can only equal a number.
                if kind == Kind::Number {
                    number()?;
                }
                let test = Test::text(column, How::Equals, &lower);
                (test, operator == Operator::NotEquals)
            }
            Operator::Contains => (Test::text(column, How::Contains, &lower), false),
            Operator::NotContains => (Test::text(column, How::Contains, &lower), true),
            Operator::StartsWith => (Test::text(column, How::StartsWith, &lower), false),
            Operator::EndsWith => (Test::text(column, How::EndsWith, &lower), false),
            Operator::Like | Operator::ILike => {
                let folded = operator == Operator::ILike;
                let how = How::Like {
                    pattern: tokens(value, folded),
                    folded,
                };
                (Test::text(column, how, &lower), false)
            }
        };
        Ok(Part { test, negate })
    }

    fn null(&self) -> bool {
        matches!(self.test, Test::Missing)
    }

    fn number(&mut self, column: &Column, value: f64) -> bool {
        match &mut self.test {
            Test::Missing => false,
            Test::Compare(op, limit) => match op {
                Op::Greater => value > *limit,
                Op::GreaterOrEqual => value >= *limit,
                Op::Less => value < *limit,
                Op::LessOrEqual => value <= *limit,
            },
            Test::Text {
                needle,
                how,
                number,
                number_like,
                codes,
                seen,
                ..
            } => {
                let key = bits(value);
                if codes.contains(&key) {
                    return true;
                }
                if matches!(how, How::Equals) {
                    return *number == Some(value);
                }
                if !*number_like {
                    return false;
                }
                // The number as the grid writes it, e.g. `1042` or `3.00`.
                *seen
                    .entry(key)
                    .or_insert_with(|| how.hit(&column.code_text(&Value::Numeric(value)), needle))
            }
        }
    }

    fn string(&self, value: &str) -> bool {
        match &self.test {
            // The codebook counts an empty string as missing.
            Test::Missing => value.is_empty(),
            Test::Compare(..) => false,
            Test::Text {
                needle,
                how,
                labelled,
                ..
            } => how.hit(value, needle) || (!labelled.is_empty() && labelled.contains(value)),
        }
    }

    /// Dates and times, compared as the grid writes them.
    fn written(&self, text: &str) -> bool {
        match &self.test {
            Test::Missing | Test::Compare(..) => false,
            Test::Text { needle, how, .. } => how.hit(text, needle),
        }
    }
}

impl Matcher {
    pub fn new(column: &Column, parsed: &[Parsed]) -> Result<Matcher, String> {
        let parts: Result<Vec<Part>, String> = (parsed.iter())
            .map(|parsed| Part::new(column, parsed))
            .collect();
        Ok(Matcher {
            parts: parts?,
            strict: false,
        })
    }

    /// A condition of the browser's filter bar, made ready for `column`.
    /// A missing value passes it only when it asks for missing values.
    pub fn from_condition(column: &Column, condition: &Condition) -> Result<Matcher, String> {
        Ok(Matcher {
            parts: vec![Part::from_condition(column, condition)?],
            strict: true,
        })
    }

    fn null(&self) -> bool {
        if self.strict {
            return (self.parts.iter()).any(|part| part.null() && !part.negate);
        }
        (self.parts.iter()).any(|part| part.null() != part.negate)
    }

    fn number(&mut self, column: &Column, value: f64) -> bool {
        (self.parts.iter_mut()).any(|part| part.number(column, value) != part.negate)
    }

    fn string(&self, value: &str) -> bool {
        // An empty string is a missing value.
        if self.strict && value.is_empty() {
            return self.null();
        }
        (self.parts.iter()).any(|part| part.string(value) != part.negate)
    }

    fn written(&self, text: &str) -> bool {
        (self.parts.iter()).any(|part| part.written(text) != part.negate)
    }

    /// Test `rows` of one batch's column and push the file row numbers
    /// (`base` + row) of those that pass.
    fn test_batch(
        &mut self,
        column: &Column,
        array: &dyn Array,
        rows: impl Iterator<Item = usize>,
        base: usize,
        out: &mut Vec<u32>,
    ) {
        let mut keep = |row: usize, hit: bool| {
            if hit {
                out.push((base + row) as u32);
            }
        };
        match array.data_type() {
            DataType::Float64 => {
                let numbers = array.as_primitive::<Float64Type>();
                for row in rows {
                    let hit = if numbers.is_null(row) {
                        self.null()
                    } else {
                        self.number(column, numbers.value(row))
                    };
                    keep(row, hit);
                }
            }
            DataType::Utf8View => {
                let strings = array.as_string_view();
                for row in rows {
                    let hit = if strings.is_null(row) {
                        self.null()
                    } else {
                        self.string(strings.value(row))
                    };
                    keep(row, hit);
                }
            }
            _ => {
                for row in rows {
                    let hit = if array.is_null(row) {
                        self.null()
                    } else {
                        self.written(&column.cell(array, row, Mode::Codes).text)
                    };
                    keep(row, hit);
                }
            }
        }
    }

    /// The file rows among `candidates` that pass, in ascending order.
    /// `index` is the column's position; every batch but the last holds
    /// `batch_rows` rows.
    pub fn select(
        &mut self,
        column: &Column,
        index: usize,
        batches: &[RecordBatch],
        batch_rows: usize,
        candidates: Candidates,
    ) -> Vec<u32> {
        let mut out = Vec::new();
        let batch_rows = batch_rows.max(1);
        match candidates {
            Candidates::Range(range) => {
                for (number, batch) in batches.iter().enumerate() {
                    let base = number * batch_rows;
                    let start = range.start.max(base);
                    let end = range.end.min(base + batch.num_rows());
                    if start < end {
                        let rows = start - base..end - base;
                        self.test_batch(column, batch.column(index), rows, base, &mut out);
                    }
                }
            }
            Candidates::Rows(rows) => {
                let mut from = 0;
                while from < rows.len() {
                    let number = rows[from] as usize / batch_rows;
                    let base = number * batch_rows;
                    let run = rows[from..]
                        .iter()
                        .take_while(|&&row| (row as usize) < base + batch_rows)
                        .count();
                    if let Some(batch) = batches.get(number) {
                        let local = rows[from..from + run]
                            .iter()
                            .map(|&row| row as usize - base);
                        self.test_batch(column, batch.column(index), local, base, &mut out);
                    }
                    from += run;
                }
            }
        }
        out
    }
}

/// The variables whose name or label contains `text`, ignoring case, in
/// file order. An empty text keeps every variable.
pub fn variables(columns: &[Column], text: &str) -> Vec<usize> {
    let needle = text.trim().to_lowercase();
    (columns.iter().enumerate())
        .filter(|(_, column)| contains(&column.name, &needle) || contains(&column.label, &needle))
        .map(|(index, _)| index)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::tests::fixture;

    /// The 1-based rows of `file` that `text` keeps in `variable`.
    fn rows(file: &str, variable: &str, text: &str) -> Vec<u32> {
        let (batch, columns) = fixture(file);
        let index = columns.iter().position(|c| c.name == variable).unwrap();
        let parsed = parse(text).unwrap().unwrap();
        let mut matcher = Matcher::new(&columns[index], &parsed).unwrap();
        let all = Candidates::Range(0..batch.num_rows());
        let found = matcher.select(
            &columns[index],
            index,
            std::slice::from_ref(&batch),
            batch.num_rows(),
            all,
        );
        found.into_iter().map(|row| row + 1).collect()
    }

    #[test]
    fn what_is_typed_is_read() {
        let query = |text: &str| parse(text).unwrap().unwrap().remove(0);
        assert_eq!(parse("  ").unwrap(), None);
        assert_eq!(query(" Jap ").query, Query::Contains("jap".to_string()));
        assert_eq!(query("=Japan").query, Query::Equals("japan".to_string()));
        assert!(!query("=Japan").negate);
        assert_eq!(query("!= 3").query, Query::Equals("3".to_string()));
        assert!(query("!=3").negate);
        assert_eq!(
            query(">=30").query,
            Query::Compare(Op::GreaterOrEqual, 30.0)
        );
        assert_eq!(query("< 2.5").query, Query::Compare(Op::Less, 2.5));
        assert_eq!(query("=.").query, Query::Missing);
        assert!(query("!=.").negate);
        // Half-typed filters say what is missing.
        assert_eq!(parse(">").unwrap_err(), "type a number after >");
        assert_eq!(parse("<=x").unwrap_err(), "type a number after <=");
        assert_eq!(parse("=").unwrap_err(), "type a value after =");
        assert_eq!(parse("!=").unwrap_err(), "type a value after !=");
    }

    #[test]
    fn a_bar_separates_alternatives() {
        let queries = |text: &str| -> Vec<Query> {
            let parts = parse(text).unwrap().unwrap();
            parts.into_iter().map(|parsed| parsed.query).collect()
        };
        assert_eq!(
            queries("Japan | france"),
            [
                Query::Contains("japan".to_string()),
                Query::Contains("france".to_string())
            ]
        );
        assert_eq!(
            queries("=1|>30"),
            [
                Query::Equals("1".to_string()),
                Query::Compare(Op::Greater, 30.0)
            ]
        );
        // Half-typed: the empty alternative is not one yet.
        assert_eq!(queries("japan|"), [Query::Contains("japan".to_string())]);
        assert_eq!(parse(" | ").unwrap(), None);
        // A broken alternative is still reported.
        assert_eq!(parse("japan|>").unwrap_err(), "type a number after >");

        // Either city; a label or a code; a range from both ends.
        assert_eq!(
            rows("survey.sav", "city", "osa|lima"),
            [2, 3, 6, 7, 10, 11, 14, 15, 18, 19]
        );
        assert_eq!(rows("survey.sav", "city", "=osaka|=lisbon").len(), 10);
        assert_eq!(
            rows("survey.sav", "satisfaction", "refused|=1"),
            [1, 5, 6, 10, 11, 15, 16, 20]
        );
        assert_eq!(rows("survey.sav", "satisfaction", "=.|=99").len(), 6);
        assert_eq!(rows("survey.sav", "id", "<3|>18"), [1, 2, 19, 20]);
        assert_eq!(rows("survey.sav", "id", "=4|=12|=20"), [4, 12, 20]);
    }

    #[test]
    fn a_label_and_a_code_both_match() {
        // gender: 1 Male (odd rows), 2 Female (even rows).
        let female: Vec<u32> = (1..=10).map(|n| n * 2).collect();
        assert_eq!(rows("survey.sav", "gender", "fem"), female);
        assert_eq!(rows("survey.sav", "gender", "=FEMALE"), female);
        assert_eq!(rows("survey.sav", "gender", "=2"), female);
        assert_eq!(rows("survey.sav", "gender", "=2.00"), female);
        // "male" is in "Female" too; an exact match tells them apart.
        assert_eq!(rows("survey.sav", "gender", "male").len(), 20);
        assert_eq!(rows("survey.sav", "gender", "=male").len(), 10);
        assert_eq!(rows("survey.sav", "gender", "!=male"), female);
    }

    #[test]
    fn numbers_match_as_written_and_by_comparison() {
        // id runs 1..=20: "1" is in 1, 10..=19.
        assert_eq!(
            rows("survey.sav", "id", "1"),
            [1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
        );
        assert_eq!(rows("survey.sav", "id", "=12"), [12]);
        assert_eq!(rows("survey.sav", "id", ">18"), [19, 20]);
        assert_eq!(rows("survey.sav", "id", ">=18"), [18, 19, 20]);
        assert_eq!(rows("survey.sav", "id", "<3"), [1, 2]);
        assert_eq!(rows("survey.sav", "id", "<=3"), [1, 2, 3]);
        // Letters cannot be in a number.
        assert_eq!(rows("survey.sav", "id", "abc"), [] as [u32; 0]);
    }

    #[test]
    fn missing_rows_are_found_and_never_match_a_value() {
        // satisfaction is system-missing in rows 7 and 14.
        assert_eq!(rows("survey.sav", "satisfaction", "=."), [7, 14]);
        assert_eq!(rows("survey.sav", "satisfaction", "!=.").len(), 18);
        assert_eq!(rows("survey.sav", "satisfaction", ">0").len(), 18);
        // "Everything but" keeps the missing rows.
        let refused = rows("survey.sav", "satisfaction", "=99");
        assert_eq!(refused, [5, 10, 15, 20]);
        assert_eq!(rows("survey.sav", "satisfaction", "!=99").len(), 16);
        // An empty string counts as missing, as in the codebook.
        assert_eq!(rows("imperfect_labels.sav", "city", "=."), [5]);
    }

    #[test]
    fn text_is_matched_ignoring_case() {
        assert_eq!(rows("survey.sav", "city", "osa"), [2, 6, 10, 14, 18]);
        assert_eq!(rows("survey.sav", "city", "=lima"), [3, 7, 11, 15, 19]);
        // Lisbon and Lima.
        assert_eq!(rows("survey.sav", "city", "li").len(), 10);
        assert_eq!(rows("survey.sav", "city", "!=osaka").len(), 15);
    }

    #[test]
    fn dates_match_as_written() {
        let all = rows("survey.sav", "interview_date", "-");
        assert_eq!(all.len(), 20, "every date is written with dashes");
        assert_eq!(rows("survey.sav", "interview_date", "zzz"), [] as [u32; 0]);
    }

    #[test]
    fn a_comparison_needs_a_numeric_variable() {
        let (_, columns) = fixture("survey.sav");
        let city = columns.iter().find(|c| c.name == "city").unwrap();
        let parsed = parse(">3").unwrap().unwrap();
        assert_eq!(
            Matcher::new(city, &parsed).err().unwrap(),
            "city is not numeric: > and < need numbers"
        );
    }

    #[test]
    fn a_second_filter_tests_only_the_rows_the_first_kept() {
        let (batch, columns) = fixture("survey.sav");
        let column = |name: &str| columns.iter().position(|c| c.name == name).unwrap();
        let (gender, city) = (column("gender"), column("city"));
        let batches = [batch.clone()];
        let size = batch.num_rows();

        let mut first =
            Matcher::new(&columns[gender], &parse("=female").unwrap().unwrap()).unwrap();
        let kept = first.select(
            &columns[gender],
            gender,
            &batches,
            size,
            Candidates::Range(0..size),
        );
        let mut second = Matcher::new(&columns[city], &parse("osaka").unwrap().unwrap()).unwrap();
        let both = second.select(
            &columns[city],
            city,
            &batches,
            size,
            Candidates::Rows(&kept),
        );
        // 0-based: the women in Osaka.
        assert_eq!(both, [1, 5, 9, 13, 17]);
    }

    #[test]
    fn rows_are_found_across_batches() {
        // The same column cut into batches of 6 rows.
        let (batch, columns) = fixture("survey.sav");
        let index = columns.iter().position(|c| c.name == "city").unwrap();
        let batches: Vec<RecordBatch> = (0..4)
            .map(|n| batch.slice(n * 6, 6.min(batch.num_rows() - n * 6)))
            .collect();
        let parsed = parse("osa").unwrap().unwrap();
        let mut matcher = Matcher::new(&columns[index], &parsed).unwrap();
        let all = matcher.select(
            &columns[index],
            index,
            &batches,
            6,
            Candidates::Range(0..20),
        );
        assert_eq!(all, [1, 5, 9, 13, 17]);
        // Only the rows that arrived with the last two batches.
        let late = matcher.select(
            &columns[index],
            index,
            &batches,
            6,
            Candidates::Range(12..20),
        );
        assert_eq!(late, [13, 17]);
        let some = matcher.select(
            &columns[index],
            index,
            &batches,
            6,
            Candidates::Rows(&[0, 1, 5, 6, 13, 19]),
        );
        assert_eq!(some, [1, 5, 13]);
    }

    /// The 1-based rows of `file` that a condition keeps in `variable`,
    /// or why the variable cannot take it.
    fn condition(
        file: &str,
        variable: &str,
        operator: Operator,
        value: &str,
    ) -> Result<Vec<u32>, String> {
        let (batch, columns) = fixture(file);
        let index = columns.iter().position(|c| c.name == variable).unwrap();
        let condition = Condition {
            operator,
            value: value.to_string(),
        };
        let mut matcher = Matcher::from_condition(&columns[index], &condition)?;
        let found = matcher.select(
            &columns[index],
            index,
            std::slice::from_ref(&batch),
            batch.num_rows(),
            Candidates::Range(0..batch.num_rows()),
        );
        Ok(found.into_iter().map(|row| row + 1).collect())
    }

    /// As `condition`, for one that the variable takes.
    fn kept(file: &str, variable: &str, operator: Operator, value: &str) -> Vec<u32> {
        condition(file, variable, operator, value).unwrap()
    }

    #[test]
    fn a_pattern_stands_for_the_whole_value() {
        let fits = |pattern: &str, text: &str| like(&tokens(pattern, false), text, false);
        assert!(fits("Lisbon", "Lisbon"));
        assert!(!fits("Lis", "Lisbon"));
        assert!(fits("Lis%", "Lisbon"));
        assert!(fits("%bon", "Lisbon"));
        assert!(fits("L_sb_n", "Lisbon"));
        assert!(!fits("L_bon", "Lisbon"));
        assert!(fits("%", ""));
        assert!(fits("%", "anything"));
        assert!(!fits("_", ""));
        // The first `%` may have to give text back.
        assert!(fits("%ab%ab", "abxabab"));
        assert!(fits("%a%a%", "banana"));
        assert!(!fits("%ab%ab", "abxa"));
        // Case counts, unless folded; one character may be many bytes.
        assert!(!fits("lisbon", "Lisbon"));
        let folded = |pattern: &str, text: &str| like(&tokens(pattern, true), text, true);
        assert!(folded("LIS%", "lisbon"));
        assert!(folded("é_ole", "École"));
        assert!(fits("S_o Paulo", "São Paulo"));
        // The spaces around a value are not part of it.
        assert!(fits("Lima", " Lima  "));
    }

    #[test]
    fn a_text_condition_keeps_what_its_operator_says() {
        use Operator::*;
        // city: Lisbon (1, 5, …), Osaka (2, 6, …), Lima (3, 7, …), N/A (4, 8, …).
        let lisbon = [1, 5, 9, 13, 17];
        let osaka = [2, 6, 10, 14, 18];
        let lima = [3, 7, 11, 15, 19];
        let none = [4, 8, 12, 16, 20];
        let city = |operator, value| kept("survey.sav", "city", operator, value);
        assert_eq!(city(Equals, "osaka"), osaka);
        assert_eq!(city(Equals, " Osaka "), osaka);
        assert_eq!(city(NotEquals, "Osaka").len(), 15);
        assert_eq!(city(Contains, "li").len(), 10);
        assert_eq!(city(NotContains, "li").len(), 10);
        assert_eq!(city(StartsWith, "LI").len(), 10);
        assert_eq!(city(StartsWith, "is"), [] as [u32; 0]);
        assert_eq!(city(EndsWith, "ma"), lima);
        assert_eq!(city(EndsWith, "bon"), lisbon);
        // LIKE minds the case and wants the whole value.
        assert_eq!(city(Like, "L%").len(), 10);
        assert_eq!(city(Like, "l%"), [] as [u32; 0]);
        assert_eq!(city(ILike, "l%").len(), 10);
        assert_eq!(city(Like, "L_ma"), lima);
        assert_eq!(city(Like, "Lis"), [] as [u32; 0]);
        assert_eq!(city(Like, "%").len(), 20);
        // A labelled string is found by its label too: N/A is "Not answered".
        assert_eq!(city(Contains, "not ans"), none);
        assert_eq!(city(StartsWith, "not"), none);
    }

    #[test]
    fn a_missing_value_passes_only_a_condition_about_missing() {
        use Operator::*;
        // satisfaction: missing in rows 7 and 14, 99 (Refused) in 5, 10, 15, 20.
        let satisfaction = |operator, value| kept("survey.sav", "satisfaction", operator, value);
        assert_eq!(satisfaction(IsEmpty, ""), [7, 14]);
        assert_eq!(satisfaction(IsNotEmpty, "").len(), 18);
        // Not the refused ones, and not the ones with no answer either.
        assert_eq!(satisfaction(NotEquals, "99").len(), 14);
        assert_eq!(satisfaction(NotEquals, "refused").len(), 14);
        assert_eq!(satisfaction(NotContains, "satisfied").len(), 8);
        // The typed filter of the terminal keeps the rows with no answer.
        assert_eq!(rows("survey.sav", "satisfaction", "!=99").len(), 16);
        // A value declared user-missing is still a value.
        assert_eq!(satisfaction(Equals, "99"), [5, 10, 15, 20]);

        // An empty string is a missing value: row 5 of this file's city.
        let city = |operator, value| kept("imperfect_labels.sav", "city", operator, value);
        assert_eq!(city(IsEmpty, ""), [5]);
        assert_eq!(city(IsNotEmpty, "").len(), 11);
        assert_eq!(city(NotEquals, "NYC"), [2, 3, 6, 7, 9, 10, 12]);
        assert_eq!(city(NotContains, "y"), [2, 3, 6, 7, 9, 12]);
        assert_eq!(city(Like, "%"), [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12]);
    }

    #[test]
    fn a_labelled_number_is_found_by_label_or_code() {
        use Operator::*;
        // gender: 1 Male (odd rows), 2 Female (even rows).
        let female: Vec<u32> = (1..=10).map(|n| n * 2).collect();
        let male: Vec<u32> = (1..=10).map(|n| n * 2 - 1).collect();
        let gender = |operator, value| kept("survey.sav", "gender", operator, value);
        assert_eq!(gender(Equals, "Female"), female);
        assert_eq!(gender(Equals, "2"), female);
        assert_eq!(gender(NotEquals, "male"), female);
        assert_eq!(gender(Greater, "1"), female);
        assert_eq!(gender(LessOrEqual, "1"), male);
        assert_eq!(gender(StartsWith, "fem"), female);
        assert_eq!(gender(StartsWith, "2"), female);
        assert_eq!(gender(EndsWith, "male").len(), 20);
        assert_eq!(gender(Contains, "fe"), female);
        assert_eq!(gender(Like, "F%"), female);
        assert_eq!(gender(Like, "f%"), [] as [u32; 0]);
        assert_eq!(gender(ILike, "m%"), male);
    }

    #[test]
    fn a_number_is_compared_and_a_date_is_read_as_written() {
        use Operator::*;
        let id = |operator, value| kept("survey.sav", "id", operator, value);
        assert_eq!(id(Greater, "18"), [19, 20]);
        assert_eq!(id(GreaterOrEqual, "18"), [18, 19, 20]);
        assert_eq!(id(Less, "3"), [1, 2]);
        assert_eq!(id(LessOrEqual, " 3 "), [1, 2, 3]);
        assert_eq!(id(Equals, "12"), [12]);
        assert_eq!(id(Equals, "12.0"), [12]);
        assert_eq!(id(NotEquals, "12").len(), 19);

        // The interviews run from 2024-01-01, one a day.
        let date = |operator, value| kept("survey.sav", "interview_date", operator, value);
        assert_eq!(date(StartsWith, "2024-01-0").len(), 9);
        assert_eq!(date(Equals, "2024-01-20"), [20]);
        assert_eq!(date(EndsWith, "-15"), [15]);
        assert_eq!(date(Like, "2024-01-1_").len(), 10);
    }

    #[test]
    fn a_variable_refuses_a_condition_that_is_not_for_it() {
        use Operator::*;
        let refused = |variable, operator, value| {
            condition("survey.sav", variable, operator, value).unwrap_err()
        };
        assert_eq!(
            refused("city", Greater, "3"),
            "city is text: \"greater than\" does not apply"
        );
        assert_eq!(
            refused("id", Contains, "1"),
            "id is a number: \"contains\" does not apply"
        );
        assert_eq!(
            refused("interview_date", Less, "2024"),
            "interview_date is a date or time: \"less than\" does not apply"
        );
        assert_eq!(refused("id", Equals, "abc"), "enter a number");
        assert_eq!(refused("gender", Greater, "male"), "enter a number");
        assert_eq!(refused("city", Equals, "  "), "enter a value");

        let (_, columns) = fixture("survey.sav");
        let kind = |name: &str| Kind::of(columns.iter().find(|c| c.name == name).unwrap());
        assert_eq!(kind("id"), Kind::Number);
        assert_eq!(kind("gender"), Kind::Categorical);
        assert_eq!(kind("city"), Kind::Text);
        assert_eq!(kind("interview_date"), Kind::Date);
        assert_eq!(kind("duration"), Kind::Date);
        // Every kind can ask about missing values; only numbers compare.
        for kind in Kind::ALL {
            let offered = kind.operators();
            assert!(offered.contains(&IsEmpty) && offered.contains(&Equals));
            assert_eq!(
                offered.contains(&Greater),
                matches!(kind, Kind::Number | Kind::Categorical)
            );
        }
    }

    #[test]
    fn variables_are_found_by_name_or_label() {
        let (_, columns) = fixture("survey.sav");
        let names = |text: &str| -> Vec<&str> {
            (variables(&columns, text).into_iter())
                .map(|index| columns[index].name.as_str())
                .collect()
        };
        assert_eq!(names("BRAND"), ["brand_a", "brand_b", "brand_c"]);
        // By label: "Interview date", "Interview start", "Interview length".
        assert_eq!(names("interview l"), ["duration"]);
        assert_eq!(names("").len(), 13);
        assert_eq!(names("no such thing"), [] as [&str; 0]);
    }
}
