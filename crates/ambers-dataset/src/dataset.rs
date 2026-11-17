//! A file as it loads: its columns, the rows that have arrived, what the
//! codebook says about each variable, and which rows a set of filters
//! keeps. No user interface lives here; the viewers hold a `Dataset` and
//! ask it.

use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ambers::{SpssMetadata, Value};
use ambers_codebook::profile::{Profiler, ValueRow, VariableSummary};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::detail::{self, Detail};
use crate::filter::{self, Candidates, Condition, Matcher, Parsed};
use crate::format::{Cell, Column, Mode};
use crate::loader::{End, Message};

/// How far loading has come.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Load {
    Loading,
    Done,
    /// Loading stopped at the row limit; the file has more rows.
    Limit,
    /// Loading stopped at the memory budget; the file has more rows.
    Memory,
    /// Loading failed part-way; the rows read so far are kept.
    Failed(String),
}

/// A row filter on one column: what was typed, made ready for matching.
pub struct RowFilter {
    pub column: usize,
    pub text: String,
    pub matcher: Matcher,
}

/// What a message from the loader changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Arrived {
    /// The file rows that were added, when the message was a batch.
    pub rows: Option<Range<usize>>,
    /// The cached types, counts and label checks were brought up to date.
    pub refreshed: bool,
}

/// What the label audit found for one variable, as numbers. The text the
/// terminal shows (`Dataset::label_check`) says the same in words.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LabelAudit {
    /// The variable is audited: numeric, with value labels.
    pub audited: bool,
    /// Values in the data that have no label.
    pub unlabeled: usize,
    /// The variable has more distinct values than are counted, so
    /// `unlabeled` is only what was seen.
    pub many: bool,
    /// Labels carried by more than one code.
    pub duplicates: usize,
}

pub struct Dataset {
    pub columns: Vec<Column>,
    batches: Vec<RecordBatch>,
    batch_rows: usize,
    /// Rows loaded so far.
    pub rows: usize,
    /// Rows in the file according to its header.
    pub total_rows: Option<usize>,
    /// Rows to load at most, when the user set a limit.
    pub limit: Option<usize>,
    pub load: Load,

    /// The loader thread publishes a copy of its profiler (types and
    /// counts) here after every batch.
    profiler: Arc<Mutex<Profiler>>,
    /// What a screen needs from the profiler, per variable, cached so that
    /// no frame or request computes it: the codebook summary and the
    /// label check.
    summaries: Vec<VariableSummary>,
    label_checks: Vec<String>,
    audits: Vec<LabelAudit>,
    counted_rows: u64,
    refreshed: Option<Instant>,
    /// While loading, the cache is brought up to date this often.
    pub refresh_every: Duration,
}

impl Dataset {
    /// A dataset with no rows yet. `batch_rows` is the size of every batch
    /// the loader will send (the last one may be shorter).
    pub fn new(
        meta: &SpssMetadata,
        schema: &Schema,
        batch_rows: usize,
        limit: Option<usize>,
    ) -> Dataset {
        let mut dataset = Dataset {
            columns: Column::from_metadata(meta, schema),
            batches: Vec::new(),
            batch_rows,
            rows: 0,
            total_rows: meta.number_rows.and_then(|n| usize::try_from(n).ok()),
            limit,
            load: Load::Loading,
            profiler: Arc::new(Mutex::new(Profiler::new(meta, schema))),
            summaries: Vec::new(),
            label_checks: Vec::new(),
            audits: Vec::new(),
            counted_rows: 0,
            refreshed: None,
            refresh_every: Duration::from_millis(250),
        };
        dataset.refresh_codebook(true);
        dataset
    }

    /// The slot the loader thread publishes its profiler into.
    pub fn profiler_handle(&self) -> Arc<Mutex<Profiler>> {
        Arc::clone(&self.profiler)
    }

    /// A snapshot of the published profiler. The loader holds the lock
    /// only to swap a copy in, so this does not wait on its counting.
    fn profiler(&self) -> Profiler {
        // A panic in the loader does not invalidate the tallies so far.
        self.profiler
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    // --- loading ----------------------------------------------------------

    /// Take a message from the loader. A batch adds rows; the caller tests
    /// them against its filters (`passing` with the returned range).
    pub fn receive(&mut self, message: Message) -> Arrived {
        match message {
            Message::Batch(batch) => {
                let before = self.rows;
                self.rows += batch.num_rows();
                self.batches.push(batch);
                Arrived {
                    rows: Some(before..self.rows),
                    refreshed: self.refresh_codebook(false),
                }
            }
            Message::Finished(end) => {
                let more = self.total_rows.is_none_or(|total| total > self.rows);
                self.load = match end {
                    End::MemoryBudget if more => Load::Memory,
                    End::Data if more && self.limit.is_some_and(|limit| self.rows >= limit) => {
                        Load::Limit
                    }
                    _ => Load::Done,
                };
                Arrived {
                    rows: None,
                    refreshed: self.refresh_codebook(true),
                }
            }
            Message::Failed(error) => {
                self.load = Load::Failed(error);
                Arrived {
                    rows: None,
                    refreshed: self.refresh_codebook(true),
                }
            }
        }
    }

    // --- cells ------------------------------------------------------------

    /// The batches as the loader sent them, each `batch_rows` long but the
    /// last.
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// The cell at a file row.
    pub fn cell(&self, row: usize, column: usize, mode: Mode) -> Cell {
        let batch = &self.batches[row / self.batch_rows];
        self.columns[column].cell(batch.column(column), row % self.batch_rows, mode)
    }

    /// The cell at a file row as its label with the stored value beside
    /// it (`Column::cell_apart`).
    pub fn cell_apart(&self, row: usize, column: usize) -> Cell {
        let batch = &self.batches[row / self.batch_rows];
        self.columns[column].cell_apart(batch.column(column), row % self.batch_rows)
    }

    // --- codebook: types, counts, label audit -----------------------------

    /// Bring the cached types, counts and label checks up to date with
    /// what the loader has counted. While loading this happens a few times
    /// a second at most (`refresh_every`), not on every batch; `force`
    /// does it now. Returns whether it did.
    fn refresh_codebook(&mut self, force: bool) -> bool {
        let due = self
            .refreshed
            .is_none_or(|at| at.elapsed() >= self.refresh_every);
        if !(force || due) {
            return false;
        }
        let profiler = self.profiler();
        self.counted_rows = profiler.rows();
        self.summaries = profiler.summaries();
        (self.audits, self.label_checks) = (0..self.columns.len())
            .map(|column| self.check_labels(&profiler, column))
            .unzip();
        self.refreshed = Some(Instant::now());
        true
    }

    /// Rows the counts cover. The loader counts a batch just before it
    /// hands it over, and the cache is refreshed a few times a second, so
    /// this can differ slightly from `rows` while loading.
    pub fn counted_rows(&self) -> u64 {
        self.counted_rows
    }

    /// The codebook's summary of one variable: type and counts.
    pub fn summary(&self, column: usize) -> &VariableSummary {
        &self.summaries[column]
    }

    /// The codebook's value rows of one variable: `MISSING`, then a row
    /// per code for a categorical variable.
    pub fn value_rows(&self, column: usize) -> Vec<ValueRow> {
        self.profiler().values(column)
    }

    /// How many variables hold values that have no label.
    pub fn unlabeled_variables(&self) -> usize {
        (self.audits.iter())
            .filter(|audit| audit.unlabeled > 0)
            .count()
    }

    /// The audit of one variable as numbers.
    pub fn label_audit(&self, column: usize) -> LabelAudit {
        self.audits[column]
    }

    /// The audit of one variable: `ok`, the unlabelled values, the
    /// duplicated labels, or empty when it is not audited.
    pub fn label_check(&self, column: usize) -> &str {
        &self.label_checks[column]
    }

    /// Whether the label audit covers a variable: numeric, with labels.
    fn is_audited(&self, column: usize) -> bool {
        self.columns[column].numeric && !self.columns[column].labels.is_empty()
    }

    fn check_labels(&self, profiler: &Profiler, column: usize) -> (LabelAudit, String) {
        if !self.is_audited(column) {
            return (LabelAudit::default(), String::new());
        }
        let labels = &self.columns[column].labels;
        let many = profiler.overflowed(column);
        // The codes that occur in the data.
        let seen: Vec<Value> = (profiler.values(column).into_iter())
            .filter(|row| row.n > 0)
            .filter_map(|row| row.code)
            .collect();
        let mut audit = LabelAudit {
            audited: true,
            many,
            ..LabelAudit::default()
        };
        let mut findings = Vec::new();
        if let Some(found) = ambers_codebook::validate::unlabeled_values(&seen, labels) {
            audit.unlabeled = found.values.len();
            let shown: Vec<String> = (found.values.iter().take(5))
                .map(|value| value.to_string())
                .collect();
            let more = if found.values.len() > 5 { ", …" } else { "" };
            let count = if many {
                "many".to_string()
            } else {
                found.values.len().to_string()
            };
            findings.push(format!("{count} unlabelled: {}{more}", shown.join(", ")));
        }
        for duplicate in ambers_codebook::validate::duplicate_labels(labels) {
            audit.duplicates += 1;
            let codes: Vec<String> = duplicate.codes.iter().map(Value::to_string).collect();
            findings.push(format!(
                "duplicate: {} ({})",
                duplicate.label,
                codes.join(", ")
            ));
        }
        let text = if findings.is_empty() {
            "ok".to_string()
        } else {
            findings.join("; ")
        };
        (audit, text)
    }

    /// The value table of one variable: for a categorical variable the
    /// codebook's rows, otherwise every distinct value counted now from
    /// the rows loaded so far.
    pub fn detail(&self, variable: usize) -> Detail {
        let column = &self.columns[variable];
        let categorical = self.summary(variable).variable_type.is_categorical();
        let (rows, counted) = if categorical {
            // One snapshot for the rows and the number of cases they cover.
            let profiler = self.profiler();
            let rows = detail::categorical(column, profiler.values(variable));
            (rows, profiler.rows())
        } else {
            let rows = detail::distinct(column, variable, &self.batches);
            (rows, self.rows as u64)
        };
        Detail {
            variable,
            rows_loaded: self.rows,
            counted,
            categorical,
            rows,
        }
    }

    // --- filters ----------------------------------------------------------
    //
    // A filter is the list of file rows it keeps. Nothing is copied or
    // reformatted: matching runs on the Arrow column.

    /// What was typed for `column`, ready for matching. `Ok(None)` is an
    /// empty filter; `Err` says what is missing.
    pub fn matcher(
        &self,
        column: usize,
        text: &str,
    ) -> Result<Option<(Vec<Parsed>, Matcher)>, String> {
        match filter::parse(text)? {
            Some(parsed) => {
                let matcher = Matcher::new(&self.columns[column], &parsed)?;
                Ok(Some((parsed, matcher)))
            }
            None => Ok(None),
        }
    }

    /// A condition of the browser's filter bar made ready for `column`, or
    /// why the column cannot take it.
    pub fn condition(&self, column: usize, condition: &Condition) -> Result<Matcher, String> {
        Matcher::from_condition(&self.columns[column], condition)
    }

    /// The rows among `candidates` that `matcher` keeps, for `column`.
    pub fn select(
        &self,
        matcher: &mut Matcher,
        column: usize,
        candidates: Candidates<'_>,
    ) -> Vec<u32> {
        matcher.select(
            &self.columns[column],
            column,
            &self.batches,
            self.batch_rows,
            candidates,
        )
    }

    /// The rows of `range` that every filter lets through; `None` without
    /// filters. Each filter tests only the rows the ones before it kept.
    ///
    /// With `0..rows` this is the whole result. With the range of a batch
    /// that just arrived it is what to add to an earlier result, so a
    /// filter is never re-run from the start while a file loads.
    pub fn passing(&self, filters: &mut [RowFilter], range: Range<usize>) -> Option<Vec<u32>> {
        let mut kept: Option<Vec<u32>> = None;
        for filter in filters {
            let candidates = match &kept {
                Some(rows) => Candidates::Rows(rows),
                None => Candidates::Range(range.clone()),
            };
            kept = Some(self.select(&mut filter.matcher, filter.column, candidates));
        }
        kept
    }

    /// The rows of `range` that at least one filter lets through, in
    /// ascending order; `None` without filters. Like `passing`, it can be
    /// asked for the range of a batch that just arrived.
    pub fn passing_any(&self, filters: &mut [RowFilter], range: Range<usize>) -> Option<Vec<u32>> {
        let mut kept: Option<Vec<u32>> = None;
        for filter in filters {
            let candidates = Candidates::Range(range.clone());
            let rows = self.select(&mut filter.matcher, filter.column, candidates);
            kept = Some(match kept {
                Some(have) => union(&have, &rows),
                None => rows,
            });
        }
        kept
    }
}

/// The rows of two ascending lists, each once, in ascending order.
fn union(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut out = Vec::with_capacity(a.len().max(b.len()));
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => {
                out.push(a[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                out.push(b[j]);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out.extend_from_slice(&a[i..]);
    out.extend_from_slice(&b[j..]);
    out
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::fs::File;
    use std::io::BufReader;
    use std::path::Path;

    use ambers::scanner::SavScanner;

    use crate::filter::Operator;

    /// A dataset with every row of a fixture loaded, five rows a batch.
    pub(crate) fn loaded(name: &str) -> Dataset {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/data")
            .join(name);
        let reader = BufReader::new(File::open(path).unwrap());
        let scanner = SavScanner::open(reader, 5).unwrap();
        let mut dataset = Dataset::new(scanner.metadata(), &scanner.schema(), 5, None);
        dataset.refresh_every = Duration::ZERO;
        for message in crate::loader::spawn(scanner, dataset.profiler_handle(), None) {
            dataset.receive(message);
        }
        dataset
    }

    fn column(dataset: &Dataset, name: &str) -> usize {
        (dataset.columns.iter())
            .position(|column| column.name == name)
            .unwrap()
    }

    fn filter(dataset: &Dataset, name: &str, text: &str) -> RowFilter {
        let column = column(dataset, name);
        let (_, matcher) = dataset.matcher(column, text).unwrap().unwrap();
        RowFilter {
            column,
            text: text.to_string(),
            matcher,
        }
    }

    #[test]
    fn rows_arrive_in_batches_and_loading_finishes() {
        let dataset = loaded("survey.sav");
        assert_eq!((dataset.rows, dataset.total_rows), (20, Some(20)));
        assert_eq!(dataset.load, Load::Done);
        assert_eq!(dataset.counted_rows(), 20);
        assert_eq!(dataset.columns.len(), 13);
    }

    #[test]
    fn a_batch_reports_the_rows_it_added() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/data/survey.sav");
        let reader = BufReader::new(File::open(path).unwrap());
        let scanner = SavScanner::open(reader, 5).unwrap();
        let mut dataset = Dataset::new(scanner.metadata(), &scanner.schema(), 5, None);
        let mut ranges = Vec::new();
        for message in crate::loader::spawn(scanner, dataset.profiler_handle(), None) {
            if let Some(range) = dataset.receive(message).rows {
                ranges.push(range);
            }
        }
        assert_eq!(ranges, [0..5, 5..10, 10..15, 15..20]);
    }

    #[test]
    fn a_cell_is_found_by_its_file_row_across_batches() {
        let dataset = loaded("survey.sav");
        let city = column(&dataset, "city");
        let gender = column(&dataset, "gender");
        assert_eq!(dataset.cell(0, city, Mode::Labels).text, "Lisbon");
        // Row 7 (the second of the second batch) is respondent 7.
        assert_eq!(dataset.cell(6, city, Mode::Labels).text, "Lima");
        assert_eq!(dataset.cell(1, gender, Mode::Labels).text, "Female");
        assert_eq!(dataset.cell(1, gender, Mode::Codes).text, "2.00");
    }

    #[test]
    fn filters_stack_and_each_tests_what_the_one_before_kept() {
        let dataset = loaded("survey.sav");
        assert_eq!(dataset.passing(&mut [], 0..dataset.rows), None);

        let mut filters = vec![filter(&dataset, "city", "lis")];
        assert_eq!(
            dataset.passing(&mut filters, 0..dataset.rows),
            Some(vec![0, 4, 8, 12, 16])
        );
        filters.push(filter(&dataset, "gender", "=1"));
        // Lisbon rows are all odd respondents, who are all male.
        assert_eq!(
            dataset.passing(&mut filters, 0..dataset.rows),
            Some(vec![0, 4, 8, 12, 16])
        );
        filters.push(filter(&dataset, "id", ">10"));
        assert_eq!(
            dataset.passing(&mut filters, 0..dataset.rows),
            Some(vec![12, 16])
        );
    }

    #[test]
    fn a_new_batch_is_tested_alone() {
        let dataset = loaded("survey.sav");
        let mut filters = vec![filter(&dataset, "city", "lis")];
        // Only the third batch: rows 10 to 14.
        assert_eq!(dataset.passing(&mut filters, 10..15), Some(vec![12]));
        // The parts add up to the whole.
        let mut whole = Vec::new();
        for start in [0, 5, 10, 15] {
            whole.extend(dataset.passing(&mut filters, start..start + 5).unwrap());
        }
        assert_eq!(Some(whole), dataset.passing(&mut filters, 0..20));
    }

    /// A condition of the filter bar as a row filter.
    fn condition(dataset: &Dataset, name: &str, operator: Operator, value: &str) -> RowFilter {
        let index = column(dataset, name);
        let condition = Condition {
            operator,
            value: value.to_string(),
        };
        RowFilter {
            column: index,
            text: String::new(),
            matcher: dataset.condition(index, &condition).unwrap(),
        }
    }

    #[test]
    fn any_condition_is_enough_when_asked_so() {
        let dataset = loaded("survey.sav");
        assert_eq!(dataset.passing_any(&mut [], 0..dataset.rows), None);

        // Lisbon is rows 0, 4, …; ids above 17 are rows 17, 18, 19.
        let mut filters = vec![
            condition(&dataset, "city", Operator::Equals, "Lisbon"),
            condition(&dataset, "id", Operator::Greater, "17"),
        ];
        assert_eq!(
            dataset.passing_any(&mut filters, 0..dataset.rows),
            Some(vec![0, 4, 8, 12, 16, 17, 18, 19])
        );
        // All of them: only what both keep.
        assert_eq!(dataset.passing(&mut filters, 0..dataset.rows), Some(vec![]));
        // A row both conditions keep is there once.
        filters.push(condition(&dataset, "id", Operator::Less, "2"));
        assert_eq!(
            dataset.passing_any(&mut filters, 0..dataset.rows),
            Some(vec![0, 4, 8, 12, 16, 17, 18, 19])
        );

        // Batch by batch, the parts add up to the whole.
        let mut whole = Vec::new();
        for start in [0, 5, 10, 15] {
            whole.extend(dataset.passing_any(&mut filters, start..start + 5).unwrap());
        }
        assert_eq!(Some(whole), dataset.passing_any(&mut filters, 0..20));
    }

    #[test]
    fn a_half_typed_filter_says_what_is_missing() {
        let dataset = loaded("survey.sav");
        let age = column(&dataset, "age");
        let city = column(&dataset, "city");
        assert!(dataset.matcher(age, "").unwrap().is_none());
        assert_eq!(
            dataset.matcher(age, ">").err().as_deref(),
            Some("type a number after >")
        );
        assert!(dataset.matcher(city, ">3").is_err(), "city is not numeric");
    }

    #[test]
    fn the_codebook_is_cached_per_variable() {
        let dataset = loaded("imperfect_labels.sav");
        let q1 = column(&dataset, "q1");
        assert!(dataset.summary(q1).variable_type.is_categorical());
        assert!(dataset.label_check(q1).contains("unlabelled"));
        assert!(dataset.unlabeled_variables() >= 1);
        // The same finding as numbers.
        let audit = dataset.label_audit(q1);
        assert!(audit.audited && audit.unlabeled >= 1 && !audit.many);
        // A variable without value labels is not audited.
        let id = column(&dataset, "id");
        assert_eq!(dataset.label_check(id), "");
        assert_eq!(dataset.label_audit(id), LabelAudit::default());
    }

    #[test]
    fn the_value_table_counts_what_is_loaded() {
        let dataset = loaded("survey.sav");
        let gender = dataset.detail(column(&dataset, "gender"));
        assert!(gender.categorical);
        assert_eq!(gender.rows_loaded, 20);
        assert!(gender.rows.iter().any(|row| row.label == "Male"));
        let totals = gender.totals();
        assert_eq!(totals.total, 20);
        assert_eq!(totals.valid + totals.missing, 20);
        // A text variable: every distinct value, counted on demand.
        let city = dataset.detail(column(&dataset, "city"));
        assert!(!city.categorical);
        assert!(
            city.rows
                .iter()
                .any(|row| row.code == "Lisbon" && row.n == 5)
        );
    }
}
