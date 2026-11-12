//! `ambers info`: a file summary and variable list from the metadata
//! reader. On request it also reads the data: `--counts` for valid and
//! missing cases per variable, `--rows` for a preview of the first rows.

use std::io::Write;
use std::ops::Range;
use std::path::Path;

use std::collections::HashMap;

use ambers::{Compression, SpssMetadata};
use ambers_codebook::profile::{Profiler, VariableSummary};
use ambers_codebook::types::TypeDetector;
use anyhow::{Context, Result};
use arrow::array::{Array, AsArray};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow::util::display::{ArrayFormatter, FormatOptions};

use crate::render::{self, Status};

/// With no `--head` / `--tail`, a long variable list shows this many
/// variables from the start and from the end.
const DEFAULT_HEAD: usize = 10;
const DEFAULT_TAIL: usize = 5;
/// Columns shown by `--rows`; more would not fit a terminal.
const MAX_ROW_COLUMNS: usize = 8;

pub struct Options {
    /// List only the first N variables.
    pub head: Option<usize>,
    /// List only the last N variables.
    pub tail: Option<usize>,
    /// Read the data to count valid and missing cases per variable.
    pub counts: bool,
    /// Also show the first N data rows.
    pub rows: Option<usize>,
}

/// What reading the data found for the listed variables.
struct Counts {
    /// By variable name.
    summaries: HashMap<String, VariableSummary>,
    rows: u64,
}

pub fn run(out: &mut impl Write, file: &Path, options: &Options) -> Result<()> {
    let size = std::fs::metadata(file)?.len();
    // Opening the scanner reads the dictionary only; no data yet.
    let scanner = ambers::scan_sav(file)?;
    let meta = scanner.metadata().clone();
    let schema = scanner.schema();
    drop(scanner);

    let (first, last) = select_variables(meta.variable_names.len(), options.head, options.tail);
    // Read everything before printing anything, so a file whose data
    // cannot be read fails without a half-written report on stdout.
    let counts = match options.counts {
        true => Some(count(file, &meta, first.clone().chain(last.clone()))?),
        false => None,
    };
    let preview = match options.rows {
        Some(n) => Some(read_rows(file, &meta, n)?),
        None => None,
    };

    let counted = counts.as_ref().map(|counts| counts.rows);
    writeln!(out, "{}", summary(file, size, &meta, counted))?;
    if let Some(table) = variables(&meta, &schema, first, last, counts.as_ref()) {
        writeln!(out, "{table}")?;
    }
    if let Some(batch) = preview {
        writeln!(out, "{}", rows(&batch)?)?;
        let total = meta.variable_names.len();
        if batch.num_columns() < total {
            writeln!(out, "{} of {total} columns shown", batch.num_columns())?;
        }
    }
    Ok(())
}

/// Read the whole file once, keeping only the listed variables, and
/// profile them as the codebook does.
fn count(file: &Path, meta: &SpssMetadata, listed: impl Iterator<Item = usize>) -> Result<Counts> {
    let names: Vec<&str> = listed
        .map(|index| meta.variable_names[index].as_str())
        .collect();
    let mut scanner = ambers::scan_sav(file)?;
    scanner.select(&names)?;
    let mut profiler = Profiler::new(meta, &scanner.schema());
    while let Some(batch) = scanner.next_batch().context("cannot read the data rows")? {
        profiler.update(&batch);
    }
    Ok(Counts {
        rows: profiler.rows(),
        summaries: (profiler.summaries().into_iter())
            .map(|summary| (summary.name.clone(), summary))
            .collect(),
    })
}

/// The red summary printed in place of a report when the file cannot be read.
pub fn error_summary(file: &Path, error: &anyhow::Error) -> String {
    let rows = [
        ("File", file.display().to_string()),
        ("Status", Status::Error.text()),
        ("Error", format!("{error:#}")),
    ];
    render::summary_table(&rows, Status::Error)
}

fn summary(file: &Path, size: u64, meta: &SpssMetadata, counted: Option<u64>) -> String {
    let status = match meta.warnings.len() {
        0 => Status::Ok,
        n => Status::Warnings(n),
    };
    let compression = match meta.compression {
        Compression::None => "uncompressed",
        Compression::Bytecode => "bytecode",
        Compression::Zlib => "zlib",
    };
    let row_count = match meta.number_rows {
        Some(n) if n >= 0 => group_digits(n as u64),
        _ => "unknown".to_string(),
    };
    let shape = format!(
        "{row_count} rows × {} columns",
        group_digits(meta.number_columns as u64)
    );

    let mut rows = vec![("File", file.display().to_string())];
    if !meta.file_label.is_empty() {
        rows.push(("Label", meta.file_label.clone()));
    }
    rows.extend([
        ("Shape", shape),
        ("Size", human_size(size)),
        ("Compression", compression.to_string()),
        ("Encoding", meta.file_encoding.clone()),
        ("Created", meta.creation_time.clone()),
    ]);
    if let Some(counted) = counted {
        // What N and Missing in the variables table cover.
        rows.push(("Counted", format!("{} rows", group_digits(counted))));
    }
    rows.push(("Status", status.text()));
    rows.extend(meta.warnings.iter().map(|w| ("Warning", w.clone())));
    render::summary_table(&rows, status)
}

/// The variables table, or `None` when no variable is to be listed (an
/// empty file, or `--head 0`).
///
/// The type is the codebook's. Without `counts` it comes from the
/// metadata alone, which is exact except for an unlabelled column holding
/// only 0 and 1: that needs the data to be recognised as categorical.
fn variables(
    meta: &SpssMetadata,
    schema: &Schema,
    first: Range<usize>,
    last: Range<usize>,
    counts: Option<&Counts>,
) -> Option<String> {
    if first.is_empty() && last.is_empty() {
        return None;
    }
    let gap = !first.is_empty() && !last.is_empty() && first.end < last.start;
    let detector = TypeDetector::new(meta);

    let row = |index: usize| {
        let name = &meta.variable_names[index];
        let counted = counts.and_then(|counts| counts.summaries.get(name));
        let variable_type = match counted {
            Some(summary) => summary.variable_type,
            None => detector.detect(meta, name, schema.field(index).data_type(), None),
        };
        let mut row = vec![
            (index + 1).to_string(),
            // Names are identifiers: always shown in full.
            name.clone(),
            variable_type.name().to_string(),
            render::shorten_middle(meta.label(name).unwrap_or("")),
        ];
        if let Some(summary) = counted {
            row.push(group_digits(summary.n_valid));
            row.push(group_digits(summary.n_missing));
        }
        row
    };
    let mut header = vec!["#", "Name", "Type", "Label"];
    if counts.is_some() {
        header.extend(["N", "Missing"]);
    }
    let mut rows: Vec<Vec<String>> = first.map(row).collect();
    if gap {
        rows.push(vec!["...".to_string(); header.len()]);
    }
    rows.extend(last.map(row));

    let header: Vec<String> = header.into_iter().map(String::from).collect();
    // Counts line up on the right.
    Some(render::grid_table(&header, rows, &[4, 5]))
}

/// Which variables to list, as index ranges from the start and from the end
/// of the file's variable list. The ranges never overlap.
///
/// With neither flag: everything when the list is short, otherwise the first
/// `DEFAULT_HEAD` and the last `DEFAULT_TAIL`. With a flag, only what was
/// asked for.
fn select_variables(
    total: usize,
    head: Option<usize>,
    tail: Option<usize>,
) -> (Range<usize>, Range<usize>) {
    let (head, tail) = match (head, tail) {
        (None, None) if total <= DEFAULT_HEAD + DEFAULT_TAIL => (total, 0),
        (None, None) => (DEFAULT_HEAD, DEFAULT_TAIL),
        (head, tail) => (head.unwrap_or(0), tail.unwrap_or(0)),
    };
    let head = head.min(total);
    let tail = tail.min(total - head);
    (0..head, total - tail..total)
}

fn read_rows(file: &Path, meta: &SpssMetadata, n: usize) -> Result<RecordBatch> {
    let columns: Vec<&str> = meta
        .variable_names
        .iter()
        .take(MAX_ROW_COLUMNS)
        .map(String::as_str)
        .collect();
    let mut scanner = ambers::scan_sav(file)?;
    scanner.select(&columns)?;
    scanner.limit(n);
    scanner
        .collect_single()
        .context("cannot read the data rows")
}

fn rows(batch: &RecordBatch) -> Result<String> {
    let schema = batch.schema();
    let header: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    let options = FormatOptions::default().with_null("null");
    let mut table = vec![Vec::with_capacity(batch.num_columns()); batch.num_rows()];
    for column in batch.columns() {
        let strings = column.as_string_view_opt();
        let formatter = ArrayFormatter::try_new(column.as_ref(), &options)?;
        for (index, row) in table.iter_mut().enumerate() {
            row.push(match strings {
                Some(strings) if !strings.is_null(index) => {
                    render::shorten_end(strings.value(index))
                }
                _ => formatter.value(index).try_to_string()?,
            });
        }
    }
    Ok(render::grid_table(&header, table, &[]))
}

/// `1234567` -> `1,234,567`.
pub(crate) fn group_digits(n: u64) -> String {
    let digits = n.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, ch) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(ch);
    }
    grouped
}

/// `3640` -> `3.6 KB (3,640 bytes)`.
fn human_size(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["KB", "MB", "GB", "TB"];
    if bytes < 1024 {
        return format!("{bytes} bytes");
    }
    let mut value = bytes as f64 / 1024.0;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {} ({} bytes)", UNITS[unit], group_digits(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digits_are_grouped_in_threes() {
        assert_eq!(group_digits(0), "0");
        assert_eq!(group_digits(999), "999");
        assert_eq!(group_digits(1000), "1,000");
        assert_eq!(group_digits(1234567), "1,234,567");
    }

    #[test]
    fn sizes_pick_a_readable_unit() {
        assert_eq!(human_size(512), "512 bytes");
        assert_eq!(human_size(3640), "3.6 KB (3,640 bytes)");
        assert_eq!(human_size(5 * 1024 * 1024), "5.0 MB (5,242,880 bytes)");
    }

    #[test]
    fn default_lists_everything_when_short() {
        assert_eq!(select_variables(0, None, None), (0..0, 0..0));
        assert_eq!(select_variables(3, None, None), (0..3, 3..3));
        assert_eq!(select_variables(15, None, None), (0..15, 15..15));
    }

    #[test]
    fn default_lists_both_ends_when_long() {
        assert_eq!(select_variables(16, None, None), (0..10, 11..16));
        assert_eq!(select_variables(677, None, None), (0..10, 672..677));
    }

    #[test]
    fn head_or_tail_alone_lists_only_that_end() {
        assert_eq!(select_variables(677, Some(3), None), (0..3, 677..677));
        assert_eq!(select_variables(677, None, Some(2)), (0..0, 675..677));
        assert_eq!(select_variables(677, Some(0), None), (0..0, 677..677));
    }

    #[test]
    fn head_and_tail_together() {
        assert_eq!(select_variables(677, Some(3), Some(2)), (0..3, 675..677));
    }

    #[test]
    fn requests_larger_than_the_list_never_overlap() {
        assert_eq!(select_variables(5, Some(100), None), (0..5, 5..5));
        assert_eq!(select_variables(5, None, Some(100)), (0..0, 0..5));
        assert_eq!(select_variables(5, Some(4), Some(4)), (0..4, 4..5));
        assert_eq!(select_variables(5, Some(100), Some(100)), (0..5, 5..5));
    }
}
