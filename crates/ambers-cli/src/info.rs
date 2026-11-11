//! `ambers info`: a file summary from the metadata reader, plus an optional
//! preview of the first rows.

use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use ambers::{Compression, SpssMetadata};
use anyhow::{Context, Result};
use arrow::array::{ArrayRef, AsArray, StringViewArray};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;

/// Variables listed in the summary before the rest are counted.
const MAX_VARIABLES: usize = 10;
/// Columns shown by `--head`; more would not fit a terminal.
const MAX_HEAD_COLUMNS: usize = 8;
/// Characters of a string cell shown by `--head` before it is cut.
const MAX_CELL_CHARS: usize = 40;

pub fn run(out: &mut impl Write, file: &Path, head: Option<usize>) -> Result<()> {
    let display = file.display();
    let size = std::fs::metadata(file)
        .with_context(|| format!("cannot read {display}"))?
        .len();
    let meta = ambers::read_sav_metadata(file).with_context(|| format!("cannot read {display}"))?;

    write_summary(out, &display.to_string(), size, &meta)?;
    if let Some(n) = head {
        write_head(out, file, &meta, n)?;
    }
    Ok(())
}

fn write_summary(out: &mut impl Write, path: &str, size: u64, meta: &SpssMetadata) -> Result<()> {
    let compression = match meta.compression {
        Compression::None => "uncompressed",
        Compression::Bytecode => "bytecode",
        Compression::Zlib => "zlib",
    };
    let rows = match meta.number_rows {
        Some(n) if n >= 0 => group_digits(n as u64),
        _ => "unknown (not recorded in the file header)".to_string(),
    };

    writeln!(out, "File:         {path}")?;
    writeln!(out, "Size:         {}", human_size(size))?;
    writeln!(out, "Format:       {}", meta.file_format)?;
    writeln!(out, "Compression:  {compression}")?;
    writeln!(out, "Encoding:     {}", meta.file_encoding)?;
    writeln!(out, "Created:      {}", meta.creation_time)?;
    if !meta.file_label.is_empty() {
        writeln!(out, "Label:        {}", meta.file_label)?;
    }
    writeln!(out, "Rows:         {rows}")?;
    writeln!(
        out,
        "Columns:      {}",
        group_digits(meta.number_columns as u64)
    )?;
    for warning in &meta.warnings {
        writeln!(out, "Warning:      {warning}")?;
    }

    let total = meta.variable_names.len();
    let shown = total.min(MAX_VARIABLES);
    if shown == 0 {
        return Ok(());
    }
    writeln!(out)?;
    if shown < total {
        writeln!(out, "Variables (first {shown} of {total}):")?;
    } else {
        writeln!(out, "Variables:")?;
    }
    let names = &meta.variable_names[..shown];
    let formats: Vec<&str> = names.iter().map(|n| meta.format(n).unwrap_or("")).collect();
    let name_width = names.iter().map(|n| n.chars().count()).max().unwrap_or(0);
    let format_width = formats.iter().map(|f| f.len()).max().unwrap_or(0);
    for (name, format) in names.iter().zip(&formats) {
        let label = meta.label(name).unwrap_or("");
        let line = format!("  {name:<name_width$}  {format:<format_width$}  {label}");
        writeln!(out, "{}", line.trim_end())?;
    }
    Ok(())
}

fn write_head(out: &mut impl Write, file: &Path, meta: &SpssMetadata, n: usize) -> Result<()> {
    let display = file.display();
    let total = meta.variable_names.len();
    let columns: Vec<&str> = meta
        .variable_names
        .iter()
        .take(MAX_HEAD_COLUMNS)
        .map(String::as_str)
        .collect();

    let mut scanner = ambers::scan_sav(file).with_context(|| format!("cannot read {display}"))?;
    scanner.select(&columns)?;
    scanner.limit(n);
    let batch = scanner
        .collect_single()
        .with_context(|| format!("cannot read data from {display}"))?;

    writeln!(out)?;
    if columns.len() < total {
        writeln!(
            out,
            "First {} rows (first {} of {total} columns):",
            batch.num_rows(),
            columns.len()
        )?;
    } else {
        writeln!(out, "First {} rows:", batch.num_rows())?;
    }
    writeln!(
        out,
        "{}",
        pretty_format_batches(&[shorten_strings(&batch)?])?
    )?;
    Ok(())
}

/// Cut string cells to `MAX_CELL_CHARS` so one long open-ended answer does
/// not stretch the preview table past the terminal.
fn shorten_strings(batch: &RecordBatch) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|column| match column.as_string_view_opt() {
            Some(strings) => Arc::new(
                strings
                    .iter()
                    .map(|cell| cell.map(shorten))
                    .collect::<StringViewArray>(),
            ) as ArrayRef,
            None => Arc::clone(column),
        })
        .collect();
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

fn shorten(cell: &str) -> String {
    match cell.char_indices().nth(MAX_CELL_CHARS) {
        Some((cut, _)) => format!("{}…", &cell[..cut]),
        None => cell.to_string(),
    }
}

/// `1234567` -> `1,234,567`.
fn group_digits(n: u64) -> String {
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
}
