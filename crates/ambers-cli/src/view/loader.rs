//! Reads the file on a background thread, so the first screen appears at
//! once and the viewer stays responsive while the rest loads.

use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;
use std::sync::mpsc::{self, Receiver};

use ambers::scanner::SavScanner;
use arrow::array::AsArray;
use arrow::datatypes::Float64Type;
use arrow::record_batch::RecordBatch;

/// Rows per batch: small enough that the first screen is immediate.
pub const BATCH_ROWS: usize = 20_000;
/// Distinct values tracked per audited variable. Variables with value
/// labels have few; one with more is reported as having "many".
pub const DISTINCT_CAP: usize = 1_000;

pub enum Message {
    Batch {
        batch: RecordBatch,
        /// For the label audit: values not seen before in earlier batches,
        /// as (column index, values).
        new_values: Vec<(usize, Vec<f64>)>,
    },
    /// The scanner has no more rows (end of file, or the row limit).
    Finished,
    Failed(String),
}

pub type Scanner = SavScanner<BufReader<File>>;

/// Start reading. `audited` are the indices of the numeric columns that
/// have value labels; their distinct values are collected for the audit.
///
/// The thread stops by itself when the receiver is dropped.
pub fn spawn(mut scanner: Scanner, audited: Vec<usize>) -> Receiver<Message> {
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let mut seen: Vec<HashSet<u64>> = vec![HashSet::new(); audited.len()];
        loop {
            let message = match scanner.next_batch() {
                Ok(Some(batch)) => {
                    let new_values = new_values(&batch, &audited, &mut seen);
                    Message::Batch { batch, new_values }
                }
                Ok(None) => Message::Finished,
                Err(error) => Message::Failed(error.to_string()),
            };
            let last = !matches!(message, Message::Batch { .. });
            if sender.send(message).is_err() || last {
                return;
            }
        }
    });
    receiver
}

fn new_values(
    batch: &RecordBatch,
    audited: &[usize],
    seen: &mut [HashSet<u64>],
) -> Vec<(usize, Vec<f64>)> {
    let mut found = Vec::new();
    for (&column, seen) in audited.iter().zip(seen) {
        // One past the cap is enough for the receiver to see "more than the cap".
        if seen.len() > DISTINCT_CAP {
            continue;
        }
        let values = batch.column(column).as_primitive::<Float64Type>();
        let mut fresh = Vec::new();
        let mut previous = None;
        for value in values.iter().flatten() {
            let bits = value.to_bits();
            // Survey columns repeat values in runs; skip the set lookup then.
            if previous == Some(bits) {
                continue;
            }
            previous = Some(bits);
            if seen.len() <= DISTINCT_CAP && seen.insert(bits) {
                fresh.push(value);
            }
        }
        if !fresh.is_empty() {
            found.push((column, fresh));
        }
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn open(name: &str, limit: Option<usize>) -> Scanner {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/data")
            .join(name);
        let reader = BufReader::new(File::open(path).unwrap());
        let mut scanner = SavScanner::open(reader, 5).unwrap();
        if let Some(limit) = limit {
            scanner.limit(limit);
        }
        scanner
    }

    /// Drain the loader: (rows, batches, new values per column, finished cleanly).
    fn drain(receiver: Receiver<Message>) -> (usize, usize, Vec<(usize, Vec<f64>)>, bool) {
        let (mut rows, mut batches, mut values) = (0, 0, Vec::new());
        for message in receiver {
            match message {
                Message::Batch { batch, new_values } => {
                    rows += batch.num_rows();
                    batches += 1;
                    values.extend(new_values);
                }
                Message::Finished => return (rows, batches, values, true),
                Message::Failed(_) => return (rows, batches, values, false),
            }
        }
        (rows, batches, values, false)
    }

    #[test]
    fn delivers_every_row_in_batches() {
        let (rows, batches, _, finished) = drain(spawn(open("survey.sav", None), vec![]));
        assert_eq!((rows, batches, finished), (20, 4, true));
    }

    #[test]
    fn stops_at_the_limit() {
        let (rows, _, _, finished) = drain(spawn(open("survey.sav", Some(7)), vec![]));
        assert_eq!((rows, finished), (7, true));
    }

    #[test]
    fn each_distinct_value_is_reported_once() {
        // Column 1 is q1: 1, 2, 3, 7, 1, 99, null, 8, 2, 3, 7, 1.
        let (_, _, values, _) = drain(spawn(open("imperfect_labels.sav", None), vec![1]));
        assert!(values.iter().all(|(column, _)| *column == 1));
        let mut all: Vec<f64> = values.into_iter().flat_map(|(_, v)| v).collect();
        all.sort_by(f64::total_cmp);
        assert_eq!(all, [1.0, 2.0, 3.0, 7.0, 8.0, 99.0]);
    }

    #[test]
    fn tracking_stops_past_the_cap() {
        let values: Vec<f64> = (0..3_000).map(f64::from).collect();
        let batch = RecordBatch::try_from_iter([(
            "x",
            std::sync::Arc::new(arrow::array::Float64Array::from(values)) as arrow::array::ArrayRef,
        )])
        .unwrap();
        let mut seen = vec![HashSet::new()];
        let first = new_values(&batch, &[0], &mut seen);
        assert_eq!(first[0].1.len(), DISTINCT_CAP + 1);
        assert!(new_values(&batch, &[0], &mut seen).is_empty());
    }
}
