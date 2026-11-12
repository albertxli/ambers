//! Reads the file on a background thread, so the first screen appears at
//! once and the viewer stays responsive while the rest loads.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};

use ambers::scanner::SavScanner;
use ambers_codebook::profile::Profiler;
use arrow::record_batch::RecordBatch;

/// Cells per batch. A batch is sized by cells, not rows, so a file with
/// hundreds of columns still shows its first screen at once and arrives
/// in small steps.
const BATCH_CELLS: usize = 2_000_000;

/// Rows per batch for a file with this many columns.
pub fn batch_rows(columns: usize) -> usize {
    (BATCH_CELLS / columns.max(1)).clamp(1_000, 20_000)
}

pub enum Message {
    Batch(RecordBatch),
    Finished(End),
    Failed(String),
}

/// Why loading stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum End {
    /// The scanner has no more rows: the end of the file, or its row limit.
    Data,
    /// The loaded rows reached the memory budget.
    MemoryBudget,
}

pub type Scanner = SavScanner<BufReader<File>>;

/// Open a file for loading: the scanner and the rows per batch it uses.
///
/// A scanner's batch size is fixed when it opens, and the right size
/// depends on the number of columns, which only the dictionary gives. So
/// the dictionary is read first (it is small) and the file opened again.
pub fn open(file: &Path, limit: Option<usize>) -> ambers::error::Result<(Scanner, usize)> {
    let rows = batch_rows(ambers::read_sav_metadata(file)?.number_columns);
    let reader = BufReader::with_capacity(8 * 1024 * 1024, File::open(file)?);
    let mut scanner = SavScanner::open(reader, rows)?;
    if let Some(limit) = limit {
        scanner.limit(limit);
    }
    Ok((scanner, rows))
}

/// Start reading. Each batch is counted into the loader's own profiler
/// (the codebook's types and counts), and a copy is published for the
/// screen before the batch is handed over. The lock is held only to swap
/// the copy in, so the screen never waits while a batch is counted.
///
/// Loading stops when the Arrow memory of the batches passes `budget`
/// bytes. The thread also stops by itself when the receiver is dropped.
pub fn spawn(
    mut scanner: Scanner,
    published: Arc<Mutex<Profiler>>,
    budget: Option<u64>,
) -> Receiver<Message> {
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        // A poisoned lock still holds a usable profiler.
        let mut profiler = published.lock().unwrap_or_else(|p| p.into_inner()).clone();
        let mut loaded = 0u64;
        loop {
            let message = match scanner.next_batch() {
                Ok(Some(batch)) => {
                    profiler.update(&batch);
                    *published.lock().unwrap_or_else(|p| p.into_inner()) = profiler.clone();
                    loaded += batch.get_array_memory_size() as u64;
                    Message::Batch(batch)
                }
                Ok(None) => Message::Finished(End::Data),
                Err(error) => Message::Failed(error.to_string()),
            };
            let last = !matches!(message, Message::Batch(_));
            if sender.send(message).is_err() || last {
                return;
            }
            if budget.is_some_and(|budget| loaded >= budget) {
                let _ = sender.send(Message::Finished(End::MemoryBudget));
                return;
            }
        }
    });
    receiver
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Load a fixture five rows at a time: (rows, batches, how it ended, rows counted).
    fn load(
        name: &str,
        limit: Option<usize>,
        budget: Option<u64>,
    ) -> (usize, usize, Option<End>, u64) {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/data")
            .join(name);
        let reader = BufReader::new(File::open(path).unwrap());
        let mut scanner = SavScanner::open(reader, 5).unwrap();
        if let Some(limit) = limit {
            scanner.limit(limit);
        }
        let profiler = Arc::new(Mutex::new(Profiler::new(
            scanner.metadata(),
            &scanner.schema(),
        )));

        let (mut rows, mut batches, mut end) = (0, 0, None);
        for message in spawn(scanner, Arc::clone(&profiler), budget) {
            match message {
                Message::Batch(batch) => {
                    rows += batch.num_rows();
                    batches += 1;
                }
                Message::Finished(how) => end = Some(how),
                Message::Failed(_) => {}
            }
        }
        let counted = profiler.lock().unwrap().rows();
        (rows, batches, end, counted)
    }

    #[test]
    fn delivers_and_counts_every_row_in_batches() {
        assert_eq!(load("survey.sav", None, None), (20, 4, Some(End::Data), 20));
    }

    #[test]
    fn stops_at_the_row_limit() {
        assert_eq!(
            load("survey.sav", Some(7), None),
            (7, 2, Some(End::Data), 7)
        );
    }

    #[test]
    fn stops_when_the_memory_budget_is_reached() {
        // One byte: the first batch is already over it.
        assert_eq!(
            load("survey.sav", None, Some(1)),
            (5, 1, Some(End::MemoryBudget), 5)
        );
        // A budget nothing reaches changes nothing.
        assert_eq!(
            load("survey.sav", None, Some(u64::MAX)),
            (20, 4, Some(End::Data), 20)
        );
    }

    #[test]
    fn batches_are_sized_by_cells() {
        // Narrow files: the cap on rows. Wide files: fewer rows per batch.
        assert_eq!(batch_rows(13), 20_000);
        assert_eq!(batch_rows(100), 20_000);
        assert_eq!(batch_rows(916), 2_183);
        // Very wide files still move a useful number of rows.
        assert_eq!(batch_rows(5_000), 1_000);
        assert_eq!(batch_rows(0), 20_000);
    }
}
