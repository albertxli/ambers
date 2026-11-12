//! Reads the file on a background thread, so the first screen appears at
//! once and the viewer stays responsive while the rest loads.

use std::fs::File;
use std::io::BufReader;
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};

use ambers::scanner::SavScanner;
use ambers_codebook::profile::Profiler;
use arrow::record_batch::RecordBatch;

/// Rows per batch: small enough that the first screen is immediate.
pub const BATCH_ROWS: usize = 20_000;

pub enum Message {
    Batch(RecordBatch),
    /// The scanner has no more rows (end of file, or the row limit).
    Finished,
    Failed(String),
}

pub type Scanner = SavScanner<BufReader<File>>;

/// Start reading. Each batch is counted into `profiler` (the codebook's
/// types and counts) before it is handed over, so the counts are never
/// behind the rows on screen.
///
/// The thread stops by itself when the receiver is dropped.
pub fn spawn(mut scanner: Scanner, profiler: Arc<Mutex<Profiler>>) -> Receiver<Message> {
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        loop {
            let message = match scanner.next_batch() {
                Ok(Some(batch)) => {
                    profiler
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .update(&batch);
                    Message::Batch(batch)
                }
                Ok(None) => Message::Finished,
                Err(error) => Message::Failed(error.to_string()),
            };
            let last = !matches!(message, Message::Batch(_));
            if sender.send(message).is_err() || last {
                return;
            }
        }
    });
    receiver
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    /// Load a fixture five rows at a time: (rows, batches, finished cleanly, rows counted).
    fn load(name: &str, limit: Option<usize>) -> (usize, usize, bool, u64) {
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

        let (mut rows, mut batches, mut finished) = (0, 0, false);
        for message in spawn(scanner, Arc::clone(&profiler)) {
            match message {
                Message::Batch(batch) => {
                    rows += batch.num_rows();
                    batches += 1;
                }
                Message::Finished => finished = true,
                Message::Failed(_) => {}
            }
        }
        let counted = profiler.lock().unwrap().rows();
        (rows, batches, finished, counted)
    }

    #[test]
    fn delivers_and_counts_every_row_in_batches() {
        assert_eq!(load("survey.sav", None), (20, 4, true, 20));
    }

    #[test]
    fn stops_at_the_limit() {
        assert_eq!(load("survey.sav", Some(7)), (7, 2, true, 7));
    }
}
