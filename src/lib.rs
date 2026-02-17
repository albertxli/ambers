//! ambers: Pure Rust reader for SPSS .sav and .zsav files.
//!
//! This library reads SPSS SAV/ZSAV files natively in Rust with no C dependencies.
//! Data is returned as Apache Arrow RecordBatch for seamless integration with
//! Polars, DataFusion, and other Arrow-compatible tools.
//!
//! # Quick Start
//!
//! ```no_run
//! use ambers::read_sav;
//!
//! let (batch, meta) = read_sav("survey.sav").unwrap();
//! println!("Rows: {}", batch.num_rows());
//! println!("Columns: {}", batch.num_columns());
//! meta.summary();
//! ```

pub mod arrow_convert;
pub mod compression;
pub mod constants;
pub mod data;
pub mod dictionary;
pub mod document;
pub mod encoding;
pub mod error;
pub mod header;
pub mod info_records;
pub mod io_utils;
pub mod metadata;
pub mod scanner;
pub mod value_labels;
pub mod variable;

use std::fs::File;
use std::io::{BufReader, Read, Seek};
use std::path::Path;

use arrow::record_batch::RecordBatch;

use crate::error::Result;
use crate::scanner::SavScanner;

// Re-export key public types
pub use crate::constants::{Alignment, Measure};
pub use crate::metadata::{MissingSpec, MrSet, MrType, SpssMetadata, Value};
pub use crate::scanner::SavScanner as Scanner;

/// Read an SPSS .sav or .zsav file, returning all data as an Arrow RecordBatch
/// plus the file's metadata.
///
/// This loads the entire dataset into memory. For streaming batch reads or
/// column projection, use `scan_sav()` instead.
pub fn read_sav(path: impl AsRef<Path>) -> Result<(RecordBatch, SpssMetadata)> {
    let mut scanner = scan_sav(path)?;
    let metadata = scanner.metadata().clone();
    let batch = scanner.collect_single()?;
    Ok((batch, metadata))
}

/// Read an SPSS file from any reader that supports Read + Seek.
pub fn read_sav_from_reader<R: Read + Seek>(reader: R) -> Result<(RecordBatch, SpssMetadata)> {
    let mut scanner = scan_sav_from_reader(reader, usize::MAX)?;
    let metadata = scanner.metadata().clone();
    let batch = scanner.collect_single()?;
    Ok((batch, metadata))
}

/// Read only the metadata from an SPSS file (no data).
///
/// This is much faster than `read_sav()` for files where you only need
/// variable information, labels, or other metadata.
pub fn read_sav_metadata(path: impl AsRef<Path>) -> Result<SpssMetadata> {
    let file = File::open(path)?;
    let buf_reader = BufReader::with_capacity(256 * 1024, file);
    let scanner = SavScanner::open(buf_reader, 0)?;
    Ok(scanner.metadata().clone())
}

/// Create a streaming scanner for an SPSS .sav or .zsav file.
///
/// Reads metadata immediately. Data is read on demand via `next_batch()`
/// or `collect_single()`. Supports column projection via `select()` and
/// row limits via `limit()`.
///
/// Default batch size: 100,000 rows.
///
/// # Example
/// ```no_run
/// let mut scanner = ambers::scan_sav("survey.sav").unwrap();
/// scanner.select(&["age", "gender"]).unwrap();
/// scanner.limit(1000);
/// while let Some(batch) = scanner.next_batch().unwrap() {
///     println!("Batch: {} rows", batch.num_rows());
/// }
/// ```
pub fn scan_sav(path: impl AsRef<Path>) -> Result<SavScanner<BufReader<File>>> {
    let file = File::open(path)?;
    let buf_reader = BufReader::with_capacity(256 * 1024, file);
    SavScanner::open(buf_reader, 100_000)
}

/// Create a streaming scanner from any Read+Seek source.
pub fn scan_sav_from_reader<R: Read + Seek>(
    reader: R,
    batch_size: usize,
) -> Result<SavScanner<R>> {
    SavScanner::open(reader, batch_size)
}
