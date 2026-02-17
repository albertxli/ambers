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
pub mod value_labels;
pub mod variable;

use std::fs::File;
use std::io::{BufReader, Read, Seek};
use std::path::Path;

use arrow::record_batch::RecordBatch;

use crate::constants::Compression;
use crate::error::Result;
use crate::io_utils::SavReader;

// Re-export key public types
pub use crate::constants::{Alignment, Measure};
pub use crate::metadata::{MissingSpec, MrSet, MrType, SpssMetadata, Value};

/// Read an SPSS .sav or .zsav file, returning all data as an Arrow RecordBatch
/// plus the file's metadata.
///
/// This loads the entire dataset into memory. For large files, consider
/// using `SpssReader` for streaming batch reads.
pub fn read_sav(path: impl AsRef<Path>) -> Result<(RecordBatch, SpssMetadata)> {
    let file = File::open(path)?;
    let buf_reader = BufReader::new(file);
    read_sav_from_reader(buf_reader)
}

/// Read an SPSS file from any reader that supports Read + Seek.
pub fn read_sav_from_reader<R: Read + Seek>(reader: R) -> Result<(RecordBatch, SpssMetadata)> {
    let mut sav_reader = SavReader::new(reader);

    // Parse header
    let file_header = header::FileHeader::parse(&mut sav_reader)?;

    // Parse dictionary
    let raw_dict = dictionary::parse_dictionary(&mut sav_reader, &file_header)?;
    let resolved = dictionary::resolve_dictionary(raw_dict)?;

    // Read data based on compression type
    let rows = match resolved.header.compression {
        Compression::None => data::read_uncompressed(&mut sav_reader, &resolved)?,
        Compression::Bytecode => data::read_bytecode_compressed(&mut sav_reader, &resolved)?,
        Compression::Zlib => data::read_zlib_compressed(&mut sav_reader, &resolved)?,
    };

    // Convert to Arrow
    let batch = arrow_convert::rows_to_record_batch(&rows, &resolved)?;
    let metadata = resolved.metadata;

    Ok((batch, metadata))
}

/// Read only the metadata from an SPSS file (no data).
///
/// This is much faster than `read_sav()` for files where you only need
/// variable information, labels, or other metadata.
pub fn read_sav_metadata(path: impl AsRef<Path>) -> Result<SpssMetadata> {
    let file = File::open(path)?;
    let buf_reader = BufReader::new(file);
    let mut sav_reader = SavReader::new(buf_reader);

    let file_header = header::FileHeader::parse(&mut sav_reader)?;
    let raw_dict = dictionary::parse_dictionary(&mut sav_reader, &file_header)?;
    let resolved = dictionary::resolve_dictionary(raw_dict)?;

    Ok(resolved.metadata)
}
