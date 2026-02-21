use std::io::{Read, Seek};

use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::arrow_convert;
use crate::columnar::ColumnarBatchBuilder;
use crate::compression::bytecode::BytecodeDecompressor;
use crate::compression::zlib;
use crate::constants::Compression;
use crate::dictionary::{self, ResolvedDictionary};
use crate::error::{Result, SpssError};
use crate::header;
use crate::io_utils::SavReader;
use crate::metadata::SpssMetadata;

/// Compression-specific state for the scanner.
enum ScanState {
    Uncompressed,
    Bytecode {
        data: Vec<u8>,
        decompressor: BytecodeDecompressor,
    },
    Zlib {
        data: Vec<u8>,
        decompressor: BytecodeDecompressor,
    },
}

/// A streaming reader for SPSS .sav/.zsav files.
///
/// Reads metadata immediately on construction. Data is read on demand
/// via `next_batch()` or `collect_single()`. Supports column projection
/// and row limits.
pub struct SavScanner<R: Read + Seek> {
    sav_reader: SavReader<R>,
    dict: ResolvedDictionary,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    row_limit: Option<usize>,
    rows_read: usize,
    state: ScanState,
    eof: bool,
}

impl<R: Read + Seek> SavScanner<R> {
    /// Open a scanner from a reader. Parses the header and dictionary immediately.
    pub fn open(reader: R, batch_size: usize) -> Result<Self> {
        let mut sav_reader = SavReader::new(reader);

        let file_header = header::FileHeader::parse(&mut sav_reader)?;
        let raw_dict = dictionary::parse_dictionary(&mut sav_reader, &file_header)?;
        let compression = raw_dict.header.compression;
        let bias = raw_dict.header.bias;
        let slots_per_row = raw_dict.header.nominal_case_size as usize;
        let ncases = if raw_dict.header.ncases >= 0 {
            Some(raw_dict.header.ncases as usize)
        } else {
            None
        };
        let dict = dictionary::resolve_dictionary(raw_dict)?;

        // Set up compression-specific state
        let state = match compression {
            Compression::None => ScanState::Uncompressed,
            Compression::Bytecode => {
                let estimated_size = ncases.unwrap_or(1000) * slots_per_row * 8;
                let mut compressed_data = Vec::with_capacity(estimated_size);
                sav_reader.inner_mut().read_to_end(&mut compressed_data)?;
                ScanState::Bytecode {
                    data: compressed_data,
                    decompressor: BytecodeDecompressor::new(bias),
                }
            }
            Compression::Zlib => {
                let zheader = zlib::read_zheader(&mut sav_reader)?;
                let ztrailer = zlib::read_ztrailer(&mut sav_reader, &zheader)?;
                let bytecode_data = zlib::decompress_zsav_blocks(&mut sav_reader, &ztrailer)?;
                ScanState::Zlib {
                    data: bytecode_data,
                    decompressor: BytecodeDecompressor::new(bias),
                }
            }
        };

        Ok(SavScanner {
            sav_reader,
            dict,
            batch_size,
            projection: None,
            row_limit: None,
            rows_read: 0,
            state,
            eof: false,
        })
    }

    /// Get a reference to the file metadata.
    pub fn metadata(&self) -> &SpssMetadata {
        &self.dict.metadata
    }

    /// Get the Arrow schema (respects column projection if set).
    pub fn schema(&self) -> Schema {
        if let Some(ref proj) = self.projection {
            let fields: Vec<Field> = proj
                .iter()
                .map(|&idx| {
                    let var = &self.dict.variables[idx];
                    Field::new(&var.long_name, arrow_convert::var_to_arrow_type(var), true)
                })
                .collect();
            Schema::new(fields)
        } else {
            arrow_convert::build_schema(&self.dict)
        }
    }

    /// Set column projection — only these columns will be read and returned.
    /// Returns an error if any column name is not found.
    pub fn select(&mut self, columns: &[&str]) -> Result<()> {
        let mut indices = Vec::with_capacity(columns.len());
        for &col in columns {
            let idx = self
                .dict
                .variables
                .iter()
                .position(|v| v.long_name == col)
                .ok_or_else(|| {
                    SpssError::InvalidVariable(format!("column not found: {col:?}"))
                })?;
            indices.push(idx);
        }
        self.projection = Some(indices);
        Ok(())
    }

    /// Set a row limit — stop reading after this many rows.
    pub fn limit(&mut self, n: usize) {
        self.row_limit = Some(n);
    }

    /// Read the next batch of rows, returning a RecordBatch.
    /// Returns Ok(None) when no more data is available.
    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.eof {
            return Ok(None);
        }

        // Calculate how many rows to read this batch
        let remaining = match self.row_limit {
            Some(limit) if self.rows_read >= limit => return Ok(None),
            Some(limit) => limit - self.rows_read,
            None => usize::MAX,
        };
        let n_rows = remaining.min(self.batch_size);

        let batch = self.read_batch_columnar(n_rows)?;
        match batch {
            Some(ref b) => {
                let num_rows = b.num_rows();
                if num_rows == 0 {
                    self.eof = true;
                    return Ok(None);
                }
                self.rows_read += num_rows;
            }
            None => {
                self.eof = true;
                return Ok(None);
            }
        }

        Ok(batch)
    }

    /// Read all remaining data as a single RecordBatch.
    pub fn collect_single(&mut self) -> Result<RecordBatch> {
        let remaining = match self.row_limit {
            Some(limit) if self.rows_read >= limit => 0,
            Some(limit) => limit - self.rows_read,
            None => usize::MAX,
        };

        match self.read_batch_columnar(remaining)? {
            Some(batch) => {
                self.rows_read += batch.num_rows();
                self.eof = true;
                Ok(batch)
            }
            None => {
                self.eof = true;
                let schema = if let Some(ref proj) = self.projection {
                    let fields: Vec<Field> = proj
                        .iter()
                        .map(|&idx| {
                            let var = &self.dict.variables[idx];
                            Field::new(
                                &var.long_name,
                                arrow_convert::var_to_arrow_type(var),
                                true,
                            )
                        })
                        .collect();
                    Schema::new(fields)
                } else {
                    arrow_convert::build_schema(&self.dict)
                };
                Ok(RecordBatch::new_empty(std::sync::Arc::new(schema)))
            }
        }
    }

    /// Read all remaining data as a Vec of RecordBatches.
    pub fn collect_all(&mut self) -> Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        while let Some(batch) = self.next_batch()? {
            batches.push(batch);
        }
        Ok(batches)
    }

    /// How many rows have been read so far.
    pub fn rows_read(&self) -> usize {
        self.rows_read
    }

    /// Reasonable capacity hint, avoiding usize::MAX overflow.
    fn capacity_hint(&self, n: usize) -> usize {
        let ncases = if self.dict.header.ncases >= 0 {
            self.dict.header.ncases as usize
        } else {
            1000
        };
        n.min(ncases).min(1_000_000)
    }

    /// Read up to `n` rows directly into a columnar Arrow RecordBatch.
    fn read_batch_columnar(&mut self, n: usize) -> Result<Option<RecordBatch>> {
        if n == 0 {
            return Ok(None);
        }

        let cap = self.capacity_hint(n);
        let mut builder = ColumnarBatchBuilder::new(
            &self.dict,
            self.projection.as_deref(),
            cap,
        );

        match &mut self.state {
            ScanState::Uncompressed => {
                let slots_per_row = self.dict.header.nominal_case_size as usize;
                let row_bytes = slots_per_row * 8;
                // Cap chunk size to ~256 MB for better cache behavior on large files.
                // This avoids multi-GB allocations and keeps the working set manageable
                // for L3 cache across multiple push_raw_chunk iterations.
                // Small files still read in one chunk via capacity_hint.
                let max_chunk_rows = (256 * 1024 * 1024 / row_bytes).max(1024);
                let chunk_rows = self.capacity_hint(n).min(max_chunk_rows);
                let chunk_bytes = chunk_rows * row_bytes;
                // SAFETY: Buffer is immediately filled by read_full(). Uninitialized
                // bytes never reach the builder — actual_rows check at line 275
                // ensures we only process fully-read rows.
                let mut chunk_buf = Vec::with_capacity(chunk_bytes);
                unsafe { chunk_buf.set_len(chunk_bytes); }

                let mut rows_remaining = n;
                while rows_remaining > 0 {
                    let to_read = chunk_rows.min(rows_remaining);
                    let read_bytes = to_read * row_bytes;
                    let actual = read_full(&mut self.sav_reader, &mut chunk_buf[..read_bytes])?;
                    let actual_rows = actual / row_bytes;
                    if actual_rows == 0 {
                        break;
                    }

                    // Process chunk column-at-a-time for better cache locality
                    let chunk_data = &chunk_buf[..actual_rows * row_bytes];
                    builder.push_raw_chunk(chunk_data, actual_rows, slots_per_row);
                    rows_remaining -= actual_rows;
                    if actual_rows < to_read {
                        break; // EOF
                    }
                }
            }
            ScanState::Bytecode { data, decompressor }
            | ScanState::Zlib { data, decompressor } => {
                let slots_per_row = self.dict.header.nominal_case_size as usize;
                let data_ref = data as &[u8];
                let mut slots = Vec::with_capacity(slots_per_row);

                for _ in 0..n {
                    decompressor.decompress_row(data_ref, slots_per_row, &mut slots)?;
                    if slots.is_empty() || slots.len() < slots_per_row {
                        break;
                    }
                    builder.push_slot_row(&slots);
                }
            }
        }

        if builder.len() > 0 {
            Ok(Some(builder.finish()?))
        } else {
            Ok(None)
        }
    }
}

/// Read as many bytes as possible into `buf`, handling partial reads.
/// Returns the total number of bytes read (may be less than buf.len() at EOF).
fn read_full<R: Read + Seek>(reader: &mut SavReader<R>, buf: &mut [u8]) -> Result<usize> {
    let mut pos = 0;
    while pos < buf.len() {
        match reader.inner_mut().read(&mut buf[pos..]) {
            Ok(0) => break, // EOF
            Ok(n) => pos += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(pos)
}
