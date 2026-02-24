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
        /// Trailer for on-demand block decompression.
        ztrailer: zlib::ZTrailer,
        /// Index of next block to decompress (blocks before this are already consumed).
        next_block_idx: usize,
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

                // Stream: decompress only the first block on demand instead of all blocks.
                let first_block = if !ztrailer.entries.is_empty() {
                    zlib::decompress_single_block(&mut sav_reader, &ztrailer.entries[0])?
                } else {
                    Vec::new()
                };

                ScanState::Zlib {
                    data: first_block,
                    decompressor: BytecodeDecompressor::new(bias),
                    ztrailer,
                    next_block_idx: 1,
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
                .ok_or_else(|| SpssError::InvalidVariable(format!("column not found: {col:?}")))?;
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
                            Field::new(&var.long_name, arrow_convert::var_to_arrow_type(var), true)
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
        let mut builder = ColumnarBatchBuilder::new(&self.dict, self.projection.as_deref(), cap);
        let slots_per_row = self.dict.header.nominal_case_size as usize;

        // Split borrows: state and sav_reader are independent fields of SavScanner.
        let state = &mut self.state;
        let sav_reader = &mut self.sav_reader;

        match state {
            ScanState::Uncompressed => {
                let row_bytes = slots_per_row * 8;
                // Cap chunk size to ~256 MB for better cache behavior on large files.
                // This avoids multi-GB allocations and keeps the working set manageable
                // for L3 cache across multiple push_raw_chunk iterations.
                // Small files still read in one chunk via cap (pre-computed capacity_hint).
                let max_chunk_rows = (256 * 1024 * 1024 / row_bytes).max(1024);
                let chunk_rows = cap.min(max_chunk_rows);
                let chunk_bytes = chunk_rows * row_bytes;
                let mut chunk_buf = vec![0u8; chunk_bytes];

                let mut rows_remaining = n;
                while rows_remaining > 0 {
                    let to_read = chunk_rows.min(rows_remaining);
                    let read_bytes = to_read * row_bytes;
                    let actual = read_full(sav_reader, &mut chunk_buf[..read_bytes])?;
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
            ScanState::Bytecode { data, decompressor } => {
                let row_bytes = slots_per_row * 8;
                let data_ref = data as &[u8];

                // Decompress directly into raw byte buffer (no SlotValue intermediates),
                // then process column-at-a-time via push_raw_chunk with rayon parallelism.
                let max_chunk_rows = (256 * 1024 * 1024 / row_bytes).max(1024);
                let chunk_rows = cap.min(max_chunk_rows);
                let chunk_bytes = chunk_rows * row_bytes;
                let mut raw_buf = vec![0u8; chunk_bytes];

                let mut rows_in_batch = 0;
                for _ in 0..n {
                    let out_offset = rows_in_batch * row_bytes;
                    let ok = decompressor.decompress_row_raw(
                        data_ref,
                        slots_per_row,
                        &mut raw_buf,
                        out_offset,
                    )?;
                    if !ok {
                        break;
                    }
                    rows_in_batch += 1;

                    if rows_in_batch >= chunk_rows {
                        builder.push_raw_chunk(
                            &raw_buf[..rows_in_batch * row_bytes],
                            rows_in_batch,
                            slots_per_row,
                        );
                        rows_in_batch = 0;
                    }
                }

                // Flush remaining rows
                if rows_in_batch > 0 {
                    builder.push_raw_chunk(
                        &raw_buf[..rows_in_batch * row_bytes],
                        rows_in_batch,
                        slots_per_row,
                    );
                }
            }
            ScanState::Zlib {
                data,
                decompressor,
                ztrailer,
                next_block_idx,
            } => {
                let row_bytes = slots_per_row * 8;

                // Decompress directly into raw byte buffer (no SlotValue intermediates),
                // then process column-at-a-time via push_raw_chunk with rayon parallelism.
                // On-demand: when the bytecode decompressor exhausts the current zlib
                // block buffer, shift unconsumed bytes, decompress the next block, and
                // continue. This avoids loading all decompressed blocks into memory.
                //
                // The decompressor can exhaust its buffer in two ways:
                // 1. Ok(false) at control block boundary (clean: not enough for 8-byte block)
                // 2. Err(TruncatedFile) mid-row (dirty: COMPRESS_RAW_FOLLOWS but < 8 bytes)
                // Both trigger loading the next block. For case 2, we use checkpoint/restore
                // to roll back the decompressor state before retrying.
                let max_chunk_rows = (256 * 1024 * 1024 / row_bytes).max(1024);
                let chunk_rows = cap.min(max_chunk_rows);
                let chunk_bytes = chunk_rows * row_bytes;
                let mut raw_buf = vec![0u8; chunk_bytes];

                let mut rows_in_batch = 0;
                for _ in 0..n {
                    let out_offset = rows_in_batch * row_bytes;

                    // Save decompressor state before the row attempt so we can
                    // roll back if the buffer is exhausted mid-row.
                    let cp = decompressor.checkpoint();

                    let result = decompressor.decompress_row_raw(
                        data.as_slice(),
                        slots_per_row,
                        &mut raw_buf,
                        out_offset,
                    );

                    let needs_more = match &result {
                        Ok(true) => false,                            // Row completed successfully
                        Ok(false) if decompressor.is_eof() => break,  // True EOF marker seen
                        Ok(false) => true, // Buffer exhausted at clean boundary
                        Err(SpssError::TruncatedFile { .. }) => true, // Buffer exhausted mid-row
                        Err(_) => return result.map(|_| None), // Propagate real errors
                    };

                    if needs_more {
                        // Restore decompressor to pre-row state
                        decompressor.restore(cp);

                        if *next_block_idx < ztrailer.entries.len() {
                            // Shift unconsumed bytes to front
                            let consumed = decompressor.pos();
                            let remaining = data.len() - consumed;
                            data.copy_within(consumed.., 0);
                            data.truncate(remaining);

                            // Decompress next block and append
                            let next_data = zlib::decompress_single_block(
                                sav_reader,
                                &ztrailer.entries[*next_block_idx],
                            )?;
                            data.extend_from_slice(&next_data);
                            *next_block_idx += 1;

                            // Reset decompressor position to start of unconsumed data
                            decompressor.set_pos(0);

                            // Retry the row with the expanded buffer
                            let ok = decompressor.decompress_row_raw(
                                data.as_slice(),
                                slots_per_row,
                                &mut raw_buf,
                                out_offset,
                            )?;
                            if !ok {
                                break; // True EOF after loading more data
                            }
                        } else {
                            break; // No more blocks available
                        }
                    }

                    rows_in_batch += 1;

                    if rows_in_batch >= chunk_rows {
                        builder.push_raw_chunk(
                            &raw_buf[..rows_in_batch * row_bytes],
                            rows_in_batch,
                            slots_per_row,
                        );
                        rows_in_batch = 0;
                    }
                }

                // Flush remaining rows
                if rows_in_batch > 0 {
                    builder.push_raw_chunk(
                        &raw_buf[..rows_in_batch * row_bytes],
                        rows_in_batch,
                        slots_per_row,
                    );
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Builder, StringBuilder};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use std::io::Cursor;
    use std::sync::Arc;

    /// Create a simple in-memory SAV file with 3 columns and `n` rows.
    fn make_sav_bytes(n: usize) -> Vec<u8> {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("age", DataType::Float64, true),
            Field::new("score", DataType::Float64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let mut age_b = Float64Builder::with_capacity(n);
        let mut score_b = Float64Builder::with_capacity(n);
        let mut name_b = StringBuilder::new();
        for i in 0..n {
            age_b.append_value(i as f64);
            score_b.append_value((i as f64) * 1.5);
            name_b.append_value(format!("person_{i}"));
        }
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(age_b.finish()),
                Arc::new(score_b.finish()),
                Arc::new(name_b.finish()),
            ],
        )
        .unwrap();
        let meta = SpssMetadata::from_arrow_schema(batch.schema().as_ref());
        let mut buf = Cursor::new(Vec::new());
        crate::write_sav_to_writer(&mut buf, &batch, &meta, Compression::None, None).unwrap();
        buf.into_inner()
    }

    #[test]
    fn test_select_valid_columns() {
        let data = make_sav_bytes(5);
        let mut scanner = SavScanner::open(Cursor::new(data), 100).unwrap();
        assert!(scanner.select(&["age", "name"]).is_ok());
    }

    #[test]
    fn test_select_invalid_column_errors() {
        let data = make_sav_bytes(5);
        let mut scanner = SavScanner::open(Cursor::new(data), 100).unwrap();
        let result = scanner.select(&["nonexistent"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_limit_caps_rows() {
        let data = make_sav_bytes(10);
        let mut scanner = SavScanner::open(Cursor::new(data), 100).unwrap();
        scanner.limit(3);
        let batch = scanner.collect_single().unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn test_schema_with_projection() {
        let data = make_sav_bytes(5);
        let mut scanner = SavScanner::open(Cursor::new(data), 100).unwrap();
        scanner.select(&["age", "name"]).unwrap();
        let schema = scanner.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "age");
        assert_eq!(schema.field(1).name(), "name");
    }

    #[test]
    fn test_schema_without_projection() {
        let data = make_sav_bytes(5);
        let scanner = SavScanner::open(Cursor::new(data), 100).unwrap();
        let schema = scanner.schema();
        assert_eq!(schema.fields().len(), 3);
    }

    #[test]
    fn test_collect_all_batches() {
        let data = make_sav_bytes(10);
        let mut scanner = SavScanner::open(Cursor::new(data), 3).unwrap();
        let batches = scanner.collect_all().unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 10);
        assert!(batches.len() >= 3); // At least 3 batches for 10 rows with batch_size=3
    }

    #[test]
    fn test_rows_read_counter() {
        let data = make_sav_bytes(7);
        let mut scanner = SavScanner::open(Cursor::new(data), 100).unwrap();
        let batch = scanner.collect_single().unwrap();
        assert_eq!(scanner.rows_read(), batch.num_rows());
        assert_eq!(scanner.rows_read(), 7);
    }
}
