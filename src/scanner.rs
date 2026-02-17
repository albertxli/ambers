use std::io::{Read, Seek};

use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rayon::prelude::*;

use crate::arrow_convert;
use crate::compression::bytecode::BytecodeDecompressor;
use crate::compression::zlib;
use crate::constants::Compression;
use crate::data::{self, CellValue};
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
                    let data_type = match &var.var_type {
                        crate::constants::VarType::Numeric => DataType::Float64,
                        crate::constants::VarType::String(_) => DataType::Utf8,
                    };
                    Field::new(&var.long_name, data_type, true)
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

        let rows = self.read_n_rows(n_rows)?;
        if rows.is_empty() {
            self.eof = true;
            return Ok(None);
        }
        self.rows_read += rows.len();

        let batch = if let Some(ref proj) = self.projection {
            arrow_convert::rows_to_record_batch_projected(&rows, &self.dict, proj)?
        } else {
            arrow_convert::rows_to_record_batch(&rows, &self.dict)?
        };

        Ok(Some(batch))
    }

    /// Read all remaining data as a single RecordBatch.
    pub fn collect_single(&mut self) -> Result<RecordBatch> {
        let remaining = match self.row_limit {
            Some(limit) if self.rows_read >= limit => 0,
            Some(limit) => limit - self.rows_read,
            None => usize::MAX,
        };

        let rows = self.read_n_rows(remaining)?;
        self.rows_read += rows.len();
        self.eof = true;

        if let Some(ref proj) = self.projection {
            arrow_convert::rows_to_record_batch_projected(&rows, &self.dict, proj)
        } else {
            arrow_convert::rows_to_record_batch(&rows, &self.dict)
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

    /// Read up to `n` rows from the data section, applying projection.
    fn read_n_rows(&mut self, n: usize) -> Result<Vec<Vec<CellValue>>> {
        if n == 0 {
            return Ok(Vec::new());
        }

        match &mut self.state {
            ScanState::Uncompressed => self.read_uncompressed_rows(n),
            ScanState::Bytecode { .. } | ScanState::Zlib { .. } => {
                self.read_compressed_rows(n)
            }
        }
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

    /// Read rows from an uncompressed SAV file.
    fn read_uncompressed_rows(&mut self, n: usize) -> Result<Vec<Vec<CellValue>>> {
        let slots_per_row = self.dict.header.nominal_case_size as usize;
        let mut all_rows = Vec::with_capacity(self.capacity_hint(n));

        for _ in 0..n {
            let mut raw_slots = Vec::with_capacity(slots_per_row);
            for _ in 0..slots_per_row {
                match self.sav_reader.read_8_bytes() {
                    Ok(bytes) => raw_slots.push(bytes),
                    Err(_) => {
                        if raw_slots.is_empty() {
                            return Ok(all_rows);
                        } else {
                            return Err(SpssError::TruncatedFile {
                                expected: slots_per_row * 8,
                                actual: raw_slots.len() * 8,
                            });
                        }
                    }
                }
            }

            let row = if let Some(ref proj) = self.projection {
                data::slots_to_row_projected(
                    &raw_slots,
                    &self.dict.all_slots,
                    &self.dict.variables,
                    proj,
                    self.dict.file_encoding,
                )?
            } else {
                data::slots_to_row(
                    &raw_slots,
                    &self.dict.all_slots,
                    &self.dict.variables,
                    self.dict.file_encoding,
                )?
            };
            all_rows.push(row);
        }

        Ok(all_rows)
    }

    /// Read rows from bytecode/zlib compressed data.
    fn read_compressed_rows(&mut self, n: usize) -> Result<Vec<Vec<CellValue>>> {
        let slots_per_row = self.dict.header.nominal_case_size as usize;
        let cap = self.capacity_hint(n);

        // Phase 1: Sequential bytecode decompression (stateful)
        let (data_ref, decompressor) = match &mut self.state {
            ScanState::Bytecode { data, decompressor } => {
                (data as &[u8], decompressor)
            }
            ScanState::Zlib { data, decompressor } => {
                (data as &[u8], decompressor)
            }
            _ => unreachable!(),
        };

        let mut slot_rows = Vec::with_capacity(cap);
        for _ in 0..n {
            let slots = decompressor.decompress_row(data_ref, slots_per_row)?;
            if slots.is_empty() || slots.len() < slots_per_row {
                break;
            }
            slot_rows.push(slots);
        }

        if slot_rows.is_empty() {
            return Ok(Vec::new());
        }

        // Phase 2: Parallel SlotValue -> CellValue conversion (with optional projection)
        let dict = &self.dict;
        let projection = &self.projection;

        let all_rows: Vec<Vec<CellValue>> = slot_rows
            .par_iter()
            .map(|slots| {
                if let Some(proj) = projection {
                    data::slot_values_to_row_projected(
                        slots,
                        &dict.all_slots,
                        &dict.variables,
                        proj,
                        dict.file_encoding,
                    )
                } else {
                    data::slot_values_to_row(
                        slots,
                        &dict.all_slots,
                        &dict.variables,
                        dict.file_encoding,
                    )
                }
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(all_rows)
    }
}
