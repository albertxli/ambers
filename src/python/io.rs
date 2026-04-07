//! PyO3 bindings for I/O: Arrow PyCapsule export, batch reader, read/write functions.

use std::ffi::CString;
use std::fs::File;
use std::io::BufReader;

use indexmap::IndexMap;

use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow::record_batch::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::constants::Compression;
use crate::metadata::SpssMetadata;
use crate::scanner::SavScanner;

use super::metadata::PySpssMetadata;
use super::spss_err;

// ---------------------------------------------------------------------------
// #[pyclass] _ArrowData — PyCapsule-based Arrow export (no pyarrow needed)
// ---------------------------------------------------------------------------

/// Wraps an Arrow RecordBatch and exports it via the Arrow PyCapsule Interface.
/// Consumers like Polars call `__arrow_c_stream__` to get the data with zero-copy.
#[pyclass(name = "_ArrowData")]
pub struct PyArrowData {
    batch: RecordBatch,
}

impl PyArrowData {
    pub(super) fn from_batch(batch: RecordBatch) -> Self {
        PyArrowData { batch }
    }
}

#[pymethods]
impl PyArrowData {
    /// Arrow PyCapsule Interface: export as an ArrowArrayStream capsule.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;
        let schema = self.batch.schema();
        let batches = vec![self.batch.clone()];
        let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
        let reader: Box<dyn RecordBatchReader + Send> = Box::new(reader);
        let ffi_stream = FFI_ArrowArrayStream::new(reader);

        let capsule_name = CString::new("arrow_array_stream").unwrap();
        PyCapsule::new(py, ffi_stream, Some(capsule_name))
    }

    fn __repr__(&self) -> String {
        format!(
            "_ArrowData(rows={}, cols={})",
            self.batch.num_rows(),
            self.batch.num_columns()
        )
    }
}

// ---------------------------------------------------------------------------
// #[pyclass] _SavBatchReader — streaming batch reader for scan_sav
// ---------------------------------------------------------------------------

/// Wraps a SavScanner and exposes batch iteration to Python.
/// Each batch is returned as an _ArrowData object (PyCapsule-capable).
#[pyclass(name = "_SavBatchReader")]
pub struct PySavBatchReader {
    scanner: SavScanner<BufReader<File>>,
}

#[pymethods]
impl PySavBatchReader {
    #[new]
    #[pyo3(signature = (path, batch_size=None))]
    fn new(path: &str, batch_size: Option<usize>) -> PyResult<Self> {
        let file = File::open(path).map_err(|e| PyIOError::new_err(format!("{e}")))?;
        let buf = BufReader::with_capacity(256 * 1024, file);
        let scanner = SavScanner::open(buf, batch_size.unwrap_or(100_000)).map_err(spss_err)?;
        Ok(PySavBatchReader { scanner })
    }

    /// Set column projection — only these columns will be decoded.
    fn select(&mut self, columns: Vec<String>) -> PyResult<()> {
        let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
        self.scanner.select(&col_refs).map_err(spss_err)
    }

    /// Set a row limit.
    fn limit(&mut self, n: usize) {
        self.scanner.limit(n);
    }

    /// Return the schema as an ordered dict of {column_name: type_string}.
    fn schema(&self) -> IndexMap<String, String> {
        let arrow_schema = self.scanner.schema();
        arrow_schema
            .fields()
            .iter()
            .map(|f| {
                let dtype = match f.data_type() {
                    arrow::datatypes::DataType::Float64 => "Float64",
                    arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::Utf8View => {
                        "String"
                    }
                    arrow::datatypes::DataType::Date32 => "Date",
                    arrow::datatypes::DataType::Timestamp(_, _) => "Datetime",
                    arrow::datatypes::DataType::Duration(_) => "Duration",
                    _ => "Unknown",
                };
                (f.name().clone(), dtype.to_string())
            })
            .collect()
    }

    /// Return file metadata.
    fn metadata(&self) -> PySpssMetadata {
        PySpssMetadata::from_inner(self.scanner.metadata().clone())
    }

    /// Read the next batch. Returns _ArrowData or None at EOF.
    fn next_batch(&mut self) -> PyResult<Option<PyArrowData>> {
        match self.scanner.next_batch().map_err(spss_err)? {
            Some(batch) => Ok(Some(PyArrowData::from_batch(batch))),
            None => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// #[pyfunction] read_sav / read_sav_meta
// ---------------------------------------------------------------------------

/// Read an SPSS .sav/.zsav file. Returns (_ArrowData, SpssMetadata).
#[pyfunction]
#[pyo3(signature = (path, columns=None, n_rows=None))]
pub(super) fn _read_sav(
    path: &str,
    columns: Option<Vec<String>>,
    n_rows: Option<usize>,
) -> PyResult<(PyArrowData, PySpssMetadata)> {
    let mut scanner = crate::scan_sav(path).map_err(spss_err)?;
    let metadata = scanner.metadata().clone();
    if let Some(ref cols) = columns {
        let col_refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
        scanner.select(&col_refs).map_err(spss_err)?;
    }
    if let Some(n) = n_rows {
        scanner.limit(n);
    }
    let batch = scanner.collect_single().map_err(spss_err)?;
    Ok((
        PyArrowData::from_batch(batch),
        PySpssMetadata::from_inner(metadata),
    ))
}

/// Read only metadata from an SPSS file (no data).
#[pyfunction]
pub(super) fn _read_sav_meta(path: &str) -> PyResult<PySpssMetadata> {
    let meta = crate::read_sav_metadata(path).map_err(spss_err)?;
    Ok(PySpssMetadata::from_inner(meta))
}

// ---------------------------------------------------------------------------
// #[pyfunction] write_sav
// ---------------------------------------------------------------------------

/// Write a DataFrame to an SPSS .sav or .zsav file.
///
/// Accepts any object that implements the Arrow PyCapsule Interface
/// (`__arrow_c_stream__`), such as a Polars DataFrame.
#[pyfunction]
#[pyo3(signature = (path, data, metadata=None, compression="bytecode", compression_level=None))]
pub(super) fn _write_sav(
    py: Python<'_>,
    path: &str,
    data: &Bound<'_, PyAny>,
    metadata: Option<&PySpssMetadata>,
    compression: &str,
    compression_level: Option<u32>,
) -> PyResult<()> {
    // Parse compression
    let comp = match compression {
        "none" | "uncompressed" => Compression::None,
        "bytecode" => Compression::Bytecode,
        "zlib" => Compression::Zlib,
        _ => {
            return Err(PyIOError::new_err(format!(
                "unknown compression: {compression:?}. Expected 'uncompressed', 'bytecode', or 'zlib'"
            )));
        }
    };

    // Try to validate early using metadata (avoids consuming PyCapsule on invalid input)
    if let Some(py_meta) = metadata {
        let meta = py_meta.inner();
        for (var_name, specs) in &meta.variable_missing_values {
            // Determine if variable is string: check format first, then arrow type.
            // If neither is available (metadata constructed from scratch), skip
            // validation — the Rust writer will validate with the Arrow schema.
            let is_string_var = if let Some(fmt) = meta.variable_formats.get(var_name.as_str()) {
                fmt.starts_with('A')
            } else if let Some(dt) = meta.arrow_data_types.get(var_name.as_str()) {
                dt == "String" || dt == "Utf8View"
            } else {
                // Can't determine type from metadata alone — skip validation
                continue;
            };
            let has_numeric = specs.iter().any(|s| {
                matches!(
                    s,
                    crate::metadata::MissingSpec::Value(_)
                        | crate::metadata::MissingSpec::Range { .. }
                )
            });
            let has_string = specs
                .iter()
                .any(|s| matches!(s, crate::metadata::MissingSpec::StringValue(_)));
            if is_string_var && has_numeric {
                return Err(PyValueError::new_err(format!(
                    "variable '{}': numeric missing values cannot be applied to a string variable",
                    var_name
                )));
            }
            if !is_string_var && has_string {
                return Err(PyValueError::new_err(format!(
                    "variable '{}': string missing values cannot be applied to a numeric variable",
                    var_name
                )));
            }
        }
    }

    // Consume Arrow data via PyCapsule Interface
    let batch = arrow_from_pycapsule(py, data)?;

    // Build metadata: use provided or infer from schema
    let meta = match metadata {
        Some(py_meta) => py_meta.inner().clone(),
        None => SpssMetadata::from_arrow_schema(batch.schema().as_ref()),
    };

    // Write
    crate::write_sav(path, &batch, &meta, comp, compression_level).map_err(spss_err)?;

    Ok(())
}

/// Extract a RecordBatch from a Python object that implements `__arrow_c_stream__`.
fn arrow_from_pycapsule(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<RecordBatch> {
    // Call __arrow_c_stream__ to get the PyCapsule
    let capsule: Bound<'_, PyCapsule> = data
        .call_method1("__arrow_c_stream__", (py.None(),))?
        .downcast_into()?;

    // SAFETY: The PyCapsule wraps an FFI_ArrowArrayStream allocated by the producer.
    // We consume it by reading the struct and nulling the release callback on the
    // original, so the PyCapsule destructor becomes a no-op (prevents double-free).
    let stream = unsafe { capsule.reference::<FFI_ArrowArrayStream>() };
    let stream_ptr = stream as *const FFI_ArrowArrayStream as *mut FFI_ArrowArrayStream;
    let stream_owned = unsafe { std::ptr::read(stream_ptr) };
    // Prevent double-free: null out the release callback on the PyCapsule's copy
    unsafe {
        (*stream_ptr).release = None;
    }

    let reader = ArrowArrayStreamReader::try_new(stream_owned)
        .map_err(|e| PyIOError::new_err(format!("failed to read Arrow stream: {e}")))?;

    // Collect all batches into a single RecordBatch
    let schema = reader.schema();
    let mut batches: Vec<RecordBatch> = Vec::new();
    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| PyIOError::new_err(format!("error reading Arrow batch: {e}")))?;
        batches.push(batch);
    }

    if batches.is_empty() {
        // Return empty batch with the schema
        return Ok(RecordBatch::new_empty(schema));
    }

    if batches.len() == 1 {
        return Ok(batches.into_iter().next().unwrap());
    }

    // Multiple batches: use arrow-select concat_batches
    // This shouldn't happen with Polars (always single batch), but handle gracefully.
    Err(PyIOError::new_err(format!(
        "expected a single Arrow batch, got {}. Pass a single DataFrame.",
        batches.len()
    )))
}
