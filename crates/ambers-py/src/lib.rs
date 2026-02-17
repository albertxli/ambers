use indexmap::IndexMap;

use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use arrow::pyarrow::ToPyArrow;

use ambers::constants::Compression;
use ambers::metadata::{MissingSpec, MrSet, MrType, SpssMetadata, Value};

// ---------------------------------------------------------------------------
// Error conversion
// ---------------------------------------------------------------------------

fn spss_err(e: ambers::error::SpssError) -> PyErr {
    PyIOError::new_err(format!("{e}"))
}

// ---------------------------------------------------------------------------
// Type conversion helpers
// ---------------------------------------------------------------------------

fn value_to_py(py: Python<'_>, v: &Value) -> Py<PyAny> {
    match v {
        Value::Numeric(n) => n.into_pyobject(py).unwrap().into_any().unbind(),
        Value::String(s) => s.into_pyobject(py).unwrap().into_any().unbind(),
    }
}

fn missing_spec_to_py(py: Python<'_>, spec: &MissingSpec) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    match spec {
        MissingSpec::Value(v) => {
            dict.set_item("type", "value")?;
            dict.set_item("value", v)?;
        }
        MissingSpec::Range { lo, hi } => {
            dict.set_item("type", "range")?;
            dict.set_item("low", lo)?;
            dict.set_item("high", hi)?;
        }
        MissingSpec::StringValue(s) => {
            dict.set_item("type", "string_value")?;
            dict.set_item("value", s.as_str())?;
        }
    }
    Ok(dict.unbind().into_any())
}

fn mr_set_to_py(py: Python<'_>, mr: &MrSet) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("name", &mr.name)?;
    dict.set_item("label", &mr.label)?;
    dict.set_item(
        "mr_type",
        match mr.mr_type {
            MrType::MultipleDichotomy => "multiple_dichotomy",
            MrType::MultipleCategory => "multiple_category",
        },
    )?;
    dict.set_item("counted_value", mr.counted_value.as_deref())?;
    let vars = PyList::new(py, &mr.variables)?;
    dict.set_item("variables", vars)?;
    Ok(dict.unbind().into_any())
}

// ---------------------------------------------------------------------------
// #[pyclass] SpssMetadata
// ---------------------------------------------------------------------------

#[pyclass(name = "SpssMetadata", frozen)]
pub struct PySpssMetadata {
    inner: SpssMetadata,
}

#[pymethods]
impl PySpssMetadata {
    #[getter]
    fn file_label(&self) -> &str {
        &self.inner.file_label
    }

    #[getter]
    fn file_encoding(&self) -> &str {
        &self.inner.file_encoding
    }

    #[getter]
    fn compression(&self) -> &str {
        match self.inner.compression {
            Compression::None => "none",
            Compression::Bytecode => "bytecode",
            Compression::Zlib => "zlib",
        }
    }

    #[getter]
    fn creation_time(&self) -> &str {
        &self.inner.creation_time
    }

    #[getter]
    fn modification_time(&self) -> &str {
        &self.inner.modification_time
    }

    #[getter]
    fn notes(&self) -> Vec<String> {
        self.inner.notes.clone()
    }

    #[getter]
    fn number_rows(&self) -> Option<i64> {
        self.inner.number_rows
    }

    #[getter]
    fn number_columns(&self) -> usize {
        self.inner.number_columns
    }

    #[getter]
    fn file_format(&self) -> &str {
        &self.inner.file_format
    }

    #[getter]
    fn variable_names(&self) -> Vec<String> {
        self.inner.variable_names.clone()
    }

    #[getter]
    fn variable_labels<'py>(&self, py: Python<'py>) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        for name in &self.inner.variable_names {
            match self.inner.variable_labels.get(name) {
                Some(label) => dict.set_item(name, label)?,
                None => dict.set_item(name, py.None())?,
            }
        }
        Ok(dict.unbind().into_any())
    }

    #[getter]
    fn spss_variable_types(&self) -> IndexMap<String, String> {
        self.inner.spss_variable_types.clone()
    }

    #[getter]
    fn rust_variable_types(&self) -> IndexMap<String, String> {
        self.inner.rust_variable_types.clone()
    }

    #[getter]
    fn variable_value_labels<'py>(&self, py: Python<'py>) -> PyResult<Py<PyAny>> {
        let outer = PyDict::new(py);
        for (var_name, labels) in &self.inner.variable_value_labels {
            let inner = PyDict::new(py);
            for (val, label) in labels {
                inner.set_item(value_to_py(py, val), label.as_str())?;
            }
            outer.set_item(var_name.as_str(), inner)?;
        }
        Ok(outer.unbind().into_any())
    }

    #[getter]
    fn variable_alignment(&self) -> IndexMap<String, String> {
        self.inner
            .variable_alignment
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().to_string()))
            .collect()
    }

    #[getter]
    fn variable_storage_width(&self) -> IndexMap<String, usize> {
        self.inner.variable_storage_width.clone()
    }

    #[getter]
    fn variable_display_width(&self) -> IndexMap<String, u32> {
        self.inner.variable_display_width.clone()
    }

    #[getter]
    fn variable_measure(&self) -> IndexMap<String, String> {
        self.inner
            .variable_measure
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().to_string()))
            .collect()
    }

    #[getter]
    fn variable_missing<'py>(&self, py: Python<'py>) -> PyResult<Py<PyAny>> {
        let outer = PyDict::new(py);
        for (var_name, specs) in &self.inner.variable_missing {
            let inner = PyList::empty(py);
            for spec in specs {
                inner.append(missing_spec_to_py(py, spec)?)?;
            }
            outer.set_item(var_name.as_str(), inner)?;
        }
        Ok(outer.unbind().into_any())
    }

    #[getter]
    fn mr_sets<'py>(&self, py: Python<'py>) -> PyResult<Py<PyAny>> {
        let outer = PyDict::new(py);
        for (name, mr) in &self.inner.mr_sets {
            outer.set_item(name.as_str(), mr_set_to_py(py, mr)?)?;
        }
        Ok(outer.unbind().into_any())
    }

    #[getter]
    fn weight_variable(&self) -> Option<String> {
        self.inner.weight_variable.clone()
    }

    // Convenience methods
    fn label(&self, name: &str) -> Option<String> {
        self.inner.label(name).map(|s| s.to_string())
    }

    fn format(&self, name: &str) -> Option<String> {
        self.inner.format(name).map(|s| s.to_string())
    }

    fn measure(&self, name: &str) -> Option<String> {
        self.inner.measure(name).map(|m| m.as_str().to_string())
    }

    fn __repr__(&self) -> String {
        format!(
            "SpssMetadata(columns={}, rows={}, encoding={:?}, compression={:?})",
            self.inner.number_columns,
            self.inner
                .number_rows
                .map(|n| n.to_string())
                .unwrap_or_else(|| "unknown".into()),
            self.inner.file_encoding,
            self.compression(),
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

// ---------------------------------------------------------------------------
// #[pyfunction] read_sav / read_sav_metadata
// ---------------------------------------------------------------------------

/// Read an SPSS .sav/.zsav file. Returns (PyArrow RecordBatch, SpssMetadata).
#[pyfunction]
fn _read_sav(py: Python<'_>, path: &str) -> PyResult<(Py<PyAny>, PySpssMetadata)> {
    let (batch, meta) = ambers::read_sav(path).map_err(spss_err)?;
    let py_batch = batch.to_pyarrow(py)?.unbind();
    let py_meta = PySpssMetadata { inner: meta };
    Ok((py_batch, py_meta))
}

/// Read only metadata from an SPSS file (no data).
#[pyfunction]
fn _read_sav_metadata(path: &str) -> PyResult<PySpssMetadata> {
    let meta = ambers::read_sav_metadata(path).map_err(spss_err)?;
    Ok(PySpssMetadata { inner: meta })
}

// ---------------------------------------------------------------------------
// #[pymodule]
// ---------------------------------------------------------------------------

#[pymodule]
fn _ambers(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(_read_sav, m)?)?;
    m.add_function(wrap_pyfunction!(_read_sav_metadata, m)?)?;
    m.add_class::<PySpssMetadata>()?;
    Ok(())
}
