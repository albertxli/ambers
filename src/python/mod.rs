use std::collections::HashSet;
use std::ffi::CString;
use std::fs::File;
use std::io::BufReader;

use indexmap::IndexMap;

use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow::record_batch::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use pyo3::exceptions::{PyIOError, PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyDict, PyList, PyTuple};

use crate::constants::{Alignment, Compression, Measure, Role};
use crate::metadata::{MissingSpec, MrSet, MrType, SpssMetadata, Value};
use crate::scanner::SavScanner;

// ---------------------------------------------------------------------------
// Error conversion
// ---------------------------------------------------------------------------

fn spss_err(e: crate::error::SpssError) -> PyErr {
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

/// Convert a variable's MissingSpec list into a single Python dict.
///
/// Output formats:
///   {"type": "discrete", "values": [98.0, 99.0, 100.0]}     — discrete numeric
///   {"type": "discrete", "values": ["NA", "DK"]}             — discrete string
///   {"type": "range", "low": 90.0, "high": 99.5}            — range only
///   {"type": "range", "low": 90.0, "high": 99.5, "discrete": 33.0} — range + discrete
fn missing_specs_to_py(py: Python<'_>, specs: &[MissingSpec]) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);

    // Separate into range and discrete values
    let mut range: Option<(f64, f64)> = None;
    let mut discrete_f64: Vec<f64> = Vec::new();
    let mut discrete_str: Vec<&str> = Vec::new();

    for spec in specs {
        match spec {
            MissingSpec::Range { lo, hi } => range = Some((*lo, *hi)),
            MissingSpec::Value(v) => discrete_f64.push(*v),
            MissingSpec::StringValue(s) => discrete_str.push(s.as_str()),
        }
    }

    if let Some((lo, hi)) = range {
        dict.set_item("type", "range")?;
        dict.set_item("low", lo)?;
        dict.set_item("high", hi)?;
        if let Some(&val) = discrete_f64.first() {
            dict.set_item("discrete", val)?;
        }
    } else if !discrete_str.is_empty() {
        dict.set_item("type", "discrete")?;
        let vals = PyList::new(py, &discrete_str)?;
        dict.set_item("values", vals)?;
    } else {
        dict.set_item("type", "discrete")?;
        let vals = PyList::new(py, &discrete_f64)?;
        dict.set_item("values", vals)?;
    }

    Ok(dict.unbind().into_any())
}

fn mr_set_to_py(py: Python<'_>, mr: &MrSet) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("label", &mr.label)?;
    dict.set_item(
        "type",
        match mr.mr_type {
            MrType::MultipleDichotomy => "dichotomy",
            MrType::MultipleCategory => "category",
        },
    )?;
    dict.set_item("counted_value", mr.counted_value.as_deref())?;
    let vars = PyList::new(py, &mr.variables)?;
    dict.set_item("variables", vars)?;
    Ok(dict.unbind().into_any())
}

// ---------------------------------------------------------------------------
// Python → Rust conversion helpers
// ---------------------------------------------------------------------------

fn py_to_notes(obj: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    if let Ok(s) = obj.extract::<String>() {
        Ok(vec![s])
    } else if let Ok(list) = obj.extract::<Vec<String>>() {
        Ok(list)
    } else {
        Err(PyValueError::new_err(
            "notes must be a string or list of strings",
        ))
    }
}

fn py_to_measure(s: &str) -> PyResult<Measure> {
    match s.to_lowercase().as_str() {
        "nominal" => Ok(Measure::Nominal),
        "ordinal" => Ok(Measure::Ordinal),
        "scale" => Ok(Measure::Scale),
        "unknown" => Ok(Measure::Unknown),
        _ => Err(PyValueError::new_err(format!(
            "invalid measure '{s}', expected: nominal, ordinal, scale, unknown"
        ))),
    }
}

fn py_to_alignment(s: &str) -> PyResult<Alignment> {
    match s.to_lowercase().as_str() {
        "left" => Ok(Alignment::Left),
        "right" => Ok(Alignment::Right),
        "center" => Ok(Alignment::Center),
        _ => Err(PyValueError::new_err(format!(
            "invalid alignment '{s}', expected: left, right, center"
        ))),
    }
}

fn py_to_role(s: &str) -> PyResult<Role> {
    match s.to_lowercase().as_str() {
        "input" => Ok(Role::Input),
        "target" => Ok(Role::Target),
        "both" => Ok(Role::Both),
        "none" => Ok(Role::None),
        "partition" => Ok(Role::Partition),
        "split" => Ok(Role::Split),
        _ => Err(PyValueError::new_err(format!(
            "invalid role '{s}', expected: input, target, both, none, partition, split"
        ))),
    }
}

/// Parse a Python dict of missing value specs into MissingSpec list with validation.
fn py_to_missing_specs(dict: &Bound<'_, PyDict>) -> PyResult<Vec<MissingSpec>> {
    let type_val = dict.get_item("type")?.ok_or_else(|| {
        PyValueError::new_err("missing_values dict requires 'type' key ('discrete' or 'range')")
    })?;
    let type_str: String = type_val.extract()?;

    match type_str.as_str() {
        "discrete" => {
            let values_val = dict.get_item("values")?.ok_or_else(|| {
                PyValueError::new_err("discrete missing values requires 'values' key")
            })?;
            let list: &Bound<'_, PyList> = values_val.downcast()?;
            let mut specs = Vec::new();
            for item in list.iter() {
                if let Ok(f) = item.extract::<f64>() {
                    specs.push(MissingSpec::Value(f));
                } else if let Ok(i) = item.extract::<i64>() {
                    specs.push(MissingSpec::Value(i as f64));
                } else {
                    let s: String = item.extract()?;
                    if s.len() > 8 {
                        return Err(PyValueError::new_err(format!(
                            "string missing value '{s}' exceeds 8 characters"
                        )));
                    }
                    specs.push(MissingSpec::StringValue(s));
                }
            }
            if specs.len() > 3 {
                return Err(PyValueError::new_err(
                    "maximum 3 discrete missing values allowed",
                ));
            }
            // Check for mixed numeric + string types
            let has_numeric = specs.iter().any(|s| matches!(s, MissingSpec::Value(_)));
            let has_string = specs
                .iter()
                .any(|s| matches!(s, MissingSpec::StringValue(_)));
            if has_numeric && has_string {
                return Err(PyValueError::new_err(
                    "missing values cannot mix numeric and string types",
                ));
            }
            // Check uniqueness for numeric values
            let numeric_vals: Vec<u64> = specs
                .iter()
                .filter_map(|s| match s {
                    MissingSpec::Value(v) => Some(v.to_bits()),
                    _ => None,
                })
                .collect();
            let unique: HashSet<u64> = numeric_vals.iter().copied().collect();
            if unique.len() != numeric_vals.len() {
                return Err(PyValueError::new_err(
                    "discrete missing values must be unique (no duplicates)",
                ));
            }
            Ok(specs)
        }
        "range" => {
            let lo = dict
                .get_item("low")?
                .ok_or_else(|| PyValueError::new_err("range missing values requires 'low' key"))?
                .extract::<f64>()?;
            let hi = dict
                .get_item("high")?
                .ok_or_else(|| PyValueError::new_err("range missing values requires 'high' key"))?
                .extract::<f64>()?;
            if lo >= hi {
                return Err(PyValueError::new_err(format!(
                    "range 'low' ({lo}) must be less than 'high' ({hi})"
                )));
            }
            let mut specs = vec![MissingSpec::Range { lo, hi }];
            if let Some(discrete_val) = dict.get_item("discrete")? {
                if !discrete_val.is_none() {
                    let d = discrete_val.extract::<f64>()?;
                    if d > lo && d < hi {
                        return Err(PyValueError::new_err(format!(
                            "discrete value ({d}) must not fall between low ({lo}) and high ({hi})"
                        )));
                    }
                    specs.push(MissingSpec::Value(d));
                }
            }
            Ok(specs)
        }
        _ => Err(PyValueError::new_err(format!(
            "invalid missing value type '{type_str}', expected: 'discrete' or 'range'"
        ))),
    }
}

/// Parse a Python dict into an MrSet with validation.
fn py_to_mr_set(name: &str, dict: &Bound<'_, PyDict>) -> PyResult<MrSet> {
    let type_val = dict.get_item("type")?.ok_or_else(|| {
        PyValueError::new_err("MR set requires 'type' key ('dichotomy' or 'category')")
    })?;
    let type_str: String = type_val.extract()?;

    let mr_type = match type_str.as_str() {
        "dichotomy" => MrType::MultipleDichotomy,
        "category" => MrType::MultipleCategory,
        _ => {
            return Err(PyValueError::new_err(format!(
                "invalid MR set type '{type_str}', expected: 'dichotomy' or 'category'"
            )));
        }
    };

    let label: String = dict
        .get_item("label")?
        .and_then(|v| if v.is_none() { None } else { Some(v) })
        .map(|v| v.extract::<String>())
        .transpose()?
        .unwrap_or_default();

    let variables: Vec<String> = dict
        .get_item("variables")?
        .ok_or_else(|| PyValueError::new_err("MR set requires 'variables' key"))?
        .extract()?;

    if variables.len() < 2 {
        return Err(PyValueError::new_err(
            "MR set must have at least 2 variables",
        ));
    }

    let counted_value = match mr_type {
        MrType::MultipleDichotomy => {
            let cv = dict.get_item("counted_value")?.ok_or_else(|| {
                PyValueError::new_err("dichotomy MR set requires 'counted_value'")
            })?;
            if cv.is_none() {
                return Err(PyValueError::new_err(
                    "dichotomy MR set requires a non-None 'counted_value'",
                ));
            }
            if let Ok(i) = cv.extract::<i64>() {
                Some(i.to_string())
            } else if let Ok(f) = cv.extract::<f64>() {
                // Format without trailing zeros for whole numbers
                if f.fract() == 0.0 && f.is_finite() {
                    Some(format!("{}", f as i64))
                } else {
                    Some(format!("{f}"))
                }
            } else {
                Some(cv.extract::<String>()?)
            }
        }
        MrType::MultipleCategory => None,
    };

    Ok(MrSet {
        name: name.to_string(),
        label,
        mr_type,
        counted_value,
        variables,
    })
}

// ---------------------------------------------------------------------------
// apply_kwargs — shared update logic for constructor and update()
// ---------------------------------------------------------------------------

/// Apply keyword arguments to a mutable SpssMetadata.
/// Used by both `__init__` (on a default) and `update()` (on a clone).
fn apply_kwargs(meta: &mut SpssMetadata, kwargs: &Bound<'_, PyDict>) -> PyResult<()> {
    // --- Scalar fields ---

    if let Some(val) = kwargs.get_item("file_label")? {
        if val.is_none() {
            meta.file_label = String::new();
        } else {
            meta.file_label = val.extract::<String>()?;
        }
    }

    if let Some(val) = kwargs.get_item("notes")? {
        if val.is_none() {
            meta.notes = Vec::new();
        } else {
            meta.notes = py_to_notes(&val)?;
        }
    }

    if let Some(val) = kwargs.get_item("weight_variable")? {
        if val.is_none() {
            meta.weight_variable = None;
        } else {
            meta.weight_variable = Some(val.extract::<String>()?);
        }
    }

    // --- Dict fields (merge at top level) ---

    if let Some(val) = kwargs.get_item("variable_labels")? {
        if !val.is_none() {
            let dict: &Bound<'_, PyDict> = val.downcast()?;
            for (k, v) in dict.iter() {
                let key: String = k.extract()?;
                if v.is_none() {
                    meta.variable_labels.swap_remove(&key);
                } else {
                    meta.variable_labels.insert(key, v.extract::<String>()?);
                }
            }
        }
    }

    if let Some(val) = kwargs.get_item("variable_formats")? {
        if !val.is_none() {
            let dict: &Bound<'_, PyDict> = val.downcast()?;
            for (k, v) in dict.iter() {
                let key: String = k.extract()?;
                if v.is_none() {
                    meta.variable_formats.swap_remove(&key);
                } else {
                    meta.variable_formats.insert(key, v.extract::<String>()?);
                }
            }
        }
    }

    if let Some(val) = kwargs.get_item("variable_measures")? {
        if !val.is_none() {
            let dict: &Bound<'_, PyDict> = val.downcast()?;
            for (k, v) in dict.iter() {
                let key: String = k.extract()?;
                if v.is_none() {
                    meta.variable_measures.swap_remove(&key);
                } else {
                    let s: String = v.extract()?;
                    meta.variable_measures.insert(key, py_to_measure(&s)?);
                }
            }
        }
    }

    if let Some(val) = kwargs.get_item("variable_display_widths")? {
        if !val.is_none() {
            let dict: &Bound<'_, PyDict> = val.downcast()?;
            for (k, v) in dict.iter() {
                let key: String = k.extract()?;
                if v.is_none() {
                    meta.variable_display_widths.swap_remove(&key);
                } else {
                    meta.variable_display_widths
                        .insert(key, v.extract::<u32>()?);
                }
            }
        }
    }

    if let Some(val) = kwargs.get_item("variable_alignments")? {
        if !val.is_none() {
            let dict: &Bound<'_, PyDict> = val.downcast()?;
            for (k, v) in dict.iter() {
                let key: String = k.extract()?;
                if v.is_none() {
                    meta.variable_alignments.swap_remove(&key);
                } else {
                    let s: String = v.extract()?;
                    meta.variable_alignments.insert(key, py_to_alignment(&s)?);
                }
            }
        }
    }

    if let Some(val) = kwargs.get_item("variable_roles")? {
        if !val.is_none() {
            let dict: &Bound<'_, PyDict> = val.downcast()?;
            for (k, v) in dict.iter() {
                let key: String = k.extract()?;
                if v.is_none() {
                    meta.variable_roles.swap_remove(&key);
                } else {
                    let s: String = v.extract()?;
                    meta.variable_roles.insert(key, py_to_role(&s)?);
                }
            }
        }
    }

    // --- Nested dict fields (merge top level, replace inner) ---

    if let Some(val) = kwargs.get_item("variable_value_labels")? {
        if !val.is_none() {
            let dict: &Bound<'_, PyDict> = val.downcast()?;
            for (k, v) in dict.iter() {
                let var_name: String = k.extract()?;
                if v.is_none() {
                    meta.variable_value_labels.swap_remove(&var_name);
                } else {
                    let inner: &Bound<'_, PyDict> = v.downcast()?;
                    let mut labels = IndexMap::new();
                    for (val_key, val_label) in inner.iter() {
                        let label: String = val_label.extract()?;
                        if let Ok(f) = val_key.extract::<f64>() {
                            labels.insert(Value::Numeric(f), label);
                        } else if let Ok(i) = val_key.extract::<i64>() {
                            labels.insert(Value::Numeric(i as f64), label);
                        } else {
                            let s: String = val_key.extract()?;
                            labels.insert(Value::String(s), label);
                        }
                    }
                    meta.variable_value_labels.insert(var_name, labels);
                }
            }
        }
    }

    if let Some(val) = kwargs.get_item("variable_missing_values")? {
        if !val.is_none() {
            let dict: &Bound<'_, PyDict> = val.downcast()?;
            for (k, v) in dict.iter() {
                let key: String = k.extract()?;
                if v.is_none() {
                    meta.variable_missing_values.swap_remove(&key);
                } else {
                    let inner: &Bound<'_, PyDict> = v.downcast()?;
                    let specs = py_to_missing_specs(inner)?;
                    meta.variable_missing_values.insert(key, specs);
                }
            }
        }
    }

    if let Some(val) = kwargs.get_item("variable_attributes")? {
        if !val.is_none() {
            let dict: &Bound<'_, PyDict> = val.downcast()?;
            for (k, v) in dict.iter() {
                let var_name: String = k.extract()?;
                if v.is_none() {
                    meta.variable_attributes.swap_remove(&var_name);
                } else {
                    let inner: &Bound<'_, PyDict> = v.downcast()?;
                    let mut attrs = IndexMap::new();
                    for (ak, av) in inner.iter() {
                        let attr_name: String = ak.extract()?;
                        let values: Vec<String> = av.extract()?;
                        attrs.insert(attr_name, values);
                    }
                    meta.variable_attributes.insert(var_name, attrs);
                }
            }
        }
    }

    if let Some(val) = kwargs.get_item("mr_sets")? {
        if !val.is_none() {
            let dict: &Bound<'_, PyDict> = val.downcast()?;
            for (k, v) in dict.iter() {
                let set_name: String = k.extract()?;
                if v.is_none() {
                    meta.mr_sets.swap_remove(&set_name);
                } else {
                    let inner: &Bound<'_, PyDict> = v.downcast()?;
                    let mr = py_to_mr_set(&set_name, inner)?;
                    meta.mr_sets.insert(set_name, mr);
                }
            }
        }
    }

    Ok(())
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
    /// Create a new SpssMetadata, optionally with keyword arguments.
    ///
    /// All parameters are optional. Omitted fields use sensible defaults
    /// (empty strings, empty maps). At write time, missing fields are
    /// filled from the DataFrame schema automatically.
    #[new]
    #[pyo3(signature = (**kwargs))]
    fn new(kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        let mut meta = SpssMetadata::default();
        if let Some(dict) = kwargs {
            apply_kwargs(&mut meta, dict)?;
        }
        Ok(PySpssMetadata { inner: meta })
    }

    /// Return a new SpssMetadata with the given fields merged/replaced.
    ///
    /// Dict fields merge at the variable-name level (new keys added,
    /// existing overwritten). Pass ``{key: None}`` to remove a key.
    /// Scalar fields (file_label, weight_variable) are replaced.
    #[pyo3(signature = (**kwargs))]
    fn update(&self, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<PySpssMetadata> {
        let mut meta = self.inner.clone();
        if let Some(dict) = kwargs {
            apply_kwargs(&mut meta, dict)?;
        }
        Ok(PySpssMetadata { inner: meta })
    }

    // -----------------------------------------------------------------------
    // Getters (read-only properties)
    // -----------------------------------------------------------------------

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
            Compression::None => "uncompressed",
            Compression::Bytecode => "bytecode",
            Compression::Zlib => "zlib",
        }
    }

    #[getter]
    fn creation_time(&self) -> &str {
        &self.inner.creation_time
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
        if self.inner.variable_names.is_empty() {
            // From-scratch metadata: return the raw labels map
            for (name, label) in &self.inner.variable_labels {
                dict.set_item(name, label)?;
            }
        } else {
            // Read metadata: return labels ordered by variable_names
            for name in &self.inner.variable_names {
                match self.inner.variable_labels.get(name) {
                    Some(label) => dict.set_item(name, label)?,
                    None => dict.set_item(name, py.None())?,
                }
            }
        }
        Ok(dict.unbind().into_any())
    }

    #[getter]
    fn variable_formats(&self) -> IndexMap<String, String> {
        self.inner.variable_formats.clone()
    }

    #[getter]
    fn arrow_data_types(&self) -> IndexMap<String, String> {
        self.inner.arrow_data_types.clone()
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
    fn variable_alignments(&self) -> IndexMap<String, String> {
        self.inner
            .variable_alignments
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().to_string()))
            .collect()
    }

    #[getter]
    fn variable_storage_widths(&self) -> IndexMap<String, usize> {
        self.inner.variable_storage_widths.clone()
    }

    #[getter]
    fn variable_display_widths(&self) -> IndexMap<String, u32> {
        self.inner.variable_display_widths.clone()
    }

    #[getter]
    fn variable_measures(&self) -> IndexMap<String, String> {
        self.inner
            .variable_measures
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().to_string()))
            .collect()
    }

    #[getter]
    fn variable_missing_values<'py>(&self, py: Python<'py>) -> PyResult<Py<PyAny>> {
        let outer = PyDict::new(py);
        for (var_name, specs) in &self.inner.variable_missing_values {
            outer.set_item(var_name.as_str(), missing_specs_to_py(py, specs)?)?;
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
    fn variable_roles(&self) -> IndexMap<String, String> {
        self.inner
            .variable_roles
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().to_string()))
            .collect()
    }

    #[getter]
    fn variable_attributes<'py>(&self, py: Python<'py>) -> PyResult<Py<PyAny>> {
        let outer = PyDict::new(py);
        for (var_name, attrs) in &self.inner.variable_attributes {
            let inner = PyDict::new(py);
            for (attr_name, values) in attrs {
                let py_list: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
                inner.set_item(attr_name.as_str(), py_list)?;
            }
            outer.set_item(var_name.as_str(), inner)?;
        }
        Ok(outer.unbind().into_any())
    }

    #[getter]
    fn weight_variable(&self) -> Option<String> {
        self.inner.weight_variable.clone()
    }

    // -----------------------------------------------------------------------
    // Quick lookup methods
    // -----------------------------------------------------------------------

    /// Validate that a variable name exists in the metadata.
    /// For from-scratch metadata (empty variable_names), any name is valid.
    fn check_var(&self, name: &str) -> PyResult<()> {
        if self.inner.variable_names.is_empty() {
            return Ok(());
        }
        if !self.inner.variable_names.contains(&name.to_string()) {
            return Err(PyKeyError::new_err(format!(
                "variable '{name}' not found in metadata"
            )));
        }
        Ok(())
    }

    /// Get the variable label for a single variable. Returns None if unlabeled.
    fn label(&self, name: &str) -> PyResult<Option<String>> {
        self.check_var(name)?;
        Ok(self.inner.label(name).map(|s| s.to_string()))
    }

    /// Get the SPSS format string for a variable (e.g. "F8.2"). Returns None if not set.
    fn format(&self, name: &str) -> PyResult<Option<String>> {
        self.check_var(name)?;
        Ok(self.inner.format(name).map(|s| s.to_string()))
    }

    /// Get the measurement level for a variable. Returns None if not set.
    fn measure(&self, name: &str) -> PyResult<Option<String>> {
        self.check_var(name)?;
        Ok(self.inner.measure(name).map(|m| m.as_str().to_string()))
    }

    /// Get the role for a variable. Returns None if no role is set.
    fn role(&self, name: &str) -> PyResult<Option<String>> {
        self.check_var(name)?;
        Ok(self.inner.role(name).map(|r| r.as_str().to_string()))
    }

    /// Get custom attribute(s) for a variable.
    /// With one arg: returns all attributes as dict, or None.
    /// With two args: returns specific attribute values, or raises KeyError.
    #[pyo3(signature = (name, attr=None))]
    fn attribute<'py>(
        &self,
        py: Python<'py>,
        name: &str,
        attr: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.check_var(name)?;
        match attr {
            None => {
                // Return all attributes for this variable, or None
                match self.inner.attributes(name) {
                    Some(attrs) => {
                        let dict = PyDict::new(py);
                        for (k, v) in attrs {
                            let py_list: Vec<&str> = v.iter().map(|s| s.as_str()).collect();
                            dict.set_item(k.as_str(), py_list)?;
                        }
                        Ok(dict.unbind().into_any())
                    }
                    None => Ok(py.None()),
                }
            }
            Some(attr_name) => {
                // Return specific attribute values, or raise KeyError
                match self.inner.attribute(name, attr_name) {
                    Some(values) => {
                        let py_list: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
                        Ok(py_list.into_pyobject(py).unwrap().into_any().unbind())
                    }
                    None => Err(pyo3::exceptions::PyKeyError::new_err(format!(
                        "attribute '{attr_name}' not found for variable '{name}'"
                    ))),
                }
            }
        }
    }

    /// Get the value labels dict for a variable. Returns None if no value labels exist.
    fn value<'py>(&self, py: Python<'py>, name: &str) -> PyResult<Option<Py<PyAny>>> {
        self.check_var(name)?;
        match self.inner.variable_value_labels.get(name) {
            Some(labels) if !labels.is_empty() => {
                let dict = PyDict::new(py);
                for (val, label) in labels {
                    dict.set_item(value_to_py(py, val), label.as_str())?;
                }
                Ok(Some(dict.unbind().into_any()))
            }
            _ => Ok(None),
        }
    }

    // -----------------------------------------------------------------------
    // schema property — full metadata as dict
    // -----------------------------------------------------------------------

    /// Returns all metadata as a nested Python dict.
    #[getter]
    fn schema<'py>(&self, py: Python<'py>) -> PyResult<Py<PyAny>> {
        let d = PyDict::new(py);
        let m = &self.inner;

        // File-level scalars
        d.set_item("file_label", &m.file_label)?;
        d.set_item("file_format", &m.file_format)?;
        d.set_item("file_encoding", &m.file_encoding)?;
        d.set_item("creation_time", &m.creation_time)?;
        d.set_item("compression", self.compression())?;
        d.set_item("number_columns", m.number_columns)?;
        d.set_item("number_rows", m.number_rows)?;
        d.set_item("weight_variable", m.weight_variable.as_deref())?;
        d.set_item("notes", &m.notes)?;

        // Variable names
        d.set_item("variable_names", &m.variable_names)?;

        // Per-variable fields
        d.set_item("variable_labels", self.variable_labels(py)?)?;
        d.set_item("variable_value_labels", self.variable_value_labels(py)?)?;
        d.set_item("variable_formats", m.variable_formats.clone())?;
        d.set_item("variable_measures", self.variable_measures())?;
        d.set_item("variable_alignments", self.variable_alignments())?;
        d.set_item("variable_storage_widths", m.variable_storage_widths.clone())?;
        d.set_item("variable_display_widths", m.variable_display_widths.clone())?;
        d.set_item("variable_roles", self.variable_roles())?;
        d.set_item("variable_missing_values", self.variable_missing_values(py)?)?;
        d.set_item("variable_attributes", self.variable_attributes(py)?)?;
        d.set_item("mr_sets", self.mr_sets(py)?)?;
        d.set_item("arrow_data_types", m.arrow_data_types.clone())?;

        Ok(d.unbind().into_any())
    }

    // -----------------------------------------------------------------------
    // summary() — rich formatted overview
    // -----------------------------------------------------------------------

    /// Print a formatted summary of the metadata.
    fn summary(&self) {
        use crate::constants::Measure;

        let m = &self.inner;
        let ncols = m.number_columns;
        let rows_str = m
            .number_rows
            .map(|n| format_count(n as usize))
            .unwrap_or_else(|| "unknown".into());
        println!("SPSS Metadata Summary");
        println!(
            "\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}"
        );

        // File section
        println!();
        println!("File");
        println!(
            "  Label:        {}",
            if m.file_label.is_empty() {
                "(none)"
            } else {
                &m.file_label
            }
        );
        println!("  Format:       {}", m.file_format);
        println!("  Encoding:     {}", m.file_encoding);
        println!("  Created:      {}", m.creation_time);
        println!("  Rows:         {}", rows_str);
        println!("  Columns:      {}", format_count(ncols));
        println!(
            "  Weight:       {}",
            m.weight_variable.as_deref().unwrap_or("(none)")
        );
        if !m.notes.is_empty() {
            let first_line = m.notes[0].trim();
            let preview = if first_line.len() > 40 {
                format!("{}...", &first_line[..40])
            } else {
                first_line.to_string()
            };
            println!(
                "  Notes:        {} record(s) \u{2502} {}",
                m.notes.len(),
                preview
            );
        }

        // Variables section
        let mut n_numeric = 0usize;
        let mut n_string = 0usize;
        for fmt in m.variable_formats.values() {
            if fmt.starts_with('A') {
                n_string += 1;
            } else {
                n_numeric += 1;
            }
        }

        println!();
        println!("Variables");
        let pct = |n: usize| -> String {
            if ncols > 0 {
                format!("{:>5.1}%", 100.0 * n as f64 / ncols as f64)
            } else {
                String::new()
            }
        };
        println!(
            "  Numeric       {:>5}    {}",
            format_count(n_numeric),
            pct(n_numeric)
        );
        println!(
            "  String        {:>5}    {}",
            format_count(n_string),
            pct(n_string)
        );

        // Measure level distribution
        let mut n_nominal = 0usize;
        let mut n_ordinal = 0usize;
        let mut n_scale = 0usize;
        let mut n_unknown = 0usize;
        for var in &m.variable_names {
            match m.variable_measures.get(var) {
                Some(Measure::Nominal) => n_nominal += 1,
                Some(Measure::Ordinal) => n_ordinal += 1,
                Some(Measure::Scale) => n_scale += 1,
                _ => n_unknown += 1,
            }
        }
        println!();
        println!("  Nominal       {:>5}", format_count(n_nominal));
        println!("  Ordinal       {:>5}", format_count(n_ordinal));
        println!("  Scale         {:>5}", format_count(n_scale));
        if n_unknown > 0 {
            println!("  Unknown       {:>5}", format_count(n_unknown));
        }

        // Role distribution
        if !m.variable_roles.is_empty() {
            use crate::constants::Role;
            let mut n_input = 0usize;
            let mut n_target = 0usize;
            let mut n_both = 0usize;
            let mut n_none = 0usize;
            let mut n_partition = 0usize;
            let mut n_split = 0usize;
            for role in m.variable_roles.values() {
                match role {
                    Role::Input => n_input += 1,
                    Role::Target => n_target += 1,
                    Role::Both => n_both += 1,
                    Role::None => n_none += 1,
                    Role::Partition => n_partition += 1,
                    Role::Split => n_split += 1,
                }
            }
            println!();
            println!("Roles ({} variables)", m.variable_roles.len());
            if n_input > 0 {
                println!("  Input         {:>5}", format_count(n_input));
            }
            if n_target > 0 {
                println!("  Target        {:>5}", format_count(n_target));
            }
            if n_both > 0 {
                println!("  Both          {:>5}", format_count(n_both));
            }
            if n_none > 0 {
                println!("  None          {:>5}", format_count(n_none));
            }
            if n_partition > 0 {
                println!("  Partition     {:>5}", format_count(n_partition));
            }
            if n_split > 0 {
                println!("  Split         {:>5}", format_count(n_split));
            }
        }

        // Annotations section
        let n_with_labels = m.variable_labels.len();
        let n_with_values = m.variable_value_labels.len();
        let n_with_missing = m.variable_missing_values.len();
        let n_mr = m.mr_sets.len();

        println!();
        println!("Annotations");
        let ratio = |n: usize| -> String {
            if ncols > 0 && n > 0 {
                format!(
                    "{:>5} / {:<5}  {:>5.1}%",
                    format_count(n),
                    format_count(ncols),
                    100.0 * n as f64 / ncols as f64
                )
            } else {
                format!("{:>5} / {}", format_count(n), format_count(ncols))
            }
        };
        println!("  Labeled:      {}", ratio(n_with_labels));
        println!("  Value labels: {}", ratio(n_with_values));
        println!("  Missing:      {}", ratio(n_with_missing));
        println!("  MR sets:      {:>5}", format_count(n_mr));
        if !m.variable_attributes.is_empty() {
            println!("  Custom attrs: {}", ratio(m.variable_attributes.len()));
        }
    }

    // -----------------------------------------------------------------------
    // describe(var_name) — single variable deep-dive
    // -----------------------------------------------------------------------

    /// Print detailed metadata for one or more variables.
    #[pyo3(signature = (names))]
    fn describe(&self, names: &Bound<'_, PyAny>) -> PyResult<()> {
        // Accept a single string or a list of strings
        let var_names: Vec<String> = if let Ok(s) = names.extract::<String>() {
            vec![s]
        } else if let Ok(list) = names.extract::<Vec<String>>() {
            list
        } else {
            return Err(PyIOError::new_err(
                "describe() expects a variable name (str) or list of names",
            ));
        };

        let m = &self.inner;

        // Validate all names before printing anything
        for name in &var_names {
            self.check_var(name)?;
        }

        for (i, name) in var_names.iter().enumerate() {
            if i > 0 {
                println!();
            }

            let label = m
                .variable_labels
                .get(name)
                .map(|s| s.as_str())
                .unwrap_or("(none)");
            let fmt = m
                .variable_formats
                .get(name)
                .map(|s| s.as_str())
                .unwrap_or("?");
            let measure_str = m
                .variable_measures
                .get(name)
                .map(|v| v.as_str())
                .unwrap_or("?");
            let align = m
                .variable_alignments
                .get(name)
                .map(|v| v.as_str())
                .unwrap_or("?");
            let display_w = m
                .variable_display_widths
                .get(name)
                .map(|v| v.to_string())
                .unwrap_or_else(|| "?".into());
            let storage_w = m
                .variable_storage_widths
                .get(name)
                .map(|v| v.to_string())
                .unwrap_or_else(|| "?".into());

            let type_str = if fmt.starts_with('A') {
                "String"
            } else {
                "Numeric"
            };

            let role_str = m
                .variable_roles
                .get(name)
                .map(|r| r.as_str())
                .unwrap_or("(none)");

            println!("Variable: {name}");
            println!("Label:    {label}");
            println!("Format:   {fmt:<12}Measure: {measure_str}");
            println!("Type:     {type_str:<12}Align:   {align}");
            println!("Display:  {display_w:<12}Storage: {storage_w}");
            println!("Role:     {role_str}");

            // Missing values
            if let Some(specs) = m.variable_missing_values.get(name) {
                if specs.is_empty() {
                    println!("Missing:  (none)");
                } else {
                    let parts: Vec<String> = specs
                        .iter()
                        .map(|s| match s {
                            MissingSpec::Value(v) => format_f64(*v),
                            MissingSpec::Range { lo, hi } => {
                                format!("{} thru {}", format_f64(*lo), format_f64(*hi))
                            }
                            MissingSpec::StringValue(s) => format!("{s:?}"),
                        })
                        .collect();
                    println!("Missing:  {}", parts.join(", "));
                }
            } else {
                println!("Missing:  (none)");
            }

            // Value labels
            if let Some(labels) = m.variable_value_labels.get(name) {
                if !labels.is_empty() {
                    println!();
                    println!("Value Labels ({}):", labels.len());
                    for (val, lbl) in labels {
                        println!("  {:<8}{lbl}", val.to_string());
                    }
                }
            }

            // Custom attributes
            if let Some(attrs) = m.variable_attributes.get(name) {
                if !attrs.is_empty() {
                    println!();
                    println!("Custom Attributes ({}):", attrs.len());
                    for (attr_name, values) in attrs {
                        if values.len() == 1 {
                            println!("  {attr_name}: {}", values[0]);
                        } else {
                            println!("  {attr_name}: {:?}", values);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // diff(other) — metadata comparison
    // -----------------------------------------------------------------------

    /// Compare this metadata to another. Returns a MetaDiff object.
    #[pyo3(signature = (other, print_output=true))]
    fn diff<'py>(
        &self,
        py: Python<'py>,
        other: &PySpssMetadata,
        print_output: bool,
    ) -> PyResult<PyMetaDiff> {
        let a = &self.inner;
        let b = &other.inner;

        // File-level diffs
        let file_level = PyDict::new(py);
        if a.number_rows != b.number_rows {
            let t = PyTuple::new(py, &[a.number_rows, b.number_rows])?;
            file_level.set_item("number_rows", t)?;
        }
        if a.number_columns != b.number_columns {
            let t = PyTuple::new(py, &[a.number_columns, b.number_columns])?;
            file_level.set_item("number_columns", t)?;
        }
        if a.file_encoding != b.file_encoding {
            let t = PyTuple::new(py, &[&a.file_encoding, &b.file_encoding])?;
            file_level.set_item("file_encoding", t)?;
        }
        if a.file_label != b.file_label {
            let t = PyTuple::new(py, &[&a.file_label, &b.file_label])?;
            file_level.set_item("file_label", t)?;
        }

        // Variable sets
        let a_vars: HashSet<&str> = a.variable_names.iter().map(|s| s.as_str()).collect();
        let b_vars: HashSet<&str> = b.variable_names.iter().map(|s| s.as_str()).collect();
        let shared: HashSet<&str> = a_vars.intersection(&b_vars).copied().collect();
        let mut only_self: Vec<String> =
            a_vars.difference(&b_vars).map(|s| s.to_string()).collect();
        let mut only_other: Vec<String> =
            b_vars.difference(&a_vars).map(|s| s.to_string()).collect();
        only_self.sort();
        only_other.sort();

        // Per-field diffs on shared variables
        let label_diffs = diff_string_maps(py, &a.variable_labels, &b.variable_labels, &shared)?;
        let type_diffs = diff_string_maps(py, &a.variable_formats, &b.variable_formats, &shared)?;
        let measure_diffs =
            diff_measure_maps(py, &a.variable_measures, &b.variable_measures, &shared)?;
        let display_diffs = diff_u32_maps(
            py,
            &a.variable_display_widths,
            &b.variable_display_widths,
            &shared,
        )?;
        let storage_diffs = diff_usize_maps(
            py,
            &a.variable_storage_widths,
            &b.variable_storage_widths,
            &shared,
        )?;
        let vvl_diffs = diff_value_label_maps(
            py,
            &a.variable_value_labels,
            &b.variable_value_labels,
            &shared,
        )?;
        let missing_diffs = diff_missing_maps(
            py,
            &a.variable_missing_values,
            &b.variable_missing_values,
            &shared,
        )?;
        let mr_diffs = diff_key_sets(py, &a.mr_sets, &b.mr_sets)?;
        let role_diffs = diff_role_maps(py, &a.variable_roles, &b.variable_roles, &shared)?;
        let attr_diffs =
            diff_attr_maps(py, &a.variable_attributes, &b.variable_attributes, &shared)?;

        let is_match = file_level.is_empty()
            && only_self.is_empty()
            && only_other.is_empty()
            && list_len(py, &label_diffs) == 0
            && list_len(py, &type_diffs) == 0
            && list_len(py, &measure_diffs) == 0
            && list_len(py, &display_diffs) == 0
            && list_len(py, &storage_diffs) == 0
            && list_len(py, &vvl_diffs) == 0
            && list_len(py, &missing_diffs) == 0
            && list_len(py, &mr_diffs) == 0
            && list_len(py, &role_diffs) == 0
            && list_len(py, &attr_diffs) == 0;

        let result = PyMetaDiff {
            is_match,
            file_level: file_level.unbind().into_any(),
            variables_only_in_self: only_self,
            variables_only_in_other: only_other,
            variable_labels: label_diffs.clone_ref(py),
            variable_value_labels: vvl_diffs.clone_ref(py),
            variable_formats: type_diffs.clone_ref(py),
            variable_measures: measure_diffs.clone_ref(py),
            variable_display_widths: display_diffs.clone_ref(py),
            variable_storage_widths: storage_diffs.clone_ref(py),
            variable_missing_values: missing_diffs.clone_ref(py),
            mr_sets: mr_diffs.clone_ref(py),
            variable_roles: role_diffs.clone_ref(py),
            variable_attributes: attr_diffs.clone_ref(py),
        };

        if print_output {
            result.print_summary(py);
        }

        Ok(result)
    }

    // -----------------------------------------------------------------------
    // __repr__ / __str__
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // with_*() — chainable single-field convenience methods
    // -----------------------------------------------------------------------

    /// Return a new SpssMetadata with the file label set.
    fn with_file_label(&self, label: &str) -> PySpssMetadata {
        let mut meta = self.inner.clone();
        meta.file_label = label.to_string();
        PySpssMetadata { inner: meta }
    }

    /// Return a new SpssMetadata with notes set.
    #[pyo3(signature = (notes))]
    fn with_notes(&self, notes: &Bound<'_, PyAny>) -> PyResult<PySpssMetadata> {
        let mut meta = self.inner.clone();
        meta.notes = py_to_notes(notes)?;
        Ok(PySpssMetadata { inner: meta })
    }

    /// Return a new SpssMetadata with the weight variable set (or cleared with None).
    fn with_weight_variable(&self, var: Option<&str>) -> PySpssMetadata {
        let mut meta = self.inner.clone();
        meta.weight_variable = var.map(|s| s.to_string());
        PySpssMetadata { inner: meta }
    }

    /// Return a new SpssMetadata with variable labels merged.
    fn with_variable_labels(&self, labels: &Bound<'_, PyDict>) -> PyResult<PySpssMetadata> {
        let mut meta = self.inner.clone();
        for (k, v) in labels.iter() {
            let key: String = k.extract()?;
            if v.is_none() {
                meta.variable_labels.swap_remove(&key);
            } else {
                meta.variable_labels.insert(key, v.extract::<String>()?);
            }
        }
        Ok(PySpssMetadata { inner: meta })
    }

    /// Return a new SpssMetadata with variable value labels merged.
    fn with_variable_value_labels(&self, labels: &Bound<'_, PyDict>) -> PyResult<PySpssMetadata> {
        let mut meta = self.inner.clone();
        for (k, v) in labels.iter() {
            let var_name: String = k.extract()?;
            if v.is_none() {
                meta.variable_value_labels.swap_remove(&var_name);
            } else {
                let inner: &Bound<'_, PyDict> = v.downcast()?;
                let mut map = IndexMap::new();
                for (val_key, val_label) in inner.iter() {
                    let label: String = val_label.extract()?;
                    if let Ok(f) = val_key.extract::<f64>() {
                        map.insert(Value::Numeric(f), label);
                    } else if let Ok(i) = val_key.extract::<i64>() {
                        map.insert(Value::Numeric(i as f64), label);
                    } else {
                        let s: String = val_key.extract()?;
                        map.insert(Value::String(s), label);
                    }
                }
                meta.variable_value_labels.insert(var_name, map);
            }
        }
        Ok(PySpssMetadata { inner: meta })
    }

    /// Return a new SpssMetadata with variable formats merged.
    fn with_variable_formats(&self, formats: &Bound<'_, PyDict>) -> PyResult<PySpssMetadata> {
        let mut meta = self.inner.clone();
        for (k, v) in formats.iter() {
            let key: String = k.extract()?;
            if v.is_none() {
                meta.variable_formats.swap_remove(&key);
            } else {
                meta.variable_formats.insert(key, v.extract::<String>()?);
            }
        }
        Ok(PySpssMetadata { inner: meta })
    }

    /// Return a new SpssMetadata with variable measures merged.
    fn with_variable_measures(&self, measures: &Bound<'_, PyDict>) -> PyResult<PySpssMetadata> {
        let mut meta = self.inner.clone();
        for (k, v) in measures.iter() {
            let key: String = k.extract()?;
            if v.is_none() {
                meta.variable_measures.swap_remove(&key);
            } else {
                let s: String = v.extract()?;
                meta.variable_measures.insert(key, py_to_measure(&s)?);
            }
        }
        Ok(PySpssMetadata { inner: meta })
    }

    /// Return a new SpssMetadata with variable display widths merged.
    fn with_variable_display_widths(&self, widths: &Bound<'_, PyDict>) -> PyResult<PySpssMetadata> {
        let mut meta = self.inner.clone();
        for (k, v) in widths.iter() {
            let key: String = k.extract()?;
            if v.is_none() {
                meta.variable_display_widths.swap_remove(&key);
            } else {
                meta.variable_display_widths
                    .insert(key, v.extract::<u32>()?);
            }
        }
        Ok(PySpssMetadata { inner: meta })
    }

    /// Return a new SpssMetadata with variable alignments merged.
    fn with_variable_alignments(&self, alignments: &Bound<'_, PyDict>) -> PyResult<PySpssMetadata> {
        let mut meta = self.inner.clone();
        for (k, v) in alignments.iter() {
            let key: String = k.extract()?;
            if v.is_none() {
                meta.variable_alignments.swap_remove(&key);
            } else {
                let s: String = v.extract()?;
                meta.variable_alignments.insert(key, py_to_alignment(&s)?);
            }
        }
        Ok(PySpssMetadata { inner: meta })
    }

    /// Return a new SpssMetadata with variable missing values merged.
    fn with_variable_missing_values(
        &self,
        missing: &Bound<'_, PyDict>,
    ) -> PyResult<PySpssMetadata> {
        let mut meta = self.inner.clone();
        for (k, v) in missing.iter() {
            let key: String = k.extract()?;
            if v.is_none() {
                meta.variable_missing_values.swap_remove(&key);
            } else {
                let inner: &Bound<'_, PyDict> = v.downcast()?;
                let specs = py_to_missing_specs(inner)?;
                meta.variable_missing_values.insert(key, specs);
            }
        }
        Ok(PySpssMetadata { inner: meta })
    }

    /// Return a new SpssMetadata with variable roles merged.
    fn with_variable_roles(&self, roles: &Bound<'_, PyDict>) -> PyResult<PySpssMetadata> {
        let mut meta = self.inner.clone();
        for (k, v) in roles.iter() {
            let key: String = k.extract()?;
            if v.is_none() {
                meta.variable_roles.swap_remove(&key);
            } else {
                let s: String = v.extract()?;
                meta.variable_roles.insert(key, py_to_role(&s)?);
            }
        }
        Ok(PySpssMetadata { inner: meta })
    }

    /// Return a new SpssMetadata with variable attributes merged.
    fn with_variable_attributes(&self, attributes: &Bound<'_, PyDict>) -> PyResult<PySpssMetadata> {
        let mut meta = self.inner.clone();
        for (k, v) in attributes.iter() {
            let var_name: String = k.extract()?;
            if v.is_none() {
                meta.variable_attributes.swap_remove(&var_name);
            } else {
                let inner: &Bound<'_, PyDict> = v.downcast()?;
                let mut attrs = IndexMap::new();
                for (ak, av) in inner.iter() {
                    let attr_name: String = ak.extract()?;
                    let values: Vec<String> = av.extract()?;
                    attrs.insert(attr_name, values);
                }
                meta.variable_attributes.insert(var_name, attrs);
            }
        }
        Ok(PySpssMetadata { inner: meta })
    }

    /// Return a new SpssMetadata with MR sets merged.
    fn with_mr_sets(&self, mr_sets: &Bound<'_, PyDict>) -> PyResult<PySpssMetadata> {
        let mut meta = self.inner.clone();
        for (k, v) in mr_sets.iter() {
            let set_name: String = k.extract()?;
            if v.is_none() {
                meta.mr_sets.swap_remove(&set_name);
            } else {
                let inner: &Bound<'_, PyDict> = v.downcast()?;
                let mr = py_to_mr_set(&set_name, inner)?;
                meta.mr_sets.insert(set_name, mr);
            }
        }
        Ok(PySpssMetadata { inner: meta })
    }
}

// ---------------------------------------------------------------------------
// #[pyclass] MetaDiff
// ---------------------------------------------------------------------------

#[pyclass(name = "MetaDiff", frozen)]
pub struct PyMetaDiff {
    is_match: bool,
    file_level: Py<PyAny>,
    variables_only_in_self: Vec<String>,
    variables_only_in_other: Vec<String>,
    variable_labels: Py<PyAny>,
    variable_value_labels: Py<PyAny>,
    variable_formats: Py<PyAny>,
    variable_measures: Py<PyAny>,
    variable_display_widths: Py<PyAny>,
    variable_storage_widths: Py<PyAny>,
    variable_missing_values: Py<PyAny>,
    mr_sets: Py<PyAny>,
    variable_roles: Py<PyAny>,
    variable_attributes: Py<PyAny>,
}

#[pymethods]
impl PyMetaDiff {
    #[getter]
    fn is_match(&self) -> bool {
        self.is_match
    }

    #[getter]
    fn file_level(&self, py: Python<'_>) -> Py<PyAny> {
        self.file_level.clone_ref(py)
    }

    #[getter]
    fn variables_only_in_self(&self) -> Vec<String> {
        self.variables_only_in_self.clone()
    }

    #[getter]
    fn variables_only_in_other(&self) -> Vec<String> {
        self.variables_only_in_other.clone()
    }

    #[getter]
    fn variable_labels(&self, py: Python<'_>) -> Py<PyAny> {
        self.variable_labels.clone_ref(py)
    }

    #[getter]
    fn variable_value_labels(&self, py: Python<'_>) -> Py<PyAny> {
        self.variable_value_labels.clone_ref(py)
    }

    #[getter]
    fn variable_formats(&self, py: Python<'_>) -> Py<PyAny> {
        self.variable_formats.clone_ref(py)
    }

    #[getter]
    fn variable_measures(&self, py: Python<'_>) -> Py<PyAny> {
        self.variable_measures.clone_ref(py)
    }

    #[getter]
    fn variable_display_widths(&self, py: Python<'_>) -> Py<PyAny> {
        self.variable_display_widths.clone_ref(py)
    }

    #[getter]
    fn variable_storage_widths(&self, py: Python<'_>) -> Py<PyAny> {
        self.variable_storage_widths.clone_ref(py)
    }

    #[getter]
    fn variable_missing_values(&self, py: Python<'_>) -> Py<PyAny> {
        self.variable_missing_values.clone_ref(py)
    }

    #[getter]
    fn mr_sets(&self, py: Python<'_>) -> Py<PyAny> {
        self.mr_sets.clone_ref(py)
    }

    #[getter]
    fn variable_roles(&self, py: Python<'_>) -> Py<PyAny> {
        self.variable_roles.clone_ref(py)
    }

    #[getter]
    fn variable_attributes(&self, py: Python<'_>) -> Py<PyAny> {
        self.variable_attributes.clone_ref(py)
    }

    fn __repr__(&self, py: Python<'_>) -> String {
        let n_self = self.variables_only_in_self.len();
        let n_other = self.variables_only_in_other.len();
        let n_label = list_len(py, &self.variable_labels);
        let n_vvl = list_len(py, &self.variable_value_labels);
        let n_type = list_len(py, &self.variable_formats);
        let total_diffs = n_self + n_other + n_label + n_vvl + n_type;
        format!(
            "MetaDiff(is_match={}, diffs={})",
            self.is_match, total_diffs
        )
    }

    fn __str__(&self, py: Python<'_>) -> String {
        self.__repr__(py)
    }

    fn __getitem__(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        match key {
            "is_match" => Ok(self
                .is_match
                .into_pyobject(py)
                .unwrap()
                .to_owned()
                .into_any()
                .unbind()),
            "file_level" => Ok(self.file_level.clone_ref(py)),
            "variables_only_in_self" => Ok(self
                .variables_only_in_self
                .clone()
                .into_pyobject(py)
                .unwrap()
                .into_any()
                .unbind()),
            "variables_only_in_other" => Ok(self
                .variables_only_in_other
                .clone()
                .into_pyobject(py)
                .unwrap()
                .into_any()
                .unbind()),
            "variable_labels" => Ok(self.variable_labels.clone_ref(py)),
            "variable_value_labels" => Ok(self.variable_value_labels.clone_ref(py)),
            "variable_formats" => Ok(self.variable_formats.clone_ref(py)),
            "variable_measures" => Ok(self.variable_measures.clone_ref(py)),
            "variable_display_widths" => Ok(self.variable_display_widths.clone_ref(py)),
            "variable_storage_widths" => Ok(self.variable_storage_widths.clone_ref(py)),
            "variable_missing_values" => Ok(self.variable_missing_values.clone_ref(py)),
            "mr_sets" => Ok(self.mr_sets.clone_ref(py)),
            "variable_roles" => Ok(self.variable_roles.clone_ref(py)),
            "variable_attributes" => Ok(self.variable_attributes.clone_ref(py)),
            _ => Err(PyKeyError::new_err(format!("'{key}'"))),
        }
    }
}

impl PyMetaDiff {
    fn print_summary(&self, py: Python<'_>) {
        println!("Metadata Diff");
        println!("=============");

        // File-level
        let file_dict = self.file_level.bind(py);
        if let Ok(dict) = file_dict.downcast::<PyDict>() {
            if !dict.is_empty() {
                println!();
                println!("File-level:");
                for (key, val) in dict.iter() {
                    let k: String = key.extract().unwrap_or_default();
                    let v: String = val.str().map(|s| s.to_string()).unwrap_or_default();
                    println!("  {k:<25}{v}");
                }
            }
        }

        // Variable sets
        let n_self = self.variables_only_in_self.len();
        let n_other = self.variables_only_in_other.len();
        println!();
        println!("Variables:");
        if n_self == 0 && n_other == 0 {
            println!("  All variables shared");
        } else {
            if n_self > 0 {
                let preview: Vec<&str> = self
                    .variables_only_in_self
                    .iter()
                    .take(5)
                    .map(|s| s.as_str())
                    .collect();
                let suffix = if n_self > 5 {
                    format!(", ... +{}", n_self - 5)
                } else {
                    String::new()
                };
                println!(
                    "  Only in self:   {:>5}   [{}{}]",
                    n_self,
                    preview.join(", "),
                    suffix
                );
            }
            if n_other > 0 {
                let preview: Vec<&str> = self
                    .variables_only_in_other
                    .iter()
                    .take(5)
                    .map(|s| s.as_str())
                    .collect();
                let suffix = if n_other > 5 {
                    format!(", ... +{}", n_other - 5)
                } else {
                    String::new()
                };
                println!(
                    "  Only in other:  {:>5}   [{}{}]",
                    n_other,
                    preview.join(", "),
                    suffix
                );
            }
        }

        // Field diffs
        let fields: &[(&str, &Py<PyAny>)] = &[
            ("variable_labels", &self.variable_labels),
            ("variable_value_labels", &self.variable_value_labels),
            ("variable_formats", &self.variable_formats),
            ("variable_measures", &self.variable_measures),
            ("variable_display_widths", &self.variable_display_widths),
            ("variable_storage_widths", &self.variable_storage_widths),
            ("variable_missing_values", &self.variable_missing_values),
            ("mr_sets", &self.mr_sets),
            ("variable_roles", &self.variable_roles),
            ("variable_attributes", &self.variable_attributes),
        ];

        println!();
        println!("Field diffs:");
        for (name, list) in fields {
            let n = list_len(py, list);
            if n == 0 {
                println!("  {name:<28}{n:>3} diffs  \u{2713}");
            } else {
                let s = if n == 1 { "diff " } else { "diffs" };
                println!("  {name:<28}{n:>3} {s}");
            }
        }

        println!();
        if self.is_match {
            println!("Result: MATCH");
        } else {
            println!("Result: DIFFERENCES FOUND");
        }
    }
}

// ---------------------------------------------------------------------------
// Diff helper functions
// ---------------------------------------------------------------------------

fn list_len(py: Python<'_>, obj: &Py<PyAny>) -> usize {
    obj.bind(py)
        .downcast::<PyList>()
        .map(|l| l.len())
        .unwrap_or(0)
}

fn format_count(n: usize) -> String {
    if n >= 1_000_000 {
        format!("{},{:03},{:03}", n / 1_000_000, (n / 1000) % 1000, n % 1000)
    } else if n >= 1_000 {
        format!("{},{:03}", n / 1000, n % 1000)
    } else {
        n.to_string()
    }
}

fn format_f64(v: f64) -> String {
    if v.fract() == 0.0 && v.is_finite() {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

/// Diff two IndexMap<String, String> on shared variables.
fn diff_string_maps<'py>(
    py: Python<'py>,
    a: &IndexMap<String, String>,
    b: &IndexMap<String, String>,
    shared: &HashSet<&str>,
) -> PyResult<Py<PyAny>> {
    let list = PyList::empty(py);
    for var in shared {
        let va = a.get(*var).map(|s| s.as_str()).unwrap_or("");
        let vb = b.get(*var).map(|s| s.as_str()).unwrap_or("");
        if va != vb {
            let d = PyDict::new(py);
            d.set_item("variable", *var)?;
            d.set_item("self", va)?;
            d.set_item("other", vb)?;
            list.append(d)?;
        }
    }
    Ok(list.unbind().into_any())
}

/// Diff measure maps.
fn diff_measure_maps<'py>(
    py: Python<'py>,
    a: &IndexMap<String, crate::constants::Measure>,
    b: &IndexMap<String, crate::constants::Measure>,
    shared: &HashSet<&str>,
) -> PyResult<Py<PyAny>> {
    let list = PyList::empty(py);
    for var in shared {
        let va = a.get(*var).map(|m| m.as_str()).unwrap_or("?");
        let vb = b.get(*var).map(|m| m.as_str()).unwrap_or("?");
        if va != vb {
            let d = PyDict::new(py);
            d.set_item("variable", *var)?;
            d.set_item("self", va)?;
            d.set_item("other", vb)?;
            list.append(d)?;
        }
    }
    Ok(list.unbind().into_any())
}

/// Diff u32 maps.
fn diff_u32_maps<'py>(
    py: Python<'py>,
    a: &IndexMap<String, u32>,
    b: &IndexMap<String, u32>,
    shared: &HashSet<&str>,
) -> PyResult<Py<PyAny>> {
    let list = PyList::empty(py);
    for var in shared {
        let va = a.get(*var);
        let vb = b.get(*var);
        if va != vb {
            let d = PyDict::new(py);
            d.set_item("variable", *var)?;
            d.set_item("self", va)?;
            d.set_item("other", vb)?;
            list.append(d)?;
        }
    }
    Ok(list.unbind().into_any())
}

/// Diff usize maps.
fn diff_usize_maps<'py>(
    py: Python<'py>,
    a: &IndexMap<String, usize>,
    b: &IndexMap<String, usize>,
    shared: &HashSet<&str>,
) -> PyResult<Py<PyAny>> {
    let list = PyList::empty(py);
    for var in shared {
        let va = a.get(*var);
        let vb = b.get(*var);
        if va != vb {
            let d = PyDict::new(py);
            d.set_item("variable", *var)?;
            d.set_item("self", va)?;
            d.set_item("other", vb)?;
            list.append(d)?;
        }
    }
    Ok(list.unbind().into_any())
}

/// Diff value label maps — check if value label dicts differ.
fn diff_value_label_maps<'py>(
    py: Python<'py>,
    a: &IndexMap<String, IndexMap<Value, String>>,
    b: &IndexMap<String, IndexMap<Value, String>>,
    shared: &HashSet<&str>,
) -> PyResult<Py<PyAny>> {
    let list = PyList::empty(py);
    for var in shared {
        let va = a.get(*var);
        let vb = b.get(*var);
        let differs = match (va, vb) {
            (None, None) => false,
            (Some(ma), Some(mb)) => ma != mb,
            _ => true,
        };
        if differs {
            let d = PyDict::new(py);
            d.set_item("variable", *var)?;
            // Summary of what changed
            let a_count = va.map(|m| m.len()).unwrap_or(0);
            let b_count = vb.map(|m| m.len()).unwrap_or(0);
            d.set_item("self_count", a_count)?;
            d.set_item("other_count", b_count)?;
            list.append(d)?;
        }
    }
    Ok(list.unbind().into_any())
}

/// Diff missing value maps.
fn diff_missing_maps<'py>(
    py: Python<'py>,
    a: &IndexMap<String, Vec<MissingSpec>>,
    b: &IndexMap<String, Vec<MissingSpec>>,
    shared: &HashSet<&str>,
) -> PyResult<Py<PyAny>> {
    let list = PyList::empty(py);
    for var in shared {
        let a_has = a.contains_key(*var);
        let b_has = b.contains_key(*var);
        if a_has != b_has {
            let d = PyDict::new(py);
            d.set_item("variable", *var)?;
            d.set_item("self_has_missing", a_has)?;
            d.set_item("other_has_missing", b_has)?;
            list.append(d)?;
        }
    }
    Ok(list.unbind().into_any())
}

/// Diff role maps.
fn diff_role_maps<'py>(
    py: Python<'py>,
    a: &IndexMap<String, crate::constants::Role>,
    b: &IndexMap<String, crate::constants::Role>,
    shared: &HashSet<&str>,
) -> PyResult<Py<PyAny>> {
    let list = PyList::empty(py);
    for var in shared {
        let va = a.get(*var).map(|r| r.as_str()).unwrap_or("(none)");
        let vb = b.get(*var).map(|r| r.as_str()).unwrap_or("(none)");
        if va != vb {
            let d = PyDict::new(py);
            d.set_item("variable", *var)?;
            d.set_item("self", va)?;
            d.set_item("other", vb)?;
            list.append(d)?;
        }
    }
    Ok(list.unbind().into_any())
}

/// Diff key sets of two IndexMaps.
fn diff_key_sets<'py, V>(
    py: Python<'py>,
    a: &IndexMap<String, V>,
    b: &IndexMap<String, V>,
) -> PyResult<Py<PyAny>> {
    let list = PyList::empty(py);
    let a_keys: HashSet<&str> = a.keys().map(|s| s.as_str()).collect();
    let b_keys: HashSet<&str> = b.keys().map(|s| s.as_str()).collect();
    for k in a_keys.difference(&b_keys) {
        let d = PyDict::new(py);
        d.set_item("key", *k)?;
        d.set_item("status", "only_in_self")?;
        list.append(d)?;
    }
    for k in b_keys.difference(&a_keys) {
        let d = PyDict::new(py);
        d.set_item("key", *k)?;
        d.set_item("status", "only_in_other")?;
        list.append(d)?;
    }
    Ok(list.unbind().into_any())
}

/// Diff variable_attributes maps.
fn diff_attr_maps<'py>(
    py: Python<'py>,
    a: &IndexMap<String, IndexMap<String, Vec<String>>>,
    b: &IndexMap<String, IndexMap<String, Vec<String>>>,
    shared: &HashSet<&str>,
) -> PyResult<Py<PyAny>> {
    let list = PyList::empty(py);
    for &var in shared {
        let a_attrs = a.get(var);
        let b_attrs = b.get(var);
        if a_attrs != b_attrs {
            let d = PyDict::new(py);
            d.set_item("variable", var)?;
            d.set_item(
                "self",
                a_attrs
                    .map(|m| format!("{:?}", m))
                    .unwrap_or_else(|| "(none)".into()),
            )?;
            d.set_item(
                "other",
                b_attrs
                    .map(|m| format!("{:?}", m))
                    .unwrap_or_else(|| "(none)".into()),
            )?;
            list.append(d)?;
        }
    }
    Ok(list.unbind().into_any())
}

// ---------------------------------------------------------------------------
// #[pyclass] _ArrowData — PyCapsule-based Arrow export (no pyarrow needed)
// ---------------------------------------------------------------------------

/// Wraps an Arrow RecordBatch and exports it via the Arrow PyCapsule Interface.
/// Consumers like Polars call `__arrow_c_stream__` to get the data with zero-copy.
#[pyclass(name = "_ArrowData")]
pub struct PyArrowData {
    batch: RecordBatch,
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
        PySpssMetadata {
            inner: self.scanner.metadata().clone(),
        }
    }

    /// Read the next batch. Returns _ArrowData or None at EOF.
    fn next_batch(&mut self) -> PyResult<Option<PyArrowData>> {
        match self.scanner.next_batch().map_err(spss_err)? {
            Some(batch) => Ok(Some(PyArrowData { batch })),
            None => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// #[pyfunction] read_sav / read_sav_metadata
// ---------------------------------------------------------------------------

/// Read an SPSS .sav/.zsav file. Returns (_ArrowData, SpssMetadata).
#[pyfunction]
#[pyo3(signature = (path, columns=None, n_rows=None))]
fn _read_sav(
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
    Ok((PyArrowData { batch }, PySpssMetadata { inner: metadata }))
}

/// Read only metadata from an SPSS file (no data).
#[pyfunction]
fn _read_sav_metadata(path: &str) -> PyResult<PySpssMetadata> {
    let meta = crate::read_sav_metadata(path).map_err(spss_err)?;
    Ok(PySpssMetadata { inner: meta })
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
fn _write_sav(
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
        let meta = &py_meta.inner;
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
        Some(py_meta) => py_meta.inner.clone(),
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

// ---------------------------------------------------------------------------
// #[pymodule]
// ---------------------------------------------------------------------------

#[pymodule]
fn _ambers(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(_read_sav, m)?)?;
    m.add_function(wrap_pyfunction!(_read_sav_metadata, m)?)?;
    m.add_function(wrap_pyfunction!(_write_sav, m)?)?;
    m.add_class::<PySpssMetadata>()?;
    m.add_class::<PyMetaDiff>()?;
    m.add_class::<PyArrowData>()?;
    m.add_class::<PySavBatchReader>()?;
    Ok(())
}
