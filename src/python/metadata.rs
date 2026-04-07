//! PyO3 bindings for SpssMetadata — the `SpssMetadata` Python class.

use indexmap::IndexMap;

use pyo3::exceptions::{PyIOError, PyKeyError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::constants::Compression;
use crate::metadata::{MissingSpec, SpssMetadata, Value};

use super::conversions::{
    missing_specs_to_py, mr_set_to_py, py_to_alignment, py_to_measure, py_to_missing_specs,
    py_to_mr_set, py_to_notes, py_to_role, value_to_py,
};
use super::diff::PyMetaDiff;

// ---------------------------------------------------------------------------
// apply_kwargs — shared update logic for constructor and update()
// ---------------------------------------------------------------------------

/// Apply keyword arguments to a mutable SpssMetadata.
/// Used by both `__init__` (on a default) and `update()` (on a clone).
pub(super) fn apply_kwargs(meta: &mut SpssMetadata, kwargs: &Bound<'_, PyDict>) -> PyResult<()> {
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
    pub(super) inner: SpssMetadata,
}

impl PySpssMetadata {
    pub(super) fn from_inner(inner: SpssMetadata) -> Self {
        PySpssMetadata { inner }
    }

    pub(super) fn inner(&self) -> &SpssMetadata {
        &self.inner
    }
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
        PyMetaDiff::compute(py, &self.inner, &other.inner, print_output)
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
// Formatting helpers (used by summary/describe)
// ---------------------------------------------------------------------------

pub(super) fn format_count(n: usize) -> String {
    if n >= 1_000_000 {
        format!("{},{:03},{:03}", n / 1_000_000, (n / 1000) % 1000, n % 1000)
    } else if n >= 1_000 {
        format!("{},{:03}", n / 1000, n % 1000)
    } else {
        n.to_string()
    }
}

pub(super) fn format_f64(v: f64) -> String {
    if v.fract() == 0.0 && v.is_finite() {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}
