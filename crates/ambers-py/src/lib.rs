use std::collections::HashSet;

use indexmap::IndexMap;

use pyo3::exceptions::{PyIOError, PyKeyError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

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

    // -----------------------------------------------------------------------
    // Quick lookup methods
    // -----------------------------------------------------------------------

    /// Validate that a variable name exists in the metadata.
    fn check_var(&self, name: &str) -> PyResult<()> {
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

        // Combine date + time into ISO-ish datetime
        let datetime = format_spss_datetime(&m.creation_time, &m.modification_time);

        // File-level scalars
        d.set_item("file_label", &m.file_label)?;
        d.set_item("file_format", &m.file_format)?;
        d.set_item("file_encoding", &m.file_encoding)?;
        d.set_item("creation_time", &datetime)?;
        d.set_item("modification_time", &datetime)?;
        d.set_item("number_rows", m.number_rows)?;
        d.set_item("number_columns", m.number_columns)?;
        d.set_item("weight_variable", m.weight_variable.as_deref())?;

        // Lists
        d.set_item("notes", &m.notes)?;
        d.set_item("variable_names", &m.variable_names)?;

        // Per-variable fields
        d.set_item("variable_labels", self.variable_labels(py)?)?;
        d.set_item("variable_value_labels", self.variable_value_labels(py)?)?;
        d.set_item("variable_measure", self.variable_measure())?;
        d.set_item("spss_variable_types", m.spss_variable_types.clone())?;
        d.set_item("rust_variable_types", m.rust_variable_types.clone())?;
        d.set_item("variable_alignment", self.variable_alignment())?;
        d.set_item("variable_display_width", m.variable_display_width.clone())?;
        d.set_item("variable_storage_width", m.variable_storage_width.clone())?;
        d.set_item("variable_missing", self.variable_missing(py)?)?;
        d.set_item("mr_sets", self.mr_sets(py)?)?;

        Ok(d.unbind().into_any())
    }

    // -----------------------------------------------------------------------
    // summary() — rich formatted overview
    // -----------------------------------------------------------------------

    /// Print a formatted summary of the metadata.
    fn summary(&self) {
        let m = &self.inner;
        let ncols = m.number_columns;
        let rows_str = m
            .number_rows
            .map(|n| format_count(n as usize))
            .unwrap_or_else(|| "unknown".into());

        println!("SPSS Metadata Summary");
        println!("=====================");
        println!(
            "File label:  {}",
            if m.file_label.is_empty() {
                "(none)"
            } else {
                &m.file_label
            }
        );
        println!("Encoding:    {}", m.file_encoding);
        println!(
            "Format:      {} ({})",
            m.file_format,
            self.compression()
        );
        println!("Created:     {} {}", m.creation_time, m.modification_time);
        println!("Rows:        {}", rows_str);
        println!("Columns:     {}", format_count(ncols));
        println!(
            "Weight:      {}",
            m.weight_variable.as_deref().unwrap_or("(none)")
        );
        if m.notes.is_empty() {
            println!("Notes:       (none)");
        } else {
            println!("Notes:       {} document record(s)", m.notes.len());
        }

        // Variable breakdown
        let mut n_numeric = 0usize;
        let mut n_string = 0usize;
        for fmt in m.spss_variable_types.values() {
            if fmt.starts_with('A') {
                n_string += 1;
            } else {
                n_numeric += 1;
            }
        }
        let n_with_labels = m.variable_labels.len();
        let n_with_values = m.variable_value_labels.len();
        let n_with_missing = m.variable_missing.len();
        let n_mr = m.mr_sets.len();

        println!();
        println!("Variable Breakdown:");
        println!("  Numeric       {:>5}", format_count(n_numeric));
        println!("  String        {:>5}", format_count(n_string));
        println!();
        println!(
            "  With labels:  {:>5} / {}",
            format_count(n_with_labels),
            ncols
        );
        println!(
            "  With values:  {:>5} / {}",
            format_count(n_with_values),
            ncols
        );
        println!(
            "  With missing: {:>5} / {}",
            format_count(n_with_missing),
            ncols
        );
        println!("  MR sets:      {:>5}", format_count(n_mr));
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

            let label = m.variable_labels.get(name).map(|s| s.as_str()).unwrap_or("(none)");
            let fmt = m.spss_variable_types.get(name).map(|s| s.as_str()).unwrap_or("?");
            let measure_str = m
                .variable_measure
                .get(name)
                .map(|v| v.as_str())
                .unwrap_or("?");
            let align = m
                .variable_alignment
                .get(name)
                .map(|v| v.as_str())
                .unwrap_or("?");
            let display_w = m
                .variable_display_width
                .get(name)
                .map(|v| v.to_string())
                .unwrap_or_else(|| "?".into());
            let storage_w = m
                .variable_storage_width
                .get(name)
                .map(|v| v.to_string())
                .unwrap_or_else(|| "?".into());

            let type_str = if fmt.starts_with('A') { "String" } else { "Numeric" };

            println!("Variable: {name}");
            println!("Label:    {label}");
            println!("Format:   {fmt:<12}Measure: {measure_str}");
            println!("Type:     {type_str:<12}Align:   {align}");
            println!("Display:  {display_w:<12}Storage: {storage_w}");

            // Missing values
            if let Some(specs) = m.variable_missing.get(name) {
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
        let mut only_self: Vec<String> = a_vars
            .difference(&b_vars)
            .map(|s| s.to_string())
            .collect();
        let mut only_other: Vec<String> = b_vars
            .difference(&a_vars)
            .map(|s| s.to_string())
            .collect();
        only_self.sort();
        only_other.sort();

        // Per-field diffs on shared variables
        let label_diffs = diff_string_maps(py, &a.variable_labels, &b.variable_labels, &shared)?;
        let type_diffs =
            diff_string_maps(py, &a.spss_variable_types, &b.spss_variable_types, &shared)?;
        let measure_diffs = diff_measure_maps(py, &a.variable_measure, &b.variable_measure, &shared)?;
        let display_diffs = diff_u32_maps(
            py,
            &a.variable_display_width,
            &b.variable_display_width,
            &shared,
        )?;
        let storage_diffs = diff_usize_maps(
            py,
            &a.variable_storage_width,
            &b.variable_storage_width,
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
            &a.variable_missing,
            &b.variable_missing,
            &shared,
        )?;
        let mr_diffs = diff_key_sets(py, &a.mr_sets, &b.mr_sets)?;

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
            && list_len(py, &mr_diffs) == 0;

        let result = PyMetaDiff {
            is_match,
            file_level: file_level.unbind().into_any(),
            variables_only_in_self: only_self,
            variables_only_in_other: only_other,
            variable_labels: label_diffs.clone_ref(py),
            variable_value_labels: vvl_diffs.clone_ref(py),
            spss_variable_types: type_diffs.clone_ref(py),
            variable_measure: measure_diffs.clone_ref(py),
            variable_display_width: display_diffs.clone_ref(py),
            variable_storage_width: storage_diffs.clone_ref(py),
            variable_missing: missing_diffs.clone_ref(py),
            mr_sets: mr_diffs.clone_ref(py),
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
    spss_variable_types: Py<PyAny>,
    variable_measure: Py<PyAny>,
    variable_display_width: Py<PyAny>,
    variable_storage_width: Py<PyAny>,
    variable_missing: Py<PyAny>,
    mr_sets: Py<PyAny>,
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
    fn spss_variable_types(&self, py: Python<'_>) -> Py<PyAny> {
        self.spss_variable_types.clone_ref(py)
    }

    #[getter]
    fn variable_measure(&self, py: Python<'_>) -> Py<PyAny> {
        self.variable_measure.clone_ref(py)
    }

    #[getter]
    fn variable_display_width(&self, py: Python<'_>) -> Py<PyAny> {
        self.variable_display_width.clone_ref(py)
    }

    #[getter]
    fn variable_storage_width(&self, py: Python<'_>) -> Py<PyAny> {
        self.variable_storage_width.clone_ref(py)
    }

    #[getter]
    fn variable_missing(&self, py: Python<'_>) -> Py<PyAny> {
        self.variable_missing.clone_ref(py)
    }

    #[getter]
    fn mr_sets(&self, py: Python<'_>) -> Py<PyAny> {
        self.mr_sets.clone_ref(py)
    }

    fn __repr__(&self, py: Python<'_>) -> String {
        let n_self = self.variables_only_in_self.len();
        let n_other = self.variables_only_in_other.len();
        let n_label = list_len(py, &self.variable_labels);
        let n_vvl = list_len(py, &self.variable_value_labels);
        let n_type = list_len(py, &self.spss_variable_types);
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
            "is_match" => Ok(self.is_match.into_pyobject(py).unwrap().to_owned().into_any().unbind()),
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
            "spss_variable_types" => Ok(self.spss_variable_types.clone_ref(py)),
            "variable_measure" => Ok(self.variable_measure.clone_ref(py)),
            "variable_display_width" => Ok(self.variable_display_width.clone_ref(py)),
            "variable_storage_width" => Ok(self.variable_storage_width.clone_ref(py)),
            "variable_missing" => Ok(self.variable_missing.clone_ref(py)),
            "mr_sets" => Ok(self.mr_sets.clone_ref(py)),
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
                let preview: Vec<&str> = self.variables_only_in_self.iter().take(5).map(|s| s.as_str()).collect();
                let suffix = if n_self > 5 { format!(", ... +{}", n_self - 5) } else { String::new() };
                println!("  Only in self:   {:>5}   [{}{}]", n_self, preview.join(", "), suffix);
            }
            if n_other > 0 {
                let preview: Vec<&str> = self.variables_only_in_other.iter().take(5).map(|s| s.as_str()).collect();
                let suffix = if n_other > 5 { format!(", ... +{}", n_other - 5) } else { String::new() };
                println!("  Only in other:  {:>5}   [{}{}]", n_other, preview.join(", "), suffix);
            }
        }

        // Field diffs
        let fields: &[(&str, &Py<PyAny>)] = &[
            ("variable_labels", &self.variable_labels),
            ("variable_value_labels", &self.variable_value_labels),
            ("spss_variable_types", &self.spss_variable_types),
            ("variable_measure", &self.variable_measure),
            ("variable_display_width", &self.variable_display_width),
            ("variable_storage_width", &self.variable_storage_width),
            ("variable_missing", &self.variable_missing),
            ("mr_sets", &self.mr_sets),
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

/// Parse SPSS header date ("16 Feb 26") + time ("10:38:17") into "2026-02-16 10:38:17".
fn format_spss_datetime(date_str: &str, time_str: &str) -> String {
    let parts: Vec<&str> = date_str.split_whitespace().collect();
    if parts.len() == 3 {
        let day: u32 = parts[0].parse().unwrap_or(0);
        let month = match parts[1].to_lowercase().as_str() {
            "jan" => 1,
            "feb" => 2,
            "mar" => 3,
            "apr" => 4,
            "may" => 5,
            "jun" => 6,
            "jul" => 7,
            "aug" => 8,
            "sep" => 9,
            "oct" => 10,
            "nov" => 11,
            "dec" => 12,
            _ => 0,
        };
        let yy: u32 = parts[2].parse().unwrap_or(0);
        let year = 2000 + yy;
        if day > 0 && month > 0 {
            return format!("{year:04}-{month:02}-{day:02} {time_str}");
        }
    }
    // Fallback: just concatenate
    format!("{date_str} {time_str}")
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
    a: &IndexMap<String, ambers::constants::Measure>,
    b: &IndexMap<String, ambers::constants::Measure>,
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
    m.add_class::<PyMetaDiff>()?;
    Ok(())
}
