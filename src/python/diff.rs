//! PyO3 bindings for MetaDiff — metadata comparison between two SpssMetadata.

use std::collections::HashSet;

use indexmap::IndexMap;

use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use crate::constants::{Measure, Role};
use crate::metadata::{MissingSpec, SpssMetadata, Value};

// format_count and format_f64 available from super::metadata if needed in the future.

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

// ---------------------------------------------------------------------------
// compute() constructor + print_summary()
// ---------------------------------------------------------------------------

impl PyMetaDiff {
    /// Compute a diff between two SpssMetadata instances.
    /// Called from `PySpssMetadata::diff()`.
    pub(super) fn compute(
        py: Python<'_>,
        a: &SpssMetadata,
        b: &SpssMetadata,
        print_output: bool,
    ) -> PyResult<PyMetaDiff> {
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
        let label_diffs =
            diff_string_maps(py, &a.variable_labels, &b.variable_labels, &shared)?;
        let type_diffs =
            diff_string_maps(py, &a.variable_formats, &b.variable_formats, &shared)?;
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
        let role_diffs =
            diff_role_maps(py, &a.variable_roles, &b.variable_roles, &shared)?;
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

pub(super) fn list_len(py: Python<'_>, obj: &Py<PyAny>) -> usize {
    obj.bind(py)
        .downcast::<PyList>()
        .map(|l| l.len())
        .unwrap_or(0)
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
    a: &IndexMap<String, Measure>,
    b: &IndexMap<String, Measure>,
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
    a: &IndexMap<String, Role>,
    b: &IndexMap<String, Role>,
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
