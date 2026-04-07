//! Rust ↔ Python type conversion helpers for the PyO3 bindings.

use std::collections::HashSet;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::constants::{Alignment, Measure, Role};
use crate::metadata::{MissingSpec, MrSet, MrType, Value};

// ---------------------------------------------------------------------------
// Rust → Python conversion helpers
// ---------------------------------------------------------------------------

pub(super) fn value_to_py(py: Python<'_>, v: &Value) -> Py<PyAny> {
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
pub(super) fn missing_specs_to_py(py: Python<'_>, specs: &[MissingSpec]) -> PyResult<Py<PyAny>> {
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

pub(super) fn mr_set_to_py(py: Python<'_>, mr: &MrSet) -> PyResult<Py<PyAny>> {
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

pub(super) fn py_to_notes(obj: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
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

pub(super) fn py_to_measure(s: &str) -> PyResult<Measure> {
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

pub(super) fn py_to_alignment(s: &str) -> PyResult<Alignment> {
    match s.to_lowercase().as_str() {
        "left" => Ok(Alignment::Left),
        "right" => Ok(Alignment::Right),
        "center" => Ok(Alignment::Center),
        _ => Err(PyValueError::new_err(format!(
            "invalid alignment '{s}', expected: left, right, center"
        ))),
    }
}

pub(super) fn py_to_role(s: &str) -> PyResult<Role> {
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
pub(super) fn py_to_missing_specs(dict: &Bound<'_, PyDict>) -> PyResult<Vec<MissingSpec>> {
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
pub(super) fn py_to_mr_set(name: &str, dict: &Bound<'_, PyDict>) -> PyResult<MrSet> {
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
