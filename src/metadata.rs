use indexmap::IndexMap;

use crate::constants::{Alignment, Compression, Measure};
use crate::variable::MissingValues;

/// A value that can be used as a key in value label maps.
#[derive(Debug, Clone)]
pub enum Value {
    Numeric(f64),
    String(String),
}

// Manual Hash/Eq for Value since f64 doesn't implement Hash.
// We use the raw bit pattern for numeric values.
impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Value::Numeric(v) => {
                0_u8.hash(state);
                v.to_bits().hash(state);
            }
            Value::String(s) => {
                1_u8.hash(state);
                s.hash(state);
            }
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Numeric(a), Value::Numeric(b)) => a.to_bits() == b.to_bits(),
            (Value::String(a), Value::String(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Value::Numeric(a), Value::Numeric(b)) => {
                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
            }
            (Value::String(a), Value::String(b)) => a.cmp(b),
            // Numeric sorts before String
            (Value::Numeric(_), Value::String(_)) => std::cmp::Ordering::Less,
            (Value::String(_), Value::Numeric(_)) => std::cmp::Ordering::Greater,
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Numeric(v) => {
                // Display as integer if it's a whole number
                if v.fract() == 0.0 && v.is_finite() {
                    write!(f, "{}", *v as i64)
                } else {
                    write!(f, "{v}")
                }
            }
            Value::String(s) => write!(f, "{s}"),
        }
    }
}

/// A missing value specification for the public API.
#[derive(Debug, Clone)]
pub enum MissingSpec {
    /// A single discrete missing value.
    Value(f64),
    /// A range of missing values.
    Range { lo: f64, hi: f64 },
    /// A discrete string missing value.
    StringValue(String),
}

/// Convert internal MissingValues to public MissingSpec list.
pub fn missing_to_specs(mv: &MissingValues) -> Vec<MissingSpec> {
    match mv {
        MissingValues::None => vec![],
        MissingValues::DiscreteNumeric(vals) => {
            vals.iter().map(|&v| MissingSpec::Value(v)).collect()
        }
        MissingValues::Range { low, high } => {
            vec![MissingSpec::Range {
                lo: *low,
                hi: *high,
            }]
        }
        MissingValues::RangeAndValue { low, high, value } => {
            vec![
                MissingSpec::Range {
                    lo: *low,
                    hi: *high,
                },
                MissingSpec::Value(*value),
            ]
        }
        MissingValues::DiscreteString(vals) => vals
            .iter()
            .map(|v| {
                MissingSpec::StringValue(
                    String::from_utf8_lossy(v).trim_end().to_string(),
                )
            })
            .collect(),
    }
}

/// Multiple response set definition.
#[derive(Debug, Clone)]
pub struct MrSet {
    pub name: String,
    pub label: String,
    pub mr_type: MrType,
    pub counted_value: Option<String>,
    pub variables: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MrType {
    MultipleDichotomy,
    MultipleCategory,
}

/// The complete metadata for an SPSS file.
#[derive(Debug, Clone)]
pub struct SpssMetadata {
    // File-level
    pub file_label: String,
    pub file_encoding: String,
    pub compression: Compression,
    pub creation_time: String,
    pub modification_time: String,
    pub notes: Vec<String>,
    pub number_rows: Option<i64>,
    pub number_columns: usize,
    pub file_format: String,

    // Variable names (ordered -- defines Arrow schema column order)
    pub variable_names: Vec<String>,

    // Variable labels: {name -> label}
    pub variable_labels: IndexMap<String, String>,

    // Type info
    pub spss_variable_types: IndexMap<String, String>,
    pub rust_variable_types: IndexMap<String, String>,

    // Value labels: {var_name -> {value -> label}}
    pub variable_value_labels: IndexMap<String, IndexMap<Value, String>>,

    // Display properties
    pub variable_alignment: IndexMap<String, Alignment>,
    pub variable_storage_width: IndexMap<String, usize>,
    pub variable_display_width: IndexMap<String, u32>,
    pub variable_measure: IndexMap<String, Measure>,

    // Missing values
    pub variable_missing: IndexMap<String, Vec<MissingSpec>>,

    // SPSS-specific
    pub mr_sets: IndexMap<String, MrSet>,
    pub weight_variable: Option<String>,
}

impl SpssMetadata {
    /// Get a variable label by name.
    pub fn label(&self, name: &str) -> Option<&str> {
        self.variable_labels.get(name).map(|s| s.as_str())
    }

    /// Get value labels for a variable.
    pub fn value_labels(&self, name: &str) -> Option<&IndexMap<Value, String>> {
        self.variable_value_labels.get(name)
    }

    /// Get the SPSS format string for a variable (e.g., "F8.2", "A50").
    pub fn format(&self, name: &str) -> Option<&str> {
        self.spss_variable_types.get(name).map(|s| s.as_str())
    }

    /// Get the measurement level for a variable.
    pub fn measure(&self, name: &str) -> Option<Measure> {
        self.variable_measure.get(name).copied()
    }

}

impl Default for SpssMetadata {
    fn default() -> Self {
        SpssMetadata {
            file_label: String::new(),
            file_encoding: "UTF-8".to_string(),
            compression: Compression::None,
            creation_time: String::new(),
            modification_time: String::new(),
            notes: Vec::new(),
            number_rows: None,
            number_columns: 0,
            file_format: "sav".to_string(),
            variable_names: Vec::new(),
            variable_labels: IndexMap::new(),
            spss_variable_types: IndexMap::new(),
            rust_variable_types: IndexMap::new(),
            variable_value_labels: IndexMap::new(),
            variable_alignment: IndexMap::new(),
            variable_storage_width: IndexMap::new(),
            variable_display_width: IndexMap::new(),
            variable_measure: IndexMap::new(),
            variable_missing: IndexMap::new(),
            mr_sets: IndexMap::new(),
            weight_variable: None,
        }
    }
}
