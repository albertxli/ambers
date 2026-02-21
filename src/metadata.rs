use arrow::datatypes::{DataType, Schema, TimeUnit};
use indexmap::IndexMap;

use crate::constants::{Alignment, Compression, Measure, Role};
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

/// Convert public MissingSpec list back to internal MissingValues.
pub fn specs_to_missing(specs: &[MissingSpec]) -> MissingValues {
    if specs.is_empty() {
        return MissingValues::None;
    }
    // Separate ranges from discrete values
    let mut ranges: Vec<(f64, f64)> = Vec::new();
    let mut discrete_f64: Vec<f64> = Vec::new();
    let mut discrete_str: Vec<Vec<u8>> = Vec::new();
    for spec in specs {
        match spec {
            MissingSpec::Range { lo, hi } => ranges.push((*lo, *hi)),
            MissingSpec::Value(v) => discrete_f64.push(*v),
            MissingSpec::StringValue(s) => {
                let mut bytes = s.as_bytes().to_vec();
                bytes.resize(8, b' ');
                discrete_str.push(bytes);
            }
        }
    }
    if !discrete_str.is_empty() {
        return MissingValues::DiscreteString(discrete_str);
    }
    if let Some((lo, hi)) = ranges.first() {
        if let Some(&val) = discrete_f64.first() {
            return MissingValues::RangeAndValue {
                low: *lo,
                high: *hi,
                value: val,
            };
        }
        return MissingValues::Range {
            low: *lo,
            high: *hi,
        };
    }
    MissingValues::DiscreteNumeric(discrete_f64)
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
    pub variable_format: IndexMap<String, String>,
    pub arrow_data_types: IndexMap<String, String>,

    // Value labels: {var_name -> {value -> label}}
    pub variable_value_labels: IndexMap<String, IndexMap<Value, String>>,

    // Display properties
    pub variable_alignment: IndexMap<String, Alignment>,
    pub variable_storage_width: IndexMap<String, usize>,
    pub variable_display_width: IndexMap<String, u32>,
    pub variable_measure: IndexMap<String, Measure>,

    // Missing values
    pub variable_missing_values: IndexMap<String, Vec<MissingSpec>>,

    // SPSS-specific
    pub mr_sets: IndexMap<String, MrSet>,
    pub variable_role: IndexMap<String, Role>,
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
        self.variable_format.get(name).map(|s| s.as_str())
    }

    /// Get the measurement level for a variable.
    pub fn measure(&self, name: &str) -> Option<Measure> {
        self.variable_measure.get(name).copied()
    }

    /// Get the role for a variable.
    pub fn role(&self, name: &str) -> Option<Role> {
        self.variable_role.get(name).copied()
    }

    /// Infer metadata from an Arrow schema (for write_sav without prior read metadata).
    pub fn from_arrow_schema(schema: &Schema) -> Self {
        let mut meta = SpssMetadata::default();
        meta.file_encoding = "UTF-8".to_string();
        meta.file_format = "sav".to_string();
        meta.number_columns = schema.fields().len();

        for field in schema.fields() {
            let name = field.name().clone();
            meta.variable_names.push(name.clone());

            let (fmt_str, rust_type, measure, alignment) = match field.data_type() {
                DataType::Float64 => ("F8.2".to_string(), "f64", Measure::Scale, Alignment::Right),
                DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8 => {
                    ("F8.0".to_string(), "f64", Measure::Scale, Alignment::Right)
                }
                DataType::Boolean => ("F1.0".to_string(), "f64", Measure::Nominal, Alignment::Right),
                DataType::Date32 => ("DATE11".to_string(), "Date32", Measure::Scale, Alignment::Right),
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    ("DATETIME23.2".to_string(), "Timestamp[us]", Measure::Scale, Alignment::Right)
                }
                DataType::Duration(TimeUnit::Microsecond) => {
                    ("TIME11.2".to_string(), "Duration[us]", Measure::Scale, Alignment::Right)
                }
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {
                    ("A255".to_string(), "String", Measure::Nominal, Alignment::Left)
                }
                _ => ("F8.2".to_string(), "f64", Measure::Scale, Alignment::Right),
            };

            // String storage width must match the format width so compute_layout()
            // uses the correct VLS segment count.
            let sw = if fmt_str.starts_with('A') {
                fmt_str[1..].parse::<usize>().unwrap_or(255)
            } else {
                8
            };
            meta.variable_format.insert(name.clone(), fmt_str);
            meta.arrow_data_types.insert(name.clone(), rust_type.to_string());
            meta.variable_measure.insert(name.clone(), measure);
            meta.variable_alignment.insert(name.clone(), alignment);
            meta.variable_display_width.insert(name.clone(), 8);
            meta.variable_storage_width.insert(name.clone(), sw);
        }

        meta
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
            variable_format: IndexMap::new(),
            arrow_data_types: IndexMap::new(),
            variable_value_labels: IndexMap::new(),
            variable_alignment: IndexMap::new(),
            variable_storage_width: IndexMap::new(),
            variable_display_width: IndexMap::new(),
            variable_measure: IndexMap::new(),
            variable_missing_values: IndexMap::new(),
            mr_sets: IndexMap::new(),
            variable_role: IndexMap::new(),
            weight_variable: None,
        }
    }
}
