use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use crate::constants::{TemporalKind, VarType};
use crate::dictionary::ResolvedDictionary;
use crate::variable::VariableRecord;

/// Determine the Arrow DataType for a resolved SPSS variable.
pub fn var_to_arrow_type(var: &VariableRecord) -> DataType {
    match &var.var_type {
        VarType::Numeric => {
            match var
                .print_format
                .as_ref()
                .and_then(|f| f.format_type.temporal_kind())
            {
                Some(TemporalKind::Date) => DataType::Date32,
                Some(TemporalKind::Timestamp) => {
                    DataType::Timestamp(TimeUnit::Microsecond, None)
                }
                Some(TemporalKind::Duration) => DataType::Duration(TimeUnit::Microsecond),
                None => DataType::Float64,
            }
        }
        VarType::String(_) => DataType::Utf8View,
    }
}

/// Build an Arrow Schema from the resolved dictionary.
pub fn build_schema(dict: &ResolvedDictionary) -> Schema {
    let fields: Vec<Field> = dict
        .variables
        .iter()
        .map(|var| Field::new(&var.long_name, var_to_arrow_type(var), true))
        .collect();

    Schema::new(fields)
}
