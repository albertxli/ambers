use arrow::datatypes::{DataType, Field, Schema};

use crate::constants::VarType;
use crate::dictionary::ResolvedDictionary;

/// Build an Arrow Schema from the resolved dictionary.
pub fn build_schema(dict: &ResolvedDictionary) -> Schema {
    let fields: Vec<Field> = dict
        .variables
        .iter()
        .map(|var| {
            let data_type = match &var.var_type {
                VarType::Numeric => DataType::Float64,
                VarType::String(_) => DataType::Utf8,
            };
            Field::new(&var.long_name, data_type, true) // all nullable
        })
        .collect();

    Schema::new(fields)
}
