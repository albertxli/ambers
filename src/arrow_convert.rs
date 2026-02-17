use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::constants::VarType;
use crate::data::CellValue;
use crate::dictionary::ResolvedDictionary;
use crate::error::Result;

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

/// Convert row-based data into an Arrow RecordBatch.
pub fn rows_to_record_batch(
    rows: &[Vec<CellValue>],
    dict: &ResolvedDictionary,
) -> Result<RecordBatch> {
    let schema = Arc::new(build_schema(dict));
    let n_cols = dict.variables.len();
    let n_rows = rows.len();

    if n_cols == 0 {
        return Ok(RecordBatch::new_empty(schema));
    }

    // Build column arrays
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(n_cols);

    for col_idx in 0..n_cols {
        let var = &dict.variables[col_idx];

        match &var.var_type {
            VarType::Numeric => {
                let mut builder = Float64Builder::with_capacity(n_rows);
                for row in rows {
                    if col_idx < row.len() {
                        match &row[col_idx] {
                            CellValue::Numeric(v) => builder.append_value(*v),
                            CellValue::Missing => builder.append_null(),
                            CellValue::Text(_) => builder.append_null(),
                        }
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            VarType::String(_) => {
                let mut builder = StringBuilder::with_capacity(n_rows, n_rows * 32);
                for row in rows {
                    if col_idx < row.len() {
                        match &row[col_idx] {
                            CellValue::Text(s) => builder.append_value(s),
                            CellValue::Missing => builder.append_null(),
                            CellValue::Numeric(_) => builder.append_null(),
                        }
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
        }
    }

    let batch = RecordBatch::try_new(schema, columns)?;
    Ok(batch)
}
