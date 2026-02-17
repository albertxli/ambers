use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rayon::prelude::*;

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
/// Columns are built in parallel using rayon.
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

    // Build column arrays in parallel — each column builder is thread-local,
    // rows is &[Vec<CellValue>] which is Sync for read access.
    let columns: Vec<ArrayRef> = dict
        .variables
        .par_iter()
        .enumerate()
        .map(|(col_idx, var)| -> ArrayRef {
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
                    Arc::new(builder.finish())
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
                    Arc::new(builder.finish())
                }
            }
        })
        .collect();

    let batch = RecordBatch::try_new(schema, columns)?;
    Ok(batch)
}

/// Convert row-based data into an Arrow RecordBatch with column projection.
/// `projected_vars` is a slice of indices into `dict.variables`.
/// Each row in `rows` has length = projected_vars.len(), with cells in projection order.
pub fn rows_to_record_batch_projected(
    rows: &[Vec<CellValue>],
    dict: &ResolvedDictionary,
    projected_vars: &[usize],
) -> Result<RecordBatch> {
    let n_rows = rows.len();

    let fields: Vec<Field> = projected_vars
        .iter()
        .map(|&idx| {
            let var = &dict.variables[idx];
            let data_type = match &var.var_type {
                VarType::Numeric => DataType::Float64,
                VarType::String(_) => DataType::Utf8,
            };
            Field::new(&var.long_name, data_type, true)
        })
        .collect();
    let schema = Arc::new(Schema::new(fields));

    if projected_vars.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    // Build columns in parallel. Row cell index = position in projected_vars.
    let columns: Vec<ArrayRef> = (0..projected_vars.len())
        .into_par_iter()
        .map(|proj_col_idx| -> ArrayRef {
            let var = &dict.variables[projected_vars[proj_col_idx]];
            match &var.var_type {
                VarType::Numeric => {
                    let mut builder = Float64Builder::with_capacity(n_rows);
                    for row in rows {
                        if proj_col_idx < row.len() {
                            match &row[proj_col_idx] {
                                CellValue::Numeric(v) => builder.append_value(*v),
                                CellValue::Missing => builder.append_null(),
                                CellValue::Text(_) => builder.append_null(),
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                VarType::String(_) => {
                    let mut builder = StringBuilder::with_capacity(n_rows, n_rows * 32);
                    for row in rows {
                        if proj_col_idx < row.len() {
                            match &row[proj_col_idx] {
                                CellValue::Text(s) => builder.append_value(s),
                                CellValue::Missing => builder.append_null(),
                                CellValue::Numeric(_) => builder.append_null(),
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
            }
        })
        .collect();

    let batch = RecordBatch::try_new(schema, columns)?;
    Ok(batch)
}
