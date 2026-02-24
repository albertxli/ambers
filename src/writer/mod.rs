//! SAV/ZSAV file writer.
//!
//! Writes Arrow RecordBatch + SpssMetadata to SPSS .sav/.zsav binary format.
//! Supports uncompressed, bytecode-compressed (.sav), and zlib-compressed (.zsav) output.

mod data;
mod layout;
mod records;

#[cfg(test)]
mod tests;

use std::fs::File;
use std::io::{BufWriter, Seek, Write};
use std::path::Path;

use crate::constants::*;
use crate::error::Result;
use crate::metadata::SpssMetadata;
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Write an Arrow RecordBatch to an SPSS .sav file.
///
/// If `metadata` is provided, it controls variable labels, value labels,
/// formats, and other SPSS metadata. If default metadata is used, types
/// are inferred from the Arrow schema.
///
/// # Compression
/// - `Compression::None` — uncompressed .sav
/// - `Compression::Bytecode` — row-compressed .sav
/// - `Compression::Zlib` — block-compressed .zsav
///
/// # Compression Level (zsav only)
/// Controls zlib compression intensity. Ignored for non-zsav files.
/// - `None` — default (level 6, compact)
/// - `Some(1)` — "fast": fastest compression, largest files
/// - `Some(3)` — "balanced": moderate speed, moderate size
/// - `Some(6)` — "compact": slower compression, smallest files (default)
pub fn write_sav(
    path: impl AsRef<Path>,
    batch: &RecordBatch,
    metadata: &SpssMetadata,
    compression: Compression,
    compression_level: Option<u32>,
) -> Result<()> {
    let file = File::create(path)?;
    let writer = BufWriter::with_capacity(4 * 1024 * 1024, file);
    write_sav_to_writer(writer, batch, metadata, compression, compression_level)
}

/// Fill missing metadata fields from the Arrow schema with sensible defaults.
///
/// For each column in the schema, if the metadata is missing a field
/// (format, measure, alignment, display_width, role, storage_width),
/// it is filled using the same type-based logic as `from_arrow_schema()`.
/// User-set fields are never overwritten.
fn fill_defaults_from_schema(meta: &mut SpssMetadata, schema: &arrow::datatypes::Schema) {
    for field in schema.fields() {
        let name = field.name();

        // Infer defaults from Arrow type (same logic as SpssMetadata::from_arrow_schema)
        let (default_fmt, default_measure, default_alignment) = match field.data_type() {
            DataType::Float64 => ("F8.2", Measure::Scale, Alignment::Right),
            DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8 => {
                ("F8.0", Measure::Scale, Alignment::Right)
            }
            DataType::Boolean => ("F1.0", Measure::Nominal, Alignment::Right),
            DataType::Date32 => ("DATE11", Measure::Scale, Alignment::Right),
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                ("DATETIME23.2", Measure::Scale, Alignment::Right)
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                ("TIME11.2", Measure::Scale, Alignment::Right)
            }
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {
                ("A255", Measure::Nominal, Alignment::Left)
            }
            _ => ("F8.2", Measure::Scale, Alignment::Right),
        };

        if !meta.variable_formats.contains_key(name.as_str()) {
            meta.variable_formats
                .insert(name.clone(), default_fmt.to_string());
        }

        if !meta.variable_measures.contains_key(name.as_str()) {
            meta.variable_measures.insert(name.clone(), default_measure);
        }

        if !meta.variable_alignments.contains_key(name.as_str()) {
            meta.variable_alignments
                .insert(name.clone(), default_alignment);
        }

        if !meta.variable_display_widths.contains_key(name.as_str()) {
            // Use the format width (already set above or by user)
            let fmt_str = meta.variable_formats.get(name.as_str()).unwrap();
            let w = SpssFormat::from_string(fmt_str)
                .map(|f| f.width as u32)
                .unwrap_or(8);
            meta.variable_display_widths.insert(name.clone(), w);
        }

        if !meta.variable_roles.contains_key(name.as_str()) {
            meta.variable_roles.insert(name.clone(), Role::Input);
        }

        if !meta.variable_storage_widths.contains_key(name.as_str()) {
            let fmt_str = meta.variable_formats.get(name.as_str()).unwrap();
            let sw = if let Some(rest) = fmt_str.strip_prefix('A') {
                rest.split('.')
                    .next()
                    .and_then(|w| w.parse::<usize>().ok())
                    .unwrap_or(255)
            } else {
                8
            };
            meta.variable_storage_widths.insert(name.clone(), sw);
        }
    }
}

/// Write to any writer that implements Write + Seek.
///
/// Seek is required for zsav (zlib) compression to backpatch the zheader.
/// For non-zsav files, `std::io::Cursor<Vec<u8>>` is a convenient seekable wrapper.
///
/// # Compression Level (zsav only)
/// Controls zlib compression intensity. Ignored for non-zsav files.
/// - `None` — default (level 6, compact)
/// - `Some(1)` — "fast": fastest compression, largest files
/// - `Some(3)` — "balanced": moderate speed, moderate size
/// - `Some(6)` — "compact": slower compression, smallest files (default)
pub fn write_sav_to_writer<W: Write + Seek>(
    mut writer: W,
    batch: &RecordBatch,
    metadata: &SpssMetadata,
    compression: Compression,
    compression_level: Option<u32>,
) -> Result<()> {
    // Fill any missing metadata fields from the schema
    let mut meta = metadata.clone();
    fill_defaults_from_schema(&mut meta, batch.schema().as_ref());
    let metadata = &meta;

    // Validate inputs before heavy computation
    layout::validate_write_inputs(batch.schema().as_ref(), metadata)?;

    let layout = layout::compute_layout(batch, metadata)?;
    let nrows = batch.num_rows() as i32;

    // Write dictionary
    records::write_header(&mut writer, &layout, metadata, compression, nrows)?;
    records::write_variable_records(&mut writer, &layout)?;
    records::write_value_label_records(&mut writer, &layout, metadata)?;
    records::write_document_record(&mut writer, metadata)?;
    records::write_info_integer(&mut writer, compression)?;
    records::write_info_float(&mut writer)?;
    records::write_info_var_display(&mut writer, &layout)?;
    records::write_info_long_names(&mut writer, &layout)?;
    records::write_info_very_long_strings(&mut writer, &layout)?;
    records::write_info_encoding(&mut writer)?;
    records::write_info_long_string_labels(&mut writer, &layout, metadata)?;
    records::write_info_long_string_missing(&mut writer, &layout, metadata)?;
    records::write_info_mr_sets(&mut writer, metadata, &layout)?;
    records::write_info_mr_sets_v2(&mut writer, metadata)?;
    records::write_info_var_attributes(&mut writer, metadata)?;
    records::write_dict_termination(&mut writer)?;

    // Write data
    match compression {
        Compression::None => data::write_data_uncompressed(&mut writer, batch, &layout)?,
        Compression::Bytecode => data::write_data_bytecode(&mut writer, batch, &layout)?,
        Compression::Zlib => {
            let level = flate2::Compression::new(compression_level.unwrap_or(6).min(9));
            data::write_data_zsav(&mut writer, batch, &layout, level)?;
        }
    }

    writer.flush()?;
    Ok(())
}
