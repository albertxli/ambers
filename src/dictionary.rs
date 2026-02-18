use std::collections::HashMap;
use std::io::Read;

use indexmap::IndexMap;

use encoding_rs::Encoding;

use crate::constants::*;
use crate::encoding;
use crate::error::{Result, SpssError};
use crate::header::FileHeader;
use crate::info_records::{self, InfoRecord, InfoRecordHeader};
use crate::io_utils::SavReader;
use crate::metadata::{self, MissingSpec, SpssMetadata, Value};
use crate::value_labels::{self, RawValue, ValueLabelSet};
use crate::variable::VariableRecord;
use crate::{document, value_labels as vl};

/// All parsed dictionary data before resolution.
#[allow(dead_code)]
pub struct RawDictionary {
    pub header: FileHeader,
    pub variables: Vec<VariableRecord>,
    pub value_label_sets: Vec<ValueLabelSet>,
    pub document_lines: Vec<Vec<u8>>,
    pub integer_info: Option<crate::info_records::integer_info::IntegerInfo>,
    pub float_info: Option<crate::info_records::float_info::FloatInfo>,
    pub var_display: Vec<crate::info_records::var_display::VarDisplayEntry>,
    pub long_names: Vec<(String, String)>,
    pub very_long_strings: Vec<(String, usize)>,
    pub encoding_name: Option<String>,
    pub long_string_labels: Vec<crate::info_records::long_string_labels::LongStringLabelSet>,
    pub long_string_missing: Vec<crate::info_records::long_string_missing::LongStringMissingEntry>,
    pub mr_sets: Vec<crate::info_records::mr_sets::RawMrSet>,
}

/// The resolved dictionary ready for data reading.
pub struct ResolvedDictionary {
    pub header: FileHeader,
    /// Only non-ghost variables, in order.
    pub variables: Vec<VariableRecord>,
    /// The file's character encoding.
    pub file_encoding: &'static Encoding,
    /// Assembled metadata.
    pub metadata: SpssMetadata,
}

/// Parse the entire dictionary section of a SAV file.
///
/// Reads from the current position (after the header) through the type 999
/// termination record. Returns the raw dictionary data.
pub fn parse_dictionary<R: Read>(
    reader: &mut SavReader<R>,
    header: &FileHeader,
) -> Result<RawDictionary> {
    let mut variables = Vec::new();
    let mut value_label_sets = Vec::new();
    let mut document_lines = Vec::new();
    let mut integer_info = None;
    let mut float_info = None;
    let mut var_display = Vec::new();
    let mut long_names = Vec::new();
    let mut very_long_strings = Vec::new();
    let mut encoding_name = None;
    let mut long_string_labels = Vec::new();
    let mut long_string_missing = Vec::new();
    let mut mr_sets = Vec::new();

    let mut slot_index = 0;

    loop {
        let record_type = reader.read_i32()?;

        match record_type {
            RECORD_TYPE_VARIABLE => {
                let var = VariableRecord::parse(reader, slot_index)?;
                slot_index += 1;
                variables.push(var);
            }

            RECORD_TYPE_VALUE_LABEL => {
                let labels = value_labels::parse_value_labels(reader)?;
                // Type 4 record must follow immediately
                let next_type = reader.read_i32()?;
                if next_type != RECORD_TYPE_VALUE_LABEL_VARS {
                    return Err(SpssError::InvalidValueLabel(format!(
                        "expected type 4 record after type 3, got type {next_type}"
                    )));
                }
                let indices = vl::parse_value_label_variables(reader)?;
                value_label_sets.push(ValueLabelSet {
                    labels,
                    variable_indices: indices,
                });
            }

            RECORD_TYPE_DOCUMENT => {
                let lines = document::parse_document(reader)?;
                document_lines.extend(lines);
            }

            RECORD_TYPE_INFO => {
                let info_header = InfoRecordHeader::parse(reader)?;
                let record = info_records::parse_info_record(reader, &info_header)?;
                match record {
                    InfoRecord::IntegerInfo(info) => integer_info = Some(info),
                    InfoRecord::FloatInfo(info) => float_info = Some(info),
                    InfoRecord::VarDisplay(entries) => var_display = entries,
                    InfoRecord::LongNames(names) => long_names = names,
                    InfoRecord::VeryLongStrings(entries) => very_long_strings = entries,
                    InfoRecord::Encoding(name) => encoding_name = Some(name),
                    InfoRecord::LongStringLabels(labels) => long_string_labels = labels,
                    InfoRecord::LongStringMissing(entries) => long_string_missing = entries,
                    InfoRecord::MrSets(sets) => mr_sets = sets,
                    InfoRecord::Unknown { .. } => {} // skip
                }
            }

            RECORD_TYPE_DICT_TERMINATION => {
                // Read the filler int
                let _filler = reader.read_i32()?;
                break;
            }

            _ => {
                return Err(SpssError::UnexpectedRecordType {
                    record_type,
                    offset: 0, // we don't track offset in the stream
                });
            }
        }
    }

    Ok(RawDictionary {
        header: header.clone(),
        variables,
        value_label_sets,
        document_lines,
        integer_info,
        float_info,
        var_display,
        long_names,
        very_long_strings,
        encoding_name,
        long_string_labels,
        long_string_missing,
        mr_sets,
    })
}

/// Resolve the raw dictionary into a fully processed dictionary with metadata.
pub fn resolve_dictionary(raw: RawDictionary) -> Result<ResolvedDictionary> {
    let mut variables = raw.variables;

    // 1. Determine character encoding
    let file_encoding = determine_encoding(&raw.encoding_name, &raw.integer_info);

    // 2. Apply long variable names (subtype 13)
    let long_name_map: HashMap<String, String> = raw.long_names.into_iter().collect();
    for var in &mut variables {
        if let Some(long_name) = long_name_map.get(&var.short_name) {
            var.long_name = long_name.clone();
        }
    }

    // 3. Resolve very long strings (subtype 14)
    //
    // Very long strings (width > 255) are stored across multiple named variable
    // records called "segments". Each segment is a 255-byte string variable
    // (except the last which may be shorter), followed by type=-1 continuation
    // records. The type=-1 records are already marked as ghosts, but the named
    // segment records (segments 2+) need to be marked as ghosts too.
    let vls_map: HashMap<String, usize> = raw.very_long_strings.into_iter().collect();
    for i in 0..variables.len() {
        let lookup_name = variables[i].short_name.clone();
        if let Some(&true_width) = vls_map.get(&lookup_name) {
            variables[i].var_type = VarType::String(true_width);
            let n_segments = (true_width + 251) / 252;
            variables[i].n_segments = n_segments;

            // Mark subsequent named segment variables as ghosts
            if n_segments > 1 {
                let mut segments_found = 1; // first segment is this variable
                let mut j = i + 1;
                while j < variables.len() && segments_found < n_segments {
                    if !variables[j].is_ghost {
                        // This is a named segment record -- mark as ghost
                        variables[j].is_ghost = true;
                        segments_found += 1;
                    }
                    j += 1;
                }
            }
        }
    }

    // 4. Apply variable display info (subtype 11)
    //
    // Subtype 11 has one entry per non-continuation variable record (i.e. every
    // record where raw_type != -1), including the named VLS segment variables
    // that we marked as ghosts in step 3. We must consume one display entry per
    // such record, using n_segments to skip the segment entries for VLS vars.
    let mut display_idx = 0;
    let mut var_idx = 0;
    while var_idx < variables.len() {
        if variables[var_idx].raw_type == -1 {
            // Type -1 continuation records have no subtype 11 entry
            var_idx += 1;
            continue;
        }
        // This is a named variable record — it consumes one display entry
        if display_idx < raw.var_display.len() {
            let entry = &raw.var_display[display_idx];
            if !variables[var_idx].is_ghost {
                // Only apply to non-ghost (visible) variables
                variables[var_idx].measure = entry.measure;
                variables[var_idx].display_width = entry.width;
                variables[var_idx].alignment = entry.alignment;
            }
        }
        display_idx += 1;
        var_idx += 1;
    }

    // 5. Build metadata
    let mut meta = SpssMetadata::default();
    meta.file_label = raw.header.file_label.clone();
    meta.file_encoding = file_encoding.name().to_string();
    meta.compression = raw.header.compression;
    meta.creation_time = raw.header.creation_date.clone();
    meta.modification_time = raw.header.creation_time.clone();
    meta.number_rows = if raw.header.ncases >= 0 {
        Some(raw.header.ncases as i64)
    } else {
        None
    };
    meta.file_format = if raw.header.compression == Compression::Zlib {
        "zsav".to_string()
    } else {
        "sav".to_string()
    };

    // Document lines -> notes
    meta.notes = raw
        .document_lines
        .iter()
        .map(|line| encoding::decode_str_lossy(line, file_encoding).into_owned())
        .collect();

    // Build per-variable metadata
    let visible_vars: Vec<&VariableRecord> = variables.iter().filter(|v| !v.is_ghost).collect();
    meta.number_columns = visible_vars.len();

    for var in &visible_vars {
        let name = var.long_name.clone();
        meta.variable_names.push(name.clone());

        // Variable label
        if let Some(ref label_bytes) = var.label {
            let label = encoding::decode_str_lossy(label_bytes, file_encoding)
                .trim_end_matches(|c: char| c == ' ' || c == '\u{FFFD}')
                .to_string();
            if !label.is_empty() {
                meta.variable_labels.insert(name.clone(), label);
            }
        }

        // Format string
        if let Some(ref fmt) = var.print_format {
            let format_str = match &var.var_type {
                VarType::String(w) if *w > 255 => {
                    // VLS: override the u8-capped width with true width
                    format!("{}{}", fmt.format_type.prefix(), w)
                }
                _ => fmt.to_spss_string(),
            };
            meta.spss_variable_types
                .insert(name.clone(), format_str);
        }

        // Rust type
        let rust_type = match &var.var_type {
            VarType::Numeric => {
                match var
                    .print_format
                    .as_ref()
                    .and_then(|f| f.format_type.temporal_kind())
                {
                    Some(TemporalKind::Date) => "Date32".to_string(),
                    Some(TemporalKind::Timestamp) => "Timestamp[us]".to_string(),
                    Some(TemporalKind::Duration) => "Duration[us]".to_string(),
                    None => "f64".to_string(),
                }
            }
            VarType::String(_) => "String".to_string(),
        };
        meta.rust_variable_types.insert(name.clone(), rust_type);

        // Display properties
        meta.variable_measure.insert(name.clone(), var.measure);
        // For VLS variables, if display_width is 0 (from u8-capped format), use true width
        let display_width = match &var.var_type {
            VarType::String(w) if *w > 255 && var.display_width == 0 => *w as u32,
            _ => var.display_width,
        };
        meta.variable_display_width
            .insert(name.clone(), display_width);
        meta.variable_alignment.insert(name.clone(), var.alignment);

        // Storage width: normal strings round to 8-byte slot boundary,
        // VLS strings (>255) use their declared width as-is (matching pyreadstat)
        let storage_width = match &var.var_type {
            VarType::Numeric => 8,
            VarType::String(w) if *w > 255 => *w,
            VarType::String(w) => crate::io_utils::round_up(*w, 8),
        };
        meta.variable_storage_width
            .insert(name.clone(), storage_width);

        // Missing values
        let specs = metadata::missing_to_specs(&var.missing_values);
        if !specs.is_empty() {
            meta.variable_missing.insert(name.clone(), specs);
        }
    }

    // Weight variable
    if raw.header.weight_index > 0 {
        let weight_slot = (raw.header.weight_index - 1) as usize;
        if let Some(var) = variables.iter().find(|v| v.slot_index == weight_slot) {
            meta.weight_variable = Some(var.long_name.clone());
        }
    }

    // 6. Resolve value labels
    // Build slot_index -> variable name mapping
    let slot_to_name: HashMap<usize, String> = variables
        .iter()
        .filter(|v| !v.is_ghost)
        .map(|v| (v.slot_index, v.long_name.clone()))
        .collect();

    let slot_to_type: HashMap<usize, &VarType> = variables
        .iter()
        .map(|v| (v.slot_index, &v.var_type))
        .collect();

    for label_set in &raw.value_label_sets {
        // Determine if these are string or numeric labels based on first linked variable
        let is_string = label_set
            .variable_indices
            .first()
            .and_then(|&idx| slot_to_type.get(&idx))
            .is_some_and(|t| matches!(t, VarType::String(_)));

        let resolved_labels: IndexMap<Value, String> = label_set
            .labels
            .iter()
            .map(|(raw_val, label_bytes)| {
                let value = if is_string {
                    match raw_val {
                        RawValue::Numeric(v) => {
                            let bytes = v.to_le_bytes();
                            let s = encoding::decode_str_lossy(
                                crate::io_utils::trim_trailing_padding(&bytes),
                                file_encoding,
                            );
                            Value::String(s.into_owned())
                        }
                        RawValue::String(bytes) => {
                            Value::String(encoding::decode_str_lossy(
                                crate::io_utils::trim_trailing_padding(bytes),
                                file_encoding,
                            ).into_owned())
                        }
                    }
                } else {
                    match raw_val {
                        RawValue::Numeric(v) => Value::Numeric(*v),
                        RawValue::String(_) => Value::Numeric(0.0),
                    }
                };
                let label = encoding::decode_str_lossy(label_bytes, file_encoding)
                    .trim_end_matches(|c: char| c == ' ' || c == '\u{FFFD}')
                    .to_string();
                (value, label)
            })
            .collect();

        for &slot_idx in &label_set.variable_indices {
            if let Some(var_name) = slot_to_name.get(&slot_idx) {
                meta.variable_value_labels
                    .insert(var_name.clone(), resolved_labels.clone());
            }
        }
    }

    // 7. Resolve long string value labels (subtype 21)
    for ls_set in &raw.long_string_labels {
        let var_name = &ls_set.var_name;
        let labels: IndexMap<Value, String> = ls_set
            .labels
            .iter()
            .map(|(value_bytes, label_bytes)| {
                let value = Value::String(encoding::decode_str_lossy(
                    crate::io_utils::trim_trailing_padding(value_bytes),
                    file_encoding,
                ).into_owned());
                let label = encoding::decode_str_lossy(label_bytes, file_encoding)
                    .trim_end_matches(|c: char| c == ' ' || c == '\u{FFFD}')
                    .to_string();
                (value, label)
            })
            .collect();

        if !labels.is_empty() {
            meta.variable_value_labels
                .insert(var_name.clone(), labels);
        }
    }

    // 8. Resolve long string missing values (subtype 22)
    for ls_missing in &raw.long_string_missing {
        let specs: Vec<MissingSpec> = ls_missing
            .values
            .iter()
            .map(|v| {
                MissingSpec::StringValue(
                    encoding::decode_str_lossy(
                        crate::io_utils::trim_trailing_padding(v),
                        file_encoding,
                    ).into_owned(),
                )
            })
            .collect();

        if !specs.is_empty() {
            meta.variable_missing
                .insert(ls_missing.var_name.clone(), specs);
        }
    }

    // Reorder variable_value_labels to match variable_names order
    let ordered_vvl: IndexMap<String, IndexMap<Value, String>> = meta
        .variable_names
        .iter()
        .filter_map(|name| {
            meta.variable_value_labels
                .swap_remove(name)
                .map(|v| (name.clone(), v))
        })
        .collect();
    meta.variable_value_labels = ordered_vvl;

    // 9. Resolve multiple response sets (subtype 7)
    // MR set variable names are SHORT names — convert to long names
    let short_to_long: HashMap<String, String> = variables
        .iter()
        .filter(|v| !v.is_ghost)
        .map(|v| (v.short_name.clone(), v.long_name.clone()))
        .collect();
    for raw_mr in &raw.mr_sets {
        let resolved_vars: Vec<String> = raw_mr
            .var_names
            .iter()
            .filter_map(|short| {
                let key = short.to_uppercase();
                short_to_long.get(&key).cloned()
            })
            .collect();
        if !resolved_vars.is_empty() {
            meta.mr_sets.insert(
                raw_mr.name.clone(),
                metadata::MrSet {
                    name: raw_mr.name.clone(),
                    label: raw_mr.label.clone(),
                    mr_type: raw_mr.mr_type.clone(),
                    counted_value: raw_mr.counted_value.clone(),
                    variables: resolved_vars,
                },
            );
        }
    }

    // Filter to visible (non-ghost) variables
    let visible_variables: Vec<VariableRecord> =
        variables.into_iter().filter(|v| !v.is_ghost).collect();

    Ok(ResolvedDictionary {
        header: raw.header,
        variables: visible_variables,
        file_encoding,
        metadata: meta,
    })
}

/// Determine the character encoding from available info records.
fn determine_encoding(
    encoding_name: &Option<String>,
    integer_info: &Option<crate::info_records::integer_info::IntegerInfo>,
) -> &'static Encoding {
    // Priority 1: Subtype 20 encoding name
    if let Some(name) = encoding_name {
        return encoding::encoding_from_name(name);
    }

    // Priority 2: Subtype 3 character code
    if let Some(info) = integer_info {
        return encoding::encoding_from_code_page(info.character_code);
    }

    // Default: windows-1252 (historical SPSS default on Windows)
    encoding_rs::WINDOWS_1252
}
