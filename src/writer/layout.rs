//! Layout types and computation for SAV/ZSAV writer.

use std::collections::HashSet;

use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use indexmap::IndexMap;

use crate::constants::*;
use crate::error::{Result, SpssError};
use crate::metadata::{MissingSpec, SpssMetadata, specs_to_missing};
use crate::variable::MissingValues;

// ---------------------------------------------------------------------------
// Internal layout types
// ---------------------------------------------------------------------------

/// Write descriptor for one visible variable.
pub(super) struct WriteVariable {
    pub(super) long_name: String,
    pub(super) short_name: String,
    pub(super) format: SpssFormat,
    pub(super) var_type: VarType,
    pub(super) label: Option<String>,
    #[allow(dead_code)]
    pub(super) missing_values: MissingValues,
    pub(super) measure: Measure,
    pub(super) alignment: Alignment,
    pub(super) display_width: u32,
    /// Number of 8-byte slots per non-last segment (32 for VLS, variable for others).
    pub(super) n_slots: usize,
    /// For VLS (width > 255): number of segments.
    pub(super) n_segments: usize,
    /// Slots for the last segment (may differ from n_slots for VLS).
    pub(super) last_n_slots: usize,
    /// Total storage width in bytes (may exceed 255 for VLS).
    pub(super) storage_width: usize,
    /// Index of this variable in the RecordBatch columns.
    pub(super) col_index: usize,
}

impl WriteVariable {
    /// Total 8-byte slots for this variable across all segments.
    pub(super) fn total_slots(&self) -> usize {
        if self.n_segments <= 1 {
            self.n_slots
        } else {
            (self.n_segments - 1) * self.n_slots + self.last_n_slots
        }
    }
}

/// A single Type-2 record to emit (may be a primary, ghost, or segment record).
pub(super) struct SlotRecord {
    pub(super) raw_type: i32,
    pub(super) short_name: String,
    pub(super) label: Option<Vec<u8>>,
    pub(super) print_format: SpssFormat,
    pub(super) write_format: SpssFormat,
    pub(super) missing_values: MissingValues,
    /// True if this is a continuation/ghost record (raw_type = -1).
    pub(super) is_ghost: bool,
}

/// Pre-computed layout of the entire file.
pub(super) struct CaseLayout {
    /// Visible variable descriptors (one per Arrow column).
    pub(super) write_vars: Vec<WriteVariable>,
    /// All Type-2 records to emit (including ghosts and segments).
    pub(super) slot_records: Vec<SlotRecord>,
    /// Total 8-byte slots per case.
    pub(super) slots_per_row: usize,
    /// Short->long name mappings for subtype 13.
    pub(super) short_to_long: IndexMap<String, String>,
    /// VLS declarations for subtype 14: (short_name, true_width).
    pub(super) very_long_strings: Vec<(String, usize)>,
}

// ---------------------------------------------------------------------------
// Short name generation
// ---------------------------------------------------------------------------

/// Generate a unique 8-char short name from a long name.
pub(super) fn generate_short_name(long_name: &str, used: &mut HashSet<String>) -> String {
    // Convert to uppercase ASCII, replace non-alnum with '_'
    let clean: String = long_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect();

    // Ensure starts with a letter
    let clean = if clean.is_empty() || !clean.chars().next().unwrap().is_ascii_alphabetic() {
        format!("V{clean}")
    } else {
        clean
    };

    // Try truncated name first
    let candidate = if clean.len() <= 8 {
        clean.clone()
    } else {
        clean[..8].to_string()
    };

    if !used.contains(&candidate) {
        used.insert(candidate.clone());
        return candidate;
    }

    // Collision: try stem + numeric suffix
    for i in 0..10000 {
        let suffix = format!("{i}");
        let stem_len = 8 - suffix.len();
        let stem = &clean[..stem_len.min(clean.len())];
        let candidate = format!("{stem}{suffix}");
        if !used.contains(&candidate) {
            used.insert(candidate.clone());
            return candidate;
        }
    }
    panic!("exhausted short name space");
}

/// Generate segment short names for VLS (segments 1+).
fn generate_segment_names(
    primary_short: &str,
    n_segments: usize,
    used: &mut HashSet<String>,
) -> Vec<String> {
    let mut names = Vec::new();
    let stem5 = &primary_short[..5.min(primary_short.len())];
    for i in 1..n_segments {
        // Try stem + 3-digit segment index (readstat convention)
        let suffix = format!("{i:03}");
        let stem_len = (8 - suffix.len()).min(primary_short.len());
        let candidate = format!("{}{suffix}", &primary_short[..stem_len]);
        let mut final_name = candidate;
        // If collision, use incrementing counter to guarantee termination
        let mut counter = 0u32;
        while used.contains(&final_name) {
            counter += 1;
            let num = format!("{counter}");
            let stem_len = (8 - num.len()).min(stem5.len());
            final_name = format!("{}{num}", &stem5[..stem_len]);
        }
        used.insert(final_name.clone());
        names.push(final_name);
    }
    names
}

// ---------------------------------------------------------------------------
// Input validation (fail-fast, before expensive data operations)
// ---------------------------------------------------------------------------

/// Validate that metadata and schema are compatible for writing.
/// Call this before expensive data operations to fail fast on errors.
pub(crate) fn validate_write_inputs(
    schema: &arrow::datatypes::Schema,
    meta: &SpssMetadata,
) -> Result<()> {
    for field in schema.fields() {
        let name = field.name();

        // Check missing value type compatibility
        if let Some(specs) = meta.variable_missing_values.get(name.as_str()) {
            let is_string_col = matches!(
                field.data_type(),
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
            );
            let has_numeric = specs
                .iter()
                .any(|s| matches!(s, MissingSpec::Value(_) | MissingSpec::Range { .. }));
            let has_string = specs
                .iter()
                .any(|s| matches!(s, MissingSpec::StringValue(_)));

            if is_string_col && has_numeric {
                return Err(SpssError::WriteError(format!(
                    "variable '{}': numeric missing values cannot be applied to a string variable",
                    name
                )));
            }
            if !is_string_col && has_string {
                return Err(SpssError::WriteError(format!(
                    "variable '{}': string missing values cannot be applied to a numeric variable",
                    name
                )));
            }
        }

        // Validate format string is parseable if provided
        if let Some(fmt_str) = meta.variable_formats.get(name.as_str())
            && SpssFormat::from_string(fmt_str).is_none()
        {
            return Err(SpssError::WriteError(format!(
                "variable '{}': invalid format string '{}'",
                name, fmt_str
            )));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Layout computation
// ---------------------------------------------------------------------------

pub(super) fn compute_layout(batch: &RecordBatch, meta: &SpssMetadata) -> Result<CaseLayout> {
    let schema = batch.schema();
    let mut write_vars = Vec::new();
    let mut slot_records = Vec::new();
    let mut used_short_names = HashSet::new();
    let mut short_to_long = IndexMap::new();
    let mut very_long_strings = Vec::new();
    let mut slot_index = 0;

    for (col_index, field) in schema.fields().iter().enumerate() {
        let name = field.name();

        // Resolve format from metadata or infer from Arrow type
        let format = meta
            .variable_formats
            .get(name.as_str())
            .and_then(|s| SpssFormat::from_string(s))
            .unwrap_or_else(|| infer_format(field.data_type()));

        // Determine variable type and storage width.
        // Use the format string's declared width (not storage_width) to decide VLS vs non-VLS,
        // because storage_width is 8-byte rounded (e.g., 254 -> 256) and would falsely trigger VLS.
        // For true VLS (format width > 255), use storage_width from metadata.
        let (var_type, storage_width) = if format.format_type.is_string() {
            // Parse the original format string for the declared width (not u8-capped).
            // E.g., "A254" -> 254 (non-VLS), "A2000" -> 2000 (VLS).
            let declared_width = meta
                .variable_formats
                .get(name.as_str())
                .and_then(|s| {
                    let rest = s.trim_start_matches(|c: char| !c.is_ascii_digit());
                    rest.split('.').next().and_then(|w| w.parse::<usize>().ok())
                })
                .unwrap_or(format.width as usize)
                .max(1);

            if declared_width > 255 {
                // True VLS: use storage_width from metadata for the actual byte count
                let w = meta
                    .variable_storage_widths
                    .get(name.as_str())
                    .copied()
                    .unwrap_or(declared_width);
                (VarType::String(w), w)
            } else {
                // Non-VLS: use format's declared width
                (VarType::String(declared_width), declared_width)
            }
        } else {
            (VarType::Numeric, 8)
        };

        // Compute segments for VLS (255 useful bytes per segment)
        let n_segments = match &var_type {
            VarType::String(width) if *width > 255 => width.div_ceil(252),
            _ => 1,
        };

        // Slots per non-last segment
        let n_slots = match &var_type {
            VarType::Numeric => 1,
            VarType::String(width) => {
                if n_segments > 1 {
                    // VLS non-last segment: 255 bytes = ceil(255/8) = 32 slots
                    32
                } else {
                    width.div_ceil(8)
                }
            }
        };

        // Slots for the last segment (may differ for VLS)
        let last_n_slots = if n_segments > 1 {
            if let VarType::String(width) = &var_type {
                let remaining = width - (n_segments - 1) * 252;
                remaining.div_ceil(8)
            } else {
                n_slots
            }
        } else {
            n_slots
        };

        let label = meta.variable_labels.get(name.as_str()).cloned();
        let missing_values = meta
            .variable_missing_values
            .get(name.as_str())
            .map(|specs| {
                // Validate missing value type matches variable type
                let is_string_var = matches!(var_type, VarType::String(_));
                let has_numeric = specs.iter().any(|s| {
                    matches!(s, MissingSpec::Value(_) | MissingSpec::Range { .. })
                });
                let has_string = specs.iter().any(|s| matches!(s, MissingSpec::StringValue(_)));
                if is_string_var && has_numeric {
                    return Err(SpssError::WriteError(format!(
                        "variable '{}': numeric missing values cannot be applied to a string variable",
                        name
                    )));
                }
                if !is_string_var && has_string {
                    return Err(SpssError::WriteError(format!(
                        "variable '{}': string missing values cannot be applied to a numeric variable",
                        name
                    )));
                }
                Ok(specs_to_missing(specs))
            })
            .transpose()?
            .unwrap_or(MissingValues::None);
        let measure = meta
            .variable_measures
            .get(name.as_str())
            .copied()
            .unwrap_or(Measure::Unknown);
        let alignment = meta
            .variable_alignments
            .get(name.as_str())
            .copied()
            .unwrap_or(Alignment::Right);
        let display_width = meta
            .variable_display_widths
            .get(name.as_str())
            .copied()
            .unwrap_or(format.width as u32);

        // Generate short name for this variable
        let short_name = generate_short_name(name, &mut used_short_names);
        short_to_long.insert(short_name.clone(), name.clone());

        // VLS subtype 14 declaration
        if n_segments > 1 {
            very_long_strings.push((short_name.clone(), storage_width));
        }

        // Build WriteVariable
        write_vars.push(WriteVariable {
            long_name: name.clone(),
            short_name: short_name.clone(),
            format: format.clone(),
            var_type: var_type.clone(),
            label,
            missing_values: missing_values.clone(),
            measure,
            alignment,
            display_width,
            n_slots,
            n_segments,
            last_n_slots,
            storage_width,
            col_index,
        });

        // Emit slot records for this variable
        if n_segments == 1 {
            // Simple variable: one type-2 record
            let raw_type = match &var_type {
                VarType::Numeric => 0,
                VarType::String(w) => (*w).min(255) as i32,
            };
            slot_records.push(SlotRecord {
                raw_type,
                short_name: short_name.clone(),
                label: write_vars
                    .last()
                    .unwrap()
                    .label
                    .as_ref()
                    .map(|s| s.as_bytes().to_vec()),
                print_format: format.clone(),
                write_format: format.clone(),
                // Strings > 8 bytes: missing values go in subtype 22 only, not type-2 record
                missing_values: if matches!(&var_type, VarType::String(w) if *w > 8) {
                    MissingValues::None
                } else {
                    missing_values.clone()
                },
                is_ghost: false,
            });
            slot_index += 1;

            // String continuation ghosts (for width > 8)
            if let VarType::String(width) = &var_type {
                let extra_slots = n_slots - 1;
                for _ in 0..extra_slots {
                    slot_records.push(SlotRecord {
                        raw_type: -1,
                        short_name: String::new(),
                        label: None,
                        print_format: SpssFormat {
                            format_type: FormatType::A,
                            width: 0,
                            decimals: 0,
                        },
                        write_format: SpssFormat {
                            format_type: FormatType::A,
                            width: 0,
                            decimals: 0,
                        },
                        missing_values: MissingValues::None,
                        is_ghost: true,
                    });
                    slot_index += 1;
                }
                let _ = width; // suppress unused
            }
        } else {
            // VLS: n_segments segments.
            // Non-last segments: 32 slots (width=255).
            // Last segment: fewer slots based on remaining width.
            let segment_names =
                generate_segment_names(&short_name, n_segments, &mut used_short_names);

            // Remaining width for the last segment (per ReadStat convention)
            let remaining_width = if let VarType::String(w) = &var_type {
                w - (n_segments - 1) * 252
            } else {
                255
            };

            for seg in 0..n_segments {
                let is_last_seg = seg == n_segments - 1;
                let seg_short_name = if seg == 0 {
                    short_name.clone()
                } else {
                    segment_names[seg - 1].clone()
                };

                // Non-last segments declare width=255; last declares remaining width
                let seg_width: u8 = if is_last_seg {
                    remaining_width.min(255) as u8
                } else {
                    255
                };
                let seg_format = SpssFormat {
                    format_type: FormatType::A,
                    width: seg_width,
                    decimals: 0,
                };

                // Primary segment gets label; others don't.
                // VLS strings (width > 8): missing values go in subtype 22 only.
                let (seg_label, seg_missing) = if seg == 0 {
                    (
                        write_vars
                            .last()
                            .unwrap()
                            .label
                            .as_ref()
                            .map(|s| s.as_bytes().to_vec()),
                        MissingValues::None,
                    )
                } else {
                    (None, MissingValues::None)
                };

                // Primary record for this segment
                let seg_slots = if is_last_seg { last_n_slots } else { n_slots };
                slot_records.push(SlotRecord {
                    raw_type: seg_width as i32,
                    short_name: seg_short_name,
                    label: seg_label,
                    print_format: seg_format.clone(),
                    write_format: seg_format,
                    missing_values: seg_missing,
                    is_ghost: false,
                });
                slot_index += 1;

                // Ghost records: seg_slots - 1 per segment
                let n_ghosts = seg_slots - 1;
                for _ in 0..n_ghosts {
                    slot_records.push(SlotRecord {
                        raw_type: -1,
                        short_name: String::new(),
                        label: None,
                        print_format: SpssFormat {
                            format_type: FormatType::A,
                            width: 0,
                            decimals: 0,
                        },
                        write_format: SpssFormat {
                            format_type: FormatType::A,
                            width: 0,
                            decimals: 0,
                        },
                        missing_values: MissingValues::None,
                        is_ghost: true,
                    });
                    slot_index += 1;
                }
            }
        }
    }

    Ok(CaseLayout {
        write_vars,
        slot_records,
        slots_per_row: slot_index,
        short_to_long,
        very_long_strings,
    })
}

pub(super) fn infer_format(dt: &DataType) -> SpssFormat {
    match dt {
        DataType::Float64
        | DataType::Int64
        | DataType::Int32
        | DataType::Int16
        | DataType::Int8 => SpssFormat {
            format_type: FormatType::F,
            width: 8,
            decimals: 2,
        },
        DataType::Boolean => SpssFormat {
            format_type: FormatType::F,
            width: 1,
            decimals: 0,
        },
        DataType::Date32 => SpssFormat {
            format_type: FormatType::Date,
            width: 11,
            decimals: 0,
        },
        DataType::Timestamp(TimeUnit::Microsecond, _) => SpssFormat {
            format_type: FormatType::DateTime,
            width: 23,
            decimals: 2,
        },
        DataType::Duration(TimeUnit::Microsecond) => SpssFormat {
            format_type: FormatType::Time,
            width: 11,
            decimals: 2,
        },
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => SpssFormat {
            format_type: FormatType::A,
            width: 255,
            decimals: 0,
        },
        _ => SpssFormat {
            format_type: FormatType::F,
            width: 8,
            decimals: 2,
        },
    }
}
