pub mod integer_info;
pub mod float_info;
pub mod mr_sets;
pub mod var_display;
pub mod long_var_names;
pub mod very_long_strings;
pub mod encoding_record;
pub mod long_string_labels;
pub mod long_string_missing;

use std::io::Read;

use crate::constants::*;
use crate::error::Result;
use crate::io_utils::SavReader;

/// Header for a type 7 (info) record.
#[derive(Debug, Clone)]
pub struct InfoRecordHeader {
    pub subtype: i32,
    pub size: i32,
    pub count: i32,
}

impl InfoRecordHeader {
    /// Parse the info record header. The record type (7) has already been read.
    pub fn parse<R: Read>(reader: &mut SavReader<R>) -> Result<InfoRecordHeader> {
        let subtype = reader.read_i32()?;
        let size = reader.read_i32()?;
        let count = reader.read_i32()?;
        Ok(InfoRecordHeader {
            subtype,
            size,
            count,
        })
    }

    /// Total data bytes for this info record.
    pub fn data_len(&self) -> usize {
        (self.size as usize) * (self.count as usize)
    }
}

/// Parsed info record data.
#[derive(Debug)]
#[allow(dead_code)]
pub enum InfoRecord {
    IntegerInfo(integer_info::IntegerInfo),
    FloatInfo(float_info::FloatInfo),
    VarDisplay(Vec<var_display::VarDisplayEntry>),
    LongNames(Vec<(String, String)>),
    VeryLongStrings(Vec<(String, usize)>),
    Encoding(String),
    LongStringLabels(Vec<long_string_labels::LongStringLabelSet>),
    LongStringMissing(Vec<long_string_missing::LongStringMissingEntry>),
    MrSets(Vec<mr_sets::RawMrSet>),
    Unknown { subtype: i32 },
}

/// Parse a type 7 info record based on its subtype.
pub fn parse_info_record<R: Read>(
    reader: &mut SavReader<R>,
    header: &InfoRecordHeader,
) -> Result<InfoRecord> {
    let data_len = header.data_len();

    match header.subtype {
        INFO_MR_SETS => {
            let data = reader.read_bytes(data_len)?;
            let sets = mr_sets::parse_mr_sets(&data);
            Ok(InfoRecord::MrSets(sets))
        }
        INFO_INTEGER => {
            let info = integer_info::IntegerInfo::parse(reader)?;
            Ok(InfoRecord::IntegerInfo(info))
        }
        INFO_FLOAT => {
            let info = float_info::FloatInfo::parse(reader)?;
            Ok(InfoRecord::FloatInfo(info))
        }
        INFO_VAR_DISPLAY => {
            let entries = var_display::parse_var_display(reader, header.count)?;
            Ok(InfoRecord::VarDisplay(entries))
        }
        INFO_LONG_NAMES => {
            let data = reader.read_bytes(data_len)?;
            let names = long_var_names::parse_long_var_names(&data);
            Ok(InfoRecord::LongNames(names))
        }
        INFO_VERY_LONG_STRINGS => {
            let data = reader.read_bytes(data_len)?;
            let entries = very_long_strings::parse_very_long_strings(&data);
            Ok(InfoRecord::VeryLongStrings(entries))
        }
        INFO_ENCODING => {
            let data = reader.read_bytes(data_len)?;
            let name = encoding_record::parse_encoding_record(&data);
            Ok(InfoRecord::Encoding(name))
        }
        INFO_LONG_STRING_LABELS => {
            let data = reader.read_bytes(data_len)?;
            let labels = long_string_labels::parse_long_string_labels(&data)?;
            Ok(InfoRecord::LongStringLabels(labels))
        }
        INFO_LONG_STRING_MISSING => {
            let data = reader.read_bytes(data_len)?;
            let entries = long_string_missing::parse_long_string_missing(&data)?;
            Ok(InfoRecord::LongStringMissing(entries))
        }
        _ => {
            // Unknown subtype -- skip the data
            reader.skip(data_len)?;
            Ok(InfoRecord::Unknown {
                subtype: header.subtype,
            })
        }
    }
}
