use std::io::Read;

use crate::error::Result;
use crate::io_utils::SavReader;

/// Subtype 3: Machine integer information.
#[derive(Debug, Clone)]
pub struct IntegerInfo {
    pub version_major: i32,
    pub version_minor: i32,
    pub version_revision: i32,
    pub machine_code: i32,
    /// Floating point representation: 1=IEEE, 2=IBM, 3=VAX.
    pub floating_point_rep: i32,
    pub compression_code: i32,
    /// Endianness: 1=big, 2=little.
    pub endianness: i32,
    /// IANA character code page number (e.g., 65001=UTF-8, 1252=Windows-1252).
    pub character_code: i32,
}

impl IntegerInfo {
    pub fn parse<R: Read>(reader: &mut SavReader<R>) -> Result<IntegerInfo> {
        Ok(IntegerInfo {
            version_major: reader.read_i32()?,
            version_minor: reader.read_i32()?,
            version_revision: reader.read_i32()?,
            machine_code: reader.read_i32()?,
            floating_point_rep: reader.read_i32()?,
            compression_code: reader.read_i32()?,
            endianness: reader.read_i32()?,
            character_code: reader.read_i32()?,
        })
    }
}
