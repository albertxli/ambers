use std::io::Read;

use crate::error::Result;
use crate::io_utils::SavReader;

/// Subtype 4: Machine floating point information.
#[derive(Debug, Clone)]
pub struct FloatInfo {
    /// System-missing value (as raw bit pattern).
    pub sysmis: f64,
    /// Highest representable value.
    pub highest: f64,
    /// Lowest representable value.
    pub lowest: f64,
}

impl FloatInfo {
    pub fn parse<R: Read>(reader: &mut SavReader<R>) -> Result<FloatInfo> {
        Ok(FloatInfo {
            sysmis: reader.read_f64()?,
            highest: reader.read_f64()?,
            lowest: reader.read_f64()?,
        })
    }
}
