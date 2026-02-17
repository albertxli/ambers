use std::io::Read;

use crate::constants::{Alignment, Measure};
use crate::error::Result;
use crate::io_utils::SavReader;

/// A single variable display entry (from subtype 11).
#[derive(Debug, Clone)]
pub struct VarDisplayEntry {
    pub measure: Measure,
    pub width: u32,
    pub alignment: Alignment,
}

/// Parse subtype 11 variable display info.
///
/// The record contains `count` i32 values. If count is divisible by 3,
/// each variable gets (measure, width, alignment). If not divisible by 3,
/// each variable gets (measure, alignment) — no width field.
pub fn parse_var_display<R: Read>(
    reader: &mut SavReader<R>,
    count: i32,
) -> Result<Vec<VarDisplayEntry>> {
    let count = count as usize;
    let has_width = count % 3 == 0;

    let n_vars = if has_width { count / 3 } else { count / 2 };
    let mut entries = Vec::with_capacity(n_vars);

    for _ in 0..n_vars {
        let measure = Measure::from_i32(reader.read_i32()?);
        let width = if has_width {
            reader.read_i32()? as u32
        } else {
            8
        };
        let alignment = Alignment::from_i32(reader.read_i32()?);

        entries.push(VarDisplayEntry {
            measure,
            width,
            alignment,
        });
    }

    Ok(entries)
}
