use crate::constants::*;
use crate::error::{Result, SpssError};

/// The result of decompressing one 8-byte slot from bytecode.
#[derive(Debug, Clone, Copy)]
pub enum SlotValue {
    /// A numeric value (bytecodes 1..=251: value = code - bias).
    Numeric(f64),
    /// Raw 8 bytes of uncompressed data (bytecode 253).
    Raw([u8; 8]),
    /// 8 ASCII spaces (bytecode 254).
    Spaces,
    /// System-missing value (bytecode 255).
    Sysmis,
    /// End of file (bytecode 252).
    EndOfFile,
}

/// Stateful bytecode decompressor for SAV row-wise compression.
///
/// Maintains control block state across row boundaries, since SPSS control
/// blocks do NOT align with row boundaries.
pub struct BytecodeDecompressor {
    /// Compression bias (typically 100.0).
    bias: f64,
    /// Current position in the input buffer.
    pos: usize,
    /// Current control block (8 opcodes).
    control_bytes: [u8; 8],
    /// Index into the current control block (0..8).
    control_idx: usize,
    /// Whether we've hit the end-of-file marker.
    eof: bool,
}

impl BytecodeDecompressor {
    pub fn new(bias: f64) -> Self {
        BytecodeDecompressor {
            bias,
            pos: 0,
            control_bytes: [0u8; 8],
            control_idx: 8, // force reading a new control block on first use
            eof: false,
        }
    }

    /// Decompress one row of `slots_per_row` slot values from the input buffer.
    ///
    /// Control block state is preserved across calls, so this can be called
    /// repeatedly to decompress consecutive rows.
    pub fn decompress_row(
        &mut self,
        input: &[u8],
        slots_per_row: usize,
    ) -> Result<Vec<SlotValue>> {
        if self.eof {
            return Ok(Vec::new());
        }

        let mut slots = Vec::with_capacity(slots_per_row);

        while slots.len() < slots_per_row {
            // Need a new control block?
            if self.control_idx >= 8 {
                if self.pos + 8 > input.len() {
                    // Not enough data for another control block -- we're done
                    return Ok(slots);
                }
                self.control_bytes
                    .copy_from_slice(&input[self.pos..self.pos + 8]);
                self.pos += 8;
                self.control_idx = 0;
            }

            let code = self.control_bytes[self.control_idx];
            self.control_idx += 1;

            match code {
                COMPRESS_SKIP => {
                    // Padding byte at end of data -- skip, don't produce a slot
                    continue;
                }
                COMPRESS_END_OF_FILE => {
                    self.eof = true;
                    return Ok(slots);
                }
                COMPRESS_RAW_FOLLOWS => {
                    // Next 8 bytes are uncompressed data
                    if self.pos + 8 > input.len() {
                        return Err(SpssError::TruncatedFile {
                            expected: self.pos + 8,
                            actual: input.len(),
                        });
                    }
                    let mut raw = [0u8; 8];
                    raw.copy_from_slice(&input[self.pos..self.pos + 8]);
                    self.pos += 8;
                    slots.push(SlotValue::Raw(raw));
                }
                COMPRESS_EIGHT_SPACES => {
                    slots.push(SlotValue::Spaces);
                }
                COMPRESS_SYSMIS => {
                    slots.push(SlotValue::Sysmis);
                }
                _ => {
                    // Codes 1..=251: numeric value = (code - bias)
                    let value = (code as f64) - self.bias;
                    slots.push(SlotValue::Numeric(value));
                }
            }
        }

        Ok(slots)
    }
}

/// Decompress all rows from a bytecode-compressed buffer.
///
/// Returns a vector of rows, each row being a vector of SlotValues.
pub fn decompress_all_rows(
    input: &[u8],
    bias: f64,
    slots_per_row: usize,
) -> Result<Vec<Vec<SlotValue>>> {
    let mut decompressor = BytecodeDecompressor::new(bias);
    let mut rows = Vec::new();

    loop {
        let slots = decompressor.decompress_row(input, slots_per_row)?;

        if slots.is_empty() {
            break;
        }

        if slots.len() < slots_per_row {
            // Partial row at end -- discard
            break;
        }

        rows.push(slots);
    }

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numeric_bias_codes() {
        let mut decompressor = BytecodeDecompressor::new(100.0);

        // Control block: [101, 102, 0, 0, 0, 0, 0, 0]
        // code 101 = value 1.0, code 102 = value 2.0
        let input: Vec<u8> = vec![101, 102, 0, 0, 0, 0, 0, 0];

        let slots = decompressor.decompress_row(&input, 2).unwrap();
        assert_eq!(slots.len(), 2);

        match slots[0] {
            SlotValue::Numeric(v) => assert!((v - 1.0).abs() < f64::EPSILON),
            _ => panic!("expected Numeric"),
        }
        match slots[1] {
            SlotValue::Numeric(v) => assert!((v - 2.0).abs() < f64::EPSILON),
            _ => panic!("expected Numeric"),
        }
    }

    #[test]
    fn test_sysmis_and_spaces() {
        let mut decompressor = BytecodeDecompressor::new(100.0);
        let input: Vec<u8> = vec![255, 254, 0, 0, 0, 0, 0, 0];

        let slots = decompressor.decompress_row(&input, 2).unwrap();
        assert!(matches!(slots[0], SlotValue::Sysmis));
        assert!(matches!(slots[1], SlotValue::Spaces));
    }

    #[test]
    fn test_raw_follows() {
        let mut decompressor = BytecodeDecompressor::new(100.0);

        let mut input = Vec::new();
        // Control block: [253 (raw follows), 0, 0, 0, 0, 0, 0, 0]
        input.extend_from_slice(&[253, 0, 0, 0, 0, 0, 0, 0]);
        // Raw 8 bytes
        input.extend_from_slice(&3.14_f64.to_le_bytes());

        let slots = decompressor.decompress_row(&input, 1).unwrap();
        assert_eq!(slots.len(), 1);
        match slots[0] {
            SlotValue::Raw(bytes) => {
                let val = f64::from_le_bytes(bytes);
                assert!((val - 3.14).abs() < 1e-10);
            }
            _ => panic!("expected Raw"),
        }
    }

    #[test]
    fn test_cross_block_rows() {
        // Test that control block state carries across rows.
        // 2 rows of 3 slots each = 6 slots total.
        // Control block 1 has 8 codes: [101, 102, 103, 104, 105, 106, 0, 0]
        // Row 1 uses codes 101, 102, 103 (slots 1-3)
        // Row 2 uses codes 104, 105, 106 (slots 4-6, from SAME control block)
        let mut decompressor = BytecodeDecompressor::new(100.0);
        let input: Vec<u8> = vec![101, 102, 103, 104, 105, 106, 0, 0];

        let row1 = decompressor.decompress_row(&input, 3).unwrap();
        assert_eq!(row1.len(), 3);
        match row1[0] {
            SlotValue::Numeric(v) => assert!((v - 1.0).abs() < f64::EPSILON),
            _ => panic!("expected 1.0"),
        }

        let row2 = decompressor.decompress_row(&input, 3).unwrap();
        assert_eq!(row2.len(), 3);
        match row2[0] {
            SlotValue::Numeric(v) => assert!((v - 4.0).abs() < f64::EPSILON),
            _ => panic!("expected 4.0"),
        }
    }
}
