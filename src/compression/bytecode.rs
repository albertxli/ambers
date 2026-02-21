use crate::constants::*;
use crate::error::{Result, SpssError};

/// Raw byte representations for direct-to-buffer decompression.
const SYSMIS_RAW: [u8; 8] = SYSMIS_BITS.to_le_bytes();
const SPACES_RAW: [u8; 8] = [0x20u8; 8];

/// The result of decompressing one 8-byte slot from bytecode.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
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

    /// Decompress one row into SlotValue enum values (used by tests).
    /// Production code uses `decompress_row_raw` which writes directly to byte buffers.
    #[cfg(test)]
    pub fn decompress_row(
        &mut self,
        input: &[u8],
        slots_per_row: usize,
        slots: &mut Vec<SlotValue>,
    ) -> Result<()> {
        slots.clear();

        if self.eof {
            return Ok(());
        }

        while slots.len() < slots_per_row {
            // Need a new control block?
            if self.control_idx >= 8 {
                if self.pos + 8 > input.len() {
                    // Not enough data for another control block -- we're done
                    return Ok(());
                }
                self.control_bytes
                    .copy_from_slice(&input[self.pos..self.pos + 8]);
                self.pos += 8;
                self.control_idx = 0;
            }

            let code = self.control_bytes[self.control_idx];
            self.control_idx += 1;

            match code {
                // Hot path first: codes 1..=251 are small numeric values
                1..=251 => {
                    let value = (code as f64) - self.bias;
                    slots.push(SlotValue::Numeric(value));
                }
                COMPRESS_SKIP => {
                    // Padding byte at end of data -- skip, don't produce a slot
                    continue;
                }
                COMPRESS_RAW_FOLLOWS => {
                    // Next 8 bytes are uncompressed data
                    if self.pos + 8 > input.len() {
                        return Err(truncated_err(self.pos + 8, input.len()));
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
                COMPRESS_END_OF_FILE => {
                    self.eof = true;
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    /// Decompress one row directly into a raw byte buffer, skipping SlotValue intermediates.
    ///
    /// Writes `slots_per_row * 8` bytes into `output` starting at `out_offset`.
    /// Returns `true` if a complete row was written, `false` if EOF or insufficient data.
    pub fn decompress_row_raw(
        &mut self,
        input: &[u8],
        slots_per_row: usize,
        output: &mut [u8],
        out_offset: usize,
    ) -> Result<bool> {
        if self.eof {
            return Ok(false);
        }

        let mut slot = 0;
        while slot < slots_per_row {
            // Need a new control block?
            if self.control_idx >= 8 {
                if self.pos + 8 > input.len() {
                    return Ok(false);
                }
                self.control_bytes
                    .copy_from_slice(&input[self.pos..self.pos + 8]);
                self.pos += 8;
                self.control_idx = 0;
            }

            let code = self.control_bytes[self.control_idx];
            self.control_idx += 1;

            let dest_offset = out_offset + slot * 8;
            match code {
                // Hot path first: codes 1..=251 are small numeric values
                1..=251 => {
                    let value = (code as f64) - self.bias;
                    output[dest_offset..dest_offset + 8]
                        .copy_from_slice(&value.to_le_bytes());
                    slot += 1;
                }
                COMPRESS_SKIP => {
                    continue;
                }
                COMPRESS_RAW_FOLLOWS => {
                    if self.pos + 8 > input.len() {
                        return Err(truncated_err(self.pos + 8, input.len()));
                    }
                    output[dest_offset..dest_offset + 8]
                        .copy_from_slice(&input[self.pos..self.pos + 8]);
                    self.pos += 8;
                    slot += 1;
                }
                COMPRESS_EIGHT_SPACES => {
                    output[dest_offset..dest_offset + 8].copy_from_slice(&SPACES_RAW);
                    slot += 1;
                }
                COMPRESS_SYSMIS => {
                    output[dest_offset..dest_offset + 8].copy_from_slice(&SYSMIS_RAW);
                    slot += 1;
                }
                COMPRESS_END_OF_FILE => {
                    self.eof = true;
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }
}

/// Cold error path — kept out of the hot decompression loop to reduce icache pressure.
#[cold]
fn truncated_err(expected: usize, actual: usize) -> SpssError {
    SpssError::TruncatedFile { expected, actual }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numeric_bias_codes() {
        let mut decompressor = BytecodeDecompressor::new(100.0);
        let mut slots = Vec::with_capacity(8);

        // Control block: [101, 102, 0, 0, 0, 0, 0, 0]
        // code 101 = value 1.0, code 102 = value 2.0
        let input: Vec<u8> = vec![101, 102, 0, 0, 0, 0, 0, 0];

        decompressor.decompress_row(&input, 2, &mut slots).unwrap();
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
        let mut slots = Vec::with_capacity(8);
        let input: Vec<u8> = vec![255, 254, 0, 0, 0, 0, 0, 0];

        decompressor.decompress_row(&input, 2, &mut slots).unwrap();
        assert!(matches!(slots[0], SlotValue::Sysmis));
        assert!(matches!(slots[1], SlotValue::Spaces));
    }

    #[test]
    fn test_raw_follows() {
        let mut decompressor = BytecodeDecompressor::new(100.0);
        let mut slots = Vec::with_capacity(8);

        let mut input = Vec::new();
        // Control block: [253 (raw follows), 0, 0, 0, 0, 0, 0, 0]
        input.extend_from_slice(&[253, 0, 0, 0, 0, 0, 0, 0]);
        // Raw 8 bytes
        input.extend_from_slice(&3.14_f64.to_le_bytes());

        decompressor.decompress_row(&input, 1, &mut slots).unwrap();
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
        let mut slots = Vec::with_capacity(8);
        let input: Vec<u8> = vec![101, 102, 103, 104, 105, 106, 0, 0];

        decompressor.decompress_row(&input, 3, &mut slots).unwrap();
        assert_eq!(slots.len(), 3);
        match slots[0] {
            SlotValue::Numeric(v) => assert!((v - 1.0).abs() < f64::EPSILON),
            _ => panic!("expected 1.0"),
        }

        decompressor.decompress_row(&input, 3, &mut slots).unwrap();
        assert_eq!(slots.len(), 3);
        match slots[0] {
            SlotValue::Numeric(v) => assert!((v - 4.0).abs() < f64::EPSILON),
            _ => panic!("expected 4.0"),
        }
    }
}
