//! Bytecode compressor for SAV row-wise compression.
//!
//! This is the reverse of `bytecode.rs` — it converts 8-byte slot data
//! into SPSS bytecode-compressed format. The encoding rules:
//!
//! - SYSMIS (0xFFEFFFFFFFFFFFFF) → opcode 255
//! - 8 ASCII spaces (0x2020202020202020) → opcode 254
//! - f64 that equals `round(v) == v` and `v + bias` is in 1..=251 → opcode `(v + bias) as u8`
//! - Everything else → opcode 253 + 8 raw bytes appended
//! - End of file → opcode 252

use crate::constants::*;

const SPACES_RAW: [u8; 8] = [0x20u8; 8];
const SYSMIS_RAW: [u8; 8] = SYSMIS_BITS.to_le_bytes();

/// Bytecode compressor that encodes 8-byte slots into SPSS compressed format.
///
/// Opcodes are accumulated in groups of 8 (a "control block"). When the
/// block is full, it is flushed: the 8 opcode bytes followed by any raw
/// data for opcode-253 slots.
pub struct BytecodeEncoder {
    bias: f64,
    /// The 8 opcode slots for the current control block.
    control_bytes: [u8; 8],
    /// How many opcodes have been written to the current block (0..8).
    control_idx: usize,
    /// Raw 8-byte values accumulated for opcode 253 entries in this block.
    raw_buffer: Vec<u8>,
    /// The fully encoded output.
    output: Vec<u8>,
}

impl BytecodeEncoder {
    pub fn new(bias: f64) -> Self {
        BytecodeEncoder {
            bias,
            control_bytes: [0u8; 8],
            control_idx: 0,
            raw_buffer: Vec::with_capacity(64),
            output: Vec::new(),
        }
    }

    /// Pre-allocate output buffer for estimated data size.
    pub fn with_capacity(bias: f64, capacity: usize) -> Self {
        BytecodeEncoder {
            bias,
            control_bytes: [0u8; 8],
            control_idx: 0,
            raw_buffer: Vec::with_capacity(64),
            output: Vec::with_capacity(capacity),
        }
    }

    /// Encode a single 8-byte slot.
    #[inline]
    pub fn encode_slot(&mut self, slot: &[u8; 8]) {
        let opcode = classify_slot(slot, self.bias);
        self.control_bytes[self.control_idx] = opcode;
        self.control_idx += 1;

        if opcode == COMPRESS_RAW_FOLLOWS {
            self.raw_buffer.extend_from_slice(slot);
        }

        if self.control_idx == 8 {
            self.flush_block();
        }
    }

    /// Encode a row from a pre-built row buffer (slots_per_row * 8 bytes).
    pub fn encode_row(&mut self, row_buf: &[u8], slots_per_row: usize) {
        debug_assert_eq!(row_buf.len(), slots_per_row * 8);
        for i in 0..slots_per_row {
            let slot: &[u8; 8] = row_buf[i * 8..(i + 1) * 8].try_into().unwrap();
            self.encode_slot(slot);
        }
    }

    /// Returns the current accumulated output size (for drain threshold checks).
    pub fn output_len(&self) -> usize {
        self.output.len()
    }

    /// Drain all accumulated output, returning it and clearing the internal buffer.
    /// The encoder can continue encoding after this call.
    /// This enables streaming: caller writes drained data to disk periodically.
    pub fn drain_output(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.output)
    }

    /// Write the EOF opcode and flush the final control block.
    /// After calling this, the encoder should not be used for further encoding.
    /// The final bytes are available via `drain_output()`.
    pub fn write_eof(&mut self) {
        self.control_bytes[self.control_idx] = COMPRESS_END_OF_FILE;
        self.control_idx += 1;

        // Pad remaining slots with COMPRESS_SKIP (0)
        while self.control_idx < 8 {
            self.control_bytes[self.control_idx] = COMPRESS_SKIP;
            self.control_idx += 1;
        }
        self.flush_block();
    }

    /// Finalize the stream: emit EOF opcode and flush the last control block.
    #[allow(dead_code)]
    pub fn finish(mut self) -> Vec<u8> {
        self.write_eof();
        self.output
    }

    /// Flush the current control block: write 8 opcode bytes + accumulated raw data.
    fn flush_block(&mut self) {
        self.output.extend_from_slice(&self.control_bytes);
        if !self.raw_buffer.is_empty() {
            self.output.extend_from_slice(&self.raw_buffer);
            self.raw_buffer.clear();
        }
        self.control_bytes = [0u8; 8];
        self.control_idx = 0;
    }
}

/// Classify a single 8-byte slot into its bytecode opcode.
#[inline]
fn classify_slot(slot: &[u8; 8], bias: f64) -> u8 {
    // Check for SYSMIS (exact bit pattern match)
    if *slot == SYSMIS_RAW {
        return COMPRESS_SYSMIS;
    }

    // Check for 8 spaces
    if *slot == SPACES_RAW {
        return COMPRESS_EIGHT_SPACES;
    }

    // Try to encode as a small integer: f64 value v where
    // v == v.floor() (is integer), v + bias is in 1..=251,
    // and the slot bytes == the LE representation of v (exact roundtrip).
    let val = f64::from_le_bytes(*slot);

    // Must be finite, integer-valued, and in compressible range
    if val.is_finite() && val == val.floor() {
        let biased = val + bias;
        if (1.0..=251.0).contains(&biased) {
            let code = biased as u8;
            // Verify exact roundtrip: (code as f64 - bias) must produce the same bit pattern
            let roundtrip = (code as f64) - bias;
            if roundtrip.to_le_bytes() == *slot {
                return code;
            }
        }
    }

    // Everything else: raw 8 bytes
    COMPRESS_RAW_FOLLOWS
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::bytecode::BytecodeDecompressor;

    #[test]
    fn test_encode_sysmis() {
        let mut enc = BytecodeEncoder::new(100.0);
        let slot = SYSMIS_BITS.to_le_bytes();
        enc.encode_slot(&slot);
        // Should be opcode 255, not yet flushed
        assert_eq!(enc.control_bytes[0], COMPRESS_SYSMIS);
    }

    #[test]
    fn test_encode_spaces() {
        let mut enc = BytecodeEncoder::new(100.0);
        let slot = [0x20u8; 8];
        enc.encode_slot(&slot);
        assert_eq!(enc.control_bytes[0], COMPRESS_EIGHT_SPACES);
    }

    #[test]
    fn test_encode_small_integer() {
        let mut enc = BytecodeEncoder::new(100.0);
        // Value 1.0 → code = 1.0 + 100.0 = 101
        let slot = 1.0_f64.to_le_bytes();
        enc.encode_slot(&slot);
        assert_eq!(enc.control_bytes[0], 101);
    }

    #[test]
    fn test_encode_negative_integer() {
        let mut enc = BytecodeEncoder::new(100.0);
        // Value -99.0 → code = -99.0 + 100.0 = 1
        let slot = (-99.0_f64).to_le_bytes();
        enc.encode_slot(&slot);
        assert_eq!(enc.control_bytes[0], 1);
    }

    #[test]
    fn test_encode_raw_float() {
        let mut enc = BytecodeEncoder::new(100.0);
        // 3.14 is not an integer → opcode 253
        let slot = 3.14_f64.to_le_bytes();
        enc.encode_slot(&slot);
        assert_eq!(enc.control_bytes[0], COMPRESS_RAW_FOLLOWS);
        assert_eq!(enc.raw_buffer.len(), 8);
    }

    #[test]
    fn test_encode_out_of_range() {
        let mut enc = BytecodeEncoder::new(100.0);
        // Value 200.0 → biased = 300 > 251 → raw
        let slot = 200.0_f64.to_le_bytes();
        enc.encode_slot(&slot);
        assert_eq!(enc.control_bytes[0], COMPRESS_RAW_FOLLOWS);
    }

    #[test]
    fn test_full_block_flush() {
        let mut enc = BytecodeEncoder::new(100.0);
        // Encode 8 SYSMIS slots → should flush one control block
        for _ in 0..8 {
            let slot = SYSMIS_BITS.to_le_bytes();
            enc.encode_slot(&slot);
        }
        assert_eq!(enc.output.len(), 8); // just the control block
        assert_eq!(enc.output, vec![255; 8]);
        assert_eq!(enc.control_idx, 0); // reset
    }

    #[test]
    fn test_finish_with_eof() {
        let mut enc = BytecodeEncoder::new(100.0);
        let slot = 1.0_f64.to_le_bytes();
        enc.encode_slot(&slot);
        let output = enc.finish();
        // Control block: [101, 252(EOF), 0, 0, 0, 0, 0, 0]
        assert_eq!(output.len(), 8);
        assert_eq!(output[0], 101);
        assert_eq!(output[1], COMPRESS_END_OF_FILE);
        // Rest should be COMPRESS_SKIP (0)
        for &b in &output[2..8] {
            assert_eq!(b, COMPRESS_SKIP);
        }
    }

    #[test]
    fn test_roundtrip_with_decompressor() {
        // Encode some data, then decompress and verify we get the same slots back
        let bias = 100.0;
        let slots: Vec<[u8; 8]> = vec![
            1.0_f64.to_le_bytes(),     // small int → opcode
            0.0_f64.to_le_bytes(),     // zero → opcode 100
            3.14_f64.to_le_bytes(),    // float → raw
            SYSMIS_BITS.to_le_bytes(), // sysmis → 255
            [0x20; 8],                 // spaces → 254
            (-50.0_f64).to_le_bytes(), // negative int → opcode 50
        ];

        let mut enc = BytecodeEncoder::new(bias);
        for slot in &slots {
            enc.encode_slot(slot);
        }
        let compressed = enc.finish();

        // Decompress
        let mut dec = BytecodeDecompressor::new(bias);
        let mut result = Vec::new();
        dec.decompress_row(&compressed, slots.len(), &mut result)
            .unwrap();
        assert_eq!(result.len(), slots.len());

        // Verify each slot
        for (i, (original, decompressed)) in slots.iter().zip(result.iter()).enumerate() {
            match decompressed {
                crate::compression::bytecode::SlotValue::Numeric(v) => {
                    let expected = f64::from_le_bytes(*original);
                    assert!(
                        (v - expected).abs() < f64::EPSILON,
                        "slot {i}: expected {expected}, got {v}"
                    );
                }
                crate::compression::bytecode::SlotValue::Raw(bytes) => {
                    assert_eq!(bytes, original, "slot {i}: raw bytes mismatch");
                }
                crate::compression::bytecode::SlotValue::Spaces => {
                    assert_eq!(original, &[0x20; 8], "slot {i}: expected spaces");
                }
                crate::compression::bytecode::SlotValue::Sysmis => {
                    assert_eq!(
                        original,
                        &SYSMIS_BITS.to_le_bytes(),
                        "slot {i}: expected SYSMIS"
                    );
                }
                crate::compression::bytecode::SlotValue::EndOfFile => {
                    panic!("unexpected EOF at slot {i}");
                }
            }
        }
    }

    #[test]
    fn test_roundtrip_raw_with_decompressor() {
        // Test using the raw decompression path (production code path)
        let bias = 100.0;
        let original_slots: Vec<[u8; 8]> = vec![
            42.0_f64.to_le_bytes(),
            0.0_f64.to_le_bytes(),
            (-99.0_f64).to_le_bytes(),
            3.14159_f64.to_le_bytes(),
            SYSMIS_BITS.to_le_bytes(),
            [0x20; 8],
            150.0_f64.to_le_bytes(),
            1e10_f64.to_le_bytes(),
        ];

        let mut enc = BytecodeEncoder::new(bias);
        for slot in &original_slots {
            enc.encode_slot(slot);
        }
        let compressed = enc.finish();

        // Decompress with raw path
        let mut dec = BytecodeDecompressor::new(bias);
        let mut output = vec![0u8; original_slots.len() * 8];
        let ok = dec
            .decompress_row_raw(&compressed, original_slots.len(), &mut output, 0)
            .unwrap();
        assert!(ok, "decompression should succeed");

        // Compare slot by slot
        for (i, original) in original_slots.iter().enumerate() {
            let decoded = &output[i * 8..(i + 1) * 8];
            assert_eq!(
                decoded, original,
                "slot {i}: mismatch. original={original:?}, decoded={decoded:?}"
            );
        }
    }

    #[test]
    fn test_multi_row_roundtrip() {
        let bias = 100.0;
        let slots_per_row = 3;
        let rows: Vec<Vec<[u8; 8]>> = vec![
            vec![
                1.0_f64.to_le_bytes(),
                2.0_f64.to_le_bytes(),
                3.0_f64.to_le_bytes(),
            ],
            vec![10.0_f64.to_le_bytes(), SYSMIS_BITS.to_le_bytes(), [0x20; 8]],
            vec![
                (-50.0_f64).to_le_bytes(),
                99.99_f64.to_le_bytes(),
                0.0_f64.to_le_bytes(),
            ],
        ];

        // Encode all rows
        let mut enc = BytecodeEncoder::new(bias);
        for row in &rows {
            let mut row_buf = Vec::with_capacity(slots_per_row * 8);
            for slot in row {
                row_buf.extend_from_slice(slot);
            }
            enc.encode_row(&row_buf, slots_per_row);
        }
        let compressed = enc.finish();

        // Decompress row by row
        let mut dec = BytecodeDecompressor::new(bias);
        let mut output = vec![0u8; slots_per_row * 8];

        for (row_idx, original_row) in rows.iter().enumerate() {
            let ok = dec
                .decompress_row_raw(&compressed, slots_per_row, &mut output, 0)
                .unwrap();
            assert!(ok, "row {row_idx}: decompression should succeed");

            for (slot_idx, original) in original_row.iter().enumerate() {
                let decoded = &output[slot_idx * 8..(slot_idx + 1) * 8];
                assert_eq!(decoded, original, "row {row_idx} slot {slot_idx}: mismatch");
            }
        }
    }
}
