use std::io::Read;

use crate::error::Result;
use crate::io_utils::{self, SavReader};

/// Parse a type 6 (document) record. The record type i32 has already been read.
///
/// Returns a vector of document lines (each originally 80 chars, trimmed).
pub fn parse_document<R: Read>(reader: &mut SavReader<R>) -> Result<Vec<Vec<u8>>> {
    let n_lines = reader.read_i32()? as usize;
    let mut lines = Vec::with_capacity(n_lines);

    for _ in 0..n_lines {
        let line_bytes = reader.read_bytes(80)?;
        let trimmed = io_utils::trim_trailing_padding(&line_bytes).to_vec();
        lines.push(trimmed);
    }

    Ok(lines)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_document() {
        let mut buf = Vec::new();
        // 2 lines
        buf.extend_from_slice(&2_i32.to_le_bytes());
        // Line 1: "This is a note" padded to 80 bytes
        let line1 = b"This is a note";
        buf.extend_from_slice(line1);
        buf.extend_from_slice(&vec![b' '; 80 - line1.len()]);
        // Line 2: "Second line" padded to 80 bytes
        let line2 = b"Second line";
        buf.extend_from_slice(line2);
        buf.extend_from_slice(&vec![b' '; 80 - line2.len()]);

        let mut reader = SavReader::new(&buf[..]);
        let lines = parse_document(&mut reader).unwrap();

        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], b"This is a note");
        assert_eq!(lines[1], b"Second line");
    }
}
