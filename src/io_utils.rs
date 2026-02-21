use std::io::Read;

use crate::error::{Result, SpssError};

/// Endian-aware binary reader that wraps a `Read` source.
///
/// All multi-byte reads are little-endian by default, with optional byte-swapping
/// when the SAV file was written on a big-endian machine.
pub struct SavReader<R: Read> {
    inner: R,
    bswap: bool,
}

impl<R: Read> SavReader<R> {
    /// Create a new reader with no byte swapping (endianness determined later from header).
    pub fn new(inner: R) -> Self {
        SavReader {
            inner,
            bswap: false,
        }
    }

    /// Enable or disable byte swapping.
    pub fn set_bswap(&mut self, bswap: bool) {
        self.bswap = bswap;
    }

    #[allow(dead_code)]
    pub fn bswap(&self) -> bool {
        self.bswap
    }

    /// Get a mutable reference to the inner reader.
    pub fn inner_mut(&mut self) -> &mut R {
        &mut self.inner
    }

    /// Read exactly `n` bytes into a new Vec.
    pub fn read_bytes(&mut self, n: usize) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; n];
        self.inner.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Read exactly `n` bytes into an existing slice.
    pub fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.inner.read_exact(buf)?;
        Ok(())
    }

    /// Read a 4-byte signed integer with endian handling.
    pub fn read_i32(&mut self) -> Result<i32> {
        let mut buf = [0u8; 4];
        self.inner.read_exact(&mut buf)?;
        let val = if self.bswap {
            i32::from_be_bytes(buf)
        } else {
            i32::from_le_bytes(buf)
        };
        Ok(val)
    }

    /// Read a 4-byte unsigned integer with endian handling.
    #[allow(dead_code)]
    pub fn read_u32(&mut self) -> Result<u32> {
        let mut buf = [0u8; 4];
        self.inner.read_exact(&mut buf)?;
        let val = if self.bswap {
            u32::from_be_bytes(buf)
        } else {
            u32::from_le_bytes(buf)
        };
        Ok(val)
    }

    /// Read an 8-byte signed integer with endian handling.
    pub fn read_i64(&mut self) -> Result<i64> {
        let mut buf = [0u8; 8];
        self.inner.read_exact(&mut buf)?;
        let val = if self.bswap {
            i64::from_be_bytes(buf)
        } else {
            i64::from_le_bytes(buf)
        };
        Ok(val)
    }

    /// Read an 8-byte float with endian handling.
    pub fn read_f64(&mut self) -> Result<f64> {
        let mut buf = [0u8; 8];
        self.inner.read_exact(&mut buf)?;
        let val = if self.bswap {
            f64::from_be_bytes(buf)
        } else {
            f64::from_le_bytes(buf)
        };
        Ok(val)
    }

    /// Read 8 raw bytes (no endian swap -- used for raw data slots).
    pub fn read_8_bytes(&mut self) -> Result<[u8; 8]> {
        let mut buf = [0u8; 8];
        self.inner.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Read a fixed-length byte string, trimming trailing spaces and NULs.
    #[allow(dead_code)]
    pub fn read_fixed_string(&mut self, len: usize) -> Result<Vec<u8>> {
        let buf = self.read_bytes(len)?;
        Ok(trim_trailing_padding(&buf).to_vec())
    }

    /// Skip `n` bytes.
    pub fn skip(&mut self, n: usize) -> Result<()> {
        let mut remaining = n;
        let mut discard = [0u8; 4096];
        while remaining > 0 {
            let to_read = remaining.min(discard.len());
            self.inner.read_exact(&mut discard[..to_read])?;
            remaining -= to_read;
        }
        Ok(())
    }
}

/// Trim trailing spaces (0x20) and NUL bytes (0x00) from a byte slice.
/// Uses reverse scan to find last non-padding byte.
pub fn trim_trailing_padding(buf: &[u8]) -> &[u8] {
    // Scan backwards for last byte that isn't space or NUL.
    // memchr doesn't help here (only finds specific bytes, not "not in set"),
    // so we use a simple reverse loop which LLVM auto-vectorizes.
    let mut end = buf.len();
    while end > 0 && (buf[end - 1] == b' ' || buf[end - 1] == 0) {
        end -= 1;
    }
    &buf[..end]
}

/// Round a length up to the next multiple of `alignment`.
pub fn round_up(len: usize, alignment: usize) -> usize {
    if alignment == 0 {
        return len;
    }
    let remainder = len % alignment;
    if remainder == 0 {
        len
    } else {
        len + alignment - remainder
    }
}

/// Convert a byte slice to a string, trying UTF-8 first and falling back to lossy.
pub fn bytes_to_string_lossy(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec()).unwrap_or_else(|_| String::from_utf8_lossy(bytes).into_owned())
}

/// Read a pascal-style string: 4-byte length prefix, then that many bytes.
#[allow(dead_code)]
pub fn read_pascal_string<R: Read>(reader: &mut SavReader<R>) -> Result<Vec<u8>> {
    let len = reader.read_i32()? as usize;
    if len == 0 {
        return Ok(Vec::new());
    }
    reader.read_bytes(len)
}

/// Read a pascal-style string and skip padding to align to 4 bytes.
#[allow(dead_code)]
pub fn read_pascal_string_aligned<R: Read>(reader: &mut SavReader<R>) -> Result<Vec<u8>> {
    let len = reader.read_i32()? as usize;
    if len == 0 {
        return Ok(Vec::new());
    }
    let padded_len = round_up(len, 4);
    let data = reader.read_bytes(padded_len)?;
    Ok(data[..len].to_vec())
}

/// Detect endianness from the SAV header's layout_code field.
/// Returns `true` if byte-swapping is needed.
pub fn detect_endianness(layout_code_bytes: [u8; 4]) -> Result<bool> {
    let le_val = i32::from_le_bytes(layout_code_bytes);
    let be_val = i32::from_be_bytes(layout_code_bytes);

    if le_val == 2 || le_val == 3 {
        Ok(false) // little-endian, no swap needed
    } else if be_val == 2 || be_val == 3 {
        Ok(true) // big-endian, swap needed
    } else {
        Err(SpssError::InvalidVariable(format!(
            "cannot determine endianness from layout_code bytes: {layout_code_bytes:?}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trim_trailing_padding() {
        assert_eq!(trim_trailing_padding(b"hello   "), b"hello");
        assert_eq!(trim_trailing_padding(b"hello\0\0\0"), b"hello");
        assert_eq!(trim_trailing_padding(b"hello \0 "), b"hello");
        assert_eq!(trim_trailing_padding(b"   "), b"");
        assert_eq!(trim_trailing_padding(b""), b"");
    }

    #[test]
    fn test_round_up() {
        assert_eq!(round_up(0, 4), 0);
        assert_eq!(round_up(1, 4), 4);
        assert_eq!(round_up(4, 4), 4);
        assert_eq!(round_up(5, 4), 8);
        assert_eq!(round_up(7, 8), 8);
        assert_eq!(round_up(8, 8), 8);
    }

    #[test]
    fn test_detect_endianness_le() {
        let bytes = 2_i32.to_le_bytes();
        assert!(!detect_endianness(bytes).unwrap());
    }

    #[test]
    fn test_detect_endianness_be() {
        let bytes = 2_i32.to_be_bytes();
        assert!(detect_endianness(bytes).unwrap());
    }

    #[test]
    fn test_sav_reader_i32() {
        let data = 42_i32.to_le_bytes();
        let mut reader = SavReader::new(&data[..]);
        assert_eq!(reader.read_i32().unwrap(), 42);
    }

    #[test]
    fn test_sav_reader_f64() {
        let data = 3.14_f64.to_le_bytes();
        let mut reader = SavReader::new(&data[..]);
        let val = reader.read_f64().unwrap();
        assert!((val - 3.14).abs() < 1e-10);
    }
}
