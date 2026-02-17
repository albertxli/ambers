use std::io::Read;

use crate::constants::Compression;
use crate::error::{Result, SpssError};
use crate::io_utils::{self, SavReader};

/// Parsed SAV file header.
#[derive(Debug, Clone)]
pub struct FileHeader {
    /// Magic string: "$FL2" (standard) or "$FL3".
    pub magic: [u8; 4],
    /// SPSS product that created the file.
    pub product: String,
    /// Endianness indicator (2 = native layout).
    pub layout_code: i32,
    /// Number of 8-byte slots per case (row).
    pub nominal_case_size: i32,
    /// Compression type.
    pub compression: Compression,
    /// 1-based index of weight variable (0 = unweighted).
    pub weight_index: i32,
    /// Number of cases (-1 = unknown).
    pub ncases: i32,
    /// Compression bias (typically 100.0).
    pub bias: f64,
    /// Creation date string (e.g., "01 Jan 24").
    pub creation_date: String,
    /// Creation time string (e.g., "14:30:00").
    pub creation_time: String,
    /// File label (up to 64 chars).
    pub file_label: String,
    /// Whether byte-swapping is needed for this file.
    pub bswap: bool,
}

impl FileHeader {
    /// Parse the SAV file header from a reader.
    ///
    /// After this call, the reader is positioned right after the header,
    /// ready to read variable records.
    pub fn parse<R: Read>(reader: &mut SavReader<R>) -> Result<FileHeader> {
        // Magic: 4 bytes
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;

        if &magic != b"$FL2" && &magic != b"$FL3" {
            return Err(SpssError::InvalidMagic { found: magic });
        }

        // Product name: 60 bytes
        let product_bytes = reader.read_bytes(60)?;
        let product = io_utils::bytes_to_string_lossy(io_utils::trim_trailing_padding(&product_bytes));

        // Layout code: 4 bytes (used to detect endianness)
        let layout_bytes = reader.read_bytes(4)?;
        let layout_arr: [u8; 4] = layout_bytes.try_into().unwrap();
        let bswap = io_utils::detect_endianness(layout_arr)?;
        reader.set_bswap(bswap);

        let layout_code = if bswap {
            i32::from_be_bytes(layout_arr)
        } else {
            i32::from_le_bytes(layout_arr)
        };

        // Now read the rest with correct endianness
        let nominal_case_size = reader.read_i32()?;
        let compression_code = reader.read_i32()?;
        let compression = Compression::from_i32(compression_code).ok_or(
            SpssError::UnsupportedCompression(compression_code),
        )?;
        let weight_index = reader.read_i32()?;
        let ncases = reader.read_i32()?;
        let bias = reader.read_f64()?;

        // Creation date: 9 bytes
        let date_bytes = reader.read_bytes(9)?;
        let creation_date =
            io_utils::bytes_to_string_lossy(io_utils::trim_trailing_padding(&date_bytes));

        // Creation time: 8 bytes
        let time_bytes = reader.read_bytes(8)?;
        let creation_time =
            io_utils::bytes_to_string_lossy(io_utils::trim_trailing_padding(&time_bytes));

        // File label: 64 bytes
        let label_bytes = reader.read_bytes(64)?;
        let file_label =
            io_utils::bytes_to_string_lossy(io_utils::trim_trailing_padding(&label_bytes));

        // Padding: 3 bytes
        reader.skip(3)?;

        Ok(FileHeader {
            magic,
            product,
            layout_code,
            nominal_case_size,
            compression,
            weight_index,
            ncases,
            bias,
            creation_date,
            creation_time,
            file_label,
            bswap,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_header_bytes(compression: i32, ncases: i32) -> Vec<u8> {
        let mut buf = Vec::new();

        // Magic
        buf.extend_from_slice(b"$FL2");
        // Product (60 bytes, space-padded)
        let product = b"test product";
        buf.extend_from_slice(product);
        buf.extend_from_slice(&vec![b' '; 60 - product.len()]);
        // Layout code = 2 (LE)
        buf.extend_from_slice(&2_i32.to_le_bytes());
        // Nominal case size
        buf.extend_from_slice(&10_i32.to_le_bytes());
        // Compression
        buf.extend_from_slice(&compression.to_le_bytes());
        // Weight index
        buf.extend_from_slice(&0_i32.to_le_bytes());
        // Number of cases
        buf.extend_from_slice(&ncases.to_le_bytes());
        // Bias
        buf.extend_from_slice(&100.0_f64.to_le_bytes());
        // Creation date (9 bytes)
        buf.extend_from_slice(b"01 Jan 24");
        // Creation time (8 bytes)
        buf.extend_from_slice(b"14:30:00");
        // File label (64 bytes)
        let label = b"Test file";
        buf.extend_from_slice(label);
        buf.extend_from_slice(&vec![b' '; 64 - label.len()]);
        // Padding (3 bytes)
        buf.extend_from_slice(&[0u8; 3]);

        buf
    }

    #[test]
    fn test_parse_header() {
        let data = make_header_bytes(1, 100);
        let mut reader = SavReader::new(&data[..]);
        let header = FileHeader::parse(&mut reader).unwrap();

        assert_eq!(&header.magic, b"$FL2");
        assert_eq!(header.product, "test product");
        assert_eq!(header.layout_code, 2);
        assert_eq!(header.nominal_case_size, 10);
        assert_eq!(header.compression, Compression::Bytecode);
        assert_eq!(header.weight_index, 0);
        assert_eq!(header.ncases, 100);
        assert!((header.bias - 100.0).abs() < f64::EPSILON);
        assert_eq!(header.creation_date, "01 Jan 24");
        assert_eq!(header.creation_time, "14:30:00");
        assert_eq!(header.file_label, "Test file");
        assert!(!header.bswap);
    }

    #[test]
    fn test_invalid_magic() {
        let mut data = make_header_bytes(1, 100);
        data[0..4].copy_from_slice(b"XXXX");
        let mut reader = SavReader::new(&data[..]);
        let err = FileHeader::parse(&mut reader).unwrap_err();
        assert!(matches!(err, SpssError::InvalidMagic { .. }));
    }
}
