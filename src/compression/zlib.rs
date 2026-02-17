use std::io::{Read, Seek, SeekFrom};

use flate2::Decompress;

use crate::error::{Result, SpssError};
use crate::io_utils::SavReader;

/// ZSAV zlib header: offsets to the trailer.
#[derive(Debug, Clone)]
pub struct ZHeader {
    pub zheader_offset: i64,
    pub ztrailer_offset: i64,
    pub ztrailer_length: i64,
}

/// ZSAV trailer: compression parameters.
#[derive(Debug, Clone)]
pub struct ZTrailer {
    pub bias: i64,
    pub zero: i64,
    pub block_size: i32,
    pub n_blocks: i32,
    pub entries: Vec<ZTrailerEntry>,
}

/// A single block entry in the ZSAV trailer.
#[derive(Debug, Clone)]
pub struct ZTrailerEntry {
    pub uncompressed_offset: i64,
    pub compressed_offset: i64,
    pub uncompressed_size: i32,
    pub compressed_size: i32,
}

/// Read the ZSAV zlib header (24 bytes, immediately after the dictionary termination).
pub fn read_zheader<R: Read>(reader: &mut SavReader<R>) -> Result<ZHeader> {
    Ok(ZHeader {
        zheader_offset: reader.read_i64()?,
        ztrailer_offset: reader.read_i64()?,
        ztrailer_length: reader.read_i64()?,
    })
}

/// Read the ZSAV trailer from the file.
///
/// Requires a seekable reader to jump to the trailer offset.
pub fn read_ztrailer<R: Read + Seek>(
    reader: &mut SavReader<R>,
    zheader: &ZHeader,
) -> Result<ZTrailer> {
    reader
        .inner_mut()
        .seek(SeekFrom::Start(zheader.ztrailer_offset as u64))?;

    let bias = reader.read_i64()?;
    let zero = reader.read_i64()?;
    let block_size = reader.read_i32()?;
    let n_blocks = reader.read_i32()?;

    let mut entries = Vec::with_capacity(n_blocks as usize);
    for _ in 0..n_blocks {
        entries.push(ZTrailerEntry {
            uncompressed_offset: reader.read_i64()?,
            compressed_offset: reader.read_i64()?,
            uncompressed_size: reader.read_i32()?,
            compressed_size: reader.read_i32()?,
        });
    }

    Ok(ZTrailer {
        bias,
        zero,
        block_size,
        n_blocks,
        entries,
    })
}

/// Decompress all ZSAV blocks into a single byte buffer.
///
/// Each block is zlib-compressed. The decompressed blocks contain
/// bytecode-compressed data that must be further processed by the
/// bytecode decompressor.
pub fn decompress_zsav_blocks<R: Read + Seek>(
    reader: &mut SavReader<R>,
    trailer: &ZTrailer,
) -> Result<Vec<u8>> {
    let mut output = Vec::new();

    for entry in &trailer.entries {
        // Seek to the compressed block
        reader
            .inner_mut()
            .seek(SeekFrom::Start(entry.compressed_offset as u64))?;

        // Read compressed data
        let compressed = reader.read_bytes(entry.compressed_size as usize)?;

        // Decompress using zlib
        let mut decompressed = vec![0u8; entry.uncompressed_size as usize];
        let mut decompressor = Decompress::new(true); // zlib format

        match decompressor.decompress(&compressed, &mut decompressed, flate2::FlushDecompress::Finish) {
            Ok(flate2::Status::Ok | flate2::Status::StreamEnd) => {}
            Ok(flate2::Status::BufError) => {
                return Err(SpssError::Zlib(
                    "decompression buffer too small".to_string(),
                ));
            }
            Err(e) => {
                return Err(SpssError::Zlib(format!("zlib decompression error: {e}")));
            }
        }

        // Trim to actual decompressed size
        let actual_out = decompressor.total_out() as usize;
        decompressed.truncate(actual_out);

        output.extend_from_slice(&decompressed);
    }

    Ok(output)
}
