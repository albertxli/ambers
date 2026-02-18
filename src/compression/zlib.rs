use std::io::{Read, Seek, SeekFrom};

use flate2::Decompress;
use rayon::prelude::*;

use crate::error::{Result, SpssError};
use crate::io_utils::SavReader;

/// ZSAV zlib header: offsets to the trailer.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ZHeader {
    pub zheader_offset: i64,
    pub ztrailer_offset: i64,
    pub ztrailer_length: i64,
}

/// ZSAV trailer: compression parameters.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ZTrailer {
    pub bias: i64,
    pub zero: i64,
    pub block_size: i32,
    pub n_blocks: i32,
    pub entries: Vec<ZTrailerEntry>,
}

/// A single block entry in the ZSAV trailer.
#[derive(Debug, Clone)]
#[allow(dead_code)]
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
///
/// Phase 1: Read all compressed blocks sequentially (I/O-bound).
/// Phase 2: Decompress all blocks in parallel (CPU-bound).
pub fn decompress_zsav_blocks<R: Read + Seek>(
    reader: &mut SavReader<R>,
    trailer: &ZTrailer,
) -> Result<Vec<u8>> {
    // Phase 1: Sequential I/O — read all compressed blocks
    let compressed_blocks: Vec<(Vec<u8>, usize)> = trailer
        .entries
        .iter()
        .map(|entry| {
            reader
                .inner_mut()
                .seek(SeekFrom::Start(entry.compressed_offset as u64))?;
            let compressed = reader.read_bytes(entry.compressed_size as usize)?;
            Ok((compressed, entry.uncompressed_size as usize))
        })
        .collect::<Result<Vec<_>>>()?;

    // Phase 2: Parallel decompression — each thread gets its own Decompress instance
    let decompressed_blocks: Vec<Vec<u8>> = compressed_blocks
        .par_iter()
        .map(|(compressed, uncompressed_size)| {
            let mut decompressed = vec![0u8; *uncompressed_size];
            let mut decompressor = Decompress::new(true);

            match decompressor.decompress(
                compressed,
                &mut decompressed,
                flate2::FlushDecompress::Finish,
            ) {
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

            let actual_out = decompressor.total_out() as usize;
            decompressed.truncate(actual_out);
            Ok(decompressed)
        })
        .collect::<Result<Vec<_>>>()?;

    // Concatenate in order
    let total_uncompressed: usize = decompressed_blocks.iter().map(|b| b.len()).sum();
    let mut output = Vec::with_capacity(total_uncompressed);
    for block in decompressed_blocks {
        output.extend_from_slice(&block);
    }

    Ok(output)
}
