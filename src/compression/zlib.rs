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

/// Decompress all ZSAV blocks into a single pre-allocated byte buffer.
///
/// Each block is zlib-compressed. The decompressed blocks contain
/// bytecode-compressed data that must be further processed by the
/// bytecode decompressor.
///
/// Phase 1: Read all compressed blocks sequentially (I/O-bound).
/// Phase 2: Decompress all blocks in parallel directly into the output buffer,
///          avoiding per-block Vec allocations and the final concat copy.
pub fn decompress_zsav_blocks<R: Read + Seek>(
    reader: &mut SavReader<R>,
    trailer: &ZTrailer,
) -> Result<Vec<u8>> {
    // Phase 1: Sequential I/O — read all compressed blocks + compute output offsets
    let mut compressed_blocks: Vec<(Vec<u8>, usize, usize)> = Vec::with_capacity(trailer.entries.len());
    let mut total_uncompressed: usize = 0;

    for entry in &trailer.entries {
        reader
            .inner_mut()
            .seek(SeekFrom::Start(entry.compressed_offset as u64))?;
        let compressed = reader.read_bytes(entry.compressed_size as usize)?;
        let uncompressed_size = entry.uncompressed_size as usize;
        compressed_blocks.push((compressed, uncompressed_size, total_uncompressed));
        total_uncompressed += uncompressed_size;
    }

    // Phase 2: Pre-allocate single output buffer, decompress blocks in parallel
    // directly into non-overlapping slices — no per-block Vecs, no final concat.
    let mut output = vec![0u8; total_uncompressed];
    // Store as usize (Send+Sync) to avoid Rust 2024 disjoint field capture issues.
    let base_addr = output.as_mut_ptr() as usize;

    compressed_blocks
        .par_iter()
        .try_for_each(|(compressed, uncompressed_size, offset)| {
            // SAFETY: Each block writes to [offset..offset+uncompressed_size],
            // and these ranges are non-overlapping (offsets are cumulative sums).
            // The output Vec lives longer than this par_iter scope.
            let dest = unsafe {
                std::slice::from_raw_parts_mut(
                    (base_addr + *offset) as *mut u8,
                    *uncompressed_size,
                )
            };

            let mut decompressor = Decompress::new(true);
            match decompressor.decompress(
                compressed,
                dest,
                flate2::FlushDecompress::Finish,
            ) {
                Ok(flate2::Status::Ok | flate2::Status::StreamEnd) => Ok(()),
                Ok(flate2::Status::BufError) => {
                    Err(SpssError::Zlib("decompression buffer too small".to_string()))
                }
                Err(e) => {
                    Err(SpssError::Zlib(format!("zlib decompression error: {e}")))
                }
            }
        })?;

    Ok(output)
}
