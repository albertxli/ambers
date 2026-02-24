# Changelog

All notable changes to ambers are documented in this file.

## [0.3.3] - 2025-02-21

- Add compression field to `meta.schema` and reorder schema fields
- Fix VLS last segment width to match SPSS spec (ReadStat compatibility)

## [0.3.2] - 2025-02-19

- Optimize writer performance and redesign compression API
- Add NumPy-style docstrings to `.pyi` type stubs for IDE documentation

## [0.3.1] - 2025-02-17

- Fix variable attributes using long names in subtype 18
- Fix subtype 22 format for SPSS-compatible long string missing values
- Fix string missing values on long strings (width > 8) for SPSS compatibility
- Fix MR set double-`$` prefix and mixed-type missing values bugs

## [0.3.0] - 2025-02-14

- **Milestone 3: SAV/ZSAV Writer** — full roundtrip support
- `write_sav()` and `write_sav_to_writer()` in Rust
- Python `ambers.write_sav()` with auto-detect compression from extension
- All three compression modes: uncompressed, bytecode, zlib
- SpssMetadata construction API: `SpssMetadata()` constructor, `update()`, `with_*()` methods
- Variable attributes read and write (subtype 18)
- Variable roles read and write (subtype 18 `$@Role`)
- Subtype 19 (MRSETS2) support for modern SPSS MR set definitions
- Python roundtrip tests and write benchmarks
- Fix VLS segment count formula and ghost name leaking
- Fix A254→A256 format bug

## [0.2.6] - 2025-02-08

- Fix VLS segment assembly: use 255 bytes per segment, not 252

## [0.2.5] - 2025-02-07

- Tiled parallel column processing for wide files (>12 KB row width)
- Bias LUT optimization: pre-computed 2 KB lookup table for bytecode decompression
- Unsafe pointer copies and unchecked f64 reads in hot path

## [0.2.4] - 2025-02-06

- Unified columnar pipeline: decompress bytecode directly to raw buffer
- Eliminate intermediate `SlotValue` representation

## [0.2.3] - 2025-02-05

- Cap uncompressed chunk size to 256 MB for cache-friendly large file reads
- Switch to zlib-rs backend for faster zlib decompression
- Add mimalloc allocator for Python builds
- Direct-write decompression, zero-fill avoidance

## [0.2.2] - 2025-02-04

- Add `columns`, `n_rows`, `row_index_name`, `row_index_offset` params to Python `read_sav()`/`scan_sav()`

## [0.2.0] - 2025-02-03

- Arrow temporal types: DATE→Date32, DATETIME→Timestamp(us), TIME→Duration(us)
- Wkday/Month stay Float64 (not temporal)
- Temporal conversion in `finish()` post-processing (not in hot path)

## [0.1.8] - 2025-02-02

- Optimize large uncompressed file performance: 2.3x faster on 5.4 GB files

## [0.1.7] - 2025-02-01

- Six performance optimizations to beat polars_readstat on all file sizes:
  - Bytecode match reorder (1..=251 first)
  - `Cow<str>` string decoding (zero-copy UTF-8)
  - Bulk I/O for uncompressed (single `read_exact` per row)
  - VLS segment pre-compute
  - Smart string capacity
  - StringViewArray with deduplication
- `scan_sav()` LazyFrame with `register_io_source`
- Direct-to-columnar builders (StringViewBuilder, Float64Builder)
- Drop PyArrow runtime dependency — PyCapsule-only data transfer

## [0.1.6] - 2025-01-31

- Revamp README benchmarks

## [0.1.5] - 2025-01-30

- Initial public release on crates.io and PyPI
- **Milestone 1:** SPSS .sav/.zsav reader (all compression modes)
- **Milestone 2:** PyO3 Python bindings with Polars DataFrame output
- `read_sav()`, `scan_sav()`, `read_sav_metadata()` API
- Full SpssMetadata with 22 fields
- Streaming `SavScanner` with column projection and row limits
