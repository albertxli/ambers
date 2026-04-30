# Changelog

All notable changes to ambers are documented in this file.

## [0.4.3] - 2026-04-30

- Add `codebook(df, meta)` — generate a Polars DataFrame data dictionary documenting every variable and its values
- Two views: `view="variables"` (default, one row per variable) and `view="values"` (one row per value)
- 5-way variable type detection: single-select, multi-select, numeric, text, date — with full multi-select tiers (MR sets, binary patterns, sibling series, generic binary)
- `values_format=` controls the variables-view `values` column: `"string"` (default, newline-joined `"1=Low\n2=Medium"`) for clean marimo HTML and Excel rendering; `"struct"` for `List[Struct{value_code, value_label}]` with `.explode().unnest()` workflows
- `include_meta=True` adds `variable_measure` and `variable_format` columns to the values view
- `columns=` and `exclude=` filtering combinable
- Strict validation: rejects unknown `view` values; rejects `values_format` with `view="values"`
- 40 tests; integration verified on real SPSS files

## [0.4.2] - 2026-04-07

- Add `validate(df, meta)` — check value label quality: unlabeled values (error) and duplicate labels (warning)
- `ValidationReport` with `is_valid`, `errors`, `warnings`, `raise_if_invalid()`, `to_frame()`
- Repr truncation: max 10 issues shown, long messages shortened, box width capped at 80 chars
- Shared pure helpers between `validate()` and `apply_labels()` (logic/policy separation)
- `columns` + `exclude` can be combined in `validate()` (lenient), stay mutually exclusive in `apply_labels`/`apply_missing` (strict)
- Add `validate.md` documentation with full API reference and examples
- 29 tests covering all checks, filtering, repr truncation, and shared helper consistency

## [0.4.1] - 2026-04-07

- Add `apply_missing(df, meta)` — nullify SPSS user-defined missing value codes (discrete, range, range+discrete)
- Add `exclude=` parameter to both `apply_labels` and `apply_missing` — skip specific columns, mutually exclusive with `columns=`
- 35 new tests: 28 for apply_missing (all SPSS missing value combinations), 7 for exclude parameter
- Add `test_apply_missing.py` to CI pytest

## [0.4.0] - 2026-04-07

- **Modularity refactor:** split `src/python/mod.rs` (2,299 LOC) into 5 focused submodules: `conversions.rs`, `metadata.rs`, `diff.rs`, `io.rs`, and thin `mod.rs`
- **Python package cleanup:** slim `__init__.py` (432 → 18 LOC) to thin re-exports; implementation moved to `_containers.py` and `_io.py`
- Add `apply_labels()` with three output modes: `"enum"` (default), `"string"`, `"enum_null"`
- Dtype-aware label application: Enum for numeric columns, pass-through for string columns
- Add 42 tests for `apply_labels`: output modes, dtype-aware behavior, error handling, LazyFrame
- Add Python pytest step to CI: apply_labels, metadata_api, writer_issues
- Fix CI: make test_paths import conditional in conftest.py
- No public API changes, no performance impact

## [0.3.9] - 2026-04-05

- Add `source`, `shape`, `file_size`, `read_time`, `compression` fields to `SavFile`
- Add box-drawing `__repr__` for `SavFile` with file info, timing, and shape
- Attribute names and repr labels are 1:1 consistent (e.g. `sav.read_time` displays as "Read time")
- `SavFile` fields default to `None` for forward-compatible in-memory construction

## [0.3.8] - 2026-04-05

- Add `SavFile` Generic dataclass: `read_sav()` and `scan_sav()` now return `SavFile` with `.data` and `.meta` attributes instead of bare tuples
- Rename `read_sav_metadata()` to `read_sav_meta()` for API consistency
- Add custom `__repr__` for `SavFile` — compact summary in Jupyter/REPL
- **Breaking:** `df, meta = am.read_sav(...)` tuple unpacking no longer works; use `sav = am.read_sav(...)` then `sav.data` / `sav.meta`
- Add `uv`-only Python environment rule to project guidelines
- Add `notebook_test/` to `.gitignore`

## [0.3.7] - 2025-02-24

- Fix ZSAV writer: 3 bugs causing SPSS to crash on all ambers-written .zsav files
  - ZTrailer bias field: write -100 (negative) per PSPP spec, was incorrectly +100
  - ZTrailer block uncompressed_offset: start at zheader file position per PSPP/ReadStat, was incorrectly 0
  - Subtype 3 compression_code: always write 1 per PSPP spec, was incorrectly writing actual compression value
- Fix reader subtype 21 (long string value labels): add missing var_width field parse
- Fix writer subtype 21: use long_name instead of short_name, pad values to var_width per SPSS spec
- Add format/type mismatch validation: reject string format on numeric column and vice versa
- Add 36 writer stress tests (pyreadstat issues #267, #119, #264)
- Add CI workflow (fmt, clippy, test on Linux/Windows/macOS + Python smoke test)
- Add unit tests for arrow_convert and scanner modules (15 new tests)
- Add overflow protection: AllocationTooLarge error, 2 GB pre-allocation cap, 16 GB zlib guard
- Split writer.rs (2,930 lines) into writer/{mod, layout, records, data, tests} submodules
- Add fail-fast validation: `validate_write_inputs()` catches metadata errors before data processing
- Add Python-side early metadata validation before PyCapsule consumption
- Stream zlib decompression block-by-block instead of all blocks upfront (lower peak memory)
- Add BytecodeDecompressor checkpoint/restore for streaming support
- Fix 29 clippy warnings across codebase
- Fix CI Python smoke test to use `maturin build` instead of `maturin develop`
- Use uv instead of pip in CI for faster dependency installs
- Update write benchmark results: 6–41x faster than pyreadstat (up from 4–20x)
- Remove Co-Authored-By trailers from git history

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
