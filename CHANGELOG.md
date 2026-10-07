# Changelog

All notable changes to ambers are documented in this file.

## [Unreleased]

- Fix GitHub issue #5: on code-page encoded files (windows-1250 and every other mapped code page) variable names, long names, MR-set names/labels/counted values, variable attributes, long-string record names and the file label were decoded as UTF-8 before the file's encoding was known, turning accented characters into U+FFFD and truncating or panicking in the MR-set parser. All dictionary text now stays raw bytes until `resolve_dictionary` decodes it once with the file encoding. Fixes a related false "duplicate variable name" error for names differing only by an accented letter. UTF-8 files and data columns are unchanged

## [0.4.5] - 2026-10-07

- Add `warnings_and_errors.md`, a catalogue of every reader warning finding and error message with meaning and remedy, and a "Warnings and Errors" section at the end of the README

- Fix GitHub issue #4: a variable record whose type field is not 0, 1-255 or -1 is rejected (`InvalidVariable`); a garbage width of hundreds of millions made every row walk millions of string segments (seconds for a 4 KB file, now milliseconds). Negative values other than -1 are tolerated as continuation slots with a finding, since SPSS opens such files
- Fix GitHub issue #3: sizes declared by the file are checked before allocating. `read_bytes` refuses requests larger than the file, a variable may declare at most 3 missing values, buffer capacities are capped by the bytes actually present, and cold reservations are capped. Fuzzer files that requested 1-7 GB now fail or read in milliseconds without touching memory
- Date/time values outside years 1-9999 (or non-finite) are set to null and reported as a finding instead of being stored as saturated values that made Polars panic on display
- **Behaviour change (issue #1 follow-up):** a header row width that disagrees with the variable list is now a finding instead of an error, matching SPSS which ignores that field; the variable list is used. Duplicate variable names are now rejected with `InvalidDictionary` (SPSS rejects them too)
- Very-long-string declarations with an impossible width or more segments than variables remain are ignored with a finding
- Add file-damage warnings: when a file's header disagrees with its contents (declared row count vs rows read, compression bias other than 100, missing row width), `read_sav`/`scan_sav`/`read_sav_meta` emit `ambers.CorruptFileWarning` and list the findings in `SavFile.warnings` and `SpssMetadata.warnings` (also in `meta.schema["warnings"]` and `meta.summary()`). The data is still read exactly as stored, never silently corrected. Rust: new `SpssMetadata::warnings`; `read_sav` now returns metadata cloned after the read so it carries data-time findings
- Fix GitHub issue #2: a header declaring 0 cases while data follows made the reader write past a zero-length row buffer in release builds (process crash). The header case count is now only a capacity hint floored at one row, and the bounds check guarding the decompressor's unsafe writes is unconditional (`SpssError::Internal`) instead of debug-only. Such files now read the rows actually present, matching SPSS (the reporter's file: 5 rows). Legitimately empty files no longer trip a debug assertion. No change for files with a positive case count
- Fix GitHub issue #1: row width is now derived from the dictionary's type 2 records instead of the header's `nominal_case_size`. A file whose header disagrees with its dictionary is rejected with `invalid dictionary: header declares N slots per case but the dictionary defines M` (SPSS and pyreadstat reject such files too). Previously the mismatch caused an out-of-bounds read and returned garbage columns. A header value of 0 or -1 is tolerated, as some writers store that. No change for valid files (verified byte-identical output on all local test files)
- Add `SpssError::InvalidDictionary`
- Add `tests/test_malformed.rs` and `tests/test_malformed.py` (synthetic header/dictionary mismatch fixtures); Python test added to CI

## [0.4.4] - 2026-09-18

- Fix `FutureWarning: from_arrow(<ArrowStreamExportable>) will return a Series` emitted by `read_sav()` and `scan_sav()` on Polars >= 1.44 — Arrow streams are now converted with `pl.DataFrame()` (available since Polars 1.3, which remains the minimum)
- Polars 2.0 compatibility: `read_sav()`/`scan_sav()` no longer break when `pl.from_arrow()` returns a Series; `codebook()` type detection casts before `is_in()` to satisfy 2.0's strict coercion. Full Python test suite passes on Polars 1.40, 1.44.2 and 2.0.0rc1
- Add `tests/test_polars_compat.py` (eager, lazy, and multi-batch conversion with FutureWarning treated as error); added to CI

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
