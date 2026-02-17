# ambers: Pure Rust Statistical File Reader -- Project Summary

## Overview

The **first pure Rust SPSS reader library** (no C FFI bindings). All current Rust approaches (readstat-rs, polars_readstat) use FFI bindings to the C ReadStat library. ambers creates a native alternative.

**Branding:** ambers = "amber", a safe preservative that captures data from different stats formats. Same name across crates.io (`ambers`) and PyPI (`ambers`). Python: `import ambers` (or `import ambers as am`).

**Key requirements:**
- Read `.sav` (bytecode compressed) and `.zsav` (zlib compressed) files natively in Rust
- Full metadata extraction matching pyreadstat's `metadata_container` fields
- Output data as Apache Arrow `RecordBatch` for seamless Polars/DataFusion integration
- Library crate design ready for future PyO3 Python bindings
- **Milestone 1 scope: Reader only.** Writing comes in a future milestone.

**Reference projects:**
- `ultrasav` -- User's existing Python SPSS package (two-track Data/Metadata architecture)
- [ReadStat](https://github.com/WizardMac/ReadStat) -- C library for SAV format reference
- [pyreadstat](https://github.com/Roche/pyreadstat) -- Python wrapper, metadata_container reference

---

## Crate Structure

```
ambers/
  Cargo.toml
  src/
    lib.rs                      # Public API: read_sav(), re-exports
    main.rs                     # CLI binary for testing
    error.rs                    # SpssError enum (thiserror)
    constants.rs                # SYSMIS, format codes, bytecode constants, enums
    io_utils.rs                 # Endian-aware reading helpers (SavReader)
    header.rs                   # 176-byte file header parsing
    encoding.rs                 # IANA code page -> encoding_rs mapping + decode helpers
    variable.rs                 # Type 2 variable records + format decoding
    value_labels.rs             # Type 3/4 value label records
    document.rs                 # Type 6 document records
    metadata.rs                 # SpssMetadata struct (public output)
    dictionary.rs               # Record dispatch loop + post-dictionary resolution
    data.rs                     # Row iteration, string reassembly, column building
    arrow_convert.rs            # Column builders -> Arrow RecordBatch
    info_records/
      mod.rs                    # Type 7 subtype dispatch
      integer_info.rs           # Subtype 3: machine integer info
      float_info.rs             # Subtype 4: SYSMIS/highest/lowest
      var_display.rs            # Subtype 11: measure, width, alignment
      long_var_names.rs         # Subtype 13: short->long name map
      very_long_strings.rs      # Subtype 14: true widths for >255-byte strings
      encoding_record.rs        # Subtype 20: encoding name override
      long_string_labels.rs     # Subtype 21: value labels for long strings
      long_string_missing.rs    # Subtype 22: missing values for long strings
    compression/
      mod.rs                    # Compression module
      bytecode.rs               # Type 1 bytecode decompressor (stateful, cross-row)
      zlib.rs                   # Type 2 ZSAV block decompressor (flate2)
```

## Dependencies

```toml
[dependencies]
arrow = { version = "55", default-features = false, features = ["ffi"] }
flate2 = "1"
encoding_rs = "0.8"
thiserror = "2"

[dev-dependencies]
tempfile = "3"
```

---

## Public API

```rust
/// One-shot read: returns all data + metadata
pub fn read_sav(path: impl AsRef<Path>) -> Result<(RecordBatch, SpssMetadata)>;

/// Read from any reader (not just files)
pub fn read_sav_from_reader<R: Read + Seek>(reader: R) -> Result<(RecordBatch, SpssMetadata)>;

/// Metadata-only read: fast, skips data section
pub fn read_sav_metadata(path: impl AsRef<Path>) -> Result<SpssMetadata>;
```

---

## SpssMetadata (public output)

```rust
pub struct SpssMetadata {
    // File-level
    pub file_label: String,
    pub file_encoding: String,
    pub compression: Compression,          // None, Bytecode, Zlib
    pub creation_time: String,
    pub modification_time: String,
    pub notes: Vec<String>,                // document records
    pub number_rows: Option<i64>,
    pub number_columns: usize,
    pub file_format: String,               // "sav" or "zsav"

    // Variable names (ordered -- defines Arrow schema column order)
    pub variable_names: Vec<String>,

    // Per-variable metadata (all HashMap<String, _>)
    pub variable_labels: HashMap<String, String>,
    pub spss_variable_types: HashMap<String, String>,   // "F8.2", "A50"
    pub rust_variable_types: HashMap<String, String>,   // "f64", "String"
    pub variable_value_labels: HashMap<String, HashMap<Value, String>>,
    pub variable_alignment: HashMap<String, Alignment>,
    pub variable_storage_width: HashMap<String, usize>,
    pub variable_display_width: HashMap<String, u32>,
    pub variable_measure: HashMap<String, Measure>,
    pub variable_missing: HashMap<String, Vec<MissingSpec>>,

    // SPSS-specific
    pub mr_sets: HashMap<String, MrSet>,
    pub weight_variable: Option<String>,
}
```

---

## Key Technical Decisions

- **SYSMIS**: `-DBL_MAX` (bit pattern `0xFFEFFFFFFFFFFFFF`), NOT NaN
- **Encoding priority**: subtype 20 name > subtype 3 code page > default windows-1252
- **Format packing**: `(type << 16) | (width << 8) | decimals` in a single i32
- **Arrow types**: Numeric -> Float64 (nullable), String -> Utf8
- **Very long strings**: True width from subtype 14. `n_segments = ceil(width / 252)`. Ghost records excluded from output.
- **Bytecode decompressor**: Stateful -- control block position preserved across row boundaries (SPSS control blocks do NOT align to rows)
- **Missing values**: SYSMIS -> Arrow null. User-defined missing ranges stored in metadata only (not converted to nulls).
- **No `repr(C, packed)` structs**: Read fields individually via io_utils helpers to avoid alignment/UB issues.

---

## Data Flow

```
File open
  |
  v
[1] Parse header (176 bytes) --> endianness, compression type, case count, bias
  |
  v
[2] Dictionary loop (read i32 record type, dispatch):
    Type 2   --> Variable records
    Type 3+4 --> Value label sets
    Type 6   --> Document lines
    Type 7   --> Info records (subtypes 3,4,11,13,14,20,21,22)
    Type 999 --> Exit loop
  |
  v
[3] Post-dictionary resolution:
    - Apply long names (subtype 13)
    - Resolve very long string widths (subtype 14), mark ghost variables
    - Apply display info (subtype 11)
    - Select encoding, convert all strings to UTF-8
    - Build Arrow Schema + SpssMetadata
  |
  v
[4] Data decompression:
    None     --> raw 8-byte slots
    Bytecode --> stateful bytecode state machine
    Zlib     --> inflate blocks via flate2, then bytecode decompressor
  |
  v
[5] Row reading + Arrow column building:
    Numeric: f64, SYSMIS -> null
    String: collect segments, trim spaces, decode encoding
  |
  v
[6] Output: (RecordBatch, SpssMetadata)
```

---

## Bugs Found and Fixed

| Bug | Symptom | Root Cause | Fix |
|-----|---------|------------|-----|
| Column count wrong (847 vs 677) | Very long string segment variables counted as columns | Named segment records (SURVE0, SURVE1) not marked as ghosts | Walk forward from VLS variables, mark next `n_segments - 1` non-ghost records as ghosts |
| Row count wrong (87 vs 1500) | Most rows silently dropped | Bytecode decompressor was stateless -- control block state lost between rows | Made `BytecodeDecompressor` stateful (preserves `pos`, `control_bytes`, `control_idx`, `eof` across calls) |
| SYSMIS test assertion wrong | Test failed | Asserted `is_nan()` but SYSMIS is `-DBL_MAX`, not NaN | Fixed assertion to check `is_finite()`, `< 0.0`, `== -f64::MAX` |
| `encoding_rs::ISO_8859_1` not found | Compile error | encoding_rs doesn't have ISO_8859_1 (maps to WINDOWS_1252 per WHATWG) | Changed to `encoding_rs::WINDOWS_1252` |

---

## Test Results

- **32 unit tests** + **1 doc-test** all passing, zero warnings
- Successfully tested against 7 real-world `.sav` files:

| File | Columns | Rows | Compression | Notes |
|------|---------|------|-------------|-------|
| n=3.sav | 393 | 3 | Bytecode | First successful test |
| n=10.sav | 393 | 10 | Bytecode | |
| 251001.sav | 677 | 22,070 | Bytecode | Exposed column count bug |
| rpm_2025_data_final_raw_id.sav | 186 | 1,000 | Bytecode | Exposed row count bug |
| rcg_2025_Bahamas_Freeport_clean.sav | 124 | 211 | Uncompressed | |
| DR_Landscape SPSS (AUG 30).sav | 75 | 1,500 | Bytecode | Exposed row count bug |
| 231104.sav | 216 | 2,146 | Bytecode | Full metadata verification |

---

## Future Milestones (Not Yet Started)

- SAV/ZSAV writer
- PyO3 Python bindings for Polars integration (`import ambers`)
- Proper Arrow temporal type conversion for date/time formats
- Streaming reader with `read_batch()` for large files
- ZSAV testing with real `.zsav` files
- Additional format support (Stata .dta, SAS .sas7bdat, etc.)
