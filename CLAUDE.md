# ambers — Pure Rust Statistical File Reader

Like amber preserving ancient life, ambers safely captures and preserves data from statistical file formats. Native Rust, no C FFI bindings. Framework-agnostic Arrow output.

**Package name:** `ambers` (crates.io + PyPI). Python: `import ambers` (or `import ambers as am`).

## Milestone 1: SPSS Reader (COMPLETE)

### Public API (`src/lib.rs`)

```rust
read_sav(path) -> Result<(RecordBatch, SpssMetadata)>        // file path
read_sav_from_reader(reader) -> Result<(RecordBatch, SpssMetadata)>  // any Read+Seek source
read_sav_metadata(path) -> Result<SpssMetadata>               // metadata only, skips data
```

- **Data output:** Arrow `RecordBatch` — zero-copy to Polars, DataFusion, DuckDB
- **Metadata output:** `SpssMetadata` — HashMap-based, all fields use `variable_` prefix
- **Compression:** Uncompressed, bytecode (.sav), zlib (.zsav)
- **Tests:** 32 unit tests + 1 doc-test, verified against 7 real-world files (3–22,070 rows, 75–677 columns)

### SpssMetadata fields

| Field | Type | Description |
|-------|------|-------------|
| `file_label` | `String` | File label set in SPSS |
| `file_encoding` | `String` | Character encoding (e.g. "UTF-8") |
| `compression` | `Compression` | None / Bytecode / Zlib |
| `creation_time` | `String` | Date created |
| `modification_time` | `String` | Time created |
| `notes` | `Vec<String>` | Document records |
| `number_rows` | `Option<i64>` | Row count from header |
| `number_columns` | `usize` | Visible variable count |
| `file_format` | `String` | "sav" or "zsav" |
| `variable_names` | `Vec<String>` | Ordered column names (defines Arrow schema order) |
| `variable_labels` | `HashMap<String, String>` | Variable name -> label |
| `spss_variable_types` | `HashMap<String, String>` | SPSS format strings: "F8.2", "A50" |
| `rust_variable_types` | `HashMap<String, String>` | Rust types: "f64", "String" |
| `variable_value_labels` | `HashMap<String, HashMap<Value, String>>` | Per-variable value->label maps |
| `variable_measure` | `HashMap<String, Measure>` | Nominal / Ordinal / Scale (from file) |
| `variable_alignment` | `HashMap<String, Alignment>` | Left / Right / Center |
| `variable_display_width` | `HashMap<String, u32>` | Display width |
| `variable_storage_width` | `HashMap<String, usize>` | Storage width in bytes |
| `variable_missing` | `HashMap<String, Vec<MissingSpec>>` | Missing value specs |
| `mr_sets` | `HashMap<String, MrSet>` | Multiple response sets |
| `weight_variable` | `Option<String>` | Weight variable name |

Convenience methods: `label()`, `value_labels()`, `format()`, `measure()`

---

## Crate Structure

```
src/
  lib.rs                          Public API + re-exports
  main.rs                         CLI binary for testing
  error.rs                        SpssError enum (thiserror)
  constants.rs                    SYSMIS, bytecode codes, record types, enums (Compression, Measure, Alignment, VarType, FormatType, SpssFormat)
  io_utils.rs                     SavReader<R> with endian-aware read_i32/read_f64/etc.
  header.rs                       176-byte file header parsing
  encoding.rs                     Code page -> encoding_rs mapping, decode_str helpers
  variable.rs                     Type 2 variable records, MissingValues enum
  value_labels.rs                 Type 3+4 value label records
  document.rs                     Type 6 document records
  metadata.rs                     SpssMetadata struct, Value enum (with Ord), MissingSpec, MrSet
  dictionary.rs                   Record dispatch loop + post-dictionary resolution (the core orchestrator)
  data.rs                         Row reading: uncompressed, bytecode, zlib. String reassembly.
  arrow_convert.rs                Arrow Schema + RecordBatch builders
  info_records/
    mod.rs                        Subtype dispatch
    integer_info.rs               Subtype 3: machine info + character_code
    float_info.rs                 Subtype 4: SYSMIS/highest/lowest
    var_display.rs                Subtype 11: measure/width/alignment per variable
    long_var_names.rs             Subtype 13: short->long name mapping
    very_long_strings.rs          Subtype 14: true widths for >255-byte strings
    encoding_record.rs            Subtype 20: encoding name override
    long_string_labels.rs         Subtype 21: value labels for long strings
    long_string_missing.rs        Subtype 22: missing values for long strings
  compression/
    mod.rs                        Module declaration
    bytecode.rs                   Stateful bytecode decompressor (cross-row state preservation)
    zlib.rs                       ZSAV zheader/ztrailer + flate2 block decompression
```

---

## SAV Binary Format Quick Reference

### Record types (dictionary section)
- **Type 2** — Variable record (short name, type, width, label, missing values, format)
- **Type 3+4** — Value labels (type 3 = label pairs, type 4 = variable indices, always paired)
- **Type 6** — Document record (80-byte lines of notes)
- **Type 7** — Info/extension records (dispatched by subtype)
- **Type 999** — Dictionary termination, data follows

### Info record subtypes
- **3** — Integer info: endianness, character code page
- **4** — Float info: SYSMIS, highest, lowest bit patterns
- **11** — Variable display: measure, display_width, alignment per variable
- **13** — Long variable names: `SHORT=LongName` tab-separated pairs
- **14** — Very long strings: `VARNAME=WIDTH` for strings > 255 bytes
- **20** — Encoding name (overrides subtype 3 code page)
- **21** — Long string value labels (pascal-string format)
- **22** — Long string missing values

### Data section
After type 999, data is stored as rows of 8-byte slots:
- **Uncompressed:** Raw 8-byte values (f64 for numeric, padded bytes for string)
- **Bytecode compressed:** 8-byte control blocks where each byte is an opcode, followed by raw data for opcode 253
- **Zlib compressed (.zsav):** Block-based zlib, decompressed blocks feed into bytecode decompressor

---

## Key Technical Decisions

| Decision | Detail |
|----------|--------|
| **SYSMIS** | `-DBL_MAX` (bits `0xFFEFFFFFFFFFFFFF`), NOT NaN. Maps to Arrow null. |
| **Encoding priority** | Subtype 20 name > subtype 3 code page > default windows-1252 |
| **Bytecode decompressor** | Stateful across rows — control blocks do NOT align to row boundaries |
| **Very long strings** | Subtype 14 declares true width. `n_segments = ceil(width/252)`. Subsequent named segment variables marked as ghosts. |
| **Arrow types** | Numeric -> Float64 (nullable), String -> Utf8. Date/time stay as Float64 with format in metadata. |
| **Missing values** | SYSMIS -> Arrow null. User-defined missing ranges in metadata only (not nullified), matching pyreadstat behavior. |
| **Endianness** | Detected from header `layout_code`. `SavReader` handles byte-swapping transparently. |
| **No packed structs** | Fields read individually via `io_utils` helpers — safe, handles endian swapping. |
| **Naming** | All metadata fields use `variable_` prefix. HashMap for O(1) lookup. Vec for ordering. |
| **Value sorting** | `Value` implements `Ord` — numeric values sort by actual number, not string representation. |

---

## Bugs Found & Lessons Learned

1. **Column count wrong (847 vs 677):** Very long string segment variables (e.g. SURVE0, SURVE1) weren't marked as ghosts. Fix: walk forward from each VLS variable and mark the next `n_segments - 1` non-ghost named records as ghosts.

2. **Row count wrong (87 vs 1500):** Bytecode decompressor was stateless — control block state lost between row calls. Fix: made `BytecodeDecompressor` stateful, preserving `pos`, `control_bytes`, `control_idx`, `eof` across `decompress_row()` calls.

3. **SYSMIS is not NaN:** SYSMIS is `-DBL_MAX` (most negative finite double), not a NaN. Test assertions needed to check `is_finite()` and `== -f64::MAX`.

---

## Testing

```bash
cargo test              # 32 unit tests + 1 doc-test
cargo run -- file.sav   # Quick CLI test with any .sav file
```

### Verified test files
| File | Columns | Rows | Compression |
|------|---------|------|-------------|
| n=3.sav | 393 | 3 | Bytecode |
| n=10.sav | 393 | 10 | Bytecode |
| 251001.sav | 677 | 22,070 | Bytecode |
| rpm_2025_data_final_raw_id.sav | 186 | 1,000 | Bytecode |
| rcg_2025_Bahamas_Freeport_clean.sav | 124 | 211 | Uncompressed |
| DR_Landscape SPSS (AUG 30).sav | 75 | 1,500 | Bytecode |
| 231104.sav | 216 | 2,146 | Bytecode |

---

## Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| `arrow` | 55 | Arrow RecordBatch output |
| `flate2` | 1 | Zlib decompression for .zsav |
| `encoding_rs` | 0.8 | Character encoding conversion |
| `thiserror` | 2 | Error type derivation |

---

## Next Steps

### Milestone 2: SAV/ZSAV Writer
- Write `RecordBatch` + `SpssMetadata` back to `.sav`/`.zsav` files
- Reverse the data flow: Arrow -> rows -> bytecode/zlib compression -> binary

### Milestone 3: PyO3 Python Bindings
- Expose `read_sav()` to Python via PyO3
- Return PyArrow Table + metadata dict for Polars workflow (`pl.from_arrow()`)
- Python: `import ambers` (or `import ambers as am`)
- Target: drop-in replacement for pyreadstat in the user's data science pipeline

### Future: Additional Format Support
- ambers is designed to grow beyond SPSS — the name and architecture support adding readers/writers for other statistical formats (Stata .dta, SAS .sas7bdat, etc.)

### Future Improvements
- Arrow temporal types for DATE, TIME, DATETIME formats (currently Float64)
- Streaming reader with `read_batch(batch_size)` for large files
- Real `.zsav` file testing
- Integration test fixtures generated via pyreadstat with JSON expected values
