# ambers — Pure Rust Statistical File Reader

Like amber preserving ancient life, ambers safely captures and preserves data from statistical file formats. Native Rust, no C FFI bindings. Framework-agnostic Arrow output.

**Package name:** `ambers` (crates.io + PyPI). Python: `import ambers` (or `import ambers as am`).

## Milestone 1: SPSS Reader (COMPLETE)

### Rust API (`crates/ambers/src/lib.rs`)

```rust
// Eager read
read_sav(path) -> Result<(RecordBatch, SpssMetadata)>
read_sav_from_reader(reader) -> Result<(RecordBatch, SpssMetadata)>
read_sav_metadata(path) -> Result<SpssMetadata>

// Streaming scanner with column projection + row limits
scan_sav(path) -> Result<SavScanner<BufReader<File>>>
scan_sav_from_reader(reader, batch_size) -> Result<SavScanner<R>>
```

- **Data output:** Arrow `RecordBatch` — zero-copy to Polars, DataFusion, DuckDB
- **Metadata output:** `SpssMetadata` — IndexMap-based (insertion-ordered), all fields use `variable_` prefix
- **Compression:** Uncompressed, bytecode (.sav), zlib (.zsav)
- **Tests:** 37 unit tests + 2 doc-tests, verified against 7 real-world files (3–22,070 rows, 75–677 columns)

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
| `variable_labels` | `IndexMap<String, String>` | Variable name -> label |
| `spss_variable_types` | `IndexMap<String, String>` | SPSS format strings: "F8.2", "A50" |
| `rust_variable_types` | `IndexMap<String, String>` | Rust types: "f64", "String" |
| `variable_value_labels` | `IndexMap<String, IndexMap<Value, String>>` | Per-variable value->label maps |
| `variable_measure` | `IndexMap<String, Measure>` | Nominal / Ordinal / Scale (from file) |
| `variable_alignment` | `IndexMap<String, Alignment>` | Left / Right / Center |
| `variable_display_width` | `IndexMap<String, u32>` | Display width |
| `variable_storage_width` | `IndexMap<String, usize>` | Storage width in bytes |
| `variable_missing` | `IndexMap<String, Vec<MissingSpec>>` | Missing value specs |
| `mr_sets` | `IndexMap<String, MrSet>` | Multiple response sets |
| `weight_variable` | `Option<String>` | Weight variable name |

Convenience methods: `label()`, `value_labels()`, `format()`, `measure()`

---

## Milestone 2: PyO3 Python Bindings (COMPLETE)

### Python API

```python
import ambers

df, meta = ambers.read_sav("file.sav")            # Polars DataFrame + SpssMetadata
meta = ambers.read_sav_metadata("file.sav")        # metadata only (fast)

# SpssMetadata properties (same fields as Rust)
meta.variable_names       # list[str]
meta.variable_labels      # dict[str, str]
meta.variable_value_labels # dict[str, dict[float|str, str]]
meta.compression          # str: "none", "bytecode", "zlib"
meta.variable_measure     # dict[str, str]: "nominal", "ordinal", "scale"

# Convenience methods
meta.label("age")         # str | None
meta.format("age")        # str | None (e.g. "F8.2")
meta.measure("age")       # str | None
```

### Architecture

```
Cargo workspace
├── crates/ambers/          Pure Rust library (crates.io)
├── crates/ambers-py/       PyO3 bindings (cdylib "_ambers")
├── python/ambers/          Python package (PyPI: "ambers")
│   ├── __init__.py         Wraps native module, PyArrow → Polars
│   └── __init__.pyi        Type stubs
└── pyproject.toml          maturin build config
```

**Data flow:** Rust RecordBatch → `to_pyarrow(py)` (zero-copy Arrow FFI) → `pl.from_arrow()` → Polars DataFrame

### Build & Install

```bash
# Development build
uv venv --python 3.13 .venv
source .venv/Scripts/activate  # or .venv/bin/activate on Linux/Mac
uv pip install maturin polars pyarrow
maturin develop --release

# Verify
python -c "import ambers; df, meta = ambers.read_sav('file.sav'); print(df.shape)"
```

---

## Project Structure

```
ambers/                             Repo root (Cargo workspace)
  Cargo.toml                        Workspace definition
  pyproject.toml                    maturin build config
  CLAUDE.md                         This file
  python/
    ambers/
      __init__.py                   Python API wrapper
      __init__.pyi                  Type stubs
  crates/
    ambers/                         Pure Rust SPSS reader library
      Cargo.toml
      src/
        lib.rs                      Public API + re-exports
        main.rs                     CLI binary for testing
        scanner.rs                  SavScanner: streaming batch reader
        error.rs                    SpssError enum (thiserror)
        constants.rs                SYSMIS, enums (Compression, Measure, Alignment, VarType)
        io_utils.rs                 SavReader<R> with endian-aware reads
        header.rs                   176-byte file header parsing
        encoding.rs                 Code page -> encoding_rs mapping
        variable.rs                 Type 2 variable records, MissingValues enum
        value_labels.rs             Type 3+4 value label records
        document.rs                 Type 6 document records
        metadata.rs                 SpssMetadata struct, Value enum, MissingSpec, MrSet
        dictionary.rs               Record dispatch + post-dictionary resolution
        data.rs                     Row reading + string reassembly
        arrow_convert.rs            Arrow Schema + RecordBatch builders
        info_records/               Subtype dispatch (3,4,11,13,14,20,21,22)
        compression/
          bytecode.rs               Stateful bytecode decompressor
          zlib.rs                   ZSAV zheader/ztrailer + flate2
    ambers-py/                      PyO3 binding crate
      Cargo.toml
      src/
        lib.rs                      #[pymodule], #[pyclass] SpssMetadata, type conversions
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
| **Naming** | All metadata fields use `variable_` prefix. IndexMap for O(1) lookup with insertion-order preservation. |
| **Value sorting** | `Value` implements `Ord` — numeric values sort by actual number, not string representation. |

---

## Bugs Found & Lessons Learned

1. **Column count wrong (847 vs 677):** Very long string segment variables (e.g. SURVE0, SURVE1) weren't marked as ghosts. Fix: walk forward from each VLS variable and mark the next `n_segments - 1` non-ghost named records as ghosts.

2. **Row count wrong (87 vs 1500):** Bytecode decompressor was stateless — control block state lost between row calls. Fix: made `BytecodeDecompressor` stateful, preserving `pos`, `control_bytes`, `control_idx`, `eof` across `decompress_row()` calls.

3. **SYSMIS is not NaN:** SYSMIS is `-DBL_MAX` (most negative finite double), not a NaN. Test assertions needed to check `is_finite()` and `== -f64::MAX`.

---

## Testing

```bash
cargo test -p ambers      # 37 unit tests + 2 doc-tests
cargo run -p ambers -- file.sav  # CLI test with any .sav file
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

### Core Rust library (`crates/ambers`)

| Crate | Version | Purpose |
|-------|---------|---------|
| `arrow` | 57 | Arrow RecordBatch output |
| `flate2` | 1 | Zlib decompression for .zsav |
| `encoding_rs` | 0.8 | Character encoding conversion |
| `thiserror` | 2 | Error type derivation |
| `rayon` | 1 | Parallel row/column processing |
| `indexmap` | 2 | Insertion-ordered maps for metadata |

### Python bindings (`crates/ambers-py`)

| Crate | Version | Purpose |
|-------|---------|---------|
| `pyo3` | 0.26 | Python extension module |
| `arrow` | 57 (pyarrow feature) | Arrow ↔ PyArrow FFI conversion |

### Python dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `polars` | >=1.0 | DataFrame output |
| `pyarrow` | >=14.0 | Arrow FFI bridge |

---

## Next Steps

### Milestone 3: SAV/ZSAV Writer
- Write `RecordBatch` + `SpssMetadata` back to `.sav`/`.zsav` files
- Reverse the data flow: Arrow -> rows -> bytecode/zlib compression -> binary
- Update Python bindings with `write_sav()` function

### Future: scan_sav → Polars LazyFrame
- Implement `AnonymousScan` trait for Polars lazy evaluation
- Requires adding `polars` as Rust dependency in `ambers-py`
- Enables predicate pushdown and lazy column projection

### Future: Additional Format Support
- ambers is designed to grow beyond SPSS — the name and architecture support adding readers/writers for other statistical formats (Stata .dta, SAS .sas7bdat, etc.)

### Future Improvements
- Arrow temporal types for DATE, TIME, DATETIME formats (currently Float64)
- `columns` / `row_limit` params on Python `read_sav()`
- Real `.zsav` file testing
- PyPI publishing workflow (GitHub Actions with maturin)
