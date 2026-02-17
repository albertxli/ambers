# ambers — Architecture

## Project Identity

ambers is a **single Rust crate** published to both crates.io (Rust) and PyPI (Python). It follows the same model as [fastexcel](https://github.com/ToucanToco/fastexcel) — the Rust Excel reader that Polars adopted natively.

- **crates.io**: `ambers` — pure Rust SPSS reader, zero Python dependencies
- **PyPI**: `ambers` — Python package with rich metadata UX (summary, describe, diff)

## Structure

```
ambers/
├── Cargo.toml              Single crate: rlib (Rust) + cdylib (Python via maturin)
├── pyproject.toml           Python package config (maturin build backend)
├── src/
│   ├── lib.rs               Public API: read_sav, scan_sav, read_sav_metadata
│   ├── metadata.rs          SpssMetadata struct + convenience accessors
│   ├── scanner.rs           SavScanner: streaming reader with batching
│   ├── constants.rs         Enums: Compression, Measure, Alignment, FormatType
│   ├── error.rs             SpssError
│   ├── header.rs            176-byte file header parsing
│   ├── encoding.rs          Code page → encoding_rs mapping
│   ├── variable.rs          Type 2 variable records
│   ├── value_labels.rs      Type 3+4 value label records
│   ├── document.rs          Type 6 document records
│   ├── dictionary.rs        Record dispatch + post-dictionary resolution
│   ├── data.rs              Row reading: uncompressed, bytecode, zlib
│   ├── arrow_convert.rs     Arrow Schema + RecordBatch builders
│   ├── io_utils.rs          Endian-aware SavReader
│   ├── compression/         Bytecode + zlib decompression
│   ├── info_records/        Subtypes 3,4,11,13,14,20,21,22 + MR sets
│   └── python/              PyO3 bindings (behind #[cfg(feature = "python")])
│       └── mod.rs           PySpssMetadata, PyMetaDiff, summary/describe/diff
├── python/
│   └── ambers/
│       ├── __init__.py      read_sav → Polars DataFrame, exports
│       └── __init__.pyi     Type stubs for IDE autocomplete
└── tests/                   pytest test suite
```

## Feature Flags

```toml
[features]
default = []
python = ["dep:pyo3", "arrow/pyarrow"]   # Enables PyO3 bindings
```

- `cargo build` — pure Rust library (rlib), no Python dependency
- `maturin develop` — builds with `python` feature, produces cdylib for Python

## Public Rust API

```rust
// Read all data + metadata
pub fn read_sav(path) -> Result<(RecordBatch, SpssMetadata)>

// Read metadata only (fast, no data parsing)
pub fn read_sav_metadata(path) -> Result<SpssMetadata>

// Streaming reader with batching, column projection, row limits
pub fn scan_sav(path) -> Result<Scanner>

// Public types
pub struct SpssMetadata { ... }   // 22 metadata fields
pub struct Scanner { ... }        // SavScanner with next_batch()
pub enum Value { Numeric(f64), String(String) }
pub enum MissingSpec { Value(f64), Range { lo, hi }, StringValue(String) }
pub struct MrSet { ... }
pub enum Measure { Unknown, Nominal, Ordinal, Scale }
pub enum Alignment { Left, Right, Center }
pub enum Compression { None, Bytecode, Zlib }
```

Internal modules (header, dictionary, data, compression, etc.) are `pub(crate)` — not part of the public API.

## Python API

```python
import ambers

# Read data + metadata → Polars DataFrame
df, meta = ambers.read_sav("file.sav")

# Read metadata only
meta = ambers.read_sav_metadata("file.sav")

# Metadata inspection
meta.summary()              # Rich formatted overview
meta.describe("S2")         # Single variable deep-dive
meta.describe(["S2", "Q1"]) # Multiple variables

# Quick lookups (raise KeyError for unknown variables)
meta.label("S2")            # Variable label
meta.value("S2")            # Value labels dict
meta.format("S2")           # SPSS format string (e.g. "F8.2")
meta.measure("S2")          # Measurement level

# Full metadata as dict
meta.schema                 # Nested dict with all 20 fields

# Metadata comparison
result = meta1.diff(meta2)
result.is_match             # bool
result.variable_labels      # list of diffs
result.variables_only_in_self  # variables missing from other
```

## Layer Separation

| Layer | What it does | Audience |
|-------|-------------|----------|
| `src/` (core) | Parse SPSS binary → Arrow RecordBatch + SpssMetadata | Rust ecosystem (Polars, DataFusion, CLI) |
| `src/python/` | Rich Python UX: summary, describe, diff, formatting | Python data scientists (marimo, Jupyter) |
| `python/ambers/` | Arrow → Polars conversion, exports | Python package entry point |

The Rust core exposes **data**. The Python layer adds **presentation**.

## Polars Integration Path

With `ambers` on crates.io, Polars can depend on it directly:

```toml
# In Polars' Cargo.toml
ambers = "0.1"  # No python feature needed
```

They'd get `ambers::read_sav() -> RecordBatch` — pure Rust, zero Python dependency. Same pattern they use with fastexcel for Excel files.

## Dependencies

### Rust core (always)
| Crate | Purpose |
|-------|---------|
| `arrow` 57 | Arrow RecordBatch output |
| `flate2` 1 | Zlib decompression (.zsav) |
| `encoding_rs` 0.8 | Character encoding conversion |
| `thiserror` 2 | Error type derivation |
| `rayon` 1 | Parallel data processing |
| `indexmap` 2 | Ordered HashMaps for metadata |

### Python feature (optional)
| Crate | Purpose |
|-------|---------|
| `pyo3` 0.26 | Python FFI bindings |
| `arrow/pyarrow` | Arrow ↔ PyArrow conversion |

### Python package (PyPI)
| Package | Purpose |
|---------|---------|
| `polars` >=1.0 | DataFrame output |
| `pyarrow` >=14.0 | Arrow interop |
