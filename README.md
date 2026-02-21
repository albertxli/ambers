# ambers

<p align="center">
  <img src="https://raw.githubusercontent.com/albertxli/ambers/main/images/ambers-banner-v2.svg" alt="ambers banner" width="900">
</p>

[![Crates.io](https://img.shields.io/crates/v/ambers)](https://crates.io/crates/ambers)
[![PyPI](https://img.shields.io/pypi/v/ambers?color=blue)](https://pypi.org/project/ambers/)
[![License: MIT](https://img.shields.io/badge/license-MIT-grey.svg)](LICENSE)

Pure Rust SPSS `.sav`/`.zsav` reader and writer — Arrow-native, zero C dependencies.

## Features

- Blazing fast read and write for SPSS `.sav` (bytecode) and `.zsav` (zlib) files
- Rich metadata: variable labels, value labels, missing values, MR sets, measure levels, and more
- Lazy reader via `scan_sav()` — Polars LazyFrame with projection and row limit pushdown
- Pure Rust with a native Python API — native Arrow integration, no C dependencies
- Benchmarked up to 3–10x faster reads and 4–20x faster writes compared to current popular SPSS I/O libraries

## Installation

**Python:**

```bash
uv add ambers
```

**Rust:**

```bash
cargo add ambers
```

## Python

```python
import ambers as am
import polars as pl

# Eager read — data + metadata
df, meta = am.read_sav("survey.sav")

# Lazy read — returns Polars LazyFrame
lf, meta = am.scan_sav("survey.sav")
df = lf.select(["Q1", "Q2", "age"]).head(1000).collect()

# Explore metadata
meta.summary()
meta.describe("Q1")
meta.value("Q1")

# Read metadata only (fast, skips data)
meta = am.read_sav_metadata("survey.sav")

# Write back — roundtrip with full metadata
df = df.filter(pl.col("age") > 18)
am.write_sav(df, "filtered.sav", meta=meta)

# Write as .zsav (zlib compressed)
am.write_sav(df, "compressed.zsav", meta=meta)

# From scratch — metadata is optional, inferred from DataFrame schema
am.write_sav(df, "new.sav")
```

Use `.sav` for bytecode compression (default), `.zsav` for zlib compression. Pass `meta=` to preserve all metadata from a prior `read_sav()`, or omit it to infer formats from the DataFrame. Individual writable fields (e.g., `variable_labels`, `variable_value_labels`) can also be passed directly as keyword arguments for fine-grained control.

## Rust

```rust
use ambers::{read_sav, read_sav_metadata};

// Read data + metadata
let (batch, meta) = read_sav("survey.sav")?;
println!("{} rows, {} cols", batch.num_rows(), meta.number_columns);

// Read metadata only
let meta = read_sav_metadata("survey.sav")?;
println!("{}", meta.label("Q1").unwrap_or("(no label)"));
```

## Metadata API (Python)

| Method | Description |
|--------|-------------|
| `meta.summary()` | Formatted overview: file info, type distribution, annotations |
| `meta.describe("Q1")` | Deep-dive into a single variable (or list of variables) |
| `meta.diff(other)` | Compare two metadata objects, returns `MetaDiff` |
| `meta.label("Q1")` | Variable label |
| `meta.value("Q1")` | Value labels dict |
| `meta.format("Q1")` | SPSS format string (e.g. `"F8.2"`, `"A50"`) |
| `meta.measure("Q1")` | Measurement level (`"nominal"`, `"ordinal"`, `"scale"`) |
| `meta.schema` | Full metadata as a nested Python dict |

All variable-name methods raise `KeyError` for unknown variables.

### Metadata Fields

All fields returned by the reader. Fields marked **Write** are preserved when passed via `meta=` to `write_sav()`. Read-only fields are set automatically (encoding, timestamps, row/column counts, etc.).

> **Note:** This is a first pass — field names and behavior may change without warning in future releases.

| Field | Read | Write | Type |
|-------|:----:|:-----:|------|
| `variable_names` | yes | yes | `list[str]` |
| `variable_labels` | yes | yes | `dict[str, str]` |
| `variable_value_labels` | yes | yes | `dict[str, dict[float\|str, str]]` |
| `variable_measure` | yes | yes | `dict[str, str]` |
| `variable_alignment` | yes | yes | `dict[str, str]` |
| `variable_display_width` | yes | yes | `dict[str, int]` |
| `variable_storage_width` | yes | yes | `dict[str, int]` |
| `variable_missing_values` | yes | yes | `dict[str, list[dict]]` |
| `variable_format` | yes | yes | `dict[str, str]` |
| `arrow_data_types` | yes | — | `dict[str, str]` |
| `weight_variable` | yes | yes | `str \| None` |
| `mr_sets` | yes | yes | `dict[str, dict]` |
| `file_label` | yes | yes | `str` |
| `file_format` | yes | — | `str` |
| `file_encoding` | yes | — | `str` |
| `creation_time` | yes | — | `str` |
| `modification_time` | yes | — | `str` |
| `number_rows` | yes | — | `int \| None` |
| `number_columns` | yes | — | `int` |
| `compression` | yes | — | `str` |
| `notes` | yes | yes | `list[str]` |

## Streaming Reader (Rust)

```rust
let mut scanner = ambers::scan_sav("survey.sav")?;
scanner.select(&["age", "gender"])?;
scanner.limit(1000);

while let Some(batch) = scanner.next_batch()? {
    println!("Batch: {} rows", batch.num_rows());
}
```

## Performance

### Eager Read

All results return a Polars DataFrame. Best of 3–5 runs (with warmup) on Windows 11, Python 3.13, 24-core machine.

| File | Size | Rows | Cols | ambers | polars_readstat | pyreadstat | vs prs | vs pyreadstat |
|------|------|-----:|-----:|-------:|----------------:|-----------:|-------:|--------------:|
| test_1 (bytecode) | 0.2 MB | 1,500 | 75 | < 0.01s | < 0.01s | 0.011s | — | — |
| test_2 (bytecode) | 147 MB | 22,070 | 677 | **0.286s** | 0.897s | 3.524s | **3.1x** | **12x** |
| test_3 (uncompressed) | 1.1 GB | 79,066 | 915 | **0.322s** | 1.150s | 4.918s | **3.6x** | **15x** |
| test_4 (uncompressed) | 0.6 MB | 201 | 158 | **0.002s** | 0.003s | 0.012s | **1.5x** | **6x** |
| test_5 (uncompressed) | 0.6 MB | 203 | 136 | **0.002s** | 0.003s | 0.016s | **1.5x** | **8x** |
| test_6 (uncompressed) | 5.4 GB | 395,330 | 916 | **1.600s** | 1.752s | 25.214s | **1.1x** | **16x** |

- **Faster than polars_readstat on all tested files** — 1.1–3.6x faster
- **6–16x faster than pyreadstat** across all file sizes
- No PyArrow dependency — uses Arrow PyCapsule Interface for zero-copy transfer

### Lazy Read with Pushdown

`scan_sav()` returns a Polars LazyFrame. Unlike eager reads, it only reads the data you ask for:

| File (size) | Full collect | Select 5 cols | Head 1000 rows | Select 5 + head 1000 |
|-------------|------------:|-------------:|--------------:|--------------------:|
| test_2 (147 MB, 22K × 677) | 0.903s | 0.363s (2.5x) | 0.181s (5.0x) | **0.157s (5.7x)** |
| test_3 (1.1 GB, 79K × 915) | 0.700s | 0.554s (1.3x) | 0.020s (35x) | **0.012s (58x)** |
| test_6 (5.4 GB, 395K × 916) | 3.062s | 2.343s (1.3x) | 0.022s (139x) | **0.013s (236x)** |

On the 5.4 GB file, selecting 5 columns and 1000 rows completes in **13ms** — 236x faster than reading the full dataset.

### Write

`write_sav()` writes a Polars DataFrame + metadata back to `.sav` (bytecode) or `.zsav` (zlib). Best of 5 runs on the same machine.

| File | Size | Rows | Cols | Mode | ambers | pyreadstat | Speedup |
|------|------|-----:|-----:|------|-------:|-----------:|--------:|
| test_1 (bytecode) | 0.2 MB | 1,500 | 75 | .sav | **0.001s** | 0.019s | **13x** |
| | | | | .zsav | **0.004s** | 0.026s | **7x** |
| test_2 (bytecode) | 147 MB | 22,070 | 677 | .sav | **0.567s** | 3.849s | **7x** |
| | | | | .zsav | **1.088s** | 4.415s | **4x** |
| test_3 (uncompressed) | 1.1 GB | 79,066 | 915 | .sav | **0.950s** | 16.152s | **17x** |
| | | | | .zsav | **1.774s** | 17.362s | **10x** |
| test_6 (uncompressed) | 5.4 GB | 395,330 | 916 | .sav | **5.700s** | 79.999s | **14x** |
| | | | | .zsav | **8.193s** | 85.491s | **10x** |

- **4–20x faster than pyreadstat** on writes across all files and compression modes
- Full metadata roundtrip: variable labels, value labels, missing values, MR sets, display properties
- Bytecode (.sav) and zlib (.zsav) compression

## Roadmap

- Continued I/O performance optimization
- Expanded SPSS metadata field coverage
- Rich metadata manipulation — add, update, merge, and remove metadata programmatically
- Individual metadata field overrides in `write_sav()` — pass `variable_labels=`, `variable_value_labels=`, etc. alongside `meta=` to selectively override fields
- Currently supports read and write with Polars DataFrames (eager and lazy) — extending to pandas, Narwhals, DuckDB, and others

## License

[MIT](LICENSE)
