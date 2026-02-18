# ambers

<p align="center">
  <img src="https://raw.githubusercontent.com/albertxli/ambers/main/images/ambers-banner-v2.svg" alt="ambers banner" width="900">
</p>

[![Crates.io](https://img.shields.io/crates/v/ambers)](https://crates.io/crates/ambers)
[![PyPI](https://img.shields.io/pypi/v/ambers?color=blue)](https://pypi.org/project/ambers/)
[![License: MIT](https://img.shields.io/badge/license-MIT-grey.svg)](LICENSE)

Pure Rust SPSS `.sav`/`.zsav` reader — Arrow-native, zero C dependencies.

## Features

- Read `.sav` (bytecode) and `.zsav` (zlib) files
- Arrow `RecordBatch` output — zero-copy to Polars, DataFusion, DuckDB
- Rich metadata: variable labels, value labels, missing values, MR sets, measure levels
- Lazy reader via `scan_sav()` — returns Polars LazyFrame with projection and row limit pushdown
- No PyArrow dependency — uses Arrow PyCapsule Interface for zero-copy transfer
- One of the fastest SPSS readers — up to 2.5x faster than polars_readstat, 5–10x faster than pyreadstat
- Python + Rust dual API from a single crate

## Installation

**Python:**

```bash
pip install ambers
```

**Rust:**

```bash
cargo add ambers
```

## Quick Start

### Python

```python
import ambers as am

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
```

### Rust

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

All results return a Polars DataFrame. Average of 5 runs on Windows 11, Python 3.13, 24-core machine.

| File | Size | Rows | Cols | ambers | polars_readstat | ambers vs prs | pyreadstat | pyreadstat mp (4w) | ambers vs pyreadstat |
|------|------|-----:|-----:|-------:|----------------:|--------------:|-----------:|-------------------:|---------------------:|
| test_1 (bytecode) | 0.2 MB | 1,500 | 75 | **0.002s** | 0.004s | **2.0x faster** | 0.010s | 0.493s | **5.0x faster** |
| test_2 (bytecode) | 147 MB | 22,070 | 677 | **0.812s** | 0.991s | **1.2x faster** | 3.564s | 1.781s | **4.4x faster** |
| test_3 (uncompressed) | 1.1 GB | 79,066 | 915 | **0.509s** | 1.279s | **2.5x faster** | 4.849s | 2.764s | **9.5x faster** |
| test_4 (uncompressed) | 0.6 MB | 201 | 158 | **0.002s** | 0.004s | **2.0x faster** | 0.018s | 0.470s | **9.0x faster** |
| test_5 (uncompressed) | 0.6 MB | 203 | 136 | **0.002s** | 0.004s | **2.0x faster** | 0.015s | 0.454s | **7.5x faster** |
| test_6 (uncompressed) | 5.4 GB | 395,330 | 916 | **2.801s** | 1.809s | 1.5x slower | 24.199s | 11.718s | **8.6x faster** |

- **vs polars_readstat**: faster on 5 of 6 files — 1.2–2.5x faster (test_6 at 5.4 GB is 1.5x slower)
- **vs pyreadstat**: 4–10x faster across all file sizes
- **vs pyreadstat multiprocess (4 workers)**: ambers single-threaded still faster on every file
- No PyArrow dependency — uses Arrow PyCapsule Interface for zero-copy transfer

*pyreadstat multiprocess returns pandas; timing includes `pl.from_pandas()` conversion.*

### Lazy Read with Pushdown

`scan_sav()` returns a Polars LazyFrame. Unlike eager reads, it only reads the data you ask for:

| File (size) | Full collect | Select 5 cols | Head 1000 rows | Select 5 + head 1000 |
|-------------|------------:|-------------:|--------------:|--------------------:|
| test_2 (147 MB, 22K × 677) | 0.903s | 0.363s (2.5x) | 0.181s (5.0x) | **0.157s (5.7x)** |
| test_3 (1.1 GB, 79K × 915) | 0.700s | 0.554s (1.3x) | 0.020s (35x) | **0.012s (58x)** |
| test_6 (5.4 GB, 395K × 916) | 3.062s | 2.343s (1.3x) | 0.022s (139x) | **0.013s (236x)** |

On the 5.4 GB file, selecting 5 columns and 1000 rows completes in **13ms** — 236x faster than reading the full dataset.

## License

[MIT](LICENSE)
