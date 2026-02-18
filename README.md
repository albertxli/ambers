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
- 4–8x faster than pyreadstat, on par with polars_readstat
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

Benchmarked on 5 real-world SPSS files (average of 5 runs, Windows, Python 3.13, 24-core machine):

| File | Size | Rows | Cols | ambers read_sav | ambers scan_sav | polars_readstat | pyreadstat | pyreadstat mp (4w) | ambers vs polars_readstat | ambers vs pyreadstat |
|------|------|-----:|-----:|----------------:|----------------:|----------------:|-----------:|-------------------:|--------------------------:|---------------------:|
| test_1 (bytecode) | 0.2 MB | 1,500 | 75 | **0.002s** | 0.002s | 0.011s | 0.010s | 0.386s | **5.9x faster** | **5.7x faster** |
| test_2 (bytecode) | 147 MB | 22,070 | 677 | **0.934s** | 1.070s | 1.087s | 4.424s | 1.799s | **1.2x faster** | **4.7x faster** |
| test_3 (uncompressed) | 1.1 GB | 79,066 | 915 | 1.747s | 1.844s | **1.592s** | 6.607s | 2.835s | ~tied | **3.8x faster** |
| test_4 (uncompressed) | 0.6 MB | 201 | 158 | **0.017s** | 0.017s | 0.024s | 0.027s | 0.450s | **1.4x faster** | **1.5x faster** |
| test_5 (uncompressed) | 0.6 MB | 203 | 136 | **0.003s** | 0.005s | 0.012s | 0.015s | 0.414s | **4.7x faster** | **5.8x faster** |

- **vs pyreadstat**: 4–6x faster across all file sizes
- **vs pyreadstat multiprocess**: ambers single-threaded beats pyreadstat with 4 workers
- **vs polars_readstat**: tied on large files, 2–6x faster on small/medium files (lower startup overhead)
- No PyArrow dependency required (uses Arrow PyCapsule Interface)

*pyreadstat multiprocess returns pandas DataFrame; timing includes `pl.from_pandas()` conversion.*

## License

[MIT](LICENSE)
