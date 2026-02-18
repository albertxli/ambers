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
- 2–3x faster than pyreadstat
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

# Read data + metadata
df, meta = am.read_sav("survey.sav")

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

| File | Size | Rows | Cols | ambers | pyreadstat | Speedup |
|------|------|-----:|-----:|-------:|-----------:|--------:|
| survey_medium.sav | 147 MB | ~22,000 | ~700 | 1.27s | 3.07s | **2.4x** |
| survey_large.sav | 1.1 GB | ~80,000 | ~900 | 2.42s | 6.40s | **2.6x** |

## License

[MIT](LICENSE)
