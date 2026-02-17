# Plan: Milestone 2 — PyO3 Python Bindings for ambers

## Context

ambers has a complete SPSS .sav/.zsav reader in pure Rust (Milestone 1). The user wants Python bindings as Milestone 2 so they can use `ambers` as a drop-in replacement for `pyreadstat` in their Polars data science pipeline. The SAV/ZSAV writer moves to Milestone 3.

**Goal:** `pip install ambers` → `df, meta = ambers.read_sav("file.sav")` returns a Polars DataFrame + metadata object.

---

## Target Python API

```python
import ambers

df, meta = ambers.read_sav("file.sav")              # Polars DataFrame + SpssMetadata
meta = ambers.read_sav_metadata("file.sav")          # metadata only (fast)

# SpssMetadata has all fields as properties
meta.variable_names       # list[str]
meta.variable_labels      # dict[str, str]
meta.variable_value_labels # dict[str, dict[float|str, str]]
meta.label("age")         # convenience method
```

---

## Architecture

```
Cargo workspace (same repo)
├── crates/ambers/        Pure Rust library (crates.io: "ambers")
├── crates/ambers-py/     Thin PyO3 glue (cdylib "_ambers")
├── python/ambers/        Python package (PyPI: "ambers")
└── pyproject.toml        maturin build config
```

**Data flow:** Rust RecordBatch → `to_pyarrow(py)` (zero-copy FFI) → `pl.from_arrow()` → Polars DataFrame

**Why workspace:** Core Rust crate stays pure (no pyo3 dep). Binding crate is thin glue. Can add `ambers-cli`, format sub-crates later without restructuring.

---

## Phase 1: Workspace Restructure

Move current flat layout into workspace. All existing code goes to `crates/ambers/`.

### Files

**`Cargo.toml` (root) — REPLACE:**
```toml
[workspace]
resolver = "3"
members = ["crates/ambers", "crates/ambers-py"]

[workspace.package]
version = "0.1.0"
edition = "2024"
license = "MIT"

[profile.release]
lto = "fat"
codegen-units = 1
opt-level = 3
```

**`crates/ambers/Cargo.toml` — CREATE:**
```toml
[package]
name = "ambers"
description = "Pure Rust reader for SPSS .sav and .zsav files"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
arrow = { version = "55", default-features = false, features = ["ffi"] }
flate2 = "1"
encoding_rs = "0.8"
thiserror = "2"
rayon = "1"

[dev-dependencies]
tempfile = "3"

[[bin]]
name = "ambers"
path = "src/main.rs"
```

**Steps:**
1. `mkdir -p crates/ambers`
2. `mv src/ crates/ambers/src/`
3. Replace root `Cargo.toml` with workspace definition
4. Create `crates/ambers/Cargo.toml`
5. `cargo test -p ambers` — all 37 tests must pass

---

## Phase 2: PyO3 Binding Crate

**`crates/ambers-py/Cargo.toml` — CREATE:**
```toml
[package]
name = "ambers-py"
description = "Python bindings for the ambers SPSS reader"
version.workspace = true
edition.workspace = true
license.workspace = true

[lib]
name = "_ambers"
crate-type = ["cdylib"]

[dependencies]
ambers = { path = "../ambers" }
pyo3 = { version = "0.24", features = ["extension-module"] }
arrow = { version = "55", default-features = false, features = ["pyarrow"] }
```

**`crates/ambers-py/src/lib.rs` — CREATE:**

Core binding layer (~200 lines):
- `#[pymodule] fn _ambers` — registers functions and classes
- `#[pyfunction] fn _read_sav(path) -> (PyArrow RecordBatch, PySpssMetadata)` — calls `ambers::read_sav()`, converts via `batch.to_pyarrow(py)`
- `#[pyfunction] fn _read_sav_metadata(path) -> PySpssMetadata`
- `#[pyclass(name="SpssMetadata", frozen)] struct PySpssMetadata` — wraps `ambers::SpssMetadata`, exposes all 18 fields as `#[getter]` properties
- Type conversion helpers:
  - `Value::Numeric(f64)` → Python `float`, `Value::String` → Python `str`
  - `MissingSpec` → Python `dict` with `{"type": "value"|"range"|"string_value", ...}`
  - `MrSet` → Python `dict` with `name, label, mr_type, counted_value, variables`
  - `Compression/Measure/Alignment` → Python `str` (via `as_str()` or match)
  - `HashMap<String, T>` → Python `dict`
- Error mapping: `SpssError` → `PyIOError`

Key imports from arrow 55:
```rust
use arrow::pyarrow::ToPyArrow;  // provides batch.to_pyarrow(py)
```
If trait name differs in arrow 55, check `IntoPyArrow` instead.

---

## Phase 3: Python Package

**`python/ambers/__init__.py` — CREATE:**
```python
"""ambers: Pure Rust SPSS .sav/.zsav reader."""
from ambers._ambers import SpssMetadata, _read_sav, _read_sav_metadata

__all__ = ["read_sav", "read_sav_metadata", "SpssMetadata"]

def read_sav(path: str) -> tuple:
    """Read SPSS file → (Polars DataFrame, SpssMetadata)."""
    import polars as pl
    batch, meta = _read_sav(str(path))
    df = pl.from_arrow(batch)
    return df, meta

def read_sav_metadata(path: str) -> SpssMetadata:
    """Read only metadata from an SPSS file (no data)."""
    return _read_sav_metadata(str(path))
```

**`python/ambers/__init__.pyi` — CREATE:** Type stubs for IDE support.

**`pyproject.toml` (root) — REPLACE:**
```toml
[build-system]
requires = ["maturin>=1.0,<2.0"]
build-backend = "maturin"

[project]
name = "ambers"
version = "0.1.0"
description = "Pure Rust SPSS .sav/.zsav reader with Polars DataFrame output"
requires-python = ">=3.9"
dependencies = ["polars>=1.0", "pyarrow>=14.0"]

[tool.maturin]
manifest-path = "crates/ambers-py/Cargo.toml"
module-name = "ambers._ambers"
python-source = "python"
features = ["pyo3/extension-module"]
```

---

## Phase 4: Build, Test, Update Docs

1. `maturin develop --release` — compile and install into venv
2. `python -c "import ambers; df, meta = ambers.read_sav('test.sav'); print(df.shape, meta)"` — smoke test
3. Compare output with `pyreadstat.read_sav()` for shape match
4. `cargo test -p ambers` — Rust tests still pass
5. Update CLAUDE.md — swap Milestone 2/3 ordering, add binding docs
6. Update `.gitignore` — add `__pycache__/`, `*.pyd`, `*.so`, `*.egg-info/`, `dist/`, `.venv/`

---

## Files Summary

| File | Action | Description |
|------|--------|---------|
| `Cargo.toml` (root) | Replace | Workspace definition |
| `crates/ambers/Cargo.toml` | Create | Pure Rust library crate config |
| `crates/ambers/src/*` | Move from `src/` | All existing Rust source |
| `crates/ambers-py/Cargo.toml` | Create | PyO3 cdylib crate config |
| `crates/ambers-py/src/lib.rs` | Create | #[pymodule] bindings (~200 lines) |
| `python/ambers/__init__.py` | Create | Python API wrapper |
| `python/ambers/__init__.pyi` | Create | Type stubs |
| `pyproject.toml` | Replace | maturin build config |
| `.gitignore` | Update | Add Python artifacts |
| `CLAUDE.md` | Update | Milestone reorder + binding docs |

## Not Included (Future)

- `scan_sav()` → Polars LazyFrame via `AnonymousScan` (requires polars Rust dep, separate follow-up)
- `columns` / `row_limit` params on Python `read_sav()` (trivial to add once scanner is wired up)
- PyPI publishing workflow (GitHub Actions with maturin)
