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

**Command line:**

The `ambers` command needs no Python and no Rust. Download the archive for your platform from the [latest release](https://github.com/albertxli/ambers/releases/latest) (`ambers-<version>-x86_64-pc-windows-msvc.zip`, `-x86_64-unknown-linux-musl.tar.gz` or `-aarch64-apple-darwin.tar.gz`), unpack it, and run the binary inside.

If you already have the Python package, the same command is installed with it (`uv tool install ambers` puts it on your PATH).

`am` is a short alias for `ambers`; both are installed.

```bash
am info survey.sav                     # summary, then the first 10 and last 5 variables
am info survey.sav --head 20           # only the first 20 variables
am info survey.sav --head 5 --tail 5   # the first 5 and the last 5
am info survey.sav --counts            # read the data: adds N and Missing per variable
am info survey.sav --rows 10           # also the first 10 data rows
```

```text
╭─────────────┬─────────────────────────────────╮
│ File        │ survey.sav                      │
│ Label       │ ambers synthetic survey fixture │
│ Shape       │ 20 rows × 13 columns            │
│ Size        │ 3.6 KB (3,640 bytes)            │
│ Compression │ bytecode                        │
│ Encoding    │ UTF-8                           │
│ Created     │ 2026-11-11 23:00:15             │
│ Status      │ ok                              │
╰─────────────┴─────────────────────────────────╯
┌───┬────────┬─────────────┬───────────────┬────┬─────────┐
│ # ┆ Name   ┆ Type        ┆ Label         ┆  N ┆ Missing │
╞═══╪════════╪═════════════╪═══════════════╪════╪═════════╡
│ 1 ┆ id     ┆ numeric     ┆ Respondent ID ┆ 20 ┆       0 │
│ 2 ┆ gender ┆ categorical ┆ Gender        ┆ 20 ┆       0 │
└───┴────────┴─────────────┴───────────────┴────┴─────────┘
```

**Type** is the codebook's: `categorical`, `numeric`, `text` or `date`. Without `--counts`, `info` reads only the file's dictionary, so it is instant on any file, and the type comes from the metadata alone (an unlabelled variable holding only 0 and 1 shows as `numeric`). With `--counts` it reads the data once for the listed variables and adds **N** (valid cases) and **Missing**, with the exact type; that took 1.6 seconds on a 5.4 GB file with 916 variables.

In a terminal the summary's border is green when the file read cleanly, yellow when the reader recorded [warnings](https://github.com/albertxli/ambers/blob/main/docs/warnings_and_errors.md), and red when the file could not be read; the `Status` row says the same in text, so nothing is lost when the output is piped. Variable names are always shown in full; long labels keep their beginning and end.

### Browse the data: `am view`

```bash
am view survey.sav                  # opens on value labels
am view survey.sav --mode codes     # opens on stored codes
am view big.sav --limit 200000      # load at most 200,000 rows
am view big.sav --memory 8          # load at most 8 GB of data
am view survey.sav --theme terminal # use your terminal's colours (default: Dracula)
```

A full-screen grid in the style of SPSS's Data View. The first screen appears at once and the rest of the file loads in the background.

Loading stops by itself when the data would take more than half of the memory your machine has free at that moment (or the `--memory` you give), and the border says how many rows were loaded. The rows already loaded stay usable.

| Key | Action |
|---|---|
| arrows, `h` `j` `k` `l` | move one cell |
| mouse | the wheel scrolls up and down; Shift+wheel, Alt+wheel or a tilt wheel scrolls sideways; a click selects the cell under it |
| PgUp / PgDn, Ctrl+u / Ctrl+d | a screen, half a screen |
| Shift+arrows, or `{` `}` `<` `>` | 50 rows or columns at once |
| Ctrl+↑ / Ctrl+↓, or `g` / `G` | first / last loaded row |
| Ctrl+← / Ctrl+→, or `0` / `$`, or Home / End | far left / far right |
| digits | go to that row as you type; Enter keeps it, Esc goes back |
| Tab | switch what cells show: **Labels**, **Codes**, **Both** (`1  Male`) |
| Enter | side panel with the current respondent's answers: variable, code and label, variable label |
| `f` | freeze / unfreeze the first variable while scrolling sideways |
| `y` | copy the current cell to the clipboard |
| `L` | copy the current variable's full label |
| `i` | Variable View and back |
| `?` | list of keys |
| `q`, Esc | close the panel or view; quit |

In Variable View (`i`), one row per variable with its type, label, labelled values, valid and missing counts:

| Key | Action |
|---|---|
| arrows, click | move the highlighted cell; the same fast and edge keys work here, and the list scrolls sideways while `#` and Name stay put |
| Enter | side panel with the variable's values: full label, format, and every value with its count and share |
| `J` / `K`, wheel over the panel | scroll the value table |
| `y` | copy the highlighted cell in full (the whole label, the whole value list); with the value table open, copy the table |
| `Y` | copy the highlighted cell even when the value table is open |
| `L` | copy the variable's full label, whatever is highlighted |
| `d` | go to this variable in Data View |

Under the list, a bar shows the highlighted cell's text in full, wrapped: the whole label or the whole value list, which the list itself has to cut.

The letter forms of the fast and edge keys are there because some terminals do not pass Shift or Ctrl together with the arrow keys.

While the viewer is open it captures the mouse, so selecting text with the mouse needs Shift held down (in most terminals).

Files with incomplete labels are shown as they are, and the gaps are made visible:

- A value with no label, in a variable that has labels, shows the value itself in the warning colour.
- When two codes share a label, Labels mode shows `Other [97]` and `Other [98]`.
- Values declared user-missing are dimmed; system-missing is `.`.
- The border reports how many variables hold unlabelled values, and Variable View has a **Label check** column per variable (`ok`, `2 unlabelled: 7, 8`, `duplicate: Other (97, 98)`). These are the same checks as `ambers.validate()`, run on the rows loaded so far.

Variable View describes each variable the way `ambers.codebook()` does, with the same rules and the same numbers:

- **Type** is `categorical`, `numeric`, `text` or `date`. A numeric variable is categorical when it has value labels, belongs to a multiple-response set, or holds only 0 and 1; a string is `text` even with value labels.
- **N** is the valid cases and **Missing** the system-missing ones (for strings, empty text too). Values declared user-missing count as valid.
- The value table of a categorical variable lists every code: labelled codes that never occur (count 0), codes in the data without a label, and the system-missing row.
- For a text, numeric or date variable the value table lists every distinct value with its count, the missing row first, then text from A to Z (symbols and digits before letters, ignoring case) and numbers and dates in ascending order. This goes beyond `ambers.codebook()`, which has one summary row for these types.
- Counts cover the rows loaded so far and update while the file loads; the border says how many rows that is.

A copied value table is tab-separated with a header row (`Code`, `Label`, `N`, `% valid`, `% total`), so pasting into a spreadsheet fills one column per field. Copying goes through the system clipboard (`clip` on Windows, `pbcopy` on macOS, `wl-copy` / `xclip` / `xsel` on Linux); if none is available, the terminal's clipboard escape sequence is used, which some terminals ignore.

## Python

```python
import ambers as am
import polars as pl

# Eager read — returns SavFile with .data and .meta
sav = am.read_sav("survey.sav")
df, meta = sav.data, sav.meta

# Lazy read — .data is a Polars LazyFrame
sav = am.scan_sav("survey.sav")
lf, meta = sav.data, sav.meta
df = lf.select(["Q1", "Q2", "age"]).head(1000).collect()

# Explore metadata
meta.summary()
meta.describe("Q1")
meta.value("Q1")

# Read metadata only (fast, skips data)
meta = am.read_sav_meta("survey.sav")

# Write back — roundtrip with full metadata
sav = am.read_sav("input.sav")
df, meta = sav.data, sav.meta
df = df.filter(pl.col("age") > 18)
am.write_sav(df, "filtered.sav", meta=meta)                        # bytecode (default for .sav)
am.write_sav(df, "compressed.zsav", meta=meta)                     # zlib (default for .zsav)
am.write_sav(df, "raw.sav", meta=meta, compression="uncompressed") # no compression
am.write_sav(df, "fast.zsav", meta=meta, compression_level=1)      # fast zlib

# From scratch — metadata is optional, inferred from DataFrame schema
am.write_sav(df, "new.sav")

# Apply value labels — replace codes with labels for export/analysis
df, meta = sav.data, sav.meta
labeled = am.apply_labels(df, meta)                          # Enum dtype (ordered, strict)
labeled.write_excel("survey.xlsx")                            # Enum auto-casts to String
labeled = am.apply_labels(df, meta, output="string")          # String dtype for export
labeled = am.apply_labels(df, meta, output="enum_null")       # Enum, unmapped → null
labeled = am.apply_labels(df, meta, exclude=["weight", "id"])  # skip specific columns

# Apply missing values — nullify SPSS user-defined missing codes
clean = am.apply_missing(df, meta)                             # all columns with specs
clean = am.apply_missing(df, meta, columns=["Q1", "Q2"])       # specific columns only
clean = am.apply_missing(df, meta, exclude=["age"])            # skip specific columns

# Validate — check value label quality before analysis
report = am.validate(df, meta)
print(report)                                                   # box-drawing summary
report.is_valid                                                 # True if no errors
report.raise_if_invalid()                                       # raise if errors found
report.to_frame()                                               # DataFrame for export
```

`.sav` uses bytecode compression by default, `.zsav` uses zlib. Pass `compression=` to override (`"uncompressed"`, `"bytecode"`, `"zlib"`). Pass `meta=` to preserve all metadata from a prior `read_sav()`, or omit it to infer formats from the DataFrame.

### SavFile

`read_sav()` and `scan_sav()` return a `SavFile` object with file-level metadata alongside the data:

```
>>> sav = am.read_sav("survey_2025.sav")
>>> sav
┌─ SavFile ──────────────────────────┐
│ Data        DataFrame (polars)     │
│ Shape       22,070 rows x 677 cols │
│ Source      survey_2025.sav        │
│ File size   146.5 MB, bytecode     │
│ Read time   0.286s                 │
└────────────────────────────────────┘
```

| Attribute | Type | Description |
|-----------|------|-------------|
| `sav.data` | `DataFrame` or `LazyFrame` | The data (eager from `read_sav`, lazy from `scan_sav`) |
| `sav.meta` | `SpssMetadata` | All variable metadata (labels, formats, value labels, etc.) |
| `sav.source` | `str \| None` | Source file path |
| `sav.shape` | `tuple[int, int] \| None` | `(n_rows, n_cols)` |
| `sav.file_size` | `int \| None` | File size in bytes |
| `sav.read_time` | `float \| None` | Wall-clock read time in seconds |
| `sav.compression` | `str` | `"uncompressed"`, `"bytecode"`, or `"zlib"` |
| `sav.warnings` | `list[str]` | File-damage findings; empty for a healthy file |

For `scan_sav()`, `read_time` measures metadata/schema reading only (not lazy collection).

**Damaged files.** ambers reads a file exactly as stored and never silently "corrects" values.
If the header disagrees with the contents (declared row count or row width vs what the file
holds, a non-standard compression bias, date/time values outside years 1-9999 which become
null), it raises a `CorruptFileWarning` naming the findings and lists them in
`sav.warnings` / `meta.warnings`. Treat such a file with suspicion and re-export it from the
source. Structures that cannot be read at all (duplicate variable names, an impossible
variable type, a record larger than the file) raise an `OSError` immediately. Silence the
warning with `warnings.filterwarnings("ignore", category=ambers.CorruptFileWarning)`.

### apply_labels

Replace numeric/string codes with their SPSS value labels. By default produces Polars `Enum` columns that preserve SPSS definition order — crucial for Likert scales and survey analysis.

```python
sav = am.read_sav("survey.sav")
df, meta = sav.data, sav.meta

# Default: Enum output, strict validation
labeled = am.apply_labels(df, meta)
labeled.group_by("satisfaction").agg(pl.len())  # sorted by definition order
labeled.write_excel("survey.xlsx")              # Enum auto-casts to String

# String output for quick export
labeled = am.apply_labels(df, meta, output="string")

# Enum output with unmapped values as null
labeled = am.apply_labels(df, meta, output="enum_null")
```

| `output=` | Dtype | Unmapped values | Best for |
|-----------|-------|-----------------|----------|
| `"enum"` (default) | `pl.Enum` (ordered) | Error | Analysis — strict, validated categories |
| `"string"` | `pl.String` | Stringify (`3.0` → `"3"`) | Export — readable text for Excel/CSV |
| `"enum_null"` | `pl.Enum` (ordered) | Null | Analysis — exclude unknowns from base |

Numeric columns without value labels are skipped. String columns always pass through unmapped text. See [apply_labels.md](https://github.com/albertxli/ambers/blob/main/docs/apply_labels.md) for full documentation.

### validate

Check value label quality before analysis — catch unlabeled values and duplicate labels upfront.

```python
sav = am.read_sav("survey.sav")
df, meta = sav.data, sav.meta

report = am.validate(df, meta)
print(report)           # box-drawing summary
report.is_valid         # True if no errors (warnings OK)
report.raise_if_invalid()  # raise ValueError if errors

# Programmatic access
for error in report.errors:
    print(f"{error.column}: {error.details['unlabeled_values']}")

# Export as DataFrame
report.to_frame().write_csv("validation_issues.csv")
```

See [validate.md](https://github.com/albertxli/ambers/blob/main/docs/validate.md) for full documentation.

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
| `meta.role("Q1")` | Variable role (`"input"`, `"target"`, `"both"`, `"none"`, `"partition"`, `"split"`) |
| `meta.attribute("Q1", "CustomNote")` | Custom attribute values (`list[str]` or `None`) |
| `meta.schema` | Full metadata as a nested Python dict |

All variable-name methods raise `KeyError` for unknown variables.

### Metadata Fields

All fields returned by the reader. Fields marked **Write** are preserved when passed via `meta=` to `write_sav()`. Read-only fields are set automatically (encoding, timestamps, row/column counts, etc.).

> **Note:** This is a first pass — field names and behavior may change without warning in future releases.

| Field | Read | Write | Type |
|-------|:----:|:-----:|------|
| `file_label` | yes | yes | `str` |
| `file_format` | yes | — | `str` |
| `file_encoding` | yes | — | `str` |
| `creation_time` | yes | — | `str` |
| `compression` | yes | — | `str` |
| `number_columns` | yes | — | `int` |
| `number_rows` | yes | — | `int \| None` |
| `weight_variable` | yes | yes | `str \| None` |
| `notes` | yes | yes | `list[str]` |
| `warnings` | yes | — | `list[str]` |
| `variable_names` | yes | — | `list[str]` |
| `variable_labels` | yes | yes | `dict[str, str]` |
| `variable_value_labels` | yes | yes | `dict[str, dict[float\|str, str]]` |
| `variable_formats` | yes | yes | `dict[str, str]` |
| `variable_measures` | yes | yes | `dict[str, str]` |
| `variable_alignments` | yes | yes | `dict[str, str]` |
| `variable_storage_widths` | yes | — | `dict[str, int]` |
| `variable_display_widths` | yes | yes | `dict[str, int]` |
| `variable_roles` | yes | yes | `dict[str, str]` |
| `variable_missing_values` | yes | yes | `dict[str, dict]` |
| `variable_attributes` | yes | yes | `dict[str, dict[str, list[str]]]` |
| `mr_sets` | yes | yes | `dict[str, dict]` |
| `arrow_data_types` | yes | — | `dict[str, str]` |

**Creating metadata from scratch:**

```python
meta = am.SpssMetadata(
    file_label="Customer Survey 2026",
    variable_labels={"Q1": "Satisfaction", "Q2": "Loyalty"},
    variable_value_labels={"Q1": {1: "Low", 5: "High"}},
    variable_measures={"Q1": "ordinal", "Q2": "nominal"},
)
am.write_sav(df, "output.sav", meta=meta)
```

**Modifying existing metadata** (from `read_sav()` or a previously created `SpssMetadata`):

```python
# .update() — bulk update multiple fields at once, merges dicts, replaces scalars
meta2 = meta.update(
    file_label="Updated Survey",
    variable_labels={"Q3": "NPS"},        # Q1/Q2 labels preserved, Q3 added
    variable_measures={"Q3": "scale"},
)

# .with_*() — chainable single-field setters, with full IDE autocomplete and type hints
meta3 = (meta
    .with_file_label("Updated Survey")
    .with_variable_labels({"Q3": "NPS"})
    .with_variable_measures({"Q3": "scale"})
)
```

> **Immutability:** `SpssMetadata` is immutable. `.update()` and `.with_*()` always return a **new** instance — the original is never modified. Assign to a new variable if you need to keep both copies.

**Update logic:**
- **Dict fields** (labels, formats, measures, etc.) merge as an overlay — new keys are added, existing keys are overwritten, all other keys are preserved. Pass `{key: None}` to remove a key.
- **Scalar fields** (`file_label`, `weight_variable`) and **`notes`** are replaced entirely.
- **Column renames are not tracked.** If you rename `"Q1"` to `"Q1a"` in your DataFrame, metadata for `"Q1"` does not carry over — you must explicitly provide metadata for `"Q1a"`.

See [metadata.md](https://github.com/albertxli/ambers/blob/main/docs/metadata.md) for the full API reference including update logic details, missing values, MR sets, and validation rules.

> **SPSS tip:** Custom variable attributes are not shown in SPSS's Variable View by default. Go to **View > Customize Variable View** and click **OK**, or run `DISPLAY ATTRIBUTES` in SPSS syntax.

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

All results return a Polars DataFrame. Best of 3–5 runs (with warmup) on Windows 11, Python 3.13, Intel Core Ultra 9 275HX (24C), 64 GB RAM (6400 MT/s).

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
| | | | | .zsav | **0.004s** | 0.025s | **6x** |
| test_2 (bytecode) | 147 MB | 22,070 | 677 | .sav | **0.539s** | 3.622s | **7x** |
| | | | | .zsav | **0.386s** | 4.174s | **11x** |
| test_3 (uncompressed) | 1.1 GB | 79,066 | 915 | .sav | **0.439s** | 13.963s | **32x** |
| | | | | .zsav | **0.436s** | 17.991s | **41x** |
| test_4 (uncompressed) | 0.6 MB | 201 | 158 | .sav | **0.002s** | 0.027s | **16x** |
| | | | | .zsav | **0.004s** | 0.035s | **9x** |
| test_5 (uncompressed) | 0.6 MB | 203 | 136 | .sav | **0.001s** | 0.023s | **17x** |
| | | | | .zsav | **0.003s** | 0.027s | **9x** |
| test_6 (uncompressed) | 5.4 GB | 395,330 | 916 | .sav | **2.511s** | 84.836s | **34x** |
| | | | | .zsav | **2.255s** | 90.499s | **40x** |

- **6–41x faster than pyreadstat** on writes across all files and compression modes
- Full metadata roundtrip: variable labels, value labels, missing values, MR sets, display properties
- Bytecode (.sav) and zlib (.zsav) compression

## Roadmap

- `apply_missing_values()` — apply SPSS missing value definitions to DataFrames
- `meta.validate(df)` — validate metadata against a DataFrame
- Codebook export — generate variable documentation from metadata
- Continued I/O performance optimization
- Currently Polars-only — pandas/other DataFrame libraries may be added later

## Warnings and Errors

ambers never silently "corrects" a damaged file. It either reads the data and tells you what
was wrong, or stops when the file cannot be read at all.

- **Warning, data returned** — the file contradicts itself but a more reliable part of it says
  what the bytes mean. You get a `CorruptFileWarning` listing the findings, also available as
  `sav.warnings` / `meta.warnings`. Examples: header row count or row width that disagrees with
  the file, a non-standard compression bias (values may be shifted; SPSS assumes 100),
  date/time values outside years 1-9999 (set to null), impossible very-long-string
  declarations (ignored).
- **Error, nothing returned** — the structure needed to navigate the file is broken and no
  guess would be safe. You get an `OSError` naming the cause. Examples: duplicate variable
  names, an impossible variable type, a record larger than the file, a truncated file, a file
  that is not SPSS at all.
- **Silence or escalate** — `warnings.filterwarnings("ignore", category=ambers.CorruptFileWarning)`
  hides the warning (findings stay in `sav.warnings`); `warnings.simplefilter("error",
  ambers.CorruptFileWarning)` makes any damaged file fatal in a pipeline.

See [warnings_and_errors.md](https://github.com/albertxli/ambers/blob/main/docs/warnings_and_errors.md) for every message, what it means, what
ambers did, and what to do.

## License

[MIT](LICENSE)
