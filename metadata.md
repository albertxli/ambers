# SpssMetadata Reference

Complete reference for the `SpssMetadata` class — ambers' metadata API for reading, constructing, and writing SPSS .sav/.zsav files.

---

## Quick Start

```python
import ambers as am

# Read metadata from a file
df, meta = am.read_sav("survey.sav")
meta.summary()                        # formatted overview
meta.describe("Q1")                   # single variable deep-dive

# Construct metadata from scratch
meta = am.SpssMetadata(
    file_label="Customer Survey 2026",
    variable_labels={"Q1": "Satisfaction", "Q2": "Loyalty"},
    variable_value_labels={"Q1": {1: "Low", 5: "High"}},
    variable_measures={"Q1": "ordinal", "Q2": "nominal"},
)
am.write_sav(df, "output.sav", meta=meta)

# Override existing metadata (immutable — returns new instance)
meta2 = meta.update(file_label="Updated Survey", variable_labels={"Q3": "NPS"})

# Chainable single-field setters
meta3 = (am.SpssMetadata()
    .with_variable_labels({"Q1": "Satisfaction"})
    .with_variable_measures({"Q1": "ordinal"})
    .with_file_label("My Survey")
)
```

---

## Construction

### `SpssMetadata(**kwargs)`

Creates a new metadata object. All parameters are optional keyword arguments.

```python
meta = am.SpssMetadata()                          # empty (defaults filled at write time)
meta = am.SpssMetadata(file_label="My Survey")    # with specific fields
```

### `meta.update(**kwargs) -> SpssMetadata`

Returns a **new** `SpssMetadata` with the given fields merged/replaced. The original is unchanged (immutable).

**Merge semantics:**
- **Dict fields** — merge at the variable-name level. New keys are added, existing keys are overwritten. Pass `{key: None}` to remove a key.
- **Scalar fields** (`file_label`, `weight_variable`) — replaced entirely.
- **`notes`** — replaced entirely (pass new list or string).

```python
meta2 = meta.update(
    file_label="Updated",                         # replaces scalar
    variable_labels={"Q3": "New Label"},           # merges — Q1/Q2 labels preserved
    variable_measures={"Q3": "scale"},
)

# Remove a variable's label
meta3 = meta.update(variable_labels={"Q1": None})
```

### `meta.with_*() -> SpssMetadata`

Chainable single-field convenience methods — syntax sugar for `update()`. Each `with_*()` call is equivalent to calling `update()` with the corresponding keyword argument. They exist for IDE discoverability and fluent chaining.

| `update()` kwarg | `with_*()` method | Input type |
|---|---|---|
| `file_label=` | `with_file_label(str)` | `str` |
| `notes=` | `with_notes(str \| list[str])` | `str \| list[str]` |
| `weight_variable=` | `with_weight_variable(str \| None)` | `str \| None` |
| `variable_labels=` | `with_variable_labels(dict)` | `dict[str, str \| None]` |
| `variable_value_labels=` | `with_variable_value_labels(dict)` | `dict[str, dict \| None]` |
| `variable_formats=` | `with_variable_formats(dict)` | `dict[str, str \| None]` |
| `variable_measures=` | `with_variable_measures(dict)` | `dict[str, str \| None]` |
| `variable_display_widths=` | `with_variable_display_widths(dict)` | `dict[str, int \| None]` |
| `variable_alignments=` | `with_variable_alignments(dict)` | `dict[str, str \| None]` |
| `variable_missing_values=` | `with_variable_missing_values(dict)` | `dict[str, dict \| None]` |
| `variable_roles=` | `with_variable_roles(dict)` | `dict[str, str \| None]` |
| `variable_attributes=` | `with_variable_attributes(dict)` | `dict[str, dict \| None]` |
| `mr_sets=` | `with_mr_sets(dict)` | `dict[str, dict \| None]` |

---

## Field Reference

### Readable and Writable Fields

These fields can be set via constructor, `update()`, or `with_*()` methods, and read as properties.

| Field | Python type | Example | Validation |
|---|---|---|---|
| `file_label` | `str` | `"Customer Survey 2026"` | Any string (truncated to 64 bytes in SAV) |
| `notes` | `list[str]` or `str` | `["Wave 1 data"]` | Any string(s) |
| `weight_variable` | `str \| None` | `"weight"` | Must match a DataFrame column at write time |
| `variable_labels` | `dict[str, str]` | `{"Q1": "Satisfaction"}` | Any string values |
| `variable_value_labels` | `dict[str, dict]` | `{"Q1": {1: "Low", 5: "High"}}` | Keys: int/float/str; Values: str |
| `variable_formats` | `dict[str, str]` | `{"Q1": "F8.2", "name": "A50"}` | SPSS format strings |
| `variable_measures` | `dict[str, str]` | `{"Q1": "ordinal"}` | `"nominal"` / `"ordinal"` / `"scale"` / `"unknown"` |
| `variable_alignments` | `dict[str, str]` | `{"Q1": "right"}` | `"left"` / `"right"` / `"center"` |
| `variable_display_widths` | `dict[str, int]` | `{"Q1": 8}` | Positive integer |
| `variable_roles` | `dict[str, str]` | `{"Q1": "input"}` | `"input"` / `"target"` / `"both"` / `"none"` / `"partition"` / `"split"` |
| `variable_attributes` | `dict[str, dict]` | `{"Q1": {"Source": ["Survey"]}}` | `dict[str, list[str]]` per variable |
| `variable_missing_values` | `dict[str, dict]` | See [Missing Values](#missing-values) | Dict structure validated |
| `mr_sets` | `dict[str, dict]` | See [MR Sets](#mr-sets) | Dict structure validated |

### Read-Only Fields

These fields are populated when reading from a file. They cannot be set directly.

| Field | Python type | Example | Description |
|---|---|---|---|
| `file_encoding` | `str` | `"UTF-8"` | Character encoding (always UTF-8 for writes) |
| `compression` | `str` | `"bytecode"` | `"none"` / `"bytecode"` / `"zlib"` |
| `creation_time` | `str` | `"2026-02-21 12:38:47"` | Auto-set at write time |
| `number_rows` | `int \| None` | `22070` | From file header |
| `number_columns` | `int` | `677` | From file header |
| `file_format` | `str` | `"sav"` | `"sav"` or `"zsav"` |
| `variable_names` | `list[str]` | `["Q1", "Q2"]` | Ordered column names |
| `variable_storage_widths` | `dict[str, int]` | `{"Q1": 8}` | Computed from format |
| `arrow_data_types` | `dict[str, str]` | `{"Q1": "f64"}` | From Arrow schema |

---

## Convenience Methods

```python
meta.label("Q1")           # str | None — variable label
meta.value("Q1")           # dict | None — value labels for this variable
meta.format("Q1")          # str | None — SPSS format string (e.g. "F8.2")
meta.measure("Q1")         # str | None — measurement level
meta.role("Q1")            # str | None — variable role
meta.attribute("Q1")       # dict | None — all custom attributes
meta.attribute("Q1", "$@Role")  # list[str] | KeyError — specific attribute

meta.summary()             # formatted overview of all metadata
meta.describe("Q1")        # detailed single-variable view
meta.describe(["Q1", "Q2"])  # multiple variables
meta.diff(other_meta)      # compare two metadata objects
```

---

## Write-Time Defaults

When writing with `am.write_sav(df, path, meta=meta)`, any metadata fields not explicitly set are automatically filled from the DataFrame schema:

| Field | Default | Source |
|---|---|---|
| `variable_formats` | `F8.2` (numeric), `A255` (string), `DATE11`, `DATETIME23.2`, `TIME11.2` | Arrow type |
| `variable_measures` | `scale` (numeric/temporal), `nominal` (string/bool) | Arrow type |
| `variable_alignments` | `right` (numeric), `left` (string) | Arrow type |
| `variable_display_widths` | Format width (8 for `F8.2`, 255 for `A255`, etc.) | Format |
| `variable_storage_widths` | 8 (numeric), format width (string) | Format |
| `variable_roles` | `input` | SPSS default |
| `file_label` | `""` (empty) | — |
| `creation_time` | Current UTC time | — |
| `notes` | `[]` (none) | — |

User-set fields are never overwritten. Extra metadata for columns not in the DataFrame is silently ignored.

---

## Missing Values

SPSS supports three types of missing value specifications for variables.

### Discrete Missing Values

Up to 3 unique values that represent missing data.

```python
# Numeric discrete (most common)
meta = meta.update(variable_missing_values={
    "Q1": {"type": "discrete", "values": [98, 99]},
    "Q2": {"type": "discrete", "values": [97, 98, 99]},   # max 3 values
})

# String discrete (max 8 characters each)
meta = meta.update(variable_missing_values={
    "city": {"type": "discrete", "values": ["N/A", "DK", "RF"]},
})
```

### Range Missing Values (Numeric Only)

A continuous range where all values between `low` and `high` (inclusive) are treated as missing.

```python
meta = meta.update(variable_missing_values={
    "score": {"type": "range", "low": 900, "high": 999},
})
```

### Range + One Discrete Value (Numeric Only)

A range plus one additional discrete value outside the range.

```python
meta = meta.update(variable_missing_values={
    "income": {"type": "range", "low": 999990, "high": 999999, "discrete": 0},
})
```

### Remove Missing Values

```python
meta = meta.update(variable_missing_values={"Q1": None})
```

### Validation Rules

**General:**
- Cannot mix numeric and string values in the same spec — `ValueError` if attempted
- At write time, missing value type is validated against the actual column type (e.g., string missing values on a numeric column raises an error)

**Numeric variables:**
- Discrete: maximum 3 values, must be unique (no duplicates)
- Range: `low` must be less than `high`
- Range + discrete: the discrete value must NOT fall between `low` and `high`
- All values must be numeric (int or float)

**String variables:**
- Discrete only: maximum 3 values, each maximum 8 characters
- **Ranges are NOT available** for string variables — `ValueError` if attempted

---

## Value Labels

Map numeric or string values to human-readable labels.

```python
# Numeric value labels (most common)
meta = meta.update(variable_value_labels={
    "gender": {1: "Male", 2: "Female", 3: "Non-binary"},
    "satisfaction": {1: "Very dissatisfied", 2: "Dissatisfied",
                     3: "Neutral", 4: "Satisfied", 5: "Very satisfied"},
})

# String value labels
meta = meta.update(variable_value_labels={
    "country": {"US": "United States", "UK": "United Kingdom"},
})

# Float keys (SPSS stores all numeric values as float64)
meta = meta.update(variable_value_labels={
    "weight_class": {1.5: "Light", 2.5: "Medium", 3.5: "Heavy"},
})

# Remove value labels for a variable
meta = meta.update(variable_value_labels={"Q1": None})
```

---

## MR Sets (Multiple Response Sets)

Define groups of variables that represent multiple-response survey questions.

### Multiple Dichotomy

Binary yes/no variables where a specific `counted_value` indicates "selected".

```python
meta = meta.update(mr_sets={
    "brands": {
        "label": "Brands Mentioned",
        "type": "dichotomy",
        "counted_value": "1",
        "variables": ["brand_a", "brand_b", "brand_c"],
    }
})
```

### Multiple Category

Categorical response variables grouped together.

```python
meta = meta.update(mr_sets={
    "hobbies": {
        "label": "Hobbies Selected",
        "type": "category",
        "variables": ["hobby1", "hobby2", "hobby3"],
    }
})
```

### Remove an MR Set

```python
meta = meta.update(mr_sets={"brands": None})
```

### Validation Rules

- **Name**: any valid name (the `$` prefix is automatically added by the SAV format — do not include it yourself or it will be doubled)
- **Type**: must be `"dichotomy"` or `"category"`
- **Variables**: must have at least 2 variables; all must be the same type (all numeric or all string)
- **Dichotomy sets**: `counted_value` is required (cannot be None)
- **Category sets**: `counted_value` is ignored (forced to None internally)

---

## Variable Attributes

Attach custom key-value metadata to individual variables. Each attribute holds a list of string values.

```python
meta = meta.update(variable_attributes={
    "Q1": {
        "Source": ["Survey", "Online", "Wave 3"],   # multiple values
        "SurveySection": ["Satisfaction"],
    },
    "Q2": {
        "Source": ["Admin"],
    },
})
```

### Remove Attributes

```python
# Remove all attributes for a variable
meta = meta.update(variable_attributes={"Q1": None})
```

### Viewing in SPSS

Custom variable attributes are not shown in SPSS's Variable View by default. To view them:

1. **Variable View**: Go to **View > Customize Variable View**, click **OK** (custom attributes are toggled on by default, but the dialog must be confirmed first)
2. **Syntax**: Run `DISPLAY ATTRIBUTES` to list all custom attributes in the output viewer

---

## SPSS Format Strings

Format strings control how values are displayed in SPSS.

### Numeric Formats

| Format | Description | Example |
|---|---|---|
| `F8.2` | Standard numeric (width.decimals) | `1234.56` |
| `F8.0` | Integer (no decimals) | `1234` |
| `F1.0` | Single digit | `1` |
| `F12.4` | Wide with 4 decimals | `1234.5678` |

### String Formats

| Format | Description | Max length |
|---|---|---|
| `A8` | Short string | 8 chars |
| `A50` | Medium string | 50 chars |
| `A255` | Standard string | 255 chars |
| `A32767` | Very long string | 32767 chars |

### Temporal Formats

| Format | Arrow type | Description |
|---|---|---|
| `DATE11` | `Date32` | Date (e.g. `21-FEB-26`) |
| `DATETIME23.2` | `Timestamp(us)` | Date + time |
| `TIME11.2` | `Duration(us)` | Time duration |

---

## Complete Example: From-Scratch Workflow

```python
import ambers as am
import polars as pl

# Build DataFrame from external source
df = pl.read_excel("survey_data.xlsx")

# Build complete metadata
meta = am.SpssMetadata(
    file_label="Customer Satisfaction Survey Q1 2026",
    notes=["Collected via online panel", "N=1000 respondents"],
    weight_variable="design_weight",
    variable_labels={
        "resp_id": "Respondent ID",
        "age": "Age in years",
        "gender": "Gender",
        "q1": "Overall satisfaction",
        "q2": "Likelihood to recommend",
        "design_weight": "Survey design weight",
    },
    variable_value_labels={
        "gender": {1: "Male", 2: "Female", 3: "Non-binary"},
        "q1": {1: "Very dissatisfied", 5: "Very satisfied"},
        "q2": {0: "Not at all likely", 10: "Extremely likely"},
    },
    variable_measures={
        "resp_id": "nominal",
        "age": "scale",
        "gender": "nominal",
        "q1": "ordinal",
        "q2": "ordinal",
        "design_weight": "scale",
    },
    variable_formats={
        "resp_id": "F8.0",
        "age": "F3.0",
        "gender": "F1.0",
        "q1": "F1.0",
        "q2": "F2.0",
        "design_weight": "F8.4",
    },
    variable_missing_values={
        "age": {"type": "discrete", "values": [98, 99]},
        "q1": {"type": "discrete", "values": [8, 9]},
        "q2": {"type": "discrete", "values": [98, 99]},
    },
)

am.write_sav(df, "survey_q1_2026.sav", meta=meta)
```

## Complete Example: Roundtrip with Override

```python
import ambers as am

# Read existing file
df, meta = am.read_sav("input.sav")

# Override specific fields (everything else preserved)
meta = meta.update(
    file_label="Updated Survey — Wave 2",
    variable_labels={"Q1": "Updated Question Label"},
    variable_missing_values={"Q1": {"type": "discrete", "values": [8, 9]}},
)

am.write_sav(df, "output.sav", meta=meta)
```

## Complete Example: Chainable Builder

```python
import ambers as am

meta = (am.SpssMetadata()
    .with_file_label("NPS Survey")
    .with_variable_labels({"score": "NPS Score", "segment": "Customer Segment"})
    .with_variable_value_labels({"segment": {1: "Enterprise", 2: "SMB", 3: "Consumer"}})
    .with_variable_measures({"score": "scale", "segment": "nominal"})
    .with_variable_formats({"score": "F2.0", "segment": "F1.0"})
    .with_variable_missing_values({"score": {"type": "discrete", "values": [99]}})
    .with_notes(["Q1 2026 collection"])
)

am.write_sav(df, "nps.sav", meta=meta)
```
