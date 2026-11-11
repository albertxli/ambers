# ambers.validate()

Check SPSS value label quality — catch unlabeled values and duplicate labels before analysis.

```python
import ambers as am

sav = am.read_sav("survey.sav")
df, meta = sav.data, sav.meta

report = am.validate(df, meta)
print(report)
```

## Why validate

The most common SPSS data quality issue: categorical variables with values that have no label in metadata. You discover this only when you try to analyze the data and realize codes like `3`, `98`, `99` have no labels. `validate()` catches this upfront.

This is especially common with data received from partners, clients, or fieldwork agencies — labels are often incomplete or duplicated.

## Scope

`validate()` checks **value label quality** only:

- **Unlabeled values** (error) — data values without labels in metadata
- **Duplicate labels** (warning) — multiple codes mapping to the same label string

It does NOT check:

- Column presence mismatches (many legitimate reasons for differences)
- Type mismatches (metadata may be constructed from scratch)
- Missing value specs (researcher's judgment)
- Measure levels (too subjective)

## Signature

```python
def validate(
    df: DataFrame | LazyFrame,
    meta: SpssMetadata,
    *,
    columns: list[str] | None = None,
    exclude: list[str] | None = None,
) -> ValidationReport
```

## Parameters

### `df`

A Polars DataFrame or LazyFrame with raw SPSS data. Always unpack from a SavFile first:

```python
sav = am.read_sav("survey.sav")
df, meta = sav.data, sav.meta
report = am.validate(df, meta)
```

### `meta`

An `SpssMetadata` object containing value labels. Get it from `sav.meta`.

### `columns`

Which columns to check. Default `None` checks all numeric columns that have value labels in the metadata AND exist in the data. Can be combined with `exclude`.

```python
# All columns with value labels
am.validate(df, meta)

# Specific columns only
am.validate(df, meta, columns=["Q1", "Q2", "Q3"])
```

### `exclude`

Columns to skip. Applied after `columns` filtering. Can be combined with `columns`.

```python
# Check everything except weight and ID
am.validate(df, meta, exclude=["weight_var", "respondent_id"])

# Check Q1-Q10 but skip Q5 (known issue)
am.validate(df, meta, columns=[f"Q{i}" for i in range(1, 11)], exclude=["Q5"])
```

> **Note:** Unlike `apply_labels` and `apply_missing` where `columns` and `exclude` are mutually exclusive, `validate()` allows combining them. Validation is lenient (inspect everything), while apply functions are strict (transform precisely).

## Checks

### Check 1: Unlabeled values (severity: error)

For each numeric column with value labels, finds values in the data that have no corresponding label in metadata.

- Only checks numeric columns (Float64, Integer) — string columns are skipped
- Only checks columns that HAVE value labels — continuous numeric (age, income) is skipped
- **Directionality:** Only flags values in data without labels. Labels in metadata for values not in data are fine (partial data, no respondents chose that option).

```python
# Data has values 1, 2, 3, 98, 99
# Labels define: {1: "Yes", 2: "No"}
# → Error: 3 unlabeled values: [3, 98, 99]
```

### Check 2: Duplicate labels (severity: warning)

Finds labels where multiple codes map to the same label string. This is sometimes intentional (merging codings across waves) but often a bug.

```python
# Labels: {1: "Male", 2: "Female", 9: "Male"}
# → Warning: 1 duplicate label: 'Male' (codes: 1, 9)
```

## ValidationReport

`validate()` returns a `ValidationReport` — it does NOT raise by default. You choose how to handle results.

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `report.issues` | `list[ValidationIssue]` | All issues (errors + warnings) as Python objects |
| `report.errors` | `list[ValidationIssue]` | Only errors |
| `report.warnings` | `list[ValidationIssue]` | Only warnings |
| `report.is_valid` | `bool` | `True` if no errors (warnings don't count) |

### Methods

| Method | Description |
|--------|-------------|
| `report.raise_if_invalid()` | Raise `ValueError` if any errors exist. Silent if warnings only. |
| `report.to_frame()` | Convert all issues to a Polars DataFrame (severity, column, check, message). Full untruncated messages. |
| `repr(report)` | Box-drawing summary for terminal/notebook. Truncated: max 10 issues shown, box width capped at 80 chars. |

### `report.issues` — Python object access

Each `ValidationIssue` has:

| Field | Type | Description |
|-------|------|-------------|
| `issue.severity` | `str` | `"error"` or `"warning"` |
| `issue.column` | `str` | Variable name |
| `issue.check` | `str` | `"unlabeled_values"` or `"duplicate_labels"` |
| `issue.message` | `str` | Human-readable summary (full, untruncated) |
| `issue.details` | `dict` | Structured data for programmatic access |

#### Details for `unlabeled_values`:

```python
{
    "unlabeled_values": [3.0, 98.0, 99.0],  # values in df without labels
    "unique_in_data": 8,                      # total unique non-null values in df
    "labeled_in_data": 5,                     # how many of those have labels
}
```

#### Details for `duplicate_labels`:

```python
{
    "duplicates": {
        "Male": [1.0, 9.0],      # label string → list of codes
        "Female": [2.0, 8.0],
    },
}
```

### `report.to_frame()` — Polars DataFrame

Returns a DataFrame with all issues. Full untruncated messages — use this for filtering, sorting, and export.

```python
frame = report.to_frame()
# Schema: severity (String), column (String), check (String), message (String)

# Filter to errors only
frame.filter(pl.col("severity") == "error")

# Export
frame.write_csv("validation_report.csv")
frame.write_excel("validation_report.xlsx")
```

> **Tip:** For structured access to the `details` dict (e.g., the list of unlabeled values), use `report.issues` or `report.errors` directly — `to_frame()` only includes the `message` string.

### `repr(report)` — Terminal display

The `repr()` output is truncated for readability:

- Max 10 issues shown per category, then `... and N more errors`
- Long messages truncated to fit box width
- Box width capped at 80 characters

```
┌─ ValidationReport ────────────────────────────────────────┐
│ Status   INVALID (3 errors, 1 warning)                    │
│                                                           │
│ Errors                                                    │
│   [x] Q1                                                  │
│       2 unlabeled values: [98, 99] (3 of 5 unique values │
│   [x] Q5                                                  │
│       1 unlabeled value: [3] (4 of 5 unique values labele │
│   [x] D6                                                  │
│       1 unlabeled value: [6] (5 of 6 unique values labele │
│                                                           │
│ Warnings                                                  │
│   [!] POL_23                                              │
│       33 duplicate labels (see report.issues for details) │
└───────────────────────────────────────────────────────────┘
```

Use `report.to_frame()` or `report.errors` for full untruncated details.

## Examples

### Quick validation check

```python
sav = am.read_sav("survey.sav")
df, meta = sav.data, sav.meta

report = am.validate(df, meta)
if report.is_valid:
    print("All good!")
else:
    print(report)
```

### Strict mode — raise on errors

```python
sav = am.read_sav("survey.sav")
df, meta = sav.data, sav.meta

report = am.validate(df, meta)
report.raise_if_invalid()  # raises ValueError if errors found

# Safe to proceed — all categorical values are labeled
labeled = am.apply_labels(df, meta)
```

### Inspect and fix workflow

```python
sav = am.read_sav("survey.sav")
df, meta = sav.data, sav.meta

# 1. Validate
report = am.validate(df, meta)

# 2. Inspect issues
for error in report.errors:
    print(f"{error.column}: {error.details['unlabeled_values']}")

# 3. Fix metadata
meta2 = meta.with_variable_value_labels({
    "Q1": {98: "Don't know", 99: "Refused"},
})

# 4. Re-validate
report2 = am.validate(df, meta2)
assert report2.is_valid
```

### Export validation report

```python
report = am.validate(df, meta)
report.to_frame().write_csv("validation_issues.csv")
report.to_frame().write_excel("validation_issues.xlsx")
```

### Skip specific columns

```python
# Validate everything except weight and ID columns
report = am.validate(df, meta, exclude=["weight", "respondent_id"])

# Validate only specific question blocks
report = am.validate(df, meta, columns=["Q1", "Q2", "Q3", "Q4", "Q5"])

# Combine: check Q1-Q10 but skip Q5
report = am.validate(
    df, meta,
    columns=[f"Q{i}" for i in range(1, 11)],
    exclude=["Q5"],
)
```

### Pipeline with apply_labels

```python
sav = am.read_sav("survey.sav")
df, meta = sav.data, sav.meta

# Validate first, then apply labels with confidence
report = am.validate(df, meta)
if not report.is_valid:
    # Fix issues, or use enum_null to handle gracefully
    labeled = am.apply_labels(df, meta, output="enum_null")
else:
    labeled = am.apply_labels(df, meta)  # safe — all values labeled
```

## Edge cases

| Scenario | Behavior |
|----------|----------|
| Column has no value labels | Skipped (continuous numeric is fine) |
| String column with value labels | Skipped (string labels checked by `apply_labels`, not `validate`) |
| All values are null | No error (no data to check) |
| Empty DataFrame | No issues (no data to check) |
| Labels in meta for values not in data | Fine (partial data is legitimate) |
| Column in `columns` not in DataFrame | Silently skipped |
| Column in `columns` not in metadata | Silently skipped |
