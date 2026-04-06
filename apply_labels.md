# apply_labels

Replace SPSS numeric/string codes with their metadata value labels.

```python
import ambers as am

sav = am.read_sav("survey.sav")
df, meta = sav.data, sav.meta
labeled = am.apply_labels(df, meta)
labeled.write_excel("survey.xlsx")
```

## Why Enum is the default

SPSS categorical variables have an inherent ordering defined by their value codes (1, 2, 3...). When you apply value labels, that ordering carries semantic meaning — especially for Likert scales:

```
1 = "Strongly disagree"
2 = "Disagree"
3 = "Neutral"
4 = "Agree"
5 = "Strongly agree"
```

Polars `Enum` preserves this definition order. Sort, group_by, and value_counts all follow the original SPSS code order, not alphabetical order. This is critical for survey analysis where "Strongly disagree" should come before "Disagree", not after.

| Feature | `pl.String` | `pl.Enum` |
|---------|-------------|-----------|
| Sort order | Alphabetical | Definition order (SPSS key order) |
| Memory | Full string per row | Integer index (u8) |
| Validation | Accepts any string | Rejects undefined values |
| group_by/filter | String comparison | Integer comparison (faster) |
| Excel/CSV export | As-is | Auto-casts to String |

`Enum` is the best of both worlds: ordered and validated for analysis, readable text for export.

## Signature

```python
def apply_labels(
    df: DataFrame | LazyFrame,
    meta: SpssMetadata,
    *,
    columns: list[str] | None = None,
    output: Literal["enum", "string", "enum_null"] = "enum",
) -> DataFrame | LazyFrame
```

## Parameters

### `df`

A Polars DataFrame or LazyFrame with raw SPSS codes. Always unpack from a SavFile first:

```python
sav = am.read_sav("survey.sav")
df, meta = sav.data, sav.meta

labeled = am.apply_labels(df, meta)
```

### `meta`

An `SpssMetadata` object containing value labels. Get it from `sav.meta`.

### `columns`

Which columns to apply labels to. Default `None` applies to all columns that have value labels in the metadata AND exist in the data.

```python
# All columns with value labels
am.apply_labels(df, meta)

# Specific columns only
am.apply_labels(df, meta, columns=["gender", "satisfaction"])
```

When specified explicitly, raises `ValueError` if:
- A column doesn't exist in the data
- A column has no value labels (or an empty label dict) in the metadata

### `output`

Controls both the output dtype and how unmapped values are handled. One parameter, three clear modes:

| `output=` | Dtype | Unmapped values | Best for |
|-----------|-------|-----------------|----------|
| `"enum"` (default) | `pl.Enum` (ordered) | **Error** — raises with diagnostic | Analysis with strict validation |
| `"string"` | `pl.String` | **Stringify** — `3.0` → `"3"` | Excel/CSV export |
| `"enum_null"` | `pl.Enum` (ordered) | **Null** — unmapped become null | Analysis, exclude unknowns from base |

**Note:** The `output` mode only affects **numeric columns**. String columns always pass through unmapped text and produce `pl.String` — see [Dtype-aware behavior](#dtype-aware-behavior) below.

```python
sav = am.read_sav("survey.sav")
df, meta = sav.data, sav.meta

# output="enum" — strict, raises if any value has no label
labeled = am.apply_labels(df, meta)
labeled = am.apply_labels(df, meta, output="enum")  # same as default

# output="string" — tolerant, unmapped values become strings
labeled = am.apply_labels(df, meta, output="string")

# output="enum_null" — ordered categories, unknowns become null
labeled = am.apply_labels(df, meta, output="enum_null")
```

#### When to use each mode

**`output="enum"` (default)** — Use when you want validated, ordered categories for analysis. If your metadata is incomplete (e.g., value `99` exists in data but has no label), you'll get a clear error telling you exactly which columns and values are missing labels. Fix the metadata, then retry.

**`output="string"`** — Use when exporting to Excel or CSV. All values become readable text. Unmapped numeric codes become their string representation (`99.0` → `"99"`). No ordering, no validation — just text.

**`output="enum_null"`** — Use when you want ordered categories but some values intentionally lack labels (e.g., "other" responses coded as `99`). Unmapped values become null and are excluded from group_by, value_counts, etc. This keeps your analysis base clean.

## Dtype-aware behavior

SPSS files contain both numeric and string columns, each with different labeling semantics:

| Column type | Has labels? | Behavior |
|---|---|---|
| **Float64** | Yes | **Categorical variable.** Apply labels. `output` mode controls dtype and unmapped handling. |
| **Float64** | No | **Numeric/continuous** (age, income). Skipped entirely. |
| **String** | Yes | **Partial labeling.** Common for open-ended fields with special codes ("RF" → "Refused", "DK" → "Don't Know"). Labeled values are replaced; unlabeled text passes through unchanged. Always `pl.String` — never Enum. |
| **String** | No | Skipped. |

### Why string columns always pass through

SPSS string columns with value labels are typically open-ended text fields where only specific codes have labels:

```
Value labels for "Q5_other":
  "RF" → "Refused"
  "DK" → "Don't Know"

Actual data:
  "I like the product"  → passes through (legitimate response)
  "RF"                  → becomes "Refused"
  "Too expensive"       → passes through
  "DK"                  → becomes "Don't Know"
```

Erroring on unlabeled text would break legitimate open-ended responses. The `output` mode intentionally does not affect string columns.

## Return type

Returns the same type as input:

| Input | Output |
|-------|--------|
| `DataFrame` | `DataFrame` |
| `LazyFrame` | `LazyFrame` |

## Error messages

When `output="enum"` and unmapped values are found, `apply_labels` raises a structured `ValueError` with diagnostics:

```
apply_labels: 2 columns have values without labels (output='enum')

  S2:     1 unmapped value: [4.0]  (4 unique, 3 labeled)
  Q5:     2 unmapped values: [98.0, 99.0]  (10 unique, 8 labeled)

To fix, either:
  - Add missing labels: meta = meta.with_variable_value_labels({...})
  - Use output="enum_null" to set unmapped values to null
  - Use output="string" to keep unmapped as strings
```

When Enum output modes detect duplicate label values (multiple codes mapping to the same label):

```
apply_labels: 1 column has duplicate label values (multiple codes -> same label)

  POL_23: 3 duplicate labels: [2.0, 36.0] -> 'Socialist Party'; [3.0, 37.0] -> "People's Party"; [4.0, 38.0] -> 'Democratic Party'

With Enum output, duplicate labels collapse distinct codes into the same category.
If intentional, this is fine. If not, fix the labels in metadata.
```

Both diagnostics can appear together in a single error if both issues exist.

## Examples

### Survey analysis workflow (default)

```python
sav = am.read_sav("survey.sav")
df, meta = sav.data, sav.meta
labeled = am.apply_labels(df, meta)

# Enum columns sort by definition order
labeled.select("satisfaction").unique().sort("satisfaction")
# → "Very Dissatisfied", "Dissatisfied", "Neutral", "Satisfied", "Very Satisfied"
# NOT alphabetical!

# Group by works naturally
labeled.group_by("gender").agg(pl.col("score").mean())

# Export — Enum auto-casts to String
labeled.write_excel("survey.xlsx")
```

### Quick export (String output)

```python
sav = am.read_sav("survey.sav")
df, meta = sav.data, sav.meta
labeled = am.apply_labels(df, meta, output="string")
labeled.write_csv("survey.csv")
```

### Exclude unknowns from analysis base

```python
sav = am.read_sav("survey.sav")
df, meta = sav.data, sav.meta
labeled = am.apply_labels(df, meta, output="enum_null")

# Unmapped values (e.g., 99="other") are null — excluded from counts
labeled.group_by("Q1").agg(pl.len())
```

### Data quality check

```python
sav = am.read_sav("survey.sav")
df, meta = sav.data, sav.meta
try:
    am.apply_labels(df, meta)  # output="enum" by default
except ValueError as e:
    print(e)
    # Shows exactly which columns and values need labels
```

### After filtering

```python
sav = am.read_sav("survey.sav")
df, meta = sav.data, sav.meta
filtered = df.filter(pl.col("age") > 30)
labeled = am.apply_labels(filtered, meta)
```

### Lazy pipeline

```python
sav = am.scan_sav("big_survey.sav")
df, meta = sav.data, sav.meta
labeled = am.apply_labels(df, meta, output="enum_null")
result = labeled.select(["gender", "satisfaction"]).collect()
```

### Selective columns

```python
sav = am.read_sav("survey.sav")
df, meta = sav.data, sav.meta
labeled = am.apply_labels(df, meta, columns=["gender", "satisfaction"])
```

## Performance

`apply_labels` builds one Polars expression per column and executes them all in a single `with_columns()` call. Polars runs all expressions in parallel in Rust.

Compared to pyreadstat's `set_value_labels()`:
- **No per-column unique value iteration** — pyreadstat iterates all unique values per column to fill in unmapped entries
- **No deepcopy** — pyreadstat deep-copies the entire label dict per column
- **Single batched call** — pyreadstat calls `with_columns()` once per column in a loop
- **Enum support** — pyreadstat uses `Categorical` (unordered); ambers uses `Enum` (ordered)

## Edge cases

| Scenario | Behavior |
|----------|----------|
| Column has empty label dict `{}` | Skipped (no labels to apply) |
| All values in column are null | Null stays null (no error even with `output="enum"`) |
| Duplicate label values (`{1: "Yes", 2: "Yes"}`) | Deduplicated preserving order → `Enum(["Yes"])`. Diagnostic raised for Enum modes. |
| Label value is empty string (`{99: ""}`) | Treated as a valid label (empty string) |
| Float precision (`3.0000001`) | Matched against label keys as-is; may not match `3.0` |
| Integer-valued floats with `output="string"` | `3.0` → `"3"`, `2.5` → `"2.5"` |
