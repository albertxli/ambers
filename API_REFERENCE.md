# ambers API Reference

## Public Functions

### `read_sav`

```rust
pub fn read_sav(path: impl AsRef<Path>) -> Result<(RecordBatch, SpssMetadata)>
```

Read an SPSS `.sav` or `.zsav` file from a file path. Returns all data as an Arrow `RecordBatch` plus the file's `SpssMetadata`. Loads the entire dataset into memory.

### `read_sav_from_reader`

```rust
pub fn read_sav_from_reader<R: Read + Seek>(reader: R) -> Result<(RecordBatch, SpssMetadata)>
```

Read an SPSS file from any reader that implements `Read + Seek`. Useful for reading from in-memory buffers, network streams, or other non-file sources.

### `read_sav_metadata`

```rust
pub fn read_sav_metadata(path: impl AsRef<Path>) -> Result<SpssMetadata>
```

Read only the metadata from an SPSS file (no data). Much faster than `read_sav()` when you only need variable information, labels, or other metadata.

---

## Quick Start

```rust
use ambers::read_sav;

let (batch, meta) = read_sav("survey.sav").unwrap();
println!("Rows: {}", batch.num_rows());
println!("Columns: {}", batch.num_columns());
meta.summary();
```

---

## `SpssMetadata` Fields

### File-Level Fields

| Field | Type | Description |
|-------|------|-------------|
| `file_label` | `String` | File label set in SPSS |
| `file_encoding` | `String` | Character encoding (e.g. `"UTF-8"`) |
| `compression` | `Compression` | `None`, `Bytecode`, or `Zlib` |
| `creation_time` | `String` | Date the file was created |
| `modification_time` | `String` | Time the file was created |
| `notes` | `Vec<String>` | Document records (type 6) |
| `number_rows` | `Option<i64>` | Row count from header (may be absent) |
| `number_columns` | `usize` | Number of visible variables |
| `file_format` | `String` | `"sav"` or `"zsav"` |

### Variable Ordering

| Field | Type | Description |
|-------|------|-------------|
| `variable_names` | `Vec<String>` | Ordered column names (defines Arrow schema column order) |

### Variable Metadata (HashMap-based, keyed by variable name)

| Field | Type | Description |
|-------|------|-------------|
| `variable_labels` | `HashMap<String, String>` | Variable name to descriptive label |
| `spss_variable_types` | `HashMap<String, String>` | SPSS format strings (e.g. `"F8.2"`, `"A50"`, `"DATE11"`) |
| `rust_variable_types` | `HashMap<String, String>` | Rust type names (e.g. `"f64"`, `"String"`) |
| `variable_value_labels` | `HashMap<String, HashMap<Value, String>>` | Per-variable value-to-label maps |
| `variable_measure` | `HashMap<String, Measure>` | `Nominal`, `Ordinal`, or `Scale` |
| `variable_alignment` | `HashMap<String, Alignment>` | `Left`, `Right`, or `Center` |
| `variable_display_width` | `HashMap<String, u32>` | Column display width |
| `variable_storage_width` | `HashMap<String, usize>` | Storage width in bytes |
| `variable_missing` | `HashMap<String, Vec<MissingSpec>>` | Missing value specifications |

### SPSS-Specific Fields

| Field | Type | Description |
|-------|------|-------------|
| `mr_sets` | `HashMap<String, MrSet>` | Multiple response set definitions |
| `weight_variable` | `Option<String>` | Weight variable name, if set |

---

## `SpssMetadata` Convenience Methods

### `label`

```rust
pub fn label(&self, name: &str) -> Option<&str>
```

Get the descriptive label for a variable. Returns `None` if the variable has no label.

### `value_labels`

```rust
pub fn value_labels(&self, name: &str) -> Option<&HashMap<Value, String>>
```

Get the value label map for a variable. Returns `None` if the variable has no value labels.

### `format`

```rust
pub fn format(&self, name: &str) -> Option<&str>
```

Get the SPSS format string for a variable (e.g. `"F8.2"`, `"A50"`). Returns `None` if not found.

### `measure`

```rust
pub fn measure(&self, name: &str) -> Option<Measure>
```

Get the measurement level for a variable (`Nominal`, `Ordinal`, or `Scale`). Returns `None` if not found.

### `summary`

```rust
pub fn summary(&self)
```

Print a formatted summary of all metadata to stdout, including file-level info and a table of all variables with their format, measure, and label.

---

## Supporting Types

### `Value`

```rust
pub enum Value {
    Numeric(f64),
    String(String),
}
```

A value used as a key in value label maps. Implements `Hash`, `Eq`, `Ord`, and `Display`. Numeric values that are whole numbers display as integers (e.g. `1` not `1.0`). Numeric values sort before string values.

### `MissingSpec`

```rust
pub enum MissingSpec {
    Value(f64),              // A single discrete missing value
    Range { lo: f64, hi: f64 }, // A range of missing values
    StringValue(String),     // A discrete string missing value
}
```

### `MrSet`

```rust
pub struct MrSet {
    pub name: String,
    pub label: String,
    pub mr_type: MrType,        // MultipleDichotomy or MultipleCategory
    pub counted_value: Option<String>,
    pub variables: Vec<String>,
}
```

### `MrType`

```rust
pub enum MrType {
    MultipleDichotomy,
    MultipleCategory,
}
```

### `Compression`

```rust
pub enum Compression {
    None,
    Bytecode,
    Zlib,
}
```

### `Measure`

```rust
pub enum Measure {
    Unknown,
    Nominal,
    Ordinal,
    Scale,
}
```

### `Alignment`

```rust
pub enum Alignment {
    Left,
    Right,
    Center,
}
```

---

## Re-Exports

The following types are re-exported from `ambers` for convenience:

```rust
pub use Alignment;
pub use Measure;
pub use MissingSpec;
pub use MrSet;
pub use MrType;
pub use SpssMetadata;
pub use Value;
```

---

## Arrow Output

- **Numeric variables** map to `Float64` (nullable). SYSMIS values become Arrow nulls.
- **String variables** map to `Utf8`.
- **Date/time variables** remain `Float64` with the SPSS format available via `meta.format(name)`.
- Column order in the `RecordBatch` schema matches `meta.variable_names`.
