from __future__ import annotations

from pathlib import Path

import polars

class SpssMetadata:
    """SPSS metadata container for variable labels, formats, missing values, and other properties.

    Immutable — all mutation methods (``update()``, ``with_*()``) return a new instance.

    Examples
    --------
    >>> import ambers as am
    >>> meta = am.SpssMetadata(
    ...     file_label="Customer Survey 2026",
    ...     variable_labels={"Q1": "Satisfaction", "Q2": "Loyalty"},
    ...     variable_value_labels={"Q1": {1: "Low", 5: "High"}},
    ... )
    >>> am.write_sav(df, "output.sav", meta=meta)
    """

    def __init__(
        self,
        *,
        file_label: str | None = None,
        notes: str | list[str] | None = None,
        weight_variable: str | None = None,
        variable_labels: dict[str, str | None] | None = None,
        variable_value_labels: dict[str, dict[int | float | str, str] | None] | None = None,
        variable_formats: dict[str, str | None] | None = None,
        variable_measures: dict[str, str | None] | None = None,
        variable_display_widths: dict[str, int | None] | None = None,
        variable_alignments: dict[str, str | None] | None = None,
        variable_missing_values: dict[str, dict | None] | None = None,
        variable_roles: dict[str, str | None] | None = None,
        variable_attributes: dict[str, dict[str, list[str]] | None] | None = None,
        mr_sets: dict[str, dict | None] | None = None,
    ) -> None: ...

    def update(
        self,
        *,
        file_label: str | None = None,
        notes: str | list[str] | None = None,
        weight_variable: str | None = None,
        variable_labels: dict[str, str | None] | None = None,
        variable_value_labels: dict[str, dict[int | float | str, str] | None] | None = None,
        variable_formats: dict[str, str | None] | None = None,
        variable_measures: dict[str, str | None] | None = None,
        variable_display_widths: dict[str, int | None] | None = None,
        variable_alignments: dict[str, str | None] | None = None,
        variable_missing_values: dict[str, dict | None] | None = None,
        variable_roles: dict[str, str | None] | None = None,
        variable_attributes: dict[str, dict[str, list[str]] | None] | None = None,
        mr_sets: dict[str, dict | None] | None = None,
    ) -> SpssMetadata:
        """Return a new SpssMetadata with the given fields merged or replaced.

        Dict fields merge as an overlay: new keys are added, existing keys are
        overwritten, unlisted keys are preserved. Pass ``{key: None}`` to remove a key.
        Scalar fields (``file_label``, ``weight_variable``) and ``notes`` are replaced entirely.

        Parameters
        ----------
        file_label
            File label string. Replaced entirely (not merged).
        notes
            Document record notes. Replaced entirely. Pass a single string or list of strings.
        weight_variable
            Weight variable name, or None to clear. Replaced entirely.
        variable_labels
            Variable name to descriptive label mapping. Merged by variable name.
        variable_value_labels
            Value labels mapping numeric/string values to display labels. Merged by variable name.
        variable_formats
            SPSS format strings (e.g. ``"F8.2"``, ``"A50"``, ``"DATE11"``). Merged by variable name.
        variable_measures
            Measurement levels: ``"nominal"``, ``"ordinal"``, or ``"scale"``. Merged by variable name.
        variable_display_widths
            Display widths (positive integers). Merged by variable name.
        variable_alignments
            Column alignments: ``"left"``, ``"right"``, or ``"center"``. Merged by variable name.
        variable_missing_values
            Missing value specifications. Dict with ``"type"`` key (``"discrete"`` or ``"range"``).
            Discrete: ``{"type": "discrete", "values": [98, 99]}``.
            Range: ``{"type": "range", "low": 900, "high": 999}``.
            Range + discrete: ``{"type": "range", "low": 900, "high": 999, "discrete": 0}``.
            Merged by variable name. See ``metadata.md`` for full reference.
        variable_roles
            Variable roles: ``"input"``, ``"target"``, ``"both"``, ``"none"``, ``"partition"``,
            or ``"split"``. Merged by variable name.
        variable_attributes
            Custom variable attributes. Each variable maps to ``{attr_name: [values]}``.
            Merged by variable name.
        mr_sets
            Multiple response set definitions. Dict with ``"type"`` key (``"dichotomy"`` or
            ``"category"``), ``"variables"`` list, optional ``"label"``, and ``"counted_value"``
            (required for dichotomy). Merged by set name. See ``metadata.md`` for full reference.

        Returns
        -------
        SpssMetadata

        Examples
        --------
        File label (replaced entirely):

        >>> meta2 = meta.update(file_label="Updated Survey 2026")

        Notes (replaced entirely, accepts string or list):

        >>> meta2 = meta.update(notes="Wave 2 data collected March 2026")
        >>> meta2 = meta.update(notes=["Wave 2", "Cleaned dataset"])

        Weight variable (set or clear):

        >>> meta2 = meta.update(weight_variable="wt_var")
        >>> meta2 = meta.update(weight_variable=None)

        Variable labels (merged — existing labels preserved):

        >>> meta2 = meta.update(variable_labels={"Q1": "Satisfaction", "Q3": "NPS"})
        >>> meta2 = meta.update(variable_labels={"Q3": None})  # remove Q3 label

        Value labels (numeric and string keys):

        >>> meta2 = meta.update(variable_value_labels={
        ...     "gender": {1: "Male", 2: "Female", 3: "Non-binary"},
        ... })
        >>> meta2 = meta.update(variable_value_labels={
        ...     "country": {"US": "United States", "UK": "United Kingdom"},
        ... })

        Variable formats (SPSS format strings):

        >>> meta2 = meta.update(variable_formats={
        ...     "score": "F8.2", "name": "A50", "start_date": "DATE11",
        ... })

        Measurement levels:

        >>> meta2 = meta.update(variable_measures={
        ...     "age": "scale", "gender": "nominal", "satisfaction": "ordinal",
        ... })

        Display widths:

        >>> meta2 = meta.update(variable_display_widths={"Q1": 12, "name": 30})

        Alignments:

        >>> meta2 = meta.update(variable_alignments={
        ...     "Q1": "right", "name": "left", "header": "center",
        ... })

        Missing values — discrete numeric (up to 3 values):

        >>> meta2 = meta.update(variable_missing_values={
        ...     "Q1": {"type": "discrete", "values": [98, 99]},
        ... })

        Missing values — discrete string (max 8 chars each):

        >>> meta2 = meta.update(variable_missing_values={
        ...     "city": {"type": "discrete", "values": ["N/A", "DK", "RF"]},
        ... })

        Missing values — range (numeric only):

        >>> meta2 = meta.update(variable_missing_values={
        ...     "score": {"type": "range", "low": 900, "high": 999},
        ... })

        Missing values — range + one discrete value:

        >>> meta2 = meta.update(variable_missing_values={
        ...     "income": {"type": "range", "low": 999990, "high": 999999, "discrete": 0},
        ... })

        Variable roles:

        >>> meta2 = meta.update(variable_roles={
        ...     "age": "input", "satisfaction": "target", "weight": "none",
        ... })

        Variable attributes (custom key-value metadata):

        >>> meta2 = meta.update(variable_attributes={
        ...     "Q1": {"Source": ["Survey"]},
        ... })

        Variable attributes with multiple values:

        >>> meta2 = meta.update(variable_attributes={
        ...     "Q1": {"Source": ["Survey", "Online", "Wave 3"], "Section": ["Satisfaction"]},
        ... })

        Remove all attributes for a variable:

        >>> meta2 = meta.update(variable_attributes={"Q1": None})

        MR sets — multiple dichotomy (binary yes/no):

        >>> meta2 = meta.update(mr_sets={
        ...     "brands": {
        ...         "label": "Brands Mentioned",
        ...         "type": "dichotomy",
        ...         "counted_value": "1",
        ...         "variables": ["brand_a", "brand_b", "brand_c"],
        ...     }
        ... })

        MR sets — multiple category:

        >>> meta2 = meta.update(mr_sets={
        ...     "hobbies": {
        ...         "label": "Hobbies Selected",
        ...         "type": "category",
        ...         "variables": ["hobby1", "hobby2", "hobby3"],
        ...     }
        ... })

        Remove an MR set:

        >>> meta2 = meta.update(mr_sets={"brands": None})
        """
        ...

    def with_file_label(self, value: str) -> SpssMetadata:
        """Return a new SpssMetadata with the file label set."""
        ...
    def with_notes(self, value: str | list[str]) -> SpssMetadata:
        """Return a new SpssMetadata with file notes replaced."""
        ...
    def with_weight_variable(self, value: str | None) -> SpssMetadata:
        """Return a new SpssMetadata with the weight variable set (or cleared with None)."""
        ...
    def with_variable_labels(self, value: dict[str, str | None]) -> SpssMetadata:
        """Return a new SpssMetadata with variable labels merged. Pass ``{var: None}`` to remove."""
        ...
    def with_variable_value_labels(self, value: dict[str, dict[int | float | str, str] | None]) -> SpssMetadata:
        """Return a new SpssMetadata with value labels merged. Maps numeric/string values to display labels."""
        ...
    def with_variable_formats(self, value: dict[str, str | None]) -> SpssMetadata:
        """Return a new SpssMetadata with SPSS format strings merged (e.g. "F8.2", "A50", "DATE11")."""
        ...
    def with_variable_measures(self, value: dict[str, str | None]) -> SpssMetadata:
        """Return a new SpssMetadata with measurement levels merged. Values: "nominal", "ordinal", "scale"."""
        ...
    def with_variable_display_widths(self, value: dict[str, int | None]) -> SpssMetadata:
        """Return a new SpssMetadata with display widths merged (positive integers)."""
        ...
    def with_variable_alignments(self, value: dict[str, str | None]) -> SpssMetadata:
        """Return a new SpssMetadata with alignments merged. Values: "left", "right", "center"."""
        ...
    def with_variable_missing_values(self, value: dict[str, dict | None]) -> SpssMetadata:
        """Return a new SpssMetadata with missing value specs merged. See metadata.md for dict format."""
        ...
    def with_variable_roles(self, value: dict[str, str | None]) -> SpssMetadata:
        """Return a new SpssMetadata with roles merged. Values: "input", "target", "both", "none", "partition", "split"."""
        ...
    def with_variable_attributes(self, value: dict[str, dict[str, list[str]] | None]) -> SpssMetadata:
        """Return a new SpssMetadata with custom variable attributes merged."""
        ...
    def with_mr_sets(self, value: dict[str, dict | None]) -> SpssMetadata:
        """Return a new SpssMetadata with multiple response sets merged. See metadata.md for dict format."""
        ...

    # Properties (read-only)
    @property
    def file_label(self) -> str:
        """File label set in SPSS (truncated to 64 bytes in SAV format)."""
        ...
    @property
    def file_encoding(self) -> str:
        """Character encoding of the file (e.g. "UTF-8"). Always UTF-8 for writes."""
        ...
    @property
    def compression(self) -> str:
        """Compression type: "uncompressed", "bytecode" (.sav), or "zlib" (.zsav)."""
        ...
    @property
    def creation_time(self) -> str:
        """File creation timestamp (e.g. "2026-02-21 12:38:47"). Auto-set at write time."""
        ...
    @property
    def notes(self) -> list[str]:
        """Document record notes (list of strings)."""
        ...
    @property
    def number_rows(self) -> int | None:
        """Row count from the file header, or None if not recorded."""
        ...
    @property
    def number_columns(self) -> int:
        """Number of visible variables (columns) in the file."""
        ...
    @property
    def file_format(self) -> str:
        """File format: "sav" or "zsav"."""
        ...
    @property
    def variable_names(self) -> list[str]:
        """Ordered list of variable (column) names."""
        ...
    @property
    def variable_labels(self) -> dict[str, str]:
        """Variable name to descriptive label mapping."""
        ...
    @property
    def variable_formats(self) -> dict[str, str]:
        """Variable name to SPSS format string mapping (e.g. "F8.2", "A50", "DATE11")."""
        ...
    @property
    def arrow_data_types(self) -> dict[str, str]:
        """Variable name to Arrow data type mapping (e.g. "f64", "String", "Date32")."""
        ...
    @property
    def variable_value_labels(self) -> dict[str, dict[float | str, str]]:
        """Variable name to value-label dict mapping (e.g. {1.0: "Male", 2.0: "Female"})."""
        ...
    @property
    def variable_alignments(self) -> dict[str, str]:
        """Variable name to alignment mapping: "left", "right", or "center"."""
        ...
    @property
    def variable_storage_widths(self) -> dict[str, int]:
        """Variable name to storage width in bytes (computed from format)."""
        ...
    @property
    def variable_display_widths(self) -> dict[str, int]:
        """Variable name to display width mapping."""
        ...
    @property
    def variable_measures(self) -> dict[str, str]:
        """Variable name to measurement level mapping: "nominal", "ordinal", or "scale"."""
        ...
    @property
    def variable_missing_values(self) -> dict[str, dict]:
        """Variable name to missing value specification mapping."""
        ...
    @property
    def mr_sets(self) -> dict[str, dict]:
        """Multiple response set definitions."""
        ...
    @property
    def variable_roles(self) -> dict[str, str]:
        """Variable name to role mapping: "input", "target", "both", "none", "partition", "split"."""
        ...
    @property
    def variable_attributes(self) -> dict[str, dict[str, list[str]]]:
        """Variable name to custom attributes mapping (e.g. {"Source": ["Survey"]})."""
        ...
    @property
    def weight_variable(self) -> str | None:
        """Name of the weight variable, or None if no weighting."""
        ...
    @property
    def schema(self) -> dict:
        """Full metadata as a nested Python dict (22 fields).

        Field order: file_label, file_format, file_encoding, creation_time,
        compression, number_columns, number_rows, weight_variable, notes,
        variable_names, variable_labels, variable_value_labels,
        variable_formats, variable_measures, variable_alignments,
        variable_storage_widths, variable_display_widths, variable_roles,
        variable_missing_values, variable_attributes, mr_sets, arrow_data_types.
        """
        ...

    # Convenience methods
    def label(self, name: str) -> str | None:
        """Get the variable label for a single variable, or None if not set.

        Parameters
        ----------
        name
            Variable name (must exist in metadata).

        Raises
        ------
        KeyError
            If the variable name is not found.
        """
        ...
    def format(self, name: str) -> str | None:
        """Get the SPSS format string for a variable (e.g. "F8.2"), or None if not set.

        Parameters
        ----------
        name
            Variable name (must exist in metadata).

        Raises
        ------
        KeyError
            If the variable name is not found.
        """
        ...
    def measure(self, name: str) -> str | None:
        """Get the measurement level for a variable ("nominal", "ordinal", "scale"), or None.

        Parameters
        ----------
        name
            Variable name (must exist in metadata).

        Raises
        ------
        KeyError
            If the variable name is not found.
        """
        ...
    def role(self, name: str) -> str | None:
        """Get the role for a variable ("input", "target", "both", "none", "partition", "split"), or None.

        Parameters
        ----------
        name
            Variable name (must exist in metadata).

        Raises
        ------
        KeyError
            If the variable name is not found.
        """
        ...
    def attribute(self, name: str, attr: str | None = None) -> dict[str, list[str]] | list[str] | None:
        """Get custom attributes for a variable.

        Parameters
        ----------
        name
            Variable name (must exist in metadata).
        attr
            If provided, return values for this specific attribute (raises KeyError if missing).
            If omitted, return all attributes as a dict, or None if no attributes set.

        Raises
        ------
        KeyError
            If the variable name or specific attribute is not found.
        """
        ...
    def value(self, name: str) -> dict[float | str, str] | None:
        """Get the value labels dict for a variable, or None if not set.

        Parameters
        ----------
        name
            Variable name (must exist in metadata).

        Raises
        ------
        KeyError
            If the variable name is not found.
        """
        ...
    def check_var(self, name: str) -> None:
        """Raise KeyError if the variable name is not in metadata.

        Parameters
        ----------
        name
            Variable name to check.

        Raises
        ------
        KeyError
            If the variable name is not found.
        """
        ...
    def summary(self) -> None:
        """Print a formatted overview of all metadata: file info, type distribution, and annotations."""
        ...
    def describe(self, names: str | list[str]) -> None:
        """Print a detailed view of one or more variables.

        Parameters
        ----------
        names
            A single variable name or list of variable names.
        """
        ...
    def diff(
        self, other: SpssMetadata, print_output: bool = True
    ) -> MetaDiff:
        """Compare this metadata with another and return a MetaDiff object.

        Parameters
        ----------
        other
            The SpssMetadata to compare against.
        print_output
            If True (default), print a formatted diff summary.
        """
        ...

class MetaDiff:
    """Result of comparing two SpssMetadata objects via ``meta.diff(other)``."""

    @property
    def is_match(self) -> bool:
        """True if both metadata objects are identical."""
        ...
    @property
    def file_level(self) -> dict:
        """File-level differences (file_label, notes, weight_variable, etc.)."""
        ...
    @property
    def variables_only_in_self(self) -> list[str]:
        """Variables present in self but not in other."""
        ...
    @property
    def variables_only_in_other(self) -> list[str]:
        """Variables present in other but not in self."""
        ...
    @property
    def variable_labels(self) -> list[dict]:
        """Variables with differing labels."""
        ...
    @property
    def variable_value_labels(self) -> list[dict]:
        """Variables with differing value labels."""
        ...
    @property
    def variable_formats(self) -> list[dict]:
        """Variables with differing format strings."""
        ...
    @property
    def variable_measures(self) -> list[dict]:
        """Variables with differing measurement levels."""
        ...
    @property
    def variable_display_widths(self) -> list[dict]:
        """Variables with differing display widths."""
        ...
    @property
    def variable_storage_widths(self) -> list[dict]:
        """Variables with differing storage widths."""
        ...
    @property
    def variable_missing_values(self) -> list[dict]:
        """Variables with differing missing value specs."""
        ...
    @property
    def mr_sets(self) -> list[dict]:
        """Multiple response sets that differ."""
        ...
    @property
    def variable_roles(self) -> list[dict]:
        """Variables with differing roles."""
        ...
    @property
    def variable_attributes(self) -> list[dict]:
        """Variables with differing custom attributes."""
        ...
    def print_summary(self) -> None:
        """Print a formatted summary of all differences."""
        ...

def read_sav(
    path: str,
    *,
    columns: list[int] | list[str] | None = None,
    n_rows: int | None = None,
    row_index_name: str | None = None,
    row_index_offset: int = 0,
) -> tuple[polars.DataFrame, SpssMetadata]:
    """Read an SPSS .sav/.zsav file into a Polars DataFrame with metadata.

    Parameters
    ----------
    path
        Path to the .sav or .zsav file.
    columns
        Select specific columns by name or index. None reads all columns.
    n_rows
        Maximum number of rows to read. None reads all rows.
    row_index_name
        If set, adds a row index column with this name.
    row_index_offset
        Starting value for the row index (default 0).

    Returns
    -------
    tuple[polars.DataFrame, SpssMetadata]

    Examples
    --------
    >>> df, meta = am.read_sav("survey.sav")
    >>> df, meta = am.read_sav("survey.sav", columns=["Q1", "Q2"], n_rows=1000)
    """
    ...

def read_sav_metadata(path: str) -> SpssMetadata:
    """Read only the metadata from an SPSS .sav/.zsav file (fast, skips data).

    Parameters
    ----------
    path
        Path to the .sav or .zsav file.

    Returns
    -------
    SpssMetadata
    """
    ...

def scan_sav(
    path: str,
    *,
    columns: list[int] | list[str] | None = None,
    n_rows: int | None = None,
    row_index_name: str | None = None,
    row_index_offset: int = 0,
) -> tuple[polars.LazyFrame, SpssMetadata]:
    """Lazily scan an SPSS .sav/.zsav file, returning a Polars LazyFrame.

    Supports column projection and row limit pushdown — only reads the data you
    ask for when you call ``.collect()``.

    Parameters
    ----------
    path
        Path to the .sav or .zsav file.
    columns
        Select specific columns by name or index. None reads all columns.
    n_rows
        Maximum number of rows to read. None reads all rows.
    row_index_name
        If set, adds a row index column with this name.
    row_index_offset
        Starting value for the row index (default 0).

    Returns
    -------
    tuple[polars.LazyFrame, SpssMetadata]

    Examples
    --------
    >>> lf, meta = am.scan_sav("survey.sav")
    >>> df = lf.select(["Q1", "Q2"]).head(1000).collect()
    """
    ...

def write_sav(
    df: polars.DataFrame,
    path: str | Path,
    *,
    meta: SpssMetadata | None = None,
    compression: str | None = None,
    compression_level: int | None = None,
) -> None:
    """Write a Polars DataFrame to an SPSS .sav or .zsav file.

    If ``meta`` is omitted, formats, measures, and other properties are inferred
    from the DataFrame schema.

    Parameters
    ----------
    df
        The Polars DataFrame to write.
    path
        Output file path. Extension determines format (.sav or .zsav).
    meta
        SpssMetadata to include. If None, defaults are inferred.
    compression
        Compression mode. If None, auto-detects from extension
        (.sav → bytecode, .zsav → zlib). Valid values:

        - ``"uncompressed"`` — no compression (.sav only)
        - ``"bytecode"`` — SPSS bytecode compression (.sav only)
        - ``"zlib"`` — zlib block compression (.zsav only)
    compression_level
        Zlib compression level for .zsav files (1–9).
        Recommended values: 1 = "fast", 3 = "balanced", 6 = "compact" (default).
        If None, defaults to 6 (compact). Raises ValueError for non-zlib output.

    Raises
    ------
    ValueError
        If compression mode is invalid for the file extension, or
        compression_level is set for non-zlib output.

    Examples
    --------
    >>> am.write_sav(df, "output.sav", meta=meta)                       # bytecode
    >>> am.write_sav(df, "output.zsav", meta=meta)                      # zlib (level 6)
    >>> am.write_sav(df, "output.zsav", meta=meta, compression_level=1) # fast zlib
    >>> am.write_sav(df, "raw.sav", meta=meta, compression="uncompressed")
    """
    ...
