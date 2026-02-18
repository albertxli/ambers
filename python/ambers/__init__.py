"""ambers: Pure Rust SPSS .sav/.zsav reader."""

from __future__ import annotations

from ambers._ambers import (
    MetaDiff,
    SpssMetadata,
    _SavBatchReader,
    _read_sav,
    _read_sav_metadata,
)

__all__ = [
    "read_sav",
    "read_sav_metadata",
    "scan_sav",
    "SpssMetadata",
    "MetaDiff",
]

_DTYPE_MAP: dict | None = None


def _get_dtype_map():
    global _DTYPE_MAP
    if _DTYPE_MAP is None:
        import polars as pl

        _DTYPE_MAP = {
            "Float64": pl.Float64,
            "String": pl.String,
            "Date": pl.Date,
            "Datetime": pl.Datetime("us"),
            "Duration": pl.Duration("us"),
        }
    return _DTYPE_MAP


def _resolve_columns(
    columns: list[int] | list[str] | None,
    variable_names: list[str],
) -> list[str] | None:
    """Resolve columns param: None->None, []->None, list[int]->list[str]."""
    if columns is None or len(columns) == 0:
        return None
    if isinstance(columns[0], int):
        return [variable_names[i] for i in columns]
    return columns


def read_sav(
    path: str,
    *,
    columns: list[int] | list[str] | None = None,
    n_rows: int | None = None,
    row_index_name: str | None = None,
    row_index_offset: int = 0,
) -> tuple:
    """Read an SPSS .sav or .zsav file.

    Returns a tuple of (Polars DataFrame, SpssMetadata).

    Args:
        path: Path to the .sav or .zsav file.
        columns: Columns to select. Accepts a list of column indices
            (starting at zero) or a list of column names. None or []
            reads all columns.
        n_rows: Maximum number of rows to read. None reads all rows.
        row_index_name: Insert a row index column with the given name
            into the DataFrame as the first column. If None (default),
            no row index column is created.
        row_index_offset: Start the row index at this offset.
            Cannot be negative. Only used if row_index_name is set.

    Returns:
        A tuple (df, meta) where df is a polars.DataFrame and meta is
        an SpssMetadata object with all variable metadata.
    """
    import polars as pl

    if row_index_offset < 0:
        raise ValueError("row_index_offset cannot be negative")

    # Resolve int indices to column names (requires metadata lookup)
    resolved = columns
    if columns is not None and len(columns) > 0 and isinstance(columns[0], int):
        meta_tmp = _read_sav_metadata(str(path))
        resolved = _resolve_columns(columns, meta_tmp.variable_names)
    elif columns is not None and len(columns) == 0:
        resolved = None

    stream, meta = _read_sav(str(path), columns=resolved, n_rows=n_rows)
    df = pl.from_arrow(stream)
    if row_index_name is not None:
        df = df.with_row_index(row_index_name, offset=row_index_offset)
    return df, meta


def read_sav_metadata(path: str) -> SpssMetadata:
    """Read only the metadata from an SPSS file (no data).

    This is much faster than read_sav() when you only need variable
    information, labels, or other metadata.

    Args:
        path: Path to the .sav or .zsav file.

    Returns:
        An SpssMetadata object.
    """
    return _read_sav_metadata(str(path))


def scan_sav(
    path: str,
    *,
    columns: list[int] | list[str] | None = None,
    n_rows: int | None = None,
    row_index_name: str | None = None,
    row_index_offset: int = 0,
) -> tuple:
    """Create a LazyFrame from an SPSS .sav or .zsav file.

    Supports projection pushdown (column selection), row limit pushdown,
    and per-batch predicate filtering. Use .collect() to materialize.

    Args:
        path: Path to the .sav or .zsav file.
        columns: Columns to select. Accepts a list of column indices
            (starting at zero) or a list of column names. None or []
            includes all columns. Polars may further narrow via
            projection pushdown.
        n_rows: Maximum number of rows to read. None reads all rows.
            Polars' .head() pushdown uses the smaller of this and its
            own limit.
        row_index_name: Insert a row index column with the given name
            into the DataFrame as the first column. If None (default),
            no row index column is created.
        row_index_offset: Start the row index at this offset.
            Cannot be negative. Only used if row_index_name is set.

    Returns:
        A tuple (lf, meta) where lf is a polars.LazyFrame and meta is
        an SpssMetadata object with all variable metadata.
    """
    import polars as pl
    from polars.io.plugins import register_io_source

    if row_index_offset < 0:
        raise ValueError("row_index_offset cannot be negative")

    dtype_map = _get_dtype_map()

    # Read schema eagerly (fast — only parses the dictionary, no data)
    reader = _SavBatchReader(str(path))
    meta = reader.metadata()
    raw_schema = reader.schema()

    # Resolve int indices to column names
    resolved = _resolve_columns(columns, meta.variable_names)

    # Filter schema if columns specified
    if resolved is not None:
        schema = pl.Schema(
            {name: dtype_map[raw_schema[name]] for name in resolved}
        )
    else:
        schema = pl.Schema(
            {
                name: dtype_map.get(dtype, pl.String)
                for name, dtype in raw_schema.items()
            }
        )

    user_columns = resolved
    user_n_rows = n_rows

    def _source(with_columns, predicate, n_rows, batch_size):
        scanner = _SavBatchReader(str(path), batch_size=batch_size or 100_000)

        # Combine user columns with Polars pushdown columns
        effective_columns = with_columns if with_columns is not None else user_columns
        if effective_columns is not None:
            scanner.select(effective_columns)

        # Combine user n_rows with Polars pushdown n_rows (take minimum)
        limits = [l for l in [user_n_rows, n_rows] if l is not None]
        if limits:
            scanner.limit(min(limits))

        while (batch := scanner.next_batch()) is not None:
            df = pl.from_arrow(batch)
            if predicate is not None:
                df = df.filter(predicate)
            yield df

    lf = register_io_source(io_source=_source, schema=schema)
    if row_index_name is not None:
        lf = lf.with_row_index(row_index_name, offset=row_index_offset)
    return lf, meta
