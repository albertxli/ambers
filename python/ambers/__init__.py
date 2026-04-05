"""ambers: Pure Rust SPSS .sav/.zsav reader and writer."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Generic, TypeVar, Union

from ambers._ambers import (
    MetaDiff,
    SpssMetadata,
    _SavBatchReader,
    _read_sav,
    _read_sav_meta,
    _write_sav,
)

import polars as pl

T = TypeVar("T", pl.DataFrame, pl.LazyFrame)


@dataclass
class SavFile(Generic[T]):
    """Result of reading an SPSS .sav/.zsav file.

    Attributes:
        data: A Polars DataFrame (from ``read_sav``) or LazyFrame
            (from ``scan_sav``).
        meta: An ``SpssMetadata`` object with all variable metadata.
    """

    data: T
    meta: SpssMetadata

    def __repr__(self) -> str:
        data_type = type(self.data).__name__
        if isinstance(self.data, pl.DataFrame):
            shape = f"{self.data.height} rows x {self.data.width} cols"
        else:
            shape = f"{self.data.collect_schema().len()} cols (lazy)"
        n_vars = len(self.meta.variable_names)
        return f"SavFile({data_type}, {shape}, {n_vars} variables)"


__all__ = [
    "SavFile",
    "read_sav",
    "read_sav_meta",
    "scan_sav",
    "write_sav",
    "SpssMetadata",
    "MetaDiff",
]

_DTYPE_MAP: dict | None = None


def _get_dtype_map():
    global _DTYPE_MAP
    if _DTYPE_MAP is None:
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
) -> SavFile[pl.DataFrame]:
    """Read an SPSS .sav or .zsav file.

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
        A ``SavFile`` with ``.data`` (polars.DataFrame) and ``.meta``
        (SpssMetadata).

    Examples:
        >>> sav = am.read_sav("survey.sav")
        >>> sav.data.head()
        >>> sav.meta.variable_labels["Q1"]
    """
    if row_index_offset < 0:
        raise ValueError("row_index_offset cannot be negative")

    # Resolve int indices to column names (requires metadata lookup)
    resolved = columns
    if columns is not None and len(columns) > 0 and isinstance(columns[0], int):
        meta_tmp = _read_sav_meta(str(path))
        resolved = _resolve_columns(columns, meta_tmp.variable_names)
    elif columns is not None and len(columns) == 0:
        resolved = None

    stream, meta = _read_sav(str(path), columns=resolved, n_rows=n_rows)
    df = pl.from_arrow(stream)
    if row_index_name is not None:
        df = df.with_row_index(row_index_name, offset=row_index_offset)
    return SavFile(data=df, meta=meta)


def read_sav_meta(path: str) -> SpssMetadata:
    """Read only the metadata from an SPSS file (no data).

    This is much faster than ``read_sav()`` when you only need variable
    information, labels, or other metadata.

    Args:
        path: Path to the .sav or .zsav file.

    Returns:
        An ``SpssMetadata`` object.

    Examples:
        >>> meta = am.read_sav_meta("survey.sav")
        >>> meta.variable_names
        >>> meta.label("Q1")
    """
    return _read_sav_meta(str(path))


def scan_sav(
    path: str,
    *,
    columns: list[int] | list[str] | None = None,
    n_rows: int | None = None,
    row_index_name: str | None = None,
    row_index_offset: int = 0,
) -> SavFile[pl.LazyFrame]:
    """Create a LazyFrame from an SPSS .sav or .zsav file.

    Supports projection pushdown (column selection), row limit pushdown,
    and per-batch predicate filtering. Use ``.data.collect()`` to
    materialize.

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
        A ``SavFile`` with ``.data`` (polars.LazyFrame) and ``.meta``
        (SpssMetadata).

    Examples:
        >>> sav = am.scan_sav("survey.sav")
        >>> df = sav.data.select(["Q1", "Q2"]).head(1000).collect()
    """
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
    return SavFile(data=lf, meta=meta)


def write_sav(
    df,
    path: str | Path,
    *,
    meta: SpssMetadata | None = None,
    compression: str | None = None,
    compression_level: int | None = None,
) -> None:
    """Write a Polars DataFrame to an SPSS .sav or .zsav file.

    Supports three workflows:

    1. **Roundtrip** -- pass the ``meta`` from a prior ``read_sav()``::

        sav = am.read_sav("input.sav")
        am.write_sav(sav.data, "output.sav", meta=sav.meta)

    2. **From scratch** -- build metadata with ``SpssMetadata()``::

        meta = am.SpssMetadata(
            variable_labels={"Q1": "Satisfaction"},
            variable_measures={"Q1": "ordinal"},
        )
        am.write_sav(df, "output.sav", meta=meta)

    3. **Inferred** -- metadata is inferred from the DataFrame::

        am.write_sav(df, "new.sav")

    Missing metadata fields are automatically filled from the DataFrame
    schema at write time (formats, measures, alignments, etc.).

    Args:
        df: A Polars DataFrame (or any object implementing
            ``__arrow_c_stream__``).
        path: Output file path. Use ``.zsav`` extension for zlib
            compression.
        meta: An ``SpssMetadata`` object. Can be from ``read_sav()``,
            constructed with ``SpssMetadata()``, or built via
            ``.update()``/``.with_*()`` methods. If None, metadata
            is inferred from the DataFrame schema.
        compression: Compression mode. Valid values:

            - ``None`` — auto-detect from extension (``.sav`` → bytecode,
              ``.zsav`` → zlib)
            - ``"uncompressed"`` — no compression (``.sav`` only)
            - ``"bytecode"`` — SPSS bytecode compression (``.sav`` only)
            - ``"zlib"`` — zlib block compression (``.zsav`` only)

        compression_level: Zlib compression level for ``.zsav`` files
            (1–9). Recommended values:

            - ``1`` — "fast": fastest writes, larger files
            - ``3`` — "balanced": moderate speed, moderate size
            - ``6`` — "compact": slower writes, smallest files (default)

            If None, defaults to 6 (compact).

    Raises:
        ValueError: If ``compression`` is invalid for the file extension,
            or ``compression_level`` is set for non-zlib output.
    """
    path = str(path)
    is_zsav = path.lower().endswith(".zsav")

    # Validate and resolve compression mode
    if compression is None:
        mode = "zlib" if is_zsav else "bytecode"
    elif compression == "zlib":
        if not is_zsav:
            raise ValueError(
                "zlib compression requires .zsav extension. "
                "Use compression='bytecode' for .sav files, "
                "or save as .zsav for zlib."
            )
        mode = "zlib"
    elif compression == "bytecode":
        if is_zsav:
            raise ValueError(
                ".zsav format requires zlib compression. "
                "Use compression='zlib' or save as .sav for bytecode."
            )
        mode = "bytecode"
    elif compression == "uncompressed":
        if is_zsav:
            raise ValueError(
                ".zsav format requires zlib compression. "
                "Use compression='zlib' or save as .sav for uncompressed."
            )
        mode = "uncompressed"
    else:
        raise ValueError(
            f"Invalid compression: '{compression}'. "
            "Expected 'uncompressed', 'bytecode', 'zlib', or None."
        )

    # compression_level only valid for zlib
    if compression_level is not None and mode != "zlib":
        raise ValueError(
            "compression_level only applies to .zsav (zlib) output. "
            "Remove compression_level for .sav files."
        )

    _write_sav(path, df, metadata=meta, compression=mode,
               compression_level=compression_level)
