"""ambers: Pure Rust SPSS .sav/.zsav reader."""

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


def read_sav(path: str) -> tuple:
    """Read an SPSS .sav or .zsav file.

    Returns a tuple of (Polars DataFrame, SpssMetadata).

    Args:
        path: Path to the .sav or .zsav file.

    Returns:
        A tuple (df, meta) where df is a polars.DataFrame and meta is
        an SpssMetadata object with all variable metadata.
    """
    import polars as pl

    stream, meta = _read_sav(str(path))
    df = pl.from_arrow(stream)
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


def scan_sav(path: str) -> tuple:
    """Create a LazyFrame from an SPSS .sav or .zsav file.

    Supports projection pushdown (column selection), row limit pushdown,
    and per-batch predicate filtering. Use .collect() to materialize.

    Args:
        path: Path to the .sav or .zsav file.

    Returns:
        A tuple (lf, meta) where lf is a polars.LazyFrame and meta is
        an SpssMetadata object with all variable metadata.
    """
    import polars as pl
    from polars.io.plugins import register_io_source

    # Read schema eagerly (fast — only parses the dictionary, no data)
    reader = _SavBatchReader(str(path))
    meta = reader.metadata()
    raw_schema = reader.schema()

    dtype_map = {
        "Float64": pl.Float64,
        "String": pl.String,
    }
    schema = pl.Schema(
        {
            name: dtype_map.get(dtype, pl.String)
            for name, dtype in raw_schema.items()
        }
    )

    def _source(with_columns, predicate, n_rows, batch_size):
        scanner = _SavBatchReader(str(path), batch_size=batch_size or 100_000)

        if with_columns is not None:
            scanner.select(with_columns)
        if n_rows is not None:
            scanner.limit(n_rows)

        while (batch := scanner.next_batch()) is not None:
            df = pl.from_arrow(batch)
            if predicate is not None:
                df = df.filter(predicate)
            yield df

    lf = register_io_source(io_source=_source, schema=schema)
    return lf, meta
