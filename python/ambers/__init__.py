"""ambers: Pure Rust SPSS .sav/.zsav reader."""

from ambers._ambers import SpssMetadata, _read_sav, _read_sav_metadata

__all__ = ["read_sav", "read_sav_metadata", "SpssMetadata"]


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

    batch, meta = _read_sav(str(path))
    df = pl.from_arrow(batch)
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
