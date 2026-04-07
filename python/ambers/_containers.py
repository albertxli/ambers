"""SavFile container and internal helpers."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Generic, TypeVar

import polars as pl

from ambers._ambers import SpssMetadata

T = TypeVar("T", pl.DataFrame, pl.LazyFrame)


def _format_size(n_bytes: int) -> str:
    """Format byte count as human-readable string (e.g. ``147.2 MB``)."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n_bytes) < 1024.0 or unit == "TB":
            return f"{n_bytes:.1f} {unit}" if unit != "B" else f"{n_bytes} B"
        n_bytes /= 1024.0
    return f"{n_bytes:.1f} TB"


@dataclass
class SavFile(Generic[T]):
    """Result of reading an SPSS .sav/.zsav file.

    Attributes:
        data: A Polars DataFrame (from ``read_sav``) or LazyFrame
            (from ``scan_sav``).
        meta: An ``SpssMetadata`` object with all variable metadata.
        source: Source file path, or None if constructed from in-memory data.
        shape: ``(n_rows, n_cols)`` tuple, or None if unknown.
        file_size: Size of the source file in bytes, or None if not
            read from a file.
        read_time: Wall-clock seconds for the read operation, or None if
            not measured. For ``scan_sav`` this covers metadata/schema
            reading only (not lazy collection).
    """

    data: T
    meta: SpssMetadata
    source: str | None = None
    shape: tuple[int, int] | None = None
    file_size: int | None = None
    read_time: float | None = None

    @property
    def compression(self) -> str:
        """Compression type of the source file: ``"uncompressed"``, ``"bytecode"``, or ``"zlib"``."""
        return self.meta.compression

    def __repr__(self) -> str:
        lines: list[tuple[str, str]] = []
        # Data line
        if isinstance(self.data, pl.DataFrame):
            lines.append(("Data", "DataFrame (polars)"))
        else:
            lines.append(("Data", "LazyFrame (polars)"))
        # Shape line
        if self.shape is not None:
            n_rows, n_cols = self.shape
            lines.append(("Shape", f"{n_rows:,} rows x {n_cols} cols"))
        elif not isinstance(self.data, pl.DataFrame):
            n_cols = self.data.collect_schema().len()
            lines.append(("Shape", f"{n_cols} cols"))
        # Source line
        if self.source is not None:
            lines.append(("Source", os.path.basename(self.source)))
        # File size + compression line
        if self.file_size is not None:
            size_str = _format_size(self.file_size)
            lines.append(("File size", f"{size_str}, {self.compression}"))
        # Read time line
        if self.read_time is not None:
            time_str = f"{self.read_time:.3f}s"
            if not isinstance(self.data, pl.DataFrame):
                time_str += " (metadata only)"
            lines.append(("Read time", time_str))
        # Build box
        label_w = max(len(label) for label, _ in lines)
        content_parts = [f"{label:<{label_w}}   {value}" for label, value in lines]
        inner_w = max(len(p) for p in content_parts)
        prefix = "\u250c\u2500 SavFile "  # 11 chars
        box_w = inner_w + 4  # "│ " + content + " │"
        header = prefix + "\u2500" * (box_w - len(prefix) - 1) + "\u2510"
        footer = "\u2514" + "\u2500" * (box_w - 2) + "\u2518"
        rows = [header]
        for part in content_parts:
            rows.append(f"\u2502 {part:<{inner_w}} \u2502")
        rows.append(footer)
        return "\n".join(rows)


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
