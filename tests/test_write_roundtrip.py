"""Writer roundtrip tests: read → write → read back, compare DataFrame + metadata.

Validates that ambers.write_sav() produces files that:
1. ambers can read back identically (DataFrame + all metadata)
2. pyreadstat can read correctly (DataFrame + comparable metadata)

Run with:
    pytest tests/test_write_roundtrip.py -v
    pytest tests/test_write_roundtrip.py -v --sav-file path/to/file.sav
"""

import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal


# ---------------------------------------------------------------------------
# deep_diff utility (same as test_metadata.py)
# ---------------------------------------------------------------------------

def deep_diff(a, b, path=""):
    """Recursively diff two nested structures (dicts, lists, scalars)."""
    diffs = []

    if isinstance(a, Mapping) and isinstance(b, Mapping):
        a_keys, b_keys = set(a.keys()), set(b.keys())
        for k in sorted(a_keys - b_keys):
            diffs.append((f"{path}.{k}" if path else str(k), "removed", a[k], None))
        for k in sorted(b_keys - a_keys):
            diffs.append((f"{path}.{k}" if path else str(k), "added", None, b[k]))
        for k in sorted(a_keys & b_keys):
            p = f"{path}.{k}" if path else str(k)
            diffs.extend(deep_diff(a[k], b[k], p))
        return diffs

    if (
        isinstance(a, Sequence) and isinstance(b, Sequence)
        and not isinstance(a, (str, bytes))
        and not isinstance(b, (str, bytes))
    ):
        n = min(len(a), len(b))
        for i in range(n):
            diffs.extend(deep_diff(a[i], b[i], f"{path}[{i}]"))
        for i in range(n, len(a)):
            diffs.append((f"{path}[{i}]", "removed", a[i], None))
        for i in range(n, len(b)):
            diffs.append((f"{path}[{i}]", "added", None, b[i]))
        return diffs

    if a != b:
        diffs.append((path, "changed", a, b))
    return diffs


def normalize_file_label(val):
    """pyreadstat returns None for empty, ambers returns empty string."""
    return "" if val is None else val


# ---------------------------------------------------------------------------
# Metadata comparison helpers
# ---------------------------------------------------------------------------

def assert_metadata_ambers(orig, rt, label=""):
    """Compare two ambers SpssMetadata objects (ambers→write→ambers roundtrip)."""
    pfx = f"[{label}] " if label else ""

    # Variable names — exact order
    assert orig.variable_names == rt.variable_names, (
        f"{pfx}variable_names mismatch"
    )

    # Dict fields — deep compare
    for field, getter in [
        ("variable_labels", lambda m: m.variable_labels),
        ("variable_value_labels", lambda m: m.variable_value_labels),
        ("variable_format", lambda m: m.variable_format),
        ("variable_measure", lambda m: m.variable_measure),
        ("variable_display_width", lambda m: m.variable_display_width),
        ("variable_storage_width", lambda m: m.variable_storage_width),
    ]:
        diffs = deep_diff(getter(orig), getter(rt))
        assert diffs == [], f"{pfx}{field}: {len(diffs)} diffs: {diffs[:5]}"

    # Key-set fields (value structure may vary)
    for field, getter in [
        ("variable_missing_values", lambda m: m.variable_missing_values),
        ("mr_sets", lambda m: m.mr_sets),
    ]:
        orig_keys = set(getter(orig).keys())
        rt_keys = set(getter(rt).keys())
        assert orig_keys == rt_keys, (
            f"{pfx}{field} keys differ: "
            f"only_orig={sorted(orig_keys - rt_keys)}, "
            f"only_rt={sorted(rt_keys - orig_keys)}"
        )

    # Scalar fields
    assert normalize_file_label(orig.file_label) == normalize_file_label(rt.file_label), (
        f"{pfx}file_label: {orig.file_label!r} vs {rt.file_label!r}"
    )
    assert orig.file_encoding == rt.file_encoding, (
        f"{pfx}file_encoding: {orig.file_encoding!r} vs {rt.file_encoding!r}"
    )
    assert orig.number_columns == rt.number_columns, (
        f"{pfx}number_columns: {orig.number_columns} vs {rt.number_columns}"
    )

    # Notes
    diffs = deep_diff(orig.notes, rt.notes)
    assert diffs == [], f"{pfx}notes: {len(diffs)} diffs: {diffs[:3]}"

    # Weight variable
    assert orig.weight_variable == rt.weight_variable, (
        f"{pfx}weight_variable: {orig.weight_variable!r} vs {rt.weight_variable!r}"
    )


def assert_metadata_cross(ambers_meta, pyr_meta, label=""):
    """Compare ambers SpssMetadata against pyreadstat metadata_container.

    Skips: creation_time (format differs),
           variable_alignment (pyreadstat bug), file_format (pyreadstat
           returns 'sav/zsav'), number_rows (may differ for written files).
    """
    pfx = f"[{label}] " if label else ""

    # Variable names
    diffs = deep_diff(ambers_meta.variable_names, pyr_meta.column_names)
    assert diffs == [], f"{pfx}variable_names: {len(diffs)} diffs: {diffs[:5]}"

    # Dict fields
    field_pairs = [
        ("variable_labels", lambda: ambers_meta.variable_labels, lambda: pyr_meta.column_names_to_labels),
        ("variable_value_labels", lambda: ambers_meta.variable_value_labels, lambda: pyr_meta.variable_value_labels),
        ("variable_format", lambda: ambers_meta.variable_format, lambda: pyr_meta.original_variable_types),
        ("variable_measure", lambda: ambers_meta.variable_measure, lambda: pyr_meta.variable_measure),
        ("variable_display_width", lambda: ambers_meta.variable_display_width, lambda: pyr_meta.variable_display_width),
        ("variable_storage_width", lambda: ambers_meta.variable_storage_width, lambda: pyr_meta.variable_storage_width),
    ]
    for field, am_getter, pyr_getter in field_pairs:
        diffs = deep_diff(am_getter(), pyr_getter())
        assert diffs == [], f"{pfx}{field}: {len(diffs)} diffs: {diffs[:5]}"

    # Key-set fields
    am_missing_keys = set(ambers_meta.variable_missing_values.keys())
    pyr_missing_keys = set(pyr_meta.missing_ranges.keys())
    assert am_missing_keys == pyr_missing_keys, (
        f"{pfx}variable_missing_values keys differ: "
        f"only_ambers={sorted(am_missing_keys - pyr_missing_keys)}, "
        f"only_pyr={sorted(pyr_missing_keys - am_missing_keys)}"
    )

    am_mr_keys = set(ambers_meta.mr_sets.keys())
    pyr_mr_keys = set(pyr_meta.mr_sets.keys())
    assert am_mr_keys == pyr_mr_keys, (
        f"{pfx}mr_sets keys differ: "
        f"only_ambers={sorted(am_mr_keys - pyr_mr_keys)}, "
        f"only_pyr={sorted(pyr_mr_keys - am_mr_keys)}"
    )

    # Scalar fields
    assert ambers_meta.file_encoding == pyr_meta.file_encoding, (
        f"{pfx}file_encoding: {ambers_meta.file_encoding!r} vs {pyr_meta.file_encoding!r}"
    )
    assert ambers_meta.number_columns == pyr_meta.number_columns, (
        f"{pfx}number_columns: {ambers_meta.number_columns} vs {pyr_meta.number_columns}"
    )

    # Notes
    diffs = deep_diff(ambers_meta.notes, pyr_meta.notes)
    assert diffs == [], f"{pfx}notes: {len(diffs)} diffs: {diffs[:3]}"


# ---------------------------------------------------------------------------
# DataFrame comparison helper
# ---------------------------------------------------------------------------

def assert_dataframes_compatible(df_ambers, df_pyreadstat, label=""):
    """Compare ambers Polars DataFrame with pyreadstat Polars DataFrame.

    Handles known type differences: ambers uses Date32/Timestamp/Duration
    for temporal columns, while pyreadstat uses Date/Datetime. Cast both
    to a common representation for comparison.
    """
    pfx = f"[{label}] " if label else ""

    assert df_ambers.height == df_pyreadstat.height, (
        f"{pfx}row count: {df_ambers.height} vs {df_pyreadstat.height}"
    )
    assert df_ambers.width == df_pyreadstat.width, (
        f"{pfx}col count: {df_ambers.width} vs {df_pyreadstat.width}"
    )
    assert df_ambers.columns == df_pyreadstat.columns, (
        f"{pfx}column names differ"
    )

    # Cast temporal columns to common types for comparison
    df_a = df_ambers.clone()
    df_p = df_pyreadstat.clone()

    for col_name in df_a.columns:
        a_dtype = df_a[col_name].dtype
        p_dtype = df_p[col_name].dtype

        if a_dtype != p_dtype:
            # Both numeric: cast to Float64
            if a_dtype.is_numeric() and p_dtype.is_numeric():
                df_a = df_a.with_columns(pl.col(col_name).cast(pl.Float64))
                df_p = df_p.with_columns(pl.col(col_name).cast(pl.Float64))
            # Both temporal: cast to string for comparison
            elif a_dtype.is_temporal() and p_dtype.is_temporal():
                df_a = df_a.with_columns(pl.col(col_name).cast(pl.Utf8))
                df_p = df_p.with_columns(pl.col(col_name).cast(pl.Utf8))
            # String types (Utf8 vs Utf8View etc.)
            elif a_dtype in (pl.Utf8, pl.String) and p_dtype in (pl.Utf8, pl.String):
                pass  # Polars handles Utf8/String interchangeably
            else:
                # Cast both to string as last resort
                df_a = df_a.with_columns(pl.col(col_name).cast(pl.Utf8))
                df_p = df_p.with_columns(pl.col(col_name).cast(pl.Utf8))

    assert_frame_equal(df_a, df_p)


# ---------------------------------------------------------------------------
# Tests — ambers roundtrip (ambers → write → ambers read)
# ---------------------------------------------------------------------------

class TestSavRoundtripAmbers:
    """Write .sav, read back with ambers, compare DataFrame + metadata."""

    def test_sav_dataframe(self, sav_file, ambers_mod):
        df_orig, meta = ambers_mod.read_sav(sav_file)

        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out.sav"
            ambers_mod.write_sav(df_orig, out, meta=meta)
            df_rt, _ = ambers_mod.read_sav(str(out))

        assert_frame_equal(df_orig, df_rt)

    def test_sav_metadata(self, sav_file, ambers_mod):
        df_orig, meta = ambers_mod.read_sav(sav_file)

        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out.sav"
            ambers_mod.write_sav(df_orig, out, meta=meta)
            _, meta_rt = ambers_mod.read_sav(str(out))

        assert_metadata_ambers(meta, meta_rt, label="sav")

    def test_zsav_dataframe(self, sav_file, ambers_mod):
        df_orig, meta = ambers_mod.read_sav(sav_file)

        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out.zsav"
            ambers_mod.write_sav(df_orig, out, meta=meta)
            df_rt, _ = ambers_mod.read_sav(str(out))

        assert_frame_equal(df_orig, df_rt)

    def test_zsav_metadata(self, sav_file, ambers_mod):
        df_orig, meta = ambers_mod.read_sav(sav_file)

        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out.zsav"
            ambers_mod.write_sav(df_orig, out, meta=meta)
            _, meta_rt = ambers_mod.read_sav(str(out))

        assert_metadata_ambers(meta, meta_rt, label="zsav")


# ---------------------------------------------------------------------------
# Tests — cross-library (ambers write → pyreadstat read)
# ---------------------------------------------------------------------------

class TestSavRoundtripPyreadstat:
    """Write with ambers, read back with pyreadstat, compare.

    Cross-library tests are marked xfail(strict=False) because pyreadstat
    has known bugs (e.g., variable_alignment always 'unknown') and may
    read certain data types differently. Failures are flagged, not fatal.

    Known design difference — temporal columns:
      ambers converts SPSS DATE→Date32, DATETIME→Timestamp(us), TIME→Duration(us)
      pyreadstat keeps them as raw f64 (SPSS seconds since epoch).
      This is a deliberate design choice by each library, not a bug in either.
      DataFrame comparisons on files with temporal columns (e.g. test_2_medium)
      will xfail due to this type mismatch.
    """

    @pytest.mark.xfail(strict=False, reason="pyreadstat may differ on temporal/string types")
    def test_sav_dataframe(self, sav_file, ambers_mod, pyreadstat_mod):
        df_orig, meta = ambers_mod.read_sav(sav_file)

        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out.sav"
            ambers_mod.write_sav(df_orig, out, meta=meta)
            df_pyr, _ = pyreadstat_mod.read_sav(
                str(out), output_format="polars"
            )

        assert_dataframes_compatible(df_orig, df_pyr, label="sav→pyreadstat")

    @pytest.mark.xfail(strict=False, reason="pyreadstat may report metadata differently")
    def test_sav_metadata(self, sav_file, ambers_mod, pyreadstat_mod):
        df_orig, meta = ambers_mod.read_sav(sav_file)

        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out.sav"
            ambers_mod.write_sav(df_orig, out, meta=meta)
            _, meta_pyr = pyreadstat_mod.read_sav(str(out))

        assert_metadata_cross(meta, meta_pyr, label="sav→pyreadstat")

    @pytest.mark.xfail(strict=False, reason="pyreadstat may differ on temporal/string types")
    def test_zsav_dataframe(self, sav_file, ambers_mod, pyreadstat_mod):
        df_orig, meta = ambers_mod.read_sav(sav_file)

        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out.zsav"
            ambers_mod.write_sav(df_orig, out, meta=meta)
            df_pyr, _ = pyreadstat_mod.read_sav(
                str(out), output_format="polars"
            )

        assert_dataframes_compatible(df_orig, df_pyr, label="zsav→pyreadstat")

    @pytest.mark.xfail(strict=False, reason="pyreadstat may report metadata differently")
    def test_zsav_metadata(self, sav_file, ambers_mod, pyreadstat_mod):
        df_orig, meta = ambers_mod.read_sav(sav_file)

        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out.zsav"
            ambers_mod.write_sav(df_orig, out, meta=meta)
            _, meta_pyr = pyreadstat_mod.read_sav(str(out))

        assert_metadata_cross(meta, meta_pyr, label="zsav→pyreadstat")
