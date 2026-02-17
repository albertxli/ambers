"""Compare ambers vs pyreadstat metadata for regression testing.

Run with:
    pytest tests/ -v
    pytest tests/ -v --sav-file path/to/file.sav
"""

from collections.abc import Mapping, Sequence

import pytest


# ---------------------------------------------------------------------------
# deep_diff utility
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


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

def normalize_file_label(val):
    """pyreadstat returns None for empty, ambers returns empty string."""
    if val is None:
        return ""
    return val


def normalize_file_format(val):
    """pyreadstat returns 'sav/zsav', ambers returns 'sav' or 'zsav'."""
    if val == "sav/zsav":
        return "sav"
    return val


# ---------------------------------------------------------------------------
# Fixtures — cache loaded metadata per file path to avoid re-reading
# ---------------------------------------------------------------------------

_ambers_cache = {}
_pyreadstat_cache = {}


@pytest.fixture
def ambers_meta(sav_file, ambers_mod):
    if sav_file not in _ambers_cache:
        _, meta = ambers_mod.read_sav(sav_file)
        _ambers_cache[sav_file] = meta
    return _ambers_cache[sav_file]


@pytest.fixture
def pyreadstat_meta(sav_file, pyreadstat_mod):
    if sav_file not in _pyreadstat_cache:
        _, meta = pyreadstat_mod.read_sav(sav_file)
        _pyreadstat_cache[sav_file] = meta
    return _pyreadstat_cache[sav_file]


# ---------------------------------------------------------------------------
# Dict-field tests (deep_diff)
# ---------------------------------------------------------------------------

class TestDictFields:
    """Compare dict metadata fields via deep_diff."""

    def test_variable_labels(self, pyreadstat_meta, ambers_meta):
        diffs = deep_diff(
            pyreadstat_meta.column_names_to_labels,
            ambers_meta.variable_labels,
        )
        assert diffs == [], f"{len(diffs)} diffs: {diffs[:5]}"

    def test_variable_value_labels(self, pyreadstat_meta, ambers_meta):
        diffs = deep_diff(
            pyreadstat_meta.variable_value_labels,
            ambers_meta.variable_value_labels,
        )
        assert diffs == [], f"{len(diffs)} diffs: {diffs[:5]}"

    def test_spss_variable_types(self, pyreadstat_meta, ambers_meta):
        diffs = deep_diff(
            pyreadstat_meta.original_variable_types,
            ambers_meta.spss_variable_types,
        )
        assert diffs == [], f"{len(diffs)} diffs: {diffs[:5]}"

    def test_variable_measure(self, pyreadstat_meta, ambers_meta):
        diffs = deep_diff(
            pyreadstat_meta.variable_measure,
            ambers_meta.variable_measure,
        )
        assert diffs == [], f"{len(diffs)} diffs: {diffs[:5]}"

    def test_variable_storage_width(self, pyreadstat_meta, ambers_meta):
        diffs = deep_diff(
            pyreadstat_meta.variable_storage_width,
            ambers_meta.variable_storage_width,
        )
        assert diffs == [], f"{len(diffs)} diffs: {diffs[:5]}"

    def test_variable_display_width(self, pyreadstat_meta, ambers_meta):
        diffs = deep_diff(
            pyreadstat_meta.variable_display_width,
            ambers_meta.variable_display_width,
        )
        assert diffs == [], f"{len(diffs)} diffs: {diffs[:5]}"


class TestListFields:
    """Compare list metadata fields."""

    def test_variable_names(self, pyreadstat_meta, ambers_meta):
        diffs = deep_diff(pyreadstat_meta.column_names, ambers_meta.variable_names)
        assert diffs == [], f"{len(diffs)} diffs: {diffs[:5]}"

    def test_notes(self, pyreadstat_meta, ambers_meta):
        diffs = deep_diff(pyreadstat_meta.notes, ambers_meta.notes)
        assert diffs == [], f"{len(diffs)} diffs: {diffs[:5]}"


class TestKeySetFields:
    """Compare only the key sets of dict fields (value structures differ)."""

    def test_missing_ranges_keys(self, pyreadstat_meta, ambers_meta):
        pr_keys = set(pyreadstat_meta.missing_ranges.keys())
        am_keys = set(ambers_meta.variable_missing.keys())
        removed = sorted(pr_keys - am_keys)
        added = sorted(am_keys - pr_keys)
        assert removed == [], f"In pyreadstat only: {removed}"
        assert added == [], f"In ambers only: {added}"

    def test_mr_sets_keys(self, pyreadstat_meta, ambers_meta):
        pr_keys = set(pyreadstat_meta.mr_sets.keys())
        am_keys = set(ambers_meta.mr_sets.keys())
        removed = sorted(pr_keys - am_keys)
        added = sorted(am_keys - pr_keys)
        assert removed == [], f"In pyreadstat only: {removed}"
        assert added == [], f"In ambers only: {added}"


class TestScalarFields:
    """Compare scalar metadata fields."""

    def test_file_label(self, pyreadstat_meta, ambers_meta):
        pr = normalize_file_label(pyreadstat_meta.file_label)
        am = normalize_file_label(ambers_meta.file_label)
        assert pr == am, f"pyreadstat={pyreadstat_meta.file_label!r}, ambers={ambers_meta.file_label!r}"

    def test_file_encoding(self, pyreadstat_meta, ambers_meta):
        assert pyreadstat_meta.file_encoding == ambers_meta.file_encoding

    def test_number_rows(self, pyreadstat_meta, ambers_meta):
        assert pyreadstat_meta.number_rows == ambers_meta.number_rows

    def test_number_columns(self, pyreadstat_meta, ambers_meta):
        assert pyreadstat_meta.number_columns == ambers_meta.number_columns

    def test_file_format(self, pyreadstat_meta, ambers_meta):
        pr = normalize_file_format(pyreadstat_meta.file_format)
        am = normalize_file_format(ambers_meta.file_format)
        assert pr == am, f"pyreadstat={pyreadstat_meta.file_format!r}, ambers={ambers_meta.file_format!r}"


class TestSkippedFields:
    """Fields we intentionally skip (report values for manual inspection)."""

    @pytest.mark.skip(reason="datetime format differs between libraries")
    def test_creation_time(self, pyreadstat_meta, ambers_meta):
        assert pyreadstat_meta.creation_time == ambers_meta.creation_time

    @pytest.mark.skip(reason="datetime format differs between libraries")
    def test_modification_time(self, pyreadstat_meta, ambers_meta):
        assert pyreadstat_meta.modification_time == ambers_meta.modification_time

    @pytest.mark.skip(reason="pyreadstat always returns 'unknown' — likely its own bug")
    def test_variable_alignment(self, pyreadstat_meta, ambers_meta):
        diffs = deep_diff(
            pyreadstat_meta.variable_alignment,
            ambers_meta.variable_alignment,
        )
        assert diffs == [], f"{len(diffs)} diffs: {diffs[:5]}"
