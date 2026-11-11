"""Tests for codebook value counting engine (Phase 2).

Covers: merge_meta_and_data_values, build_rows.

Run with:
    pytest tests/test_codebook_engine.py -v
"""

import polars as pl
import pytest

import ambers as am
from ambers.codebook._engine import (
    merge_meta_and_data_values,
    build_rows,
)
from ambers.codebook._types import detect_types
from ambers.codebook import MISSING_LABEL


def _meta(**kwargs) -> am.SpssMetadata:
    return am.SpssMetadata(**kwargs)


# ---------------------------------------------------------------------------
# merge_meta_and_data_values
# ---------------------------------------------------------------------------

class TestMerge:
    def test_all_labeled(self):
        meta_vals = {1.0: "A", 2.0: "B", 3.0: "C"}
        actual = {1.0: 10, 2.0: 20, 3.0: 30}
        result = merge_meta_and_data_values(meta_vals, actual)
        assert len(result) == 3
        assert result[0] == (1.0, "A", 10, False)  # not unlabeled
        assert result[2] == (3.0, "C", 30, False)

    def test_unlabeled_value_in_data(self):
        meta_vals = {1.0: "A", 2.0: "B"}
        actual = {1.0: 10, 2.0: 20, 99.0: 5}
        result = merge_meta_and_data_values(meta_vals, actual)
        assert len(result) == 3
        # 99.0 is unlabeled
        r99 = [r for r in result if r[0] == 99.0][0]
        assert r99 == (99.0, None, 5, True)  # is_unlabeled=True

    def test_label_in_meta_not_in_data(self):
        meta_vals = {1.0: "A", 2.0: "B", 3.0: "C"}
        actual = {1.0: 10}  # only value 1 in data
        result = merge_meta_and_data_values(meta_vals, actual)
        assert len(result) == 3
        # Values 2 and 3 have count=0
        r2 = [r for r in result if r[0] == 2.0][0]
        assert r2 == (2.0, "B", 0, False)

    def test_type_mismatch_int_meta_float_data(self):
        """Meta has int keys, data has float keys — should normalize."""
        meta_vals = {1: "Yes", 2: "No"}
        actual = {1.0: 50, 2.0: 30}
        result = merge_meta_and_data_values(meta_vals, actual)
        assert len(result) == 2
        # Should find the labels despite type mismatch
        labels = {r[1] for r in result}
        assert "Yes" in labels
        assert "No" in labels

    def test_sorted_output(self):
        meta_vals = {3.0: "C", 1.0: "A", 2.0: "B"}
        actual = {3.0: 30, 1.0: 10, 2.0: 20}
        result = merge_meta_and_data_values(meta_vals, actual)
        codes = [r[0] for r in result]
        assert codes == [1.0, 2.0, 3.0]

    def test_empty_meta(self):
        meta_vals = {}
        actual = {1.0: 10, 2.0: 20}
        result = merge_meta_and_data_values(meta_vals, actual)
        assert len(result) == 2
        assert all(r[3] for r in result)  # all unlabeled

    def test_empty_data(self):
        meta_vals = {1.0: "A", 2.0: "B"}
        actual = {}
        result = merge_meta_and_data_values(meta_vals, actual)
        assert len(result) == 2
        assert all(r[2] == 0 for r in result)  # all count=0


# ---------------------------------------------------------------------------
# build_rows
# ---------------------------------------------------------------------------

class TestBuildRows:
    def test_categorical_variable(self):
        df = pl.DataFrame({"Q1": [1.0, 2.0, 2.0, 3.0]})
        meta = _meta(variable_value_labels={"Q1": {1.0: "A", 2.0: "B", 3.0: "C"}})
        types = detect_types(df, meta)
        result = build_rows(df, meta, types, ["Q1"])
        # 3 value rows (no nulls → no missing row)
        assert result.height == 3
        assert result["variable"].to_list() == ["Q1", "Q1", "Q1"]
        # Check counts
        rows = result.to_dicts()
        row_a = [r for r in rows if r["value_label"] == "A"][0]
        assert row_a["value_n"] == 1
        row_b = [r for r in rows if r["value_label"] == "B"][0]
        assert row_b["value_n"] == 2

    def test_categorical_with_nulls(self):
        df = pl.DataFrame({"Q1": [1.0, 2.0, None, None]})
        meta = _meta(variable_value_labels={"Q1": {1.0: "A", 2.0: "B"}})
        types = detect_types(df, meta)
        result = build_rows(df, meta, types, ["Q1"])
        # MISSING row + 2 value rows = 3
        assert result.height == 3
        rows = result.to_dicts()
        missing_row = [r for r in rows if r["value_label"] == MISSING_LABEL][0]
        assert missing_row["value_n"] == 2
        assert missing_row["value_code"] is None

    def test_missing_row_comes_first(self):
        """Missing row should appear before value rows."""
        df = pl.DataFrame({"Q1": [1.0, None]})
        meta = _meta(variable_value_labels={"Q1": {1.0: "Yes"}})
        types = detect_types(df, meta)
        result = build_rows(df, meta, types, ["Q1"])
        # First row should be MISSING
        assert result["value_label"][0] == MISSING_LABEL

    def test_numeric_summary_row(self):
        df = pl.DataFrame({"age": [25.0, 30.0, None]})
        meta = _meta()
        types = detect_types(df, meta)
        result = build_rows(df, meta, types, ["age"])
        # MISSING row + 1 summary row = 2
        assert result.height == 2
        rows = result.to_dicts()
        summary = [r for r in rows if r["value_label"] != MISSING_LABEL][0]
        assert summary["value_code"] is None
        assert summary["value_label"] is None
        assert summary["value_n"] == 2  # non-null count

    def test_text_summary_row(self):
        df = pl.DataFrame({"name": ["Alice", "Bob", None]})
        meta = _meta()
        types = detect_types(df, meta)
        result = build_rows(df, meta, types, ["name"])
        assert result.height == 2  # MISSING + summary
        rows = result.to_dicts()
        summary = [r for r in rows if r["value_label"] != MISSING_LABEL][0]
        assert summary["value_n"] == 2

    def test_value_code_dtype_int(self):
        """All integer-like codes → Int64 dtype."""
        df = pl.DataFrame({"Q1": [1.0, 2.0, 3.0]})
        meta = _meta(variable_value_labels={"Q1": {1.0: "A", 2.0: "B", 3.0: "C"}})
        types = detect_types(df, meta)
        result = build_rows(df, meta, types, ["Q1"])
        assert result["value_code"].dtype == pl.Int64
        # Codes should be integers, not floats
        codes = result["value_code"].drop_nulls().to_list()
        assert codes == [1, 2, 3]

    def test_unlabeled_value_in_output(self):
        df = pl.DataFrame({"Q1": [1.0, 2.0, 99.0]})
        meta = _meta(variable_value_labels={"Q1": {1.0: "A", 2.0: "B"}})
        types = detect_types(df, meta)
        result = build_rows(df, meta, types, ["Q1"])
        rows = result.to_dicts()
        row_99 = [r for r in rows if r["value_code"] == 99][0]
        assert row_99["value_label"] is None  # unlabeled

    def test_multiple_columns(self):
        df = pl.DataFrame({
            "Q1": [1.0, 2.0],
            "age": [25.0, 30.0],
            "name": ["Alice", "Bob"],
        })
        meta = _meta(variable_value_labels={"Q1": {1.0: "A", 2.0: "B"}})
        types = detect_types(df, meta)
        result = build_rows(df, meta, types, ["Q1", "age", "name"])
        # Q1: 2 value rows, age: 1 summary, name: 1 summary = 4
        assert result.height == 4
        vars_in_result = result["variable"].unique().to_list()
        assert set(vars_in_result) == {"Q1", "age", "name"}

    def test_meta_fields_populated(self):
        df = pl.DataFrame({"Q1": [1.0, 2.0]})
        meta = _meta(
            variable_value_labels={"Q1": {1.0: "A", 2.0: "B"}},
            variable_measures={"Q1": "nominal"},
            variable_formats={"Q1": "F4.0"},
        )
        types = detect_types(df, meta)
        result = build_rows(df, meta, types, ["Q1"])
        assert result["variable_measure"][0] == "nominal"
        assert result["variable_format"][0] == "F4.0"

    def test_missing_meta_fields_unknown(self):
        df = pl.DataFrame({"Q1": [1.0, 2.0]})
        meta = _meta(variable_value_labels={"Q1": {1.0: "A", 2.0: "B"}})
        types = detect_types(df, meta)
        result = build_rows(df, meta, types, ["Q1"])
        assert result["variable_measure"][0] == "unknown"
        assert result["variable_format"][0] == "unknown"

    def test_empty_df(self):
        df = pl.DataFrame({"Q1": pl.Series([], dtype=pl.Float64)})
        meta = _meta(variable_value_labels={"Q1": {1.0: "A"}})
        types = detect_types(df, meta)
        result = build_rows(df, meta, types, ["Q1"])
        # Should have rows for meta labels (count=0)
        assert result.height >= 1


# ---------------------------------------------------------------------------
# Real file test
# ---------------------------------------------------------------------------

import os

_REAL_FILE = "test_data/test_1_small.sav"


@pytest.mark.skipif(not os.path.exists(_REAL_FILE), reason="test data not available")
class TestRealFile:
    def test_build_rows_real_data(self):
        sav = am.read_sav(_REAL_FILE)
        df, meta = sav.data, sav.meta
        types = detect_types(df, meta)
        result = build_rows(df, meta, types, df.columns)
        # Should have rows for all variables
        assert result.height > 0
        # Each variable in df should appear
        result_vars = set(result["variable"].unique().to_list())
        assert result_vars == set(df.columns)
        # value_n should never be negative
        assert result["value_n"].min() >= 0
