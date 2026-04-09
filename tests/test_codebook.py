"""Tests for codebook() public function (Phase 3 + Phase 4).

Covers: detail view, summary view, computed columns, column filtering,
include_meta, LazyFrame input, edge cases.

Run with:
    pytest tests/test_codebook.py -v
"""

import os

import polars as pl
import pytest

import ambers as am
from ambers.codebook import MISSING_LABEL


def _meta(**kwargs) -> am.SpssMetadata:
    return am.SpssMetadata(**kwargs)


# ---------------------------------------------------------------------------
# Detail view: computed columns
# ---------------------------------------------------------------------------

class TestDetailComputedColumns:
    def test_n_valid_excludes_missing(self):
        df = pl.DataFrame({"Q1": [1.0, 2.0, None, None]})
        meta = _meta(variable_value_labels={"Q1": {1.0: "A", 2.0: "B"}})
        cb = am.codebook(df, meta)
        non_missing = cb.filter(pl.col("value_label") != MISSING_LABEL)
        assert non_missing["n_valid"][0] == 2

    def test_n_valid_on_missing_row(self):
        """MISSING row: n_valid = its own value_n (null count)."""
        df = pl.DataFrame({"Q1": [1.0, 2.0, None, None, None]})
        meta = _meta(variable_value_labels={"Q1": {1.0: "A", 2.0: "B"}})
        cb = am.codebook(df, meta)
        missing_row = cb.filter(pl.col("value_label") == MISSING_LABEL)
        assert missing_row["n_valid"][0] == 3
        assert missing_row["value_n"][0] == 3

    def test_pct_valid(self):
        df = pl.DataFrame({"Q1": [1.0, 1.0, 2.0, 2.0, 2.0]})
        meta = _meta(variable_value_labels={"Q1": {1.0: "A", 2.0: "B"}})
        cb = am.codebook(df, meta)
        rows = cb.to_dicts()
        row_a = [r for r in rows if r["value_label"] == "A"][0]
        row_b = [r for r in rows if r["value_label"] == "B"][0]
        assert abs(row_a["pct_valid"] - 0.4) < 0.001
        assert abs(row_b["pct_valid"] - 0.6) < 0.001

    def test_pct_valid_null_for_missing_row(self):
        df = pl.DataFrame({"Q1": [1.0, None]})
        meta = _meta(variable_value_labels={"Q1": {1.0: "A"}})
        cb = am.codebook(df, meta)
        missing_row = cb.filter(pl.col("value_label") == MISSING_LABEL)
        assert missing_row["pct_valid"][0] is None

    def test_n_total_includes_missing(self):
        df = pl.DataFrame({"Q1": [1.0, 2.0, None]})
        meta = _meta(variable_value_labels={"Q1": {1.0: "A", 2.0: "B"}})
        cb = am.codebook(df, meta)
        assert cb["n_total"].unique().to_list() == [3]

    def test_pct_total(self):
        df = pl.DataFrame({"Q1": [1.0, 2.0, None]})
        meta = _meta(variable_value_labels={"Q1": {1.0: "A", 2.0: "B"}})
        cb = am.codebook(df, meta)
        rows = cb.to_dicts()
        row_a = [r for r in rows if r.get("value_label") == "A"][0]
        assert abs(row_a["pct_total"] - 1 / 3) < 0.001

    def test_variable_type_merged_to_categorical(self):
        df = pl.DataFrame({"Q1": [1.0, 2.0]})
        meta = _meta(variable_value_labels={"Q1": {1.0: "A", 2.0: "B"}})
        cb = am.codebook(df, meta)
        assert cb["variable_type"].unique().to_list() == ["categorical"]

    def test_no_flag_columns_in_detail(self):
        """Detail view should NOT have missing_data or unlabeled_value columns."""
        df = pl.DataFrame({"Q1": [1.0, 99.0, None]})
        meta = _meta(variable_value_labels={"Q1": {1.0: "A"}})
        cb = am.codebook(df, meta)
        assert "missing_data" not in cb.columns
        assert "unlabeled_value" not in cb.columns

    def test_detail_columns_schema(self):
        df = pl.DataFrame({"Q1": [1.0]})
        meta = _meta(variable_value_labels={"Q1": {1.0: "A"}})
        cb = am.codebook(df, meta)
        expected = {
            "variable", "variable_label", "variable_type",
            "value_code", "value_label", "value_n",
            "n_valid", "pct_valid", "n_total", "pct_total",
        }
        assert set(cb.columns) == expected


# ---------------------------------------------------------------------------
# Detail view: column filtering
# ---------------------------------------------------------------------------

class TestDetailFiltering:
    def test_columns_filter(self):
        df = pl.DataFrame({"Q1": [1.0], "Q2": [2.0]})
        meta = _meta(variable_value_labels={
            "Q1": {1.0: "A"}, "Q2": {2.0: "B"},
        })
        cb = am.codebook(df, meta, columns=["Q1"])
        assert set(cb["variable"].unique().to_list()) == {"Q1"}

    def test_exclude_filter(self):
        df = pl.DataFrame({"Q1": [1.0], "Q2": [2.0]})
        meta = _meta(variable_value_labels={
            "Q1": {1.0: "A"}, "Q2": {2.0: "B"},
        })
        cb = am.codebook(df, meta, exclude=["Q1"])
        assert set(cb["variable"].unique().to_list()) == {"Q2"}

    def test_columns_and_exclude_combined(self):
        df = pl.DataFrame({"Q1": [1.0], "Q2": [2.0], "Q3": [3.0]})
        meta = _meta(variable_value_labels={
            "Q1": {1.0: "A"}, "Q2": {2.0: "B"}, "Q3": {3.0: "C"},
        })
        cb = am.codebook(df, meta, columns=["Q1", "Q2", "Q3"], exclude=["Q2"])
        vars_in = set(cb["variable"].unique().to_list())
        assert "Q2" not in vars_in
        assert "Q1" in vars_in


# ---------------------------------------------------------------------------
# Detail view: include_meta
# ---------------------------------------------------------------------------

class TestIncludeMeta:
    def test_include_meta_false(self):
        df = pl.DataFrame({"Q1": [1.0]})
        meta = _meta(variable_value_labels={"Q1": {1.0: "A"}})
        cb = am.codebook(df, meta, include_meta=False)
        assert "variable_measure" not in cb.columns

    def test_include_meta_true(self):
        df = pl.DataFrame({"Q1": [1.0]})
        meta = _meta(
            variable_value_labels={"Q1": {1.0: "A"}},
            variable_measures={"Q1": "nominal"},
            variable_formats={"Q1": "F4.0"},
        )
        cb = am.codebook(df, meta, include_meta=True)
        assert "variable_measure" in cb.columns
        assert cb["variable_measure"][0] == "nominal"


# ---------------------------------------------------------------------------
# Detail view: input types
# ---------------------------------------------------------------------------

class TestInputTypes:
    def test_lazyframe_input(self):
        lf = pl.DataFrame({"Q1": [1.0, 2.0]}).lazy()
        meta = _meta(variable_value_labels={"Q1": {1.0: "A", 2.0: "B"}})
        cb = am.codebook(lf, meta)
        assert isinstance(cb, pl.DataFrame)
        assert cb.height == 2

    def test_invalid_input_raises(self):
        with pytest.raises(TypeError):
            am.codebook([1, 2], _meta())

    def test_empty_df(self):
        df = pl.DataFrame({"Q1": pl.Series([], dtype=pl.Float64)})
        meta = _meta(variable_value_labels={"Q1": {1.0: "A"}})
        cb = am.codebook(df, meta)
        assert cb.height >= 1


# ---------------------------------------------------------------------------
# Summary view
# ---------------------------------------------------------------------------

class TestSummaryView:
    def test_one_row_per_variable(self):
        df = pl.DataFrame({
            "Q1": [1.0, 2.0, 3.0],
            "age": [25.0, 30.0, 35.0],
        })
        meta = _meta(variable_value_labels={"Q1": {1.0: "A", 2.0: "B", 3.0: "C"}})
        cb = am.codebook(df, meta, view="summary")
        assert cb.height == 2
        assert set(cb["variable"].to_list()) == {"Q1", "age"}

    def test_n_valid_n_missing(self):
        df = pl.DataFrame({"Q1": [1.0, 2.0, None, None]})
        meta = _meta(variable_value_labels={"Q1": {1.0: "A", 2.0: "B"}})
        cb = am.codebook(df, meta, view="summary")
        row = cb.to_dicts()[0]
        assert row["n_valid"] == 2
        assert row["n_missing"] == 2

    def test_n_total(self):
        """n_total = n_valid + n_missing."""
        df = pl.DataFrame({"Q1": [1.0, 2.0, None, None]})
        meta = _meta(variable_value_labels={"Q1": {1.0: "A", 2.0: "B"}})
        cb = am.codebook(df, meta, view="summary")
        row = cb.to_dicts()[0]
        assert row["n_total"] == 4
        assert row["n_total"] == row["n_valid"] + row["n_missing"]

    def test_n_labeled_n_unlabeled(self):
        df = pl.DataFrame({"Q1": [1.0, 2.0, 99.0]})
        meta = _meta(variable_value_labels={"Q1": {1.0: "A", 2.0: "B"}})
        cb = am.codebook(df, meta, view="summary")
        row = cb.to_dicts()[0]
        assert row["n_labeled"] == 2
        assert row["n_unlabeled"] == 1

    def test_values_is_list_struct(self):
        df = pl.DataFrame({"Q1": [1.0, 2.0]})
        meta = _meta(variable_value_labels={"Q1": {1.0: "Low", 2.0: "High"}})
        cb = am.codebook(df, meta, view="summary")
        # values column should be List[Struct{value_code, value_label}]
        vals = cb["values"][0].to_list()
        assert len(vals) == 2
        codes = {v["value_code"] for v in vals}
        labels = {v["value_label"] for v in vals}
        assert 1 in codes  # native int, not string
        assert "Low" in labels
        assert "High" in labels

    def test_values_explodable(self):
        """values column should be explodable + unnestable."""
        df = pl.DataFrame({"Q1": [1.0, 2.0]})
        meta = _meta(variable_value_labels={"Q1": {1.0: "A", 2.0: "B"}})
        cb = am.codebook(df, meta, view="summary")
        exploded = cb.explode("values").unnest("values")
        assert "value_code" in exploded.columns
        assert "value_label" in exploded.columns
        assert exploded.height == 2

    def test_numeric_has_null_label_fields(self):
        df = pl.DataFrame({"age": [25.0, 30.0]})
        meta = _meta()
        cb = am.codebook(df, meta, view="summary")
        row = cb.to_dicts()[0]
        assert row["n_labeled"] is None
        assert row["n_unlabeled"] is None
        assert row["values"] is None or row["values"] == []

    def test_summary_columns(self):
        df = pl.DataFrame({"Q1": [1.0]})
        meta = _meta(variable_value_labels={"Q1": {1.0: "A"}})
        cb = am.codebook(df, meta, view="summary")
        expected = {
            "variable", "variable_label", "variable_type",
            "n_valid", "n_missing", "n_total",
            "n_unique", "n_labeled", "n_unlabeled", "values",
        }
        assert set(cb.columns) == expected

    def test_summary_column_order(self):
        """n_total should come right after n_missing."""
        df = pl.DataFrame({"Q1": [1.0]})
        meta = _meta(variable_value_labels={"Q1": {1.0: "A"}})
        cb = am.codebook(df, meta, view="summary")
        cols = cb.columns
        assert cols.index("n_total") == cols.index("n_missing") + 1


# ---------------------------------------------------------------------------
# Real file integration test
# ---------------------------------------------------------------------------

_REAL_FILE = "test_data/test_1_small.sav"


@pytest.mark.skipif(not os.path.exists(_REAL_FILE), reason="test data not available")
class TestRealFile:
    def test_detail_all_variables_present(self):
        sav = am.read_sav(_REAL_FILE)
        df, meta = sav.data, sav.meta
        cb = am.codebook(df, meta)
        df_vars = set(df.columns)
        cb_vars = set(cb["variable"].unique().to_list())
        assert df_vars == cb_vars

    def test_detail_schema(self):
        sav = am.read_sav(_REAL_FILE)
        df, meta = sav.data, sav.meta
        cb = am.codebook(df, meta)
        expected = {
            "variable", "variable_label", "variable_type",
            "value_code", "value_label", "value_n",
            "n_valid", "pct_valid", "n_total", "pct_total",
        }
        assert set(cb.columns) == expected

    def test_summary_all_variables_present(self):
        sav = am.read_sav(_REAL_FILE)
        df, meta = sav.data, sav.meta
        cb = am.codebook(df, meta, view="summary")
        assert cb.height == len(df.columns)

    def test_categorical_has_multiple_rows(self):
        sav = am.read_sav(_REAL_FILE)
        df, meta = sav.data, sav.meta
        cb = am.codebook(df, meta)
        cat_vars = cb.filter(pl.col("variable_type") == "categorical")["variable"].unique()
        for var in cat_vars.to_list():
            var_rows = cb.filter(pl.col("variable") == var)
            assert var_rows.height >= 2

    def test_value_n_never_negative(self):
        sav = am.read_sav(_REAL_FILE)
        df, meta = sav.data, sav.meta
        cb = am.codebook(df, meta)
        assert cb["value_n"].min() >= 0
