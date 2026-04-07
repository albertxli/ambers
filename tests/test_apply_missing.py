"""Tests for ambers.apply_missing().

Covers all SPSS user-defined missing value combinations:
discrete numeric (1-3 values), discrete string, range, range+discrete.

Run with:
    pytest tests/test_apply_missing.py -v
"""

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import ambers as am


# ---------------------------------------------------------------------------
# Helper: build SpssMetadata with missing specs
# ---------------------------------------------------------------------------

def _meta_with_missing(specs: dict) -> am.SpssMetadata:
    return am.SpssMetadata(variable_missing_values=specs)


# ---------------------------------------------------------------------------
# Discrete numeric
# ---------------------------------------------------------------------------

class TestDiscreteNumeric:
    def test_single_value(self):
        df = pl.DataFrame({"Q1": [1.0, 2.0, 99.0, 3.0]})
        meta = _meta_with_missing({"Q1": {"type": "discrete", "values": [99]}})
        result = am.apply_missing(df, meta)
        expected = pl.DataFrame({"Q1": [1.0, 2.0, None, 3.0]})
        assert_frame_equal(result, expected)

    def test_two_values(self):
        df = pl.DataFrame({"Q1": [1.0, 98.0, 99.0, 3.0]})
        meta = _meta_with_missing({"Q1": {"type": "discrete", "values": [98, 99]}})
        result = am.apply_missing(df, meta)
        expected = pl.DataFrame({"Q1": [1.0, None, None, 3.0]})
        assert_frame_equal(result, expected)

    def test_three_values_max(self):
        df = pl.DataFrame({"Q1": [1.0, 97.0, 98.0, 99.0, 5.0]})
        meta = _meta_with_missing({"Q1": {"type": "discrete", "values": [97, 98, 99]}})
        result = am.apply_missing(df, meta)
        expected = pl.DataFrame({"Q1": [1.0, None, None, None, 5.0]})
        assert_frame_equal(result, expected)

    def test_float_values(self):
        df = pl.DataFrame({"score": [1.5, 9.99, 99.99, 50.0]})
        meta = _meta_with_missing({"score": {"type": "discrete", "values": [9.99, 99.99]}})
        result = am.apply_missing(df, meta)
        expected = pl.DataFrame({"score": [1.5, None, None, 50.0]})
        assert_frame_equal(result, expected)

    def test_zero_value(self):
        df = pl.DataFrame({"Q1": [0.0, 1.0, 2.0, 3.0]})
        meta = _meta_with_missing({"Q1": {"type": "discrete", "values": [0]}})
        result = am.apply_missing(df, meta)
        expected = pl.DataFrame({"Q1": [None, 1.0, 2.0, 3.0]})
        assert_frame_equal(result, expected)

    def test_negative_values(self):
        df = pl.DataFrame({"Q1": [-9.0, -1.0, 1.0, 2.0]})
        meta = _meta_with_missing({"Q1": {"type": "discrete", "values": [-1, -9]}})
        result = am.apply_missing(df, meta)
        expected = pl.DataFrame({"Q1": [None, None, 1.0, 2.0]})
        assert_frame_equal(result, expected)


# ---------------------------------------------------------------------------
# Discrete string
# ---------------------------------------------------------------------------

class TestDiscreteString:
    def test_single_string(self):
        df = pl.DataFrame({"city": ["NYC", "NA", "LA"]})
        meta = _meta_with_missing({"city": {"type": "discrete", "values": ["NA"]}})
        result = am.apply_missing(df, meta)
        expected = pl.DataFrame({"city": ["NYC", None, "LA"]})
        assert_frame_equal(result, expected)

    def test_multiple_strings(self):
        df = pl.DataFrame({"city": ["NYC", "N/A", "DK", "RF", "LA"]})
        meta = _meta_with_missing({"city": {"type": "discrete", "values": ["N/A", "DK", "RF"]}})
        result = am.apply_missing(df, meta)
        expected = pl.DataFrame({"city": ["NYC", None, None, None, "LA"]})
        assert_frame_equal(result, expected)

    def test_string_with_spaces(self):
        df = pl.DataFrame({"note": ["hello", "  ", "world"]})
        meta = _meta_with_missing({"note": {"type": "discrete", "values": ["  "]}})
        result = am.apply_missing(df, meta)
        expected = pl.DataFrame({"note": ["hello", None, "world"]})
        assert_frame_equal(result, expected)


# ---------------------------------------------------------------------------
# Range (numeric only)
# ---------------------------------------------------------------------------

class TestRange:
    def test_basic_range(self):
        df = pl.DataFrame({"score": [1.0, 500.0, 900.0, 950.0, 999.0]})
        meta = _meta_with_missing({"score": {"type": "range", "low": 900, "high": 999}})
        result = am.apply_missing(df, meta)
        expected = pl.DataFrame({"score": [1.0, 500.0, None, None, None]})
        assert_frame_equal(result, expected)

    def test_range_boundaries_inclusive(self):
        """Both low and high boundaries are inclusive (closed), matching SPSS."""
        df = pl.DataFrame({"score": [899.0, 900.0, 950.0, 999.0, 1000.0]})
        meta = _meta_with_missing({"score": {"type": "range", "low": 900, "high": 999}})
        result = am.apply_missing(df, meta)
        expected = pl.DataFrame({"score": [899.0, None, None, None, 1000.0]})
        assert_frame_equal(result, expected)

    def test_range_fractional_bounds(self):
        df = pl.DataFrame({"x": [99.4, 99.5, 99.7, 99.9, 100.0]})
        meta = _meta_with_missing({"x": {"type": "range", "low": 99.5, "high": 99.9}})
        result = am.apply_missing(df, meta)
        expected = pl.DataFrame({"x": [99.4, None, None, None, 100.0]})
        assert_frame_equal(result, expected)


# ---------------------------------------------------------------------------
# Range + discrete
# ---------------------------------------------------------------------------

class TestRangeDiscrete:
    def test_range_with_discrete_below(self):
        df = pl.DataFrame({"income": [0.0, 500.0, 999990.0, 999999.0, 50000.0]})
        meta = _meta_with_missing({
            "income": {"type": "range", "low": 999990, "high": 999999, "discrete": 0}
        })
        result = am.apply_missing(df, meta)
        expected = pl.DataFrame({"income": [None, 500.0, None, None, 50000.0]})
        assert_frame_equal(result, expected)

    def test_range_with_discrete_above(self):
        df = pl.DataFrame({"score": [1.0, 900.0, 999.0, 9999.0, 500.0]})
        meta = _meta_with_missing({
            "score": {"type": "range", "low": 900, "high": 999, "discrete": 9999}
        })
        result = am.apply_missing(df, meta)
        expected = pl.DataFrame({"score": [1.0, None, None, None, 500.0]})
        assert_frame_equal(result, expected)


# ---------------------------------------------------------------------------
# Multi-column
# ---------------------------------------------------------------------------

class TestMultiColumn:
    def test_different_spec_types(self):
        df = pl.DataFrame({
            "Q1": [1.0, 99.0, 3.0],
            "Q2": [500.0, 900.0, 999.0],
            "Q3": [0.0, 50.0, 999999.0],
        })
        meta = _meta_with_missing({
            "Q1": {"type": "discrete", "values": [99]},
            "Q2": {"type": "range", "low": 900, "high": 999},
            "Q3": {"type": "range", "low": 999990, "high": 999999, "discrete": 0},
        })
        result = am.apply_missing(df, meta)
        expected = pl.DataFrame({
            "Q1": [1.0, None, 3.0],
            "Q2": [500.0, None, None],
            "Q3": [None, 50.0, None],
        })
        assert_frame_equal(result, expected)

    def test_some_columns_without_specs(self):
        df = pl.DataFrame({
            "Q1": [1.0, 99.0, 3.0],
            "age": [25.0, 30.0, 35.0],
        })
        meta = _meta_with_missing({"Q1": {"type": "discrete", "values": [99]}})
        result = am.apply_missing(df, meta)
        # Q1 has missing applied, age is untouched
        assert result["Q1"].to_list() == [1.0, None, 3.0]
        assert result["age"].to_list() == [25.0, 30.0, 35.0]


# ---------------------------------------------------------------------------
# Columns filter
# ---------------------------------------------------------------------------

class TestColumnsFilter:
    def test_columns_none_applies_all(self):
        df = pl.DataFrame({"Q1": [1.0, 99.0], "Q2": [98.0, 2.0]})
        meta = _meta_with_missing({
            "Q1": {"type": "discrete", "values": [99]},
            "Q2": {"type": "discrete", "values": [98]},
        })
        result = am.apply_missing(df, meta, columns=None)
        assert result["Q1"].to_list() == [1.0, None]
        assert result["Q2"].to_list() == [None, 2.0]

    def test_columns_filter_specific(self):
        df = pl.DataFrame({"Q1": [1.0, 99.0], "Q2": [98.0, 2.0]})
        meta = _meta_with_missing({
            "Q1": {"type": "discrete", "values": [99]},
            "Q2": {"type": "discrete", "values": [98]},
        })
        result = am.apply_missing(df, meta, columns=["Q1"])
        assert result["Q1"].to_list() == [1.0, None]
        assert result["Q2"].to_list() == [98.0, 2.0]  # NOT nullified

    def test_column_not_in_df_silent(self):
        """Column in columns= but not in df is silently skipped."""
        df = pl.DataFrame({"Q1": [1.0, 99.0]})
        meta = _meta_with_missing({
            "Q1": {"type": "discrete", "values": [99]},
            "NONEXISTENT": {"type": "discrete", "values": [1]},
        })
        result = am.apply_missing(df, meta, columns=["Q1", "NONEXISTENT"])
        assert result["Q1"].to_list() == [1.0, None]

    def test_column_no_spec_silent(self):
        """Column in columns= but no spec in metadata is silently skipped."""
        df = pl.DataFrame({"Q1": [1.0, 2.0], "Q2": [3.0, 4.0]})
        meta = _meta_with_missing({"Q1": {"type": "discrete", "values": [99]}})
        result = am.apply_missing(df, meta, columns=["Q2"])
        # Q2 has no spec, so nothing changes
        assert_frame_equal(result, df)


# ---------------------------------------------------------------------------
# Type handling
# ---------------------------------------------------------------------------

class TestTypeHandling:
    def test_dataframe_returns_dataframe(self):
        df = pl.DataFrame({"Q1": [1.0, 99.0]})
        meta = _meta_with_missing({"Q1": {"type": "discrete", "values": [99]}})
        result = am.apply_missing(df, meta)
        assert isinstance(result, pl.DataFrame)

    def test_lazyframe_returns_lazyframe(self):
        lf = pl.DataFrame({"Q1": [1.0, 99.0]}).lazy()
        meta = _meta_with_missing({"Q1": {"type": "discrete", "values": [99]}})
        result = am.apply_missing(lf, meta)
        assert isinstance(result, pl.LazyFrame)
        collected = result.collect()
        assert collected["Q1"].to_list() == [1.0, None]

    def test_existing_nulls_preserved(self):
        df = pl.DataFrame({"Q1": [1.0, None, 99.0, None, 3.0]})
        meta = _meta_with_missing({"Q1": {"type": "discrete", "values": [99]}})
        result = am.apply_missing(df, meta)
        expected = pl.DataFrame({"Q1": [1.0, None, None, None, 3.0]})
        assert_frame_equal(result, expected)

    def test_non_matching_values_preserved(self):
        df = pl.DataFrame({"Q1": [1.0, 2.0, 3.0, 4.0, 5.0]})
        meta = _meta_with_missing({"Q1": {"type": "discrete", "values": [99]}})
        result = am.apply_missing(df, meta)
        assert_frame_equal(result, df)

    def test_invalid_df_type_raises(self):
        with pytest.raises(TypeError):
            am.apply_missing([1, 2, 3], _meta_with_missing({}))


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_no_missing_specs(self):
        df = pl.DataFrame({"Q1": [1.0, 2.0, 3.0]})
        meta = am.SpssMetadata()
        result = am.apply_missing(df, meta)
        assert_frame_equal(result, df)

    def test_empty_dataframe(self):
        df = pl.DataFrame({"Q1": pl.Series([], dtype=pl.Float64)})
        meta = _meta_with_missing({"Q1": {"type": "discrete", "values": [99]}})
        result = am.apply_missing(df, meta)
        assert result.height == 0

    def test_all_values_missing(self):
        df = pl.DataFrame({"Q1": [98.0, 99.0, 99.0]})
        meta = _meta_with_missing({"Q1": {"type": "discrete", "values": [98, 99]}})
        result = am.apply_missing(df, meta)
        assert result["Q1"].null_count() == 3
