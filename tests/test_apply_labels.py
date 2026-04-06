"""Tests for ambers.apply_labels().

Run with:
    pytest tests/test_apply_labels.py -v
"""

import polars as pl
import pytest

import ambers as am


# ---------------------------------------------------------------------------
# Fixtures — synthetic data (no test files needed)
# ---------------------------------------------------------------------------

@pytest.fixture
def simple_df():
    """DataFrame with numeric and string columns."""
    return pl.DataFrame({
        "gender": [1.0, 2.0, 1.0, 2.0, None],
        "region": [1.0, 2.0, 3.0, 1.0, 2.0],
        "code": ["RF", "DK", "hello", "RF", None],
        "age": [25.0, 30.0, 45.0, 22.0, 38.0],
    })


@pytest.fixture
def simple_meta():
    """Metadata with labels for gender, region, and code (not age)."""
    return am.SpssMetadata(
        variable_value_labels={
            "gender": {1: "Male", 2: "Female"},
            "region": {1: "North", 2: "South", 3: "East"},
            "code": {"RF": "Refused", "DK": "Don't Know"},
        },
    )


@pytest.fixture
def partial_meta():
    """Metadata where region is missing label for value 3."""
    return am.SpssMetadata(
        variable_value_labels={
            "gender": {1: "Male", 2: "Female"},
            "region": {1: "North", 2: "South"},
        },
    )


# ---------------------------------------------------------------------------
# Core output modes
# ---------------------------------------------------------------------------

class TestOutputEnum:
    def test_produces_enum_dtype(self, simple_df, simple_meta):
        result = am.apply_labels(simple_df, simple_meta)
        assert result["gender"].dtype.base_type() == pl.Enum
        assert result["region"].dtype.base_type() == pl.Enum

    def test_values_replaced(self, simple_df, simple_meta):
        result = am.apply_labels(simple_df, simple_meta)
        assert result["gender"].to_list() == ["Male", "Female", "Male", "Female", None]

    def test_is_default(self, simple_df, simple_meta):
        default = am.apply_labels(simple_df, simple_meta)
        explicit = am.apply_labels(simple_df, simple_meta, output="enum")
        assert default["gender"].to_list() == explicit["gender"].to_list()


class TestOutputString:
    def test_produces_string_dtype(self, simple_df, simple_meta):
        result = am.apply_labels(simple_df, simple_meta, output="string")
        assert result["gender"].dtype == pl.String
        assert result["region"].dtype == pl.String

    def test_values_replaced(self, simple_df, simple_meta):
        result = am.apply_labels(simple_df, simple_meta, output="string")
        assert result["gender"].to_list() == ["Male", "Female", "Male", "Female", None]

    def test_unmapped_stringified(self, simple_df, partial_meta):
        result = am.apply_labels(simple_df, partial_meta, output="string")
        # region value 3.0 has no label → becomes "3"
        assert "3" in result["region"].to_list()

    def test_integer_float_stringify(self, partial_meta):
        df = pl.DataFrame({"region": [3.0, 4.5]})
        result = am.apply_labels(df, partial_meta, output="string")
        vals = result["region"].to_list()
        assert vals[0] == "3"    # 3.0 → "3"
        assert vals[1] == "4.5"  # 4.5 → "4.5"


class TestOutputEnumNull:
    def test_produces_enum_dtype(self, simple_df, simple_meta):
        result = am.apply_labels(simple_df, simple_meta, output="enum_null")
        assert result["gender"].dtype.base_type() == pl.Enum

    def test_unmapped_become_null(self, simple_df, partial_meta):
        result = am.apply_labels(simple_df, partial_meta, output="enum_null")
        region_vals = result["region"].to_list()
        # value 3.0 has no label → null
        assert region_vals[2] is None
        # values 1.0, 2.0 have labels
        assert region_vals[0] == "North"
        assert region_vals[1] == "South"


# ---------------------------------------------------------------------------
# Enum-specific
# ---------------------------------------------------------------------------

class TestEnumOrdering:
    def test_category_order_follows_label_definition(self, simple_df, simple_meta):
        result = am.apply_labels(simple_df, simple_meta)
        categories = result["region"].dtype.categories.to_list()
        # Labels defined as {1: "North", 2: "South", 3: "East"} → order preserved
        assert categories == ["North", "South", "East"]

    def test_sort_follows_definition_order(self, simple_df, simple_meta):
        result = am.apply_labels(simple_df, simple_meta)
        sorted_vals = result.select(pl.col("region").unique().sort()).to_series().to_list()
        assert sorted_vals == ["North", "South", "East"]

    def test_duplicate_labels_deduplicated(self):
        df = pl.DataFrame({"x": [1.0, 2.0, 3.0]})
        meta = am.SpssMetadata(variable_value_labels={
            "x": {1: "Yes", 2: "Yes", 3: "No"},
        })
        # Duplicate labels → validation error (duplicate check)
        with pytest.raises(ValueError, match="duplicate"):
            am.apply_labels(df, meta)


# ---------------------------------------------------------------------------
# Dtype-aware behavior
# ---------------------------------------------------------------------------

class TestDtypeAware:
    def test_numeric_columns_labeled(self, simple_df, simple_meta):
        result = am.apply_labels(simple_df, simple_meta)
        assert result["gender"].dtype.base_type() == pl.Enum

    def test_string_columns_passthrough_unmapped(self, simple_df, simple_meta):
        result = am.apply_labels(simple_df, simple_meta)
        vals = result["code"].to_list()
        assert "Refused" in vals       # "RF" → "Refused"
        assert "Don't Know" in vals    # "DK" → "Don't Know"
        assert "hello" in vals         # unmapped text passes through
        assert vals[-1] is None        # null stays null

    def test_string_columns_always_string_dtype(self, simple_df, simple_meta):
        result = am.apply_labels(simple_df, simple_meta)
        assert result["code"].dtype == pl.String

    def test_string_columns_unaffected_by_output_mode(self, simple_df, simple_meta):
        for mode in ("enum", "string", "enum_null"):
            result = am.apply_labels(simple_df, simple_meta,
                                     output=mode if mode != "enum" else "enum")
            assert result["code"].dtype == pl.String
            assert "hello" in result["code"].to_list()

    def test_columns_without_labels_skipped(self, simple_df, simple_meta):
        result = am.apply_labels(simple_df, simple_meta)
        # age has no value labels → stays Float64
        assert result["age"].dtype == pl.Float64
        assert result["age"].to_list() == simple_df["age"].to_list()

    def test_no_labeled_columns_returns_unchanged(self):
        df = pl.DataFrame({"age": [25.0, 30.0]})
        meta = am.SpssMetadata()
        result = am.apply_labels(df, meta, output="string")
        assert result["age"].dtype == pl.Float64


# ---------------------------------------------------------------------------
# Column selection
# ---------------------------------------------------------------------------

class TestColumnSelection:
    def test_columns_none_applies_all(self, simple_df, simple_meta):
        result = am.apply_labels(simple_df, simple_meta)
        # gender, region, code should all be labeled
        assert result["gender"].dtype != pl.Float64
        assert result["region"].dtype != pl.Float64
        assert result["code"].dtype == pl.String  # was String, still String but labeled

    def test_columns_specific(self, simple_df, simple_meta):
        result = am.apply_labels(simple_df, simple_meta, columns=["gender"])
        assert result["gender"].dtype.base_type() == pl.Enum
        # region should NOT be labeled (not in columns list)
        assert result["region"].dtype == pl.Float64

    def test_columns_not_in_data_raises(self, simple_df, simple_meta):
        with pytest.raises(ValueError, match="not in data"):
            am.apply_labels(simple_df, simple_meta, columns=["nonexistent"])

    def test_columns_empty_labels_raises(self):
        df = pl.DataFrame({"x": [1.0, 2.0]})
        meta = am.SpssMetadata(variable_value_labels={"x": {}})
        with pytest.raises(ValueError, match="No value labels"):
            am.apply_labels(df, meta, columns=["x"])

    def test_columns_no_labels_in_metadata_raises(self):
        df = pl.DataFrame({"x": [1.0], "y": [2.0]})
        meta = am.SpssMetadata(variable_value_labels={"x": {1: "A"}})
        with pytest.raises(ValueError, match="No value labels"):
            am.apply_labels(df, meta, columns=["y"])


# ---------------------------------------------------------------------------
# Null handling
# ---------------------------------------------------------------------------

class TestNullHandling:
    def test_null_stays_null_enum(self, simple_df, simple_meta):
        result = am.apply_labels(simple_df, simple_meta)
        assert result["gender"].to_list()[-1] is None

    def test_null_stays_null_string(self, simple_df, simple_meta):
        result = am.apply_labels(simple_df, simple_meta, output="string")
        assert result["gender"].to_list()[-1] is None

    def test_null_stays_null_enum_null(self, simple_df, simple_meta):
        result = am.apply_labels(simple_df, simple_meta, output="enum_null")
        assert result["gender"].to_list()[-1] is None

    def test_all_null_column_passes(self):
        df = pl.DataFrame({"x": [None, None, None]}, schema={"x": pl.Float64})
        meta = am.SpssMetadata(variable_value_labels={"x": {1: "A", 2: "B"}})
        result = am.apply_labels(df, meta)
        assert result["x"].dtype.base_type() == pl.Enum
        assert result["x"].null_count() == 3
        assert result["x"].dtype.categories.to_list() == ["A", "B"]


# ---------------------------------------------------------------------------
# Validation errors
# ---------------------------------------------------------------------------

class TestValidation:
    def test_invalid_output_raises(self, simple_df, simple_meta):
        with pytest.raises(ValueError, match="'enum', 'string', or 'enum_null'"):
            am.apply_labels(simple_df, simple_meta, output="bad")

    def test_non_dataframe_raises(self, simple_meta):
        with pytest.raises(TypeError, match="Expected DataFrame or LazyFrame"):
            am.apply_labels("not a df", simple_meta)

    def test_unmapped_enum_raises(self, simple_df, partial_meta):
        with pytest.raises(ValueError, match="unmapped"):
            am.apply_labels(simple_df, partial_meta, output="enum")


# ---------------------------------------------------------------------------
# LazyFrame
# ---------------------------------------------------------------------------

class TestLazyFrame:
    def test_returns_lazyframe(self, simple_df, simple_meta):
        lf = simple_df.lazy()
        result = am.apply_labels(lf, simple_meta)
        assert isinstance(result, pl.LazyFrame)

    def test_collects_correctly(self, simple_df, simple_meta):
        lf = simple_df.lazy()
        result = am.apply_labels(lf, simple_meta).collect()
        assert result["gender"].to_list() == ["Male", "Female", "Male", "Female", None]

    def test_enum_dtype_after_collect(self, simple_df, simple_meta):
        lf = simple_df.lazy()
        result = am.apply_labels(lf, simple_meta).collect()
        assert result["gender"].dtype.base_type() == pl.Enum

    def test_lazy_unmapped_raises(self, simple_df, partial_meta):
        lf = simple_df.lazy()
        with pytest.raises(ValueError, match="unmapped"):
            am.apply_labels(lf, partial_meta, output="enum")


# ---------------------------------------------------------------------------
# Structured error messages
# ---------------------------------------------------------------------------

class TestErrorMessages:
    def test_single_column_unmapped(self, simple_df, partial_meta):
        with pytest.raises(ValueError) as exc_info:
            am.apply_labels(simple_df, partial_meta, output="enum")
        msg = str(exc_info.value)
        assert "region" in msg
        assert "3.0" in msg
        assert "unmapped" in msg

    def test_multiple_columns_unmapped(self):
        df = pl.DataFrame({"a": [1.0, 9.0], "b": [1.0, 8.0]})
        meta = am.SpssMetadata(variable_value_labels={
            "a": {1: "X"},
            "b": {1: "Y"},
        })
        with pytest.raises(ValueError) as exc_info:
            am.apply_labels(df, meta, output="enum")
        msg = str(exc_info.value)
        assert "2 columns" in msg
        assert "a:" in msg
        assert "b:" in msg

    def test_duplicate_labels_detected(self):
        df = pl.DataFrame({"x": [1.0, 2.0, 3.0]})
        meta = am.SpssMetadata(variable_value_labels={
            "x": {1: "Yes", 2: "Yes", 3: "No"},
        })
        with pytest.raises(ValueError) as exc_info:
            am.apply_labels(df, meta, output="enum")
        msg = str(exc_info.value)
        assert "duplicate" in msg
        assert "'Yes'" in msg

    def test_error_references_output_mode(self, simple_df, partial_meta):
        with pytest.raises(ValueError) as exc_info:
            am.apply_labels(simple_df, partial_meta, output="enum")
        assert "output='enum'" in str(exc_info.value)

    def test_error_suggests_alternatives(self, simple_df, partial_meta):
        with pytest.raises(ValueError) as exc_info:
            am.apply_labels(simple_df, partial_meta, output="enum")
        msg = str(exc_info.value)
        assert 'output="enum_null"' in msg
        assert 'output="string"' in msg


# ---------------------------------------------------------------------------
# Real file test
# ---------------------------------------------------------------------------

class TestRealFile:
    def test_read_and_apply(self):
        sav = am.read_sav("test_data/test_1_small.sav")
        df, meta = sav.data, sav.meta
        # Use enum_null since we don't know if all values are labeled
        labeled = am.apply_labels(df, meta, output="enum_null")
        # At least some columns should now be Enum
        enum_cols = [
            c for c in labeled.columns
            if labeled[c].dtype.base_type() == pl.Enum
        ]
        assert len(enum_cols) > 0

    def test_string_mode_on_real_file(self):
        sav = am.read_sav("test_data/test_1_small.sav")
        df, meta = sav.data, sav.meta
        labeled = am.apply_labels(df, meta, output="string")
        string_cols = [c for c in labeled.columns if labeled[c].dtype == pl.String]
        assert len(string_cols) > 0

    def test_lazy_real_file(self):
        sav = am.scan_sav("test_data/test_1_small.sav")
        df, meta = sav.data, sav.meta
        labeled = am.apply_labels(df, meta, output="enum_null")
        assert isinstance(labeled, pl.LazyFrame)
        result = labeled.collect()
        assert result.height > 0
