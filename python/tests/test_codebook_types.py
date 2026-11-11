"""Tests for codebook variable type detection (Phase 1).

Covers: Phase 1 dtype detection, Phase 2 metadata refinement,
all 7 multi-select tiers, edge cases.

Run with:
    pytest tests/test_codebook_types.py -v
"""

import polars as pl
import pytest

import ambers as am
from ambers.codebook._types import detect_types, create_mr_set_lookup


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _meta(**kwargs) -> am.SpssMetadata:
    return am.SpssMetadata(**kwargs)


def _detect(df, meta=None, col=None):
    """Detect types and return for specific col or full dict."""
    if meta is None:
        meta = am.SpssMetadata()
    result = detect_types(df, meta)
    if col is not None:
        return result[col]
    return result


# ---------------------------------------------------------------------------
# Phase 1: dtype detection
# ---------------------------------------------------------------------------

class TestPhase1Dtype:
    def test_float64_is_numeric(self):
        df = pl.DataFrame({"x": [1.0, 2.0, 3.0]})
        assert _detect(df, col="x") == "numeric"

    def test_int64_is_numeric(self):
        df = pl.DataFrame({"x": pl.Series([1, 2, 3], dtype=pl.Int64)})
        assert _detect(df, col="x") == "numeric"

    def test_string_is_text(self):
        df = pl.DataFrame({"x": ["a", "b", "c"]})
        assert _detect(df, col="x") == "text"

    def test_date_is_date(self):
        from datetime import date
        df = pl.DataFrame({"x": [date(2024, 1, 1), date(2024, 1, 2)]})
        assert _detect(df, col="x") == "date"

    def test_datetime_is_date(self):
        from datetime import datetime
        df = pl.DataFrame({"x": [datetime(2024, 1, 1), datetime(2024, 1, 2)]})
        assert _detect(df, col="x") == "date"

    def test_duration_is_date(self):
        from datetime import timedelta
        df = pl.DataFrame({"x": [timedelta(hours=1), timedelta(hours=2)]})
        assert _detect(df, col="x") == "date"

    def test_boolean_is_numeric(self):
        df = pl.DataFrame({"x": [True, False, True]})
        assert _detect(df, col="x") == "numeric"


# ---------------------------------------------------------------------------
# Phase 2: metadata refinement → single-select
# ---------------------------------------------------------------------------

class TestSingleSelect:
    def test_numeric_with_labels_is_single_select(self):
        df = pl.DataFrame({"Q1": [1.0, 2.0, 3.0]})
        meta = _meta(variable_value_labels={"Q1": {1.0: "A", 2.0: "B", 3.0: "C"}})
        assert _detect(df, meta, col="Q1") == "single-select"

    def test_numeric_without_labels_is_numeric(self):
        df = pl.DataFrame({"age": [25.0, 30.0, 35.0]})
        meta = _meta()
        assert _detect(df, meta, col="age") == "numeric"

    def test_scale_measure_without_labels_is_numeric(self):
        df = pl.DataFrame({"score": [1.0, 2.0, 3.0]})
        meta = _meta(variable_measures={"score": "scale"})
        assert _detect(df, meta, col="score") == "numeric"

    def test_generic_binary_yes_no_is_single_select(self):
        """Generic binary labels (no/yes) → single-select, not multi-select."""
        df = pl.DataFrame({"gender": [0.0, 1.0]})
        meta = _meta(variable_value_labels={"gender": {0.0: "No", 1.0: "Yes"}})
        assert _detect(df, meta, col="gender") == "single-select"

    def test_generic_binary_male_female_is_single_select(self):
        df = pl.DataFrame({"sex": [0.0, 1.0]})
        meta = _meta(variable_value_labels={"sex": {0.0: "Male", 1.0: "Female"}})
        assert _detect(df, meta, col="sex") == "single-select"

    def test_non_binary_with_labels_is_single_select(self):
        df = pl.DataFrame({"Q1": [1.0, 2.0, 3.0, 4.0, 5.0]})
        meta = _meta(variable_value_labels={
            "Q1": {1.0: "Very Low", 2.0: "Low", 3.0: "Medium", 4.0: "High", 5.0: "Very High"}
        })
        assert _detect(df, meta, col="Q1") == "single-select"


# ---------------------------------------------------------------------------
# Multi-select detection — all 7 tiers
# ---------------------------------------------------------------------------

class TestMultiSelectTier1:
    """Tier 1: Variable in meta.mr_sets → multi-select (definitive)."""

    def test_in_mr_sets(self):
        df = pl.DataFrame({"Q1_1": [0.0, 1.0], "Q1_2": [1.0, 0.0]})
        meta = _meta(
            variable_value_labels={"Q1_1": {1.0: "Selected"}, "Q1_2": {1.0: "Selected"}},
            mr_sets={"Q1": {
                "type": "dichotomy",
                "label": "Question 1",
                "counted_value": "1",
                "variables": ["Q1_1", "Q1_2"],
            }},
        )
        assert _detect(df, meta, col="Q1_1") == "multi-select"
        assert _detect(df, meta, col="Q1_2") == "multi-select"


class TestMultiSelectTier2:
    """Tier 2: Binary {0,1} + metadata confirms 0/1 labels."""

    def test_strong_pattern_with_metadata(self):
        df = pl.DataFrame({"Q1_1": [0.0, 1.0, 0.0]})
        meta = _meta(variable_value_labels={"Q1_1": {0.0: "Not selected", 1.0: "Selected"}})
        # metadata_confirms_01 is True (keys normalize to {0,1})
        assert _detect(df, meta, col="Q1_1") == "multi-select"

    def test_strong_pattern_unlabeled(self):
        """Unlabeled binary {0,1} with no labels at all → multi-select (strong pattern allows unlabeled)."""
        df = pl.DataFrame({"Q1_1": [0.0, 1.0]})
        meta = _meta()  # no labels
        assert _detect(df, meta, col="Q1_1") == "multi-select"

    def test_weak_pattern_needs_evidence(self):
        """Single value {1} without metadata/sibling evidence → stays numeric."""
        df = pl.DataFrame({"Q1_1": [1.0, 1.0, 1.0]})
        meta = _meta()  # no labels, no siblings
        assert _detect(df, meta, col="Q1_1") == "numeric"


class TestMultiSelectTier3:
    """Tier 3: Binary {0,1} + sibling series confirmation."""

    def test_sibling_series_confirms(self):
        df = pl.DataFrame({
            "Q4A": [0.0, 1.0],
            "Q4B": [1.0, 0.0],
            "Q4C": [0.0, 0.0],
        })
        meta = _meta(variable_value_labels={
            "Q4A": {0.0: "No", 1.0: "Yes"},
            "Q4B": {0.0: "No", 1.0: "Yes"},
            "Q4C": {0.0: "No", 1.0: "Yes"},
        })
        # Q4A has siblings Q4B, Q4C — all with 0/1 coding
        assert _detect(df, meta, col="Q4A") == "multi-select"


class TestMultiSelectTier4:
    """Tier 4: Descriptive label on 1, empty/null on 0."""

    def test_descriptive_label_on_1(self):
        df = pl.DataFrame({"Q1_health": [0.0, 1.0]})
        meta = _meta(variable_value_labels={
            "Q1_health": {0.0: "Not selected", 1.0: "Health insurance"}
        })
        assert _detect(df, meta, col="Q1_health") == "multi-select"


class TestMultiSelectTier5:
    """Tier 5: Selection pair + naming pattern."""

    def test_selection_pair_with_name_pattern(self):
        df = pl.DataFrame({"Q5_1": [0.0, 1.0]})
        meta = _meta(variable_value_labels={
            "Q5_1": {0.0: "Not selected", 1.0: "Selected"}
        })
        # Labels match SELECTION_PAIRS AND name matches pattern (ends with _1)
        assert _detect(df, meta, col="Q5_1") == "multi-select"

    def test_selection_pair_without_name_pattern(self):
        """Selection pair labels but name doesn't match pattern.
        Still multi-select via Tier 2 (metadata confirms 0/1 coding + not generic binary)."""
        df = pl.DataFrame({"response": [0.0, 1.0]})
        meta = _meta(variable_value_labels={
            "response": {0.0: "Not selected", 1.0: "Selected"}
        })
        # "Not selected"/"Selected" is NOT generic binary → Tier 2 fires
        result = _detect(df, meta, col="response")
        assert result == "multi-select"


class TestMultiSelectTier6:
    """Tier 6: Binary sibling series (all siblings also binary)."""

    def test_binary_sibling_series(self):
        df = pl.DataFrame({
            "itemA": [0.0, 1.0],
            "itemB": [1.0, 0.0],
            "itemC": [0.0, 0.0],
        })
        meta = _meta(variable_value_labels={
            "itemA": {0.0: "Off", 1.0: "On"},
            "itemB": {0.0: "Off", 1.0: "On"},
            "itemC": {0.0: "Off", 1.0: "On"},
        })
        # Off/On is generic binary → would be single-select alone
        # But sibling series all binary → tier 6 returns multi-select
        # Actually tier 5b (binary sibling series) fires before tier 6
        assert _detect(df, meta, col="itemA") == "multi-select"


# ---------------------------------------------------------------------------
# Text and date pass-through
# ---------------------------------------------------------------------------

class TestTextDate:
    def test_text_stays_text(self):
        df = pl.DataFrame({"name": ["Alice", "Bob"]})
        meta = _meta(variable_labels={"name": "Respondent Name"})
        assert _detect(df, meta, col="name") == "text"

    def test_date_stays_date(self):
        from datetime import date
        df = pl.DataFrame({"dob": [date(1990, 1, 1)]})
        meta = _meta(variable_formats={"dob": "DATE11"})
        assert _detect(df, meta, col="dob") == "date"


# ---------------------------------------------------------------------------
# MR set lookup
# ---------------------------------------------------------------------------

class TestMrSetLookup:
    def test_ambers_format(self):
        meta = _meta(mr_sets={
            "brand": {
                "type": "dichotomy",
                "label": "Brand awareness",
                "counted_value": "1",
                "variables": ["brand_1", "brand_2", "brand_3"],
            },
        })
        result = create_mr_set_lookup(meta)
        assert result == {"brand_1", "brand_2", "brand_3"}

    def test_empty_mr_sets(self):
        meta = _meta()
        result = create_mr_set_lookup(meta)
        assert result == set()

    def test_multiple_sets(self):
        meta = _meta(mr_sets={
            "s1": {"type": "dichotomy", "label": "", "counted_value": "1", "variables": ["a", "b"]},
            "s2": {"type": "category", "label": "", "variables": ["c", "d"]},
        })
        result = create_mr_set_lookup(meta)
        assert result == {"a", "b", "c", "d"}


# ---------------------------------------------------------------------------
# Mixed columns
# ---------------------------------------------------------------------------

class TestMixedColumns:
    def test_multiple_types_in_one_df(self):
        from datetime import date
        df = pl.DataFrame({
            "id": [1.0, 2.0, 3.0],
            "Q1": [1.0, 2.0, 3.0],
            "name": ["Alice", "Bob", "Charlie"],
            "dob": [date(1990, 1, 1), date(1991, 2, 2), date(1992, 3, 3)],
        })
        meta = _meta(
            variable_value_labels={"Q1": {1.0: "Low", 2.0: "Med", 3.0: "High"}},
            variable_measures={"id": "scale"},
        )
        types = _detect(df, meta)
        assert types["id"] == "numeric"
        assert types["Q1"] == "single-select"
        assert types["name"] == "text"
        assert types["dob"] == "date"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_empty_df(self):
        df = pl.DataFrame({"Q1": pl.Series([], dtype=pl.Float64)})
        meta = _meta(variable_value_labels={"Q1": {1.0: "Yes"}})
        # Empty df but has labels → single-select (no data to check binary patterns)
        assert _detect(df, meta, col="Q1") == "single-select"

    def test_all_null_column(self):
        df = pl.DataFrame({"Q1": pl.Series([None, None], dtype=pl.Float64)})
        meta = _meta(variable_value_labels={"Q1": {1.0: "Yes", 2.0: "No"}})
        assert _detect(df, meta, col="Q1") == "single-select"

    def test_no_meta(self):
        df = pl.DataFrame({"x": [1.0, 2.0], "y": ["a", "b"]})
        meta = _meta()
        types = _detect(df, meta)
        assert types["x"] == "numeric"
        assert types["y"] == "text"


# ---------------------------------------------------------------------------
# Real file tests (skip if not available)
# ---------------------------------------------------------------------------

import os

_REAL_FILE = "test_data/test_1_small.sav"
_MR_FILE = "test_data/data500-mrsets.sav"


@pytest.mark.skipif(not os.path.exists(_REAL_FILE), reason="test data not available")
class TestRealFile:
    def test_type_detection_on_real_data(self):
        sav = am.read_sav(_REAL_FILE)
        df, meta = sav.data, sav.meta
        types = detect_types(df, meta)
        # Should have at least some categorical and numeric
        type_values = set(types.values())
        assert "single-select" in type_values or "numeric" in type_values
        # All values should be valid types
        valid_types = {"text", "date", "numeric", "single-select", "multi-select", "categorical"}
        assert all(t in valid_types for t in type_values)


@pytest.mark.skipif(not os.path.exists(_MR_FILE), reason="MR set test data not available")
class TestRealMrSets:
    def test_mr_set_detection(self):
        sav = am.read_sav(_MR_FILE)
        df, meta = sav.data, sav.meta
        mr_vars = create_mr_set_lookup(meta)
        if mr_vars:
            types = detect_types(df, meta)
            # At least some MR set variables should be multi-select
            mr_types = {v: types[v] for v in mr_vars if v in types}
            assert any(t == "multi-select" for t in mr_types.values()), \
                f"Expected multi-select for MR set vars, got: {mr_types}"
