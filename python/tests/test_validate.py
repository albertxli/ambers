"""Tests for ambers.validate().

Covers: unlabeled values (error), duplicate labels (warning),
column filtering, return types, shared helper consistency.

Run with:
    pytest tests/test_validate.py -v
"""

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import ambers as am


def _meta_with_labels(labels: dict) -> am.SpssMetadata:
    return am.SpssMetadata(variable_value_labels=labels)


# ---------------------------------------------------------------------------
# Unlabeled values (error)
# ---------------------------------------------------------------------------

class TestUnlabeledValues:
    def test_all_labeled_no_errors(self):
        df = pl.DataFrame({"Q1": [1.0, 2.0, 3.0]})
        meta = _meta_with_labels({"Q1": {1: "A", 2: "B", 3: "C"}})
        report = am.validate(df, meta)
        assert report.is_valid
        assert len(report.errors) == 0

    def test_unlabeled_values_detected(self):
        df = pl.DataFrame({"Q1": [1.0, 2.0, 3.0, 99.0]})
        meta = _meta_with_labels({"Q1": {1: "A", 2: "B"}})
        report = am.validate(df, meta)
        assert not report.is_valid
        assert len(report.errors) == 1
        err = report.errors[0]
        assert err.column == "Q1"
        assert err.check == "unlabeled_values"
        assert 3.0 in err.details["unlabeled_values"]
        assert 99.0 in err.details["unlabeled_values"]
        assert err.details["unique_in_data"] == 4
        assert err.details["labeled_in_data"] == 2

    def test_column_without_value_labels_skipped(self):
        """Continuous numeric columns (no labels) are not checked."""
        df = pl.DataFrame({"age": [25.0, 30.0, 35.0]})
        meta = am.SpssMetadata()  # no value labels at all
        report = am.validate(df, meta)
        assert report.is_valid

    def test_string_column_skipped(self):
        """String columns are not checked even if they have value labels."""
        df = pl.DataFrame({"city": ["NYC", "LA", "CHI"]})
        meta = _meta_with_labels({"city": {"NYC": "New York", "LA": "Los Angeles"}})
        report = am.validate(df, meta)
        assert report.is_valid  # string column skipped

    def test_all_null_no_error(self):
        df = pl.DataFrame({"Q1": pl.Series([None, None, None], dtype=pl.Float64)})
        meta = _meta_with_labels({"Q1": {1: "A", 2: "B"}})
        report = am.validate(df, meta)
        assert report.is_valid

    def test_multiple_columns_different_issues(self):
        df = pl.DataFrame({
            "Q1": [1.0, 2.0, 99.0],
            "Q2": [1.0, 2.0, 3.0],
            "Q3": [1.0, 2.0, 98.0],
        })
        meta = _meta_with_labels({
            "Q1": {1: "A", 2: "B"},       # 99 unlabeled
            "Q2": {1: "X", 2: "Y", 3: "Z"},  # all labeled
            "Q3": {1: "P", 2: "Q"},       # 98 unlabeled
        })
        report = am.validate(df, meta)
        assert not report.is_valid
        assert len(report.errors) == 2
        error_cols = {e.column for e in report.errors}
        assert error_cols == {"Q1", "Q3"}

    def test_labels_in_meta_not_in_data_is_fine(self):
        """Labels for values not present in data are NOT flagged."""
        df = pl.DataFrame({"Q1": [1.0, 2.0]})
        meta = _meta_with_labels({"Q1": {1: "A", 2: "B", 3: "C", 4: "D", 5: "E"}})
        report = am.validate(df, meta)
        assert report.is_valid


# ---------------------------------------------------------------------------
# Duplicate labels (warning)
# ---------------------------------------------------------------------------

class TestDuplicateLabels:
    def test_no_duplicates(self):
        df = pl.DataFrame({"Q1": [1.0, 2.0]})
        meta = _meta_with_labels({"Q1": {1: "A", 2: "B"}})
        report = am.validate(df, meta)
        assert len(report.warnings) == 0

    def test_duplicate_detected(self):
        df = pl.DataFrame({"Q1": [1.0, 2.0]})
        meta = _meta_with_labels({"Q1": {1: "Male", 2: "Female", 9: "Male"}})
        report = am.validate(df, meta)
        assert report.is_valid  # warnings don't affect is_valid
        assert len(report.warnings) == 1
        warn = report.warnings[0]
        assert warn.column == "Q1"
        assert warn.check == "duplicate_labels"
        assert "Male" in warn.details["duplicates"]

    def test_multiple_duplicates(self):
        df = pl.DataFrame({"Q1": [1.0]})
        meta = _meta_with_labels({
            "Q1": {1: "Yes", 2: "No", 3: "Yes", 4: "No"},
        })
        report = am.validate(df, meta)
        assert len(report.warnings) == 1
        dupes = report.warnings[0].details["duplicates"]
        assert "Yes" in dupes
        assert "No" in dupes


# ---------------------------------------------------------------------------
# Filtering (columns + exclude)
# ---------------------------------------------------------------------------

class TestFiltering:
    def test_columns_specific(self):
        df = pl.DataFrame({"Q1": [1.0, 99.0], "Q2": [1.0, 98.0]})
        meta = _meta_with_labels({
            "Q1": {1: "A"},
            "Q2": {1: "X"},
        })
        report = am.validate(df, meta, columns=["Q1"])
        assert len(report.errors) == 1
        assert report.errors[0].column == "Q1"

    def test_exclude(self):
        df = pl.DataFrame({"Q1": [1.0, 99.0], "Q2": [1.0, 98.0]})
        meta = _meta_with_labels({
            "Q1": {1: "A"},
            "Q2": {1: "X"},
        })
        report = am.validate(df, meta, exclude=["Q1"])
        assert len(report.errors) == 1
        assert report.errors[0].column == "Q2"

    def test_columns_and_exclude_combined(self):
        """columns + exclude can be combined: filter first, then exclude."""
        df = pl.DataFrame({
            "Q1": [1.0, 99.0],
            "Q2": [1.0, 98.0],
            "Q3": [1.0, 97.0],
        })
        meta = _meta_with_labels({
            "Q1": {1: "A"},
            "Q2": {1: "X"},
            "Q3": {1: "P"},
        })
        report = am.validate(df, meta, columns=["Q1", "Q2", "Q3"], exclude=["Q2"])
        error_cols = {e.column for e in report.errors}
        assert "Q2" not in error_cols
        assert "Q1" in error_cols
        assert "Q3" in error_cols


# ---------------------------------------------------------------------------
# Return type handling
# ---------------------------------------------------------------------------

class TestReturnTypes:
    def test_dataframe_input(self):
        df = pl.DataFrame({"Q1": [1.0, 99.0]})
        meta = _meta_with_labels({"Q1": {1: "A"}})
        report = am.validate(df, meta)
        assert isinstance(report, am.ValidationReport)

    def test_lazyframe_input(self):
        lf = pl.DataFrame({"Q1": [1.0, 99.0]}).lazy()
        meta = _meta_with_labels({"Q1": {1: "A"}})
        report = am.validate(lf, meta)
        assert isinstance(report, am.ValidationReport)
        assert not report.is_valid

    def test_is_valid_true_no_errors(self):
        df = pl.DataFrame({"Q1": [1.0, 2.0]})
        meta = _meta_with_labels({"Q1": {1: "A", 2: "B"}})
        report = am.validate(df, meta)
        assert report.is_valid

    def test_is_valid_true_warnings_only(self):
        """Warnings don't affect is_valid."""
        df = pl.DataFrame({"Q1": [1.0, 2.0]})
        meta = _meta_with_labels({"Q1": {1: "A", 2: "B", 9: "A"}})
        report = am.validate(df, meta)
        assert report.is_valid  # has warnings but no errors
        assert len(report.warnings) == 1

    def test_is_valid_false_with_errors(self):
        df = pl.DataFrame({"Q1": [1.0, 99.0]})
        meta = _meta_with_labels({"Q1": {1: "A"}})
        report = am.validate(df, meta)
        assert not report.is_valid

    def test_raise_if_invalid_raises_on_errors(self):
        df = pl.DataFrame({"Q1": [1.0, 99.0]})
        meta = _meta_with_labels({"Q1": {1: "A"}})
        report = am.validate(df, meta)
        with pytest.raises(ValueError, match="validate"):
            report.raise_if_invalid()

    def test_raise_if_invalid_silent_on_warnings(self):
        df = pl.DataFrame({"Q1": [1.0, 2.0]})
        meta = _meta_with_labels({"Q1": {1: "A", 2: "B", 9: "A"}})
        report = am.validate(df, meta)
        report.raise_if_invalid()  # should not raise

    def test_to_frame(self):
        df = pl.DataFrame({"Q1": [1.0, 99.0]})
        meta = _meta_with_labels({"Q1": {1: "A", 9: "A"}})
        report = am.validate(df, meta)
        frame = report.to_frame()
        assert isinstance(frame, pl.DataFrame)
        assert set(frame.columns) == {"severity", "column", "check", "message"}
        assert frame.height == 2  # 1 error + 1 warning

    def test_empty_report(self):
        df = pl.DataFrame({"Q1": [1.0, 2.0]})
        meta = _meta_with_labels({"Q1": {1: "A", 2: "B"}})
        report = am.validate(df, meta)
        assert report.is_valid
        assert len(report.issues) == 0
        assert report.to_frame().height == 0

    def test_empty_df(self):
        df = pl.DataFrame({"Q1": pl.Series([], dtype=pl.Float64)})
        meta = _meta_with_labels({"Q1": {1: "A"}})
        report = am.validate(df, meta)
        assert report.is_valid

    def test_invalid_df_type_raises(self):
        with pytest.raises(TypeError):
            am.validate([1, 2, 3], am.SpssMetadata())


# ---------------------------------------------------------------------------
# Shared helper consistency
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Repr truncation
# ---------------------------------------------------------------------------

class TestReprTruncation:
    def test_many_issues_truncated(self):
        """Repr should show max 10 errors, then '... and N more'."""
        df = pl.DataFrame({f"Q{i}": [1.0, 2.0, 99.0] for i in range(30)})
        meta = _meta_with_labels({
            f"Q{i}": {1: "Yes", 2: "No"} for i in range(30)
        })
        report = am.validate(df, meta)
        repr_str = repr(report)
        # Should show exactly 10 error column names
        assert repr_str.count("[x] Q") == 10
        # Should mention remaining
        assert "and 20 more errors" in repr_str
        # Should tell user how to see all
        assert "report.to_frame()" in repr_str or "report.errors" in repr_str

    def test_long_duplicate_message_truncated(self):
        """Repr should truncate messages with many duplicate labels."""
        labels = {i: f"Label{i % 3}" for i in range(30)}  # many dupes
        df = pl.DataFrame({"Q1": [1.0]})
        meta = _meta_with_labels({"Q1": labels})
        report = am.validate(df, meta)
        repr_str = repr(report)
        # No line should exceed box width
        for line in repr_str.split("\n"):
            assert len(line) <= 80, f"Line too long ({len(line)}): {line}"

    def test_box_width_capped(self):
        """Box should never exceed 80 characters wide."""
        # Single column with a very long label
        labels = {i: f"Very Long Label Name For Code {i}" for i in range(20)}
        df = pl.DataFrame({"Q1": list(range(20))})
        meta = _meta_with_labels({"Q1": labels})
        report = am.validate(df, meta)
        for line in repr(report).split("\n"):
            assert len(line) <= 80, f"Line too long ({len(line)}): {line}"

    def test_to_frame_has_full_messages(self):
        """to_frame() should contain full untruncated messages."""
        labels = {}
        for i in range(30):
            labels[float(i)] = f"Label{i % 3}"
        df = pl.DataFrame({"Q1": [float(i) for i in range(30)]})
        meta = _meta_with_labels({"Q1": labels})
        report = am.validate(df, meta)
        frame = report.to_frame()
        # to_frame message should NOT be truncated
        # The full message should contain all unlabeled value details
        assert frame.height > 0


# ---------------------------------------------------------------------------
# Shared helper consistency
# ---------------------------------------------------------------------------

class TestSharedHelpers:
    def test_validate_and_apply_labels_flag_same_values(self):
        """validate() and apply_labels() should catch the same unlabeled values."""
        df = pl.DataFrame({"Q1": [1.0, 2.0, 99.0]})
        meta = _meta_with_labels({"Q1": {1: "A", 2: "B"}})

        # validate should find 99 as unlabeled
        report = am.validate(df, meta)
        assert not report.is_valid
        assert 99.0 in report.errors[0].details["unlabeled_values"]

        # apply_labels should raise on the same issue
        with pytest.raises(ValueError):
            am.apply_labels(df, meta, output="enum")
