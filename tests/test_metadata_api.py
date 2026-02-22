"""Tests for SpssMetadata construction, update, with_*(), and validation.

Run with:
    pytest tests/test_metadata_api.py -v
"""

import os
import tempfile

import polars as pl
import pytest

import ambers as am


# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------

class TestConstructor:
    def test_empty_constructor(self):
        meta = am.SpssMetadata()
        assert meta.file_label == ""
        assert meta.variable_labels == {}
        assert meta.variable_measures == {}
        assert meta.notes == []
        assert meta.weight_variable is None

    def test_constructor_with_kwargs(self):
        meta = am.SpssMetadata(
            file_label="Customer Survey 2026",
            variable_labels={"Q1": "Satisfaction", "Q2": "Loyalty"},
            variable_measures={"Q1": "ordinal", "Q2": "nominal"},
        )
        assert meta.file_label == "Customer Survey 2026"
        assert meta.variable_labels == {"Q1": "Satisfaction", "Q2": "Loyalty"}
        assert meta.variable_measures == {"Q1": "ordinal", "Q2": "nominal"}

    def test_constructor_notes_string(self):
        meta = am.SpssMetadata(notes="single note")
        assert meta.notes == ["single note"]

    def test_constructor_notes_list(self):
        meta = am.SpssMetadata(notes=["note 1", "note 2"])
        assert meta.notes == ["note 1", "note 2"]

    def test_constructor_value_labels(self):
        meta = am.SpssMetadata(variable_value_labels={
            "gender": {1: "Male", 2: "Female"},
            "country": {"US": "United States", "UK": "United Kingdom"},
        })
        assert meta.variable_value_labels["gender"] == {1.0: "Male", 2.0: "Female"}
        assert meta.variable_value_labels["country"] == {"US": "United States", "UK": "United Kingdom"}


# ---------------------------------------------------------------------------
# Immutability — update() and with_*() never modify original
# ---------------------------------------------------------------------------

class TestImmutability:
    def test_update_returns_new_instance(self):
        meta = am.SpssMetadata(file_label="Original")
        meta2 = meta.update(file_label="Updated")
        assert meta.file_label == "Original"
        assert meta2.file_label == "Updated"

    def test_update_merges_dict_fields(self):
        meta = am.SpssMetadata(variable_labels={"Q1": "Sat", "Q2": "Loy"})
        meta2 = meta.update(variable_labels={"Q3": "NPS"})
        assert meta.variable_labels == {"Q1": "Sat", "Q2": "Loy"}
        assert meta2.variable_labels == {"Q1": "Sat", "Q2": "Loy", "Q3": "NPS"}

    def test_removal_via_none(self):
        meta = am.SpssMetadata(variable_labels={"Q1": "Sat", "Q2": "Loy"})
        meta2 = meta.update(variable_labels={"Q1": None})
        assert "Q1" in meta.variable_labels
        assert "Q1" not in meta2.variable_labels
        assert meta2.variable_labels == {"Q2": "Loy"}

    def test_with_file_label_immutable(self):
        meta = am.SpssMetadata(file_label="Original")
        meta2 = meta.with_file_label("New")
        assert meta.file_label == "Original"
        assert meta2.file_label == "New"

    def test_chainable_with_methods(self):
        meta = (
            am.SpssMetadata()
            .with_file_label("Chained")
            .with_variable_labels({"Q1": "Q1 label"})
            .with_variable_measures({"Q1": "ordinal"})
            .with_variable_roles({"Q1": "input"})
        )
        assert meta.file_label == "Chained"
        assert meta.variable_labels == {"Q1": "Q1 label"}
        assert meta.variable_measures == {"Q1": "ordinal"}
        assert meta.variable_roles == {"Q1": "input"}


# ---------------------------------------------------------------------------
# Validation — measures, alignments, roles
# ---------------------------------------------------------------------------

class TestEnumValidation:
    def test_invalid_measure(self):
        with pytest.raises(ValueError, match="invalid measure"):
            am.SpssMetadata(variable_measures={"Q1": "invalid"})

    def test_valid_measures(self):
        for m in ["nominal", "ordinal", "scale", "unknown"]:
            meta = am.SpssMetadata(variable_measures={"Q1": m})
            assert meta.variable_measures["Q1"] == m

    def test_invalid_alignment(self):
        with pytest.raises(ValueError, match="invalid alignment"):
            am.SpssMetadata(variable_alignments={"Q1": "middle"})

    def test_valid_alignments(self):
        for a in ["left", "right", "center"]:
            meta = am.SpssMetadata(variable_alignments={"Q1": a})
            assert meta.variable_alignments["Q1"] == a

    def test_invalid_role(self):
        with pytest.raises(ValueError, match="invalid role"):
            am.SpssMetadata(variable_roles={"Q1": "superuser"})

    def test_valid_roles(self):
        for r in ["input", "target", "both", "none", "partition", "split"]:
            meta = am.SpssMetadata(variable_roles={"Q1": r})
            assert meta.variable_roles["Q1"] == r


# ---------------------------------------------------------------------------
# Missing values validation
# ---------------------------------------------------------------------------

class TestMissingValuesValidation:
    def test_discrete_numeric(self):
        meta = am.SpssMetadata(variable_missing_values={
            "Q1": {"type": "discrete", "values": [98, 99]},
        })
        mv = meta.variable_missing_values["Q1"]
        assert mv["type"] == "discrete"
        assert mv["values"] == [98.0, 99.0]

    def test_discrete_string(self):
        meta = am.SpssMetadata(variable_missing_values={
            "city": {"type": "discrete", "values": ["N/A", "DK"]},
        })
        mv = meta.variable_missing_values["city"]
        assert mv["type"] == "discrete"
        assert mv["values"] == ["N/A", "DK"]

    def test_range(self):
        meta = am.SpssMetadata(variable_missing_values={
            "score": {"type": "range", "low": 900, "high": 999},
        })
        mv = meta.variable_missing_values["score"]
        assert mv["type"] == "range"
        assert mv["low"] == 900.0
        assert mv["high"] == 999.0

    def test_range_with_discrete(self):
        meta = am.SpssMetadata(variable_missing_values={
            "income": {"type": "range", "low": 999990, "high": 999999, "discrete": 0},
        })
        mv = meta.variable_missing_values["income"]
        assert mv["type"] == "range"
        assert mv["discrete"] == 0.0

    def test_max_3_discrete(self):
        with pytest.raises(ValueError, match="maximum 3"):
            am.SpssMetadata(variable_missing_values={
                "Q1": {"type": "discrete", "values": [1, 2, 3, 4]},
            })

    def test_range_low_ge_high(self):
        with pytest.raises(ValueError, match="less than"):
            am.SpssMetadata(variable_missing_values={
                "Q1": {"type": "range", "low": 100, "high": 50},
            })

    def test_discrete_between_range(self):
        with pytest.raises(ValueError, match="must not fall between"):
            am.SpssMetadata(variable_missing_values={
                "Q1": {"type": "range", "low": 10, "high": 100, "discrete": 50},
            })

    def test_duplicate_numeric_values(self):
        with pytest.raises(ValueError, match="unique"):
            am.SpssMetadata(variable_missing_values={
                "Q1": {"type": "discrete", "values": [99, 99]},
            })

    def test_string_exceeds_8_chars(self):
        with pytest.raises(ValueError, match="exceeds 8 characters"):
            am.SpssMetadata(variable_missing_values={
                "Q1": {"type": "discrete", "values": ["123456789"]},
            })

    def test_mixed_numeric_string_rejected(self):
        """Bug fix: mixed numeric and string values in same spec must raise."""
        with pytest.raises(ValueError, match="cannot mix numeric and string"):
            am.SpssMetadata(variable_missing_values={
                "gender": {"type": "discrete", "values": [3, "33"]},
            })

    def test_string_missing_on_numeric_column_at_write(self):
        """Bug fix: string missing values on numeric column caught at write time."""
        df = pl.DataFrame({"age": [25.0, 30.0, 45.0]})
        meta = am.SpssMetadata(
            variable_missing_values={"age": {"type": "discrete", "values": ["NA", "DK"]}},
        )
        with tempfile.NamedTemporaryFile(suffix=".sav", delete=False) as f:
            path = f.name
        try:
            with pytest.raises(Exception, match="string missing values cannot be applied to a numeric variable"):
                am.write_sav(df, path, meta=meta)
        finally:
            if os.path.exists(path):
                os.unlink(path)

    def test_numeric_missing_on_string_column_at_write(self):
        """Bug fix: numeric missing values on string column caught at write time."""
        df = pl.DataFrame({"name": ["Alice", "Bob", "Carol"]})
        meta = am.SpssMetadata(
            variable_missing_values={"name": {"type": "discrete", "values": [99]}},
        )
        with tempfile.NamedTemporaryFile(suffix=".sav", delete=False) as f:
            path = f.name
        try:
            with pytest.raises(Exception, match="numeric missing values cannot be applied to a string variable"):
                am.write_sav(df, path, meta=meta)
        finally:
            if os.path.exists(path):
                os.unlink(path)


# ---------------------------------------------------------------------------
# MR sets validation
# ---------------------------------------------------------------------------

class TestMrSetsValidation:
    def test_no_dollar_required(self):
        """Bug fix: $ prefix not required — SAV format adds it automatically."""
        meta = am.SpssMetadata(mr_sets={
            "Q6cat": {
                "label": "Brand selected",
                "type": "category",
                "variables": ["q6_1", "q6_2", "q6_3"],
            },
        })
        assert "Q6cat" in meta.mr_sets

    def test_dollar_prefix_accepted(self):
        """User can still pass $ if they want — stored as-is."""
        meta = am.SpssMetadata(mr_sets={
            "$Q6cat": {
                "label": "Brand selected",
                "type": "category",
                "variables": ["q6_1", "q6_2", "q6_3"],
            },
        })
        assert "$Q6cat" in meta.mr_sets

    def test_min_2_variables(self):
        with pytest.raises(ValueError, match="at least 2"):
            am.SpssMetadata(mr_sets={
                "Q6": {"type": "category", "variables": ["q6_1"]},
            })

    def test_invalid_type(self):
        with pytest.raises(ValueError, match="invalid MR set type"):
            am.SpssMetadata(mr_sets={
                "Q6": {"type": "multiple", "variables": ["a", "b"]},
            })

    def test_dichotomy_requires_counted_value(self):
        with pytest.raises(ValueError, match="counted_value"):
            am.SpssMetadata(mr_sets={
                "Q6": {
                    "type": "dichotomy",
                    "counted_value": None,
                    "variables": ["a", "b"],
                },
            })

    def test_category_ignores_counted_value(self):
        meta = am.SpssMetadata(mr_sets={
            "Q6": {
                "type": "category",
                "variables": ["a", "b"],
            },
        })
        assert meta.mr_sets["Q6"]["counted_value"] is None

    def test_mr_set_roundtrip_no_dollar(self):
        """Write MR set without $ prefix, verify single $ in readback."""
        df = pl.DataFrame({"q1": [1.0, 0.0], "q2": [0.0, 1.0], "q3": [1.0, 1.0]})
        meta = am.SpssMetadata(mr_sets={
            "Q6cat": {
                "label": "Brand selected",
                "type": "category",
                "variables": ["q1", "q2", "q3"],
            },
        })
        with tempfile.NamedTemporaryFile(suffix=".sav", delete=False) as f:
            path = f.name
        try:
            am.write_sav(df, path, meta=meta)
            _, meta2 = am.read_sav(path)
            assert "Q6cat" in meta2.mr_sets
            assert meta2.mr_sets["Q6cat"]["label"] == "Brand selected"
            assert meta2.mr_sets["Q6cat"]["type"] == "category"
            assert meta2.mr_sets["Q6cat"]["variables"] == ["q1", "q2", "q3"]
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# Write roundtrip — from-scratch metadata
# ---------------------------------------------------------------------------

class TestWriteRoundtrip:
    def test_from_scratch_metadata(self):
        df = pl.DataFrame({
            "age": [25.0, 30.0, None, 45.0],
            "gender": [1.0, 2.0, 1.0, None],
            "name": ["Alice", "Bob", "Carol", None],
        })
        meta = am.SpssMetadata(
            file_label="Test Survey",
            variable_labels={"age": "Age in years", "gender": "Gender", "name": "Name"},
            variable_value_labels={"gender": {1: "Male", 2: "Female"}},
            variable_measures={"age": "scale", "gender": "nominal", "name": "nominal"},
            variable_formats={"age": "F3.0", "gender": "F1.0", "name": "A50"},
            variable_missing_values={"age": {"type": "discrete", "values": [99]}},
            notes="Test file",
        )
        with tempfile.NamedTemporaryFile(suffix=".sav", delete=False) as f:
            path = f.name
        try:
            am.write_sav(df, path, meta=meta)
            df2, meta2 = am.read_sav(path)
            assert df2.shape == (4, 3)
            assert meta2.file_label == "Test Survey"
            assert meta2.label("age") == "Age in years"
            assert meta2.value("gender") == {1.0: "Male", 2.0: "Female"}
            assert meta2.measure("age") == "scale"
            assert meta2.format("age") == "F3.0"
            assert meta2.format("name") == "A50"
            assert meta2.notes == ["Test file"]
        finally:
            os.unlink(path)

    def test_roundtrip_with_override(self):
        df = pl.DataFrame({"age": [25.0, 30.0], "gender": [1.0, 2.0]})
        meta_init = am.SpssMetadata(
            variable_labels={"age": "Original", "gender": "Gender"},
            variable_formats={"age": "F3.0", "gender": "F1.0"},
        )
        with tempfile.NamedTemporaryFile(suffix=".sav", delete=False) as f:
            path1 = f.name
        with tempfile.NamedTemporaryFile(suffix=".sav", delete=False) as f:
            path2 = f.name
        try:
            am.write_sav(df, path1, meta=meta_init)
            df_read, meta_read = am.read_sav(path1)
            meta_updated = meta_read.update(
                file_label="Updated file",
                variable_labels={"age": "Updated Age Label"},
            )
            am.write_sav(df_read, path2, meta=meta_updated)
            _, meta3 = am.read_sav(path2)
            assert meta3.file_label == "Updated file"
            assert meta3.label("age") == "Updated Age Label"
            assert meta3.label("gender") == "Gender"
        finally:
            os.unlink(path1)
            os.unlink(path2)

    def test_no_meta_inferred(self):
        df = pl.DataFrame({"x": [1.0, 2.0], "y": ["a", "b"]})
        with tempfile.NamedTemporaryFile(suffix=".sav", delete=False) as f:
            path = f.name
        try:
            am.write_sav(df, path)
            df2, meta2 = am.read_sav(path)
            assert df2.shape == (2, 2)
            assert meta2.format("x") == "F8.2"
        finally:
            os.unlink(path)

    def test_zsav_compression(self):
        df = pl.DataFrame({"x": [1.0, 2.0]})
        with tempfile.NamedTemporaryFile(suffix=".zsav", delete=False) as f:
            path = f.name
        try:
            am.write_sav(df, path)
            _, meta = am.read_sav(path)
            assert meta.compression == "zlib"
        finally:
            os.unlink(path)

    def test_uncompressed(self):
        df = pl.DataFrame({"x": [1.0, 2.0]})
        with tempfile.NamedTemporaryFile(suffix=".sav", delete=False) as f:
            path = f.name
        try:
            am.write_sav(df, path, compress=False)
            _, meta = am.read_sav(path)
            assert meta.compression == "none"
        finally:
            os.unlink(path)
