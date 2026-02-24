"""Stress tests for pyreadstat issues #267, #119, #264.

Verifies ambers correctly handles:
- #267: String formats > 255 bytes (VLS format preservation)
- #119: Long string variables without ghost/phantom columns
- #264: Value labels on string variables with values > 8 chars

All tests save .sav output to test_data/writer_issues/ for manual SPSS validation.

Usage: python -m pytest tests/test_writer_issues.py -v
"""

import os
import pathlib

import polars as pl
import pytest

import ambers as am

try:
    import pyreadstat

    HAS_PYREADSTAT = True
except ImportError:
    HAS_PYREADSTAT = False

requires_pyreadstat = pytest.mark.skipif(
    not HAS_PYREADSTAT, reason="pyreadstat not installed"
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

WRITER_ISSUES_DIR = (
    pathlib.Path(__file__).resolve().parent.parent / "test_data" / "writer_issues"
)
WRITER_ISSUES_DIR.mkdir(parents=True, exist_ok=True)


def write_read_ambers(df, meta, name, suffix=".sav", **write_kw):
    """Write with ambers, read back with ambers, keep file for SPSS inspection."""
    path = str(WRITER_ISSUES_DIR / f"{name}{suffix}")
    am.write_sav(df, path, meta=meta, **write_kw)
    return am.read_sav(path)


def write_read_pyreadstat(df, meta, name, suffix=".sav", **write_kw):
    """Write with ambers, read back with pyreadstat."""
    path = str(WRITER_ISSUES_DIR / f"{name}{suffix}")
    am.write_sav(df, path, meta=meta, **write_kw)
    df_pyr, meta_pyr = pyreadstat.read_sav(path)
    return df_pyr, meta_pyr


# ===================================================================
# Issue #267: String formats > 255 bytes
# ===================================================================


class TestIssue267FormatWidth:
    """Format width preservation — pyreadstat caps at A255, ambers should not."""

    def test_format_a256_min_vls(self):
        """A256 is the minimum VLS format. Should trigger VLS and roundtrip."""
        df = pl.DataFrame({"text": ["A" * 200, "B" * 100]})
        meta = am.SpssMetadata(variable_formats={"text": "A256"})
        df2, meta2 = write_read_ambers(df, meta, "issue267_a256_min_vls")
        assert meta2.format("text") == "A256"
        assert meta2.variable_storage_widths["text"] == 256
        assert df2["text"][0] == "A" * 200
        assert df2["text"][1] == "B" * 100

    def test_format_a500_roundtrip(self):
        """Common VLS case — A500 format and storage_width preserved."""
        df = pl.DataFrame({"text": ["X" * 400, "Y" * 200]})
        meta = am.SpssMetadata(variable_formats={"text": "A500"})
        df2, meta2 = write_read_ambers(df, meta, "issue267_a500_roundtrip")
        assert meta2.format("text") == "A500"
        assert meta2.variable_storage_widths["text"] == 500
        assert df2["text"][0] == "X" * 400
        assert df2["text"][1] == "Y" * 200

    def test_format_a1000_roundtrip(self):
        """A1000 — 4 segments (ceil(1000/252) = 4)."""
        df = pl.DataFrame({"text": ["M" * 900, "N" * 500]})
        meta = am.SpssMetadata(variable_formats={"text": "A1000"})
        df2, meta2 = write_read_ambers(df, meta, "issue267_a1000_roundtrip")
        assert meta2.format("text") == "A1000"
        assert meta2.variable_storage_widths["text"] == 1000
        assert df2["text"][0] == "M" * 900
        assert df2["text"][1] == "N" * 500
        assert df2.width == 1

    def test_format_a2000_roundtrip(self):
        """A2000 — 8 segments (ceil(2000/252) = 8)."""
        df = pl.DataFrame({"text": ["P" * 1900, "Q" * 1000]})
        meta = am.SpssMetadata(variable_formats={"text": "A2000"})
        df2, meta2 = write_read_ambers(df, meta, "issue267_a2000_roundtrip")
        assert meta2.format("text") == "A2000"
        assert meta2.variable_storage_widths["text"] == 2000
        assert df2["text"][0] == "P" * 1900
        assert df2["text"][1] == "Q" * 1000
        assert df2.width == 1

    def test_format_width_exceeds_data(self):
        """THE #267 scenario: format A500 but data is only 10 chars.

        pyreadstat would cap this at A255. ambers should preserve A500.
        """
        df = pl.DataFrame({"text": ["short", "tiny", "x"]})
        meta = am.SpssMetadata(variable_formats={"text": "A500"})
        df2, meta2 = write_read_ambers(df, meta, "issue267_a500_short_data")
        assert meta2.format("text") == "A500"
        assert meta2.variable_storage_widths["text"] == 500
        # Data should be preserved (reader trims trailing spaces)
        assert df2["text"][0] == "short"
        assert df2["text"][1] == "tiny"
        assert df2["text"][2] == "x"

    def test_format_width_less_than_data(self):
        """Format A50 but data is 100 chars — data should be truncated."""
        df = pl.DataFrame({"text": ["X" * 100]})
        meta = am.SpssMetadata(variable_formats={"text": "A50"})
        df2, meta2 = write_read_ambers(df, meta, "issue267_a50_truncated")
        assert meta2.format("text") == "A50"
        # Data truncated to format width
        assert len(df2["text"][0]) <= 50

    def test_format_a255_non_vls_boundary(self):
        """A255 is the max non-VLS format — should NOT trigger VLS."""
        df = pl.DataFrame({"text": ["A" * 255]})
        meta = am.SpssMetadata(variable_formats={"text": "A255"})
        df2, meta2 = write_read_ambers(df, meta, "issue267_a255_boundary")
        assert meta2.format("text") == "A255"
        # Storage width rounds up to 8-byte boundary: 255 → 256
        assert meta2.variable_storage_widths["text"] == 256
        assert df2.width == 1
        assert df2["text"][0] == "A" * 255

    def test_format_a256_vls_boundary(self):
        """A256 is the min VLS format — should trigger VLS with 2 segments."""
        df = pl.DataFrame({"text": ["B" * 256]})
        meta = am.SpssMetadata(variable_formats={"text": "A256"})
        df2, meta2 = write_read_ambers(df, meta, "issue267_a256_boundary")
        assert meta2.format("text") == "A256"
        assert meta2.variable_storage_widths["text"] == 256
        assert df2.width == 1
        assert df2["text"][0] == "B" * 256

    def test_multiple_vls_different_widths(self):
        """Multiple VLS columns with different format widths — no ghost columns."""
        df = pl.DataFrame(
            {
                "s300": ["A" * 250],
                "s600": ["B" * 550],
                "s1500": ["C" * 1400],
            }
        )
        meta = am.SpssMetadata(
            variable_formats={"s300": "A300", "s600": "A600", "s1500": "A1500"}
        )
        df2, meta2 = write_read_ambers(df, meta, "issue267_multiple_vls")
        assert df2.width == 3
        assert meta2.format("s300") == "A300"
        assert meta2.format("s600") == "A600"
        assert meta2.format("s1500") == "A1500"
        assert df2["s300"][0] == "A" * 250
        assert df2["s600"][0] == "B" * 550
        assert df2["s1500"][0] == "C" * 1400

    def test_numeric_formats_roundtrip(self):
        """Various numeric formats roundtrip correctly."""
        df = pl.DataFrame({"x": [3.14], "y": [1000.0], "z": [42.0]})
        meta = am.SpssMetadata(
            variable_formats={"x": "F8.2", "y": "COMMA12.2", "z": "DOLLAR10.2"}
        )
        df2, meta2 = write_read_ambers(df, meta, "issue267_numeric_formats")
        assert meta2.format("x") == "F8.2"
        assert meta2.format("y") == "COMMA12.2"
        assert meta2.format("z") == "DOLLAR10.2"
        assert df2["x"][0] == pytest.approx(3.14)
        assert df2["y"][0] == pytest.approx(1000.0)
        assert df2["z"][0] == pytest.approx(42.0)

    @requires_pyreadstat
    def test_format_a500_cross_library(self):
        """pyreadstat reads ambers-written A500 VLS with correct format."""
        df = pl.DataFrame({"text": ["X" * 400, "Y" * 200]})
        meta = am.SpssMetadata(variable_formats={"text": "A500"})
        df_pyr, meta_pyr = write_read_pyreadstat(
            df, meta, "issue267_a500_cross_library"
        )
        # pyreadstat should see 1 column, not split
        assert df_pyr.shape[1] == 1
        # Check format is A500 not A255
        assert meta_pyr.original_variable_types.get("text") == "A500"
        # Data intact
        assert df_pyr["text"].iloc[0].rstrip() == "X" * 400

    def test_format_a32767_extreme(self):
        """Extreme VLS width stress test."""
        df = pl.DataFrame({"text": ["Z" * 10000]})
        meta = am.SpssMetadata(variable_formats={"text": "A32767"})
        df2, meta2 = write_read_ambers(df, meta, "issue267_a32767_extreme")
        assert meta2.format("text") == "A32767"
        assert meta2.variable_storage_widths["text"] == 32767
        assert df2["text"][0] == "Z" * 10000
        assert df2.width == 1

    def test_format_a500_all_compressions(self):
        """VLS format preserved across all 3 compression modes."""
        df = pl.DataFrame({"text": ["X" * 400 + str(i) for i in range(5)]})
        meta = am.SpssMetadata(variable_formats={"text": "A500"})

        # Uncompressed
        df_u, meta_u = write_read_ambers(
            df, meta, "issue267_a500_uncompressed", compression="uncompressed"
        )
        # Bytecode (default for .sav)
        df_b, meta_b = write_read_ambers(df, meta, "issue267_a500_bytecode")
        # Zlib (.zsav)
        df_z, meta_z = write_read_ambers(
            df, meta, "issue267_a500_zlib", suffix=".zsav"
        )

        for label, df_r, meta_r in [
            ("uncompressed", df_u, meta_u),
            ("bytecode", df_b, meta_b),
            ("zlib", df_z, meta_z),
        ]:
            assert meta_r.format("text") == "A500", f"{label}: format wrong"
            assert (
                meta_r.variable_storage_widths["text"] == 500
            ), f"{label}: storage_width wrong"
            assert df_r["text"][0] == "X" * 400 + "0", f"{label}: data wrong"
            assert df_r.width == 1, f"{label}: column count wrong"


# ===================================================================
# Issue #267: Format/type mismatch validation
# ===================================================================


class TestIssue267FormatTypeMismatch:
    """Format-type vs Arrow-type mismatch should raise errors, not silently corrupt."""

    def test_numeric_format_on_string_col(self):
        """F8.2 on a string column should raise, not silently write SYSMIS."""
        df = pl.DataFrame({"name": ["Alice", "Bob", "Carol"]})
        meta = am.SpssMetadata(variable_formats={"name": "F8.2"})
        with pytest.raises(Exception, match="cannot be applied to a string column"):
            am.write_sav(
                df,
                str(WRITER_ISSUES_DIR / "should_not_exist_1.sav"),
                meta=meta,
            )

    def test_string_format_on_numeric_col(self):
        """A50 on a float column should raise, not silently write empty strings."""
        df = pl.DataFrame({"score": [1.0, 2.0, 3.0]})
        meta = am.SpssMetadata(variable_formats={"score": "A50"})
        with pytest.raises(Exception, match="cannot be applied to a non-string"):
            am.write_sav(
                df,
                str(WRITER_ISSUES_DIR / "should_not_exist_2.sav"),
                meta=meta,
            )

    def test_date_format_on_string_col(self):
        """DATE11 on a string column should raise."""
        df = pl.DataFrame({"d": ["2024-01-01", "2024-06-15"]})
        meta = am.SpssMetadata(variable_formats={"d": "DATE11"})
        with pytest.raises(Exception, match="cannot be applied to a string column"):
            am.write_sav(
                df,
                str(WRITER_ISSUES_DIR / "should_not_exist_3.sav"),
                meta=meta,
            )

    def test_string_format_on_date_col(self):
        """A50 on a Date32 column should raise."""
        df = pl.DataFrame({"d": ["2024-01-01", "2024-06-15"]}).cast({"d": pl.Date})
        meta = am.SpssMetadata(variable_formats={"d": "A50"})
        with pytest.raises(Exception, match="cannot be applied to a non-string"):
            am.write_sav(
                df,
                str(WRITER_ISSUES_DIR / "should_not_exist_4.sav"),
                meta=meta,
            )

    def test_invalid_format_string(self):
        """Completely invalid format string should raise."""
        df = pl.DataFrame({"x": [1.0]})
        meta = am.SpssMetadata(variable_formats={"x": "INVALID99"})
        with pytest.raises(Exception, match="invalid format string"):
            am.write_sav(
                df,
                str(WRITER_ISSUES_DIR / "should_not_exist_5.sav"),
                meta=meta,
            )


# ===================================================================
# Issue #119: VLS segments — no ghost columns
# ===================================================================


class TestIssue119VlsSegments:
    """VLS variables should not create visible ghost/phantom columns."""

    def test_vls_no_ghosts_a500(self):
        """A500 VLS: exactly 2 visible columns (id + text), no ghosts."""
        df = pl.DataFrame({"id": [1.0, 2.0], "text": ["A" * 400, "B" * 300]})
        meta = am.SpssMetadata(
            variable_formats={"id": "F8.0", "text": "A500"},
        )
        df2, meta2 = write_read_ambers(df, meta, "issue119_vls_a500")
        assert df2.width == 2
        assert df2.columns == ["id", "text"]
        assert df2["text"][0] == "A" * 400
        assert df2["text"][1] == "B" * 300

    def test_vls_no_ghosts_a1000(self):
        """A1000 VLS: 4 segments, still only 2 visible columns."""
        df = pl.DataFrame({"id": [1.0], "text": ["C" * 900]})
        meta = am.SpssMetadata(
            variable_formats={"id": "F8.0", "text": "A1000"},
        )
        df2, meta2 = write_read_ambers(df, meta, "issue119_vls_a1000")
        assert df2.width == 2
        assert df2.columns == ["id", "text"]
        assert df2["text"][0] == "C" * 900

    def test_vls_mixed_short_and_long(self):
        """Mix of VLS and non-VLS string columns — no ghost leakage."""
        df = pl.DataFrame(
            {
                "id": [1.0, 2.0],
                "short_str": ["hello", "world"],
                "medium_str": ["M" * 180, "N" * 150],
                "long_str": ["L" * 450, "L" * 350],
                "score": [99.5, 88.3],
            }
        )
        meta = am.SpssMetadata(
            variable_formats={
                "id": "F8.0",
                "short_str": "A20",
                "medium_str": "A200",
                "long_str": "A500",
                "score": "F8.2",
            },
        )
        df2, meta2 = write_read_ambers(df, meta, "issue119_vls_mixed")
        assert df2.width == 5
        assert set(df2.columns) == {"id", "short_str", "medium_str", "long_str", "score"}
        assert df2["short_str"][0] == "hello"
        assert df2["medium_str"][0] == "M" * 180
        assert df2["long_str"][0] == "L" * 450
        assert df2["score"][0] == pytest.approx(99.5)

    def test_vls_data_boundary_252(self):
        """Data at exact 252-byte segment boundaries."""
        df = pl.DataFrame(
            {"text": ["A" * 252, "B" * 504, "C" * 756]}
        )
        meta = am.SpssMetadata(variable_formats={"text": "A1000"})
        df2, meta2 = write_read_ambers(df, meta, "issue119_vls_boundary_252")
        assert df2["text"][0] == "A" * 252
        assert df2["text"][1] == "B" * 504
        assert df2["text"][2] == "C" * 756

    def test_vls_data_boundary_253(self):
        """Data at 253 bytes — crosses into segment 2 by 1 byte."""
        df = pl.DataFrame({"text": ["D" * 253]})
        meta = am.SpssMetadata(variable_formats={"text": "A500"})
        df2, meta2 = write_read_ambers(df, meta, "issue119_vls_boundary_253")
        assert df2["text"][0] == "D" * 253

    def test_vls_nulls_and_empties(self):
        """VLS column with null and empty string values."""
        df = pl.DataFrame({"text": ["A" * 400, None, "", "B" * 300, None]})
        meta = am.SpssMetadata(variable_formats={"text": "A500"})
        df2, meta2 = write_read_ambers(df, meta, "issue119_vls_nulls")
        assert df2["text"][0] == "A" * 400
        # SPSS has no string null — null strings become all-spaces, trimmed to ""
        assert df2["text"][1] == "" or df2["text"][1] is None
        assert df2["text"][2] == "" or df2["text"][2] is None
        assert df2["text"][3] == "B" * 300
        assert df2["text"][4] == "" or df2["text"][4] is None

    @requires_pyreadstat
    def test_vls_pyreadstat_reads_correctly(self):
        """ambers-written VLS reads correctly in pyreadstat."""
        df = pl.DataFrame(
            {"id": [1.0, 2.0, 3.0], "vls": ["A" * 400, "B" * 300, "C" * 200]}
        )
        meta = am.SpssMetadata(
            variable_formats={"id": "F8.0", "vls": "A500"},
        )
        df_pyr, meta_pyr = write_read_pyreadstat(
            df, meta, "issue119_vls_pyreadstat"
        )
        # pyreadstat should see 2 columns (not ghost splits)
        assert df_pyr.shape[1] == 2
        assert "vls" in df_pyr.columns
        assert df_pyr["vls"].iloc[0].rstrip() == "A" * 400
        assert df_pyr["vls"].iloc[1].rstrip() == "B" * 300


# ===================================================================
# Issue #264: Long string value labels
# ===================================================================


class TestIssue264LongStringValueLabels:
    """Value labels on string variables with values > 8 chars (subtype 21)."""

    def test_labels_string_8_chars(self):
        """Value labels with exactly 8-char keys — should use Type 3+4."""
        df = pl.DataFrame({"code": ["AAAAAAAA", "BBBBBBBB"]})
        meta = am.SpssMetadata(
            variable_formats={"code": "A8"},
            variable_value_labels={
                "code": {"AAAAAAAA": "Code A", "BBBBBBBB": "Code B"}
            },
        )
        df2, meta2 = write_read_ambers(df, meta, "issue264_labels_8char")
        labels = meta2.variable_value_labels.get("code", {})
        assert len(labels) == 2
        assert labels.get("AAAAAAAA") == "Code A"
        assert labels.get("BBBBBBBB") == "Code B"

    def test_labels_string_9_chars(self):
        """Value labels with 9-char keys — must use subtype 21."""
        df = pl.DataFrame({"region": ["Northeast", "Southwest", "Northwest"]})
        meta = am.SpssMetadata(
            variable_formats={"region": "A20"},
            variable_value_labels={
                "region": {
                    "Northeast": "NE Region",
                    "Southwest": "SW Region",
                    "Northwest": "NW Region",
                }
            },
        )
        df2, meta2 = write_read_ambers(df, meta, "issue264_labels_9char")
        labels = meta2.variable_value_labels.get("region", {})
        assert len(labels) == 3
        assert labels["Northeast"] == "NE Region"
        assert labels["Southwest"] == "SW Region"
        assert labels["Northwest"] == "NW Region"

    def test_labels_mixed_key_lengths(self):
        """Value labels with keys ranging from 2 to 30 chars on A50 variable."""
        df = pl.DataFrame(
            {
                "status": [
                    "OK",
                    "Error: timeout exceeded",
                    "Warning: low memory alert",
                ]
            }
        )
        meta = am.SpssMetadata(
            variable_formats={"status": "A50"},
            variable_value_labels={
                "status": {
                    "OK": "Success",
                    "Error: timeout exceeded": "Timeout Error",
                    "Warning: low memory alert": "Low Memory",
                }
            },
        )
        df2, meta2 = write_read_ambers(df, meta, "issue264_labels_mixed_lengths")
        labels = meta2.variable_value_labels.get("status", {})
        assert len(labels) == 3
        assert labels["OK"] == "Success"
        assert labels["Error: timeout exceeded"] == "Timeout Error"
        assert labels["Warning: low memory alert"] == "Low Memory"

    def test_labels_vls_variable(self):
        """Value labels on a VLS variable (A500)."""
        val_a = "A" * 50
        val_b = "B" * 50
        df = pl.DataFrame({"response": [val_a, val_b, "short"]})
        meta = am.SpssMetadata(
            variable_formats={"response": "A500"},
            variable_value_labels={
                "response": {
                    val_a: "Long response A",
                    val_b: "Long response B",
                    "short": "Short response",
                }
            },
        )
        df2, meta2 = write_read_ambers(df, meta, "issue264_labels_vls")
        labels = meta2.variable_value_labels.get("response", {})
        assert len(labels) == 3
        assert labels[val_a] == "Long response A"
        assert labels[val_b] == "Long response B"
        assert labels["short"] == "Short response"

    def test_labels_mixed_numeric_and_long_string(self):
        """Both numeric (Type 3+4) and long string (subtype 21) value labels."""
        df = pl.DataFrame(
            {"gender": [1.0, 2.0], "city": ["New York City", "San Francisco"]}
        )
        meta = am.SpssMetadata(
            variable_formats={"gender": "F1.0", "city": "A50"},
            variable_value_labels={
                "gender": {1.0: "Male", 2.0: "Female"},
                "city": {
                    "New York City": "NYC",
                    "San Francisco": "SF",
                },
            },
        )
        df2, meta2 = write_read_ambers(df, meta, "issue264_labels_mixed")
        # Numeric labels
        gender_labels = meta2.variable_value_labels.get("gender", {})
        assert len(gender_labels) == 2
        assert gender_labels.get(1.0) == "Male"
        assert gender_labels.get(2.0) == "Female"
        # Long string labels
        city_labels = meta2.variable_value_labels.get("city", {})
        assert len(city_labels) == 2
        assert city_labels["New York City"] == "NYC"
        assert city_labels["San Francisco"] == "SF"

    def test_labels_many_entries(self):
        """50 value labels on a long string variable — stress test."""
        keys = [f"category_{i:03d}" for i in range(50)]
        labels_dict = {k: f"Label for {k}" for k in keys}
        df = pl.DataFrame({"cat": keys[:3]})  # Only need a few rows
        meta = am.SpssMetadata(
            variable_formats={"cat": "A50"},
            variable_value_labels={"cat": labels_dict},
        )
        df2, meta2 = write_read_ambers(df, meta, "issue264_labels_many")
        labels = meta2.variable_value_labels.get("cat", {})
        assert len(labels) == 50
        for k in keys:
            assert labels[k] == f"Label for {k}", f"Missing label for {k}"

    def test_labels_unicode_keys(self):
        """Unicode value label keys > 8 bytes."""
        df = pl.DataFrame({"mood": ["very happy", "quite sad"]})
        meta = am.SpssMetadata(
            variable_formats={"mood": "A50"},
            variable_value_labels={
                "mood": {
                    "very happy": "Positive",
                    "quite sad": "Negative",
                }
            },
        )
        df2, meta2 = write_read_ambers(df, meta, "issue264_labels_unicode")
        labels = meta2.variable_value_labels.get("mood", {})
        assert len(labels) == 2
        assert labels["very happy"] == "Positive"
        assert labels["quite sad"] == "Negative"

    @requires_pyreadstat
    def test_labels_vls_cross_library(self):
        """ambers-written long string value labels readable by pyreadstat."""
        val_a = "Northeast"
        val_b = "Southwest"
        df = pl.DataFrame({"region": [val_a, val_b]})
        meta = am.SpssMetadata(
            variable_formats={"region": "A20"},
            variable_value_labels={
                "region": {val_a: "NE", val_b: "SW"}
            },
        )
        df_pyr, meta_pyr = write_read_pyreadstat(
            df, meta, "issue264_labels_cross_library"
        )
        # pyreadstat should read the long string value labels
        pyr_labels = meta_pyr.variable_value_labels.get("region", {})
        assert len(pyr_labels) >= 2, (
            f"pyreadstat dropped long string value labels: got {pyr_labels}"
        )


# ===================================================================
# Combined tests — cross-issue integration
# ===================================================================


class TestCombinedIssues:
    """Integration tests combining multiple issues."""

    def test_vls_format_and_labels(self):
        """A500 + value labels + format verification (#267 + #264 + #119)."""
        val_a = "A" * 50
        val_b = "B" * 50
        df = pl.DataFrame({"response": [val_a, val_b]})
        meta = am.SpssMetadata(
            variable_formats={"response": "A500"},
            variable_labels={"response": "Open-ended response"},
            variable_value_labels={
                "response": {val_a: "Response A", val_b: "Response B"}
            },
        )
        df2, meta2 = write_read_ambers(df, meta, "combined_vls_format_labels")
        # #267: format preserved
        assert meta2.format("response") == "A500"
        assert meta2.variable_storage_widths["response"] == 500
        # #119: no ghost columns
        assert df2.width == 1
        # #264: value labels preserved
        labels = meta2.variable_value_labels.get("response", {})
        assert len(labels) == 2
        assert labels[val_a] == "Response A"
        # Variable label preserved
        assert meta2.label("response") == "Open-ended response"

    def test_full_metadata_roundtrip_vls(self):
        """VLS variable with ALL metadata fields populated."""
        df = pl.DataFrame({"text": ["Hello world" * 30]})
        meta = am.SpssMetadata(
            variable_formats={"text": "A500"},
            variable_labels={"text": "A very long text field"},
            variable_measures={"text": "nominal"},
            variable_alignments={"text": "left"},
            variable_display_widths={"text": 100},
            variable_roles={"text": "input"},
            variable_value_labels={
                "text": {"Hello world" * 30: "Repeated greeting"}
            },
        )
        df2, meta2 = write_read_ambers(df, meta, "combined_full_metadata_vls")
        assert meta2.format("text") == "A500"
        assert meta2.label("text") == "A very long text field"
        assert meta2.measure("text") == "nominal"
        assert meta2.variable_alignments["text"] == "left"
        assert meta2.variable_display_widths["text"] == 100
        assert meta2.variable_roles["text"] == "input"
        labels = meta2.variable_value_labels.get("text", {})
        assert len(labels) == 1

    def test_real_world_survey_pattern(self):
        """Realistic survey dataset with mixed column types and metadata."""
        df = pl.DataFrame(
            {
                "respondent_id": [1001.0, 1002.0, 1003.0],
                "satisfaction": [5.0, 3.0, 4.0],
                "brand_recall": ["Coca-Cola", "Pepsi", "Dr Pepper"],
                "open_end_comment": [
                    "I really enjoyed the product because it was refreshing and "
                    "had a great taste that reminded me of summer days by the pool "
                    "with my family and friends. Would definitely recommend to "
                    "anyone looking for a quality beverage option." * 2,
                    "It was okay, nothing special.",
                    "The packaging could be improved significantly. The current "
                    "design is hard to open and the label peels off easily. " * 3,
                ],
                "nps_score": [9.0, 6.0, 8.0],
            }
        )
        meta = am.SpssMetadata(
            file_label="Consumer Survey Q4 2025",
            variable_formats={
                "respondent_id": "F8.0",
                "satisfaction": "F1.0",
                "brand_recall": "A50",
                "open_end_comment": "A2000",
                "nps_score": "F2.0",
            },
            variable_labels={
                "respondent_id": "Respondent ID",
                "satisfaction": "Overall satisfaction (1-5)",
                "brand_recall": "First brand mentioned",
                "open_end_comment": "Open-ended feedback",
                "nps_score": "Net Promoter Score (0-10)",
            },
            variable_value_labels={
                "satisfaction": {
                    1.0: "Very dissatisfied",
                    2.0: "Dissatisfied",
                    3.0: "Neutral",
                    4.0: "Satisfied",
                    5.0: "Very satisfied",
                },
                "brand_recall": {
                    "Coca-Cola": "Coca-Cola Company",
                    "Pepsi": "PepsiCo",
                    "Dr Pepper": "Keurig Dr Pepper",
                },
            },
            variable_measures={
                "respondent_id": "nominal",
                "satisfaction": "ordinal",
                "brand_recall": "nominal",
                "open_end_comment": "nominal",
                "nps_score": "scale",
            },
        )
        df2, meta2 = write_read_ambers(df, meta, "combined_survey_pattern")

        # Structure
        assert df2.width == 5
        assert df2.height == 3

        # Formats preserved
        assert meta2.format("respondent_id") == "F8.0"
        assert meta2.format("satisfaction") == "F1.0"
        assert meta2.format("brand_recall") == "A50"
        assert meta2.format("open_end_comment") == "A2000"
        assert meta2.format("nps_score") == "F2.0"

        # Labels
        assert meta2.label("satisfaction") == "Overall satisfaction (1-5)"
        assert meta2.label("open_end_comment") == "Open-ended feedback"

        # Value labels (numeric)
        sat_labels = meta2.variable_value_labels.get("satisfaction", {})
        assert len(sat_labels) == 5
        assert sat_labels[5.0] == "Very satisfied"

        # Value labels (long string — #264)
        brand_labels = meta2.variable_value_labels.get("brand_recall", {})
        assert len(brand_labels) == 3
        assert brand_labels["Coca-Cola"] == "Coca-Cola Company"

        # File label
        assert meta2.file_label == "Consumer Survey Q4 2025"

        # VLS data integrity (#119)
        assert len(df2["open_end_comment"][0]) > 200

        # Measures
        assert meta2.measure("satisfaction") == "ordinal"
        assert meta2.measure("nps_score") == "scale"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
