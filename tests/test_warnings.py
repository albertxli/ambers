"""File-damage warnings: a .sav whose header disagrees with its contents must be read
literally (never silently corrected) and must raise ``ambers.CorruptFileWarning`` with the
findings, also exposed as ``SavFile.warnings`` and ``SpssMetadata.warnings``.
Synthetic files only; the reporter's fuzzer file is used when present."""

from __future__ import annotations

import os
import struct
import warnings
from datetime import date

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import ambers as am

CASE_COUNT_OFFSET = 80  # header ncases (i32 LE)
BIAS_OFFSET = 84        # header compression bias (f64 LE)
GARBAGE_BIAS = 9.597803938502254e-308  # value found in the fuzzer files
META = am.SpssMetadata(variable_formats={"name": "A12"})


@pytest.fixture
def source_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "age": [20.0, 21.0, None, 23.0, 24.0],  # small integers -> bytecode-compressed
            "d": [date(2024, 1, 1), date(2024, 6, 15), None, date(2020, 2, 29), date(1999, 7, 4)],
            "name": ["Alice", "Bob", "Carol-Louise", "Dan", "Eve"],
        }
    )


def _write(tmp_path, df, name):
    p = tmp_path / name
    am.write_sav(df, str(p), meta=META)
    return p


def _patch(path, offset, fmt, value, out):
    data = bytearray(path.read_bytes())
    struct.pack_into(fmt, data, offset, value)
    out.write_bytes(data)
    return str(out)


class TestHealthyFile:
    @pytest.mark.parametrize("ext", ["sav", "zsav"])
    def test_no_warning_and_empty_lists(self, tmp_path, source_df, ext):
        p = str(_write(tmp_path, source_df, f"good.{ext}"))
        with warnings.catch_warnings():
            warnings.simplefilter("error")  # any warning fails the test
            sav = am.read_sav(p)
            lazy = am.scan_sav(p)
            meta = am.read_sav_meta(p)
        assert sav.warnings == [] and lazy.warnings == [] and meta.warnings == []
        assert sav.meta.warnings == [] and meta.schema["warnings"] == []
        assert "Warnings" not in repr(sav)


class TestRowCountMismatch:
    def test_read_sav_warns_and_reads_all_rows(self, tmp_path, source_df):
        good = _write(tmp_path, source_df, "good.sav")
        expected = am.read_sav(str(good)).data
        p = _patch(good, CASE_COUNT_OFFSET, "<i", 0, tmp_path / "ncases0.sav")
        with pytest.warns(am.CorruptFileWarning, match="declares 0 rows but 5 rows were read") as rec:
            sav = am.read_sav(p)
        assert len(rec) == 1, "exactly one warning per read"
        assert "ncases0.sav appears damaged or corrupted" in str(rec[0].message)
        assert "double-check the source" in str(rec[0].message)
        assert sav.shape == (5, 3)
        assert_frame_equal(sav.data, expected)
        assert sav.warnings == sav.meta.warnings and len(sav.warnings) == 1
        assert "1 finding - file may be damaged, see .warnings" in repr(sav)

    def test_metadata_only_and_lazy_do_not_warn_for_row_count(self, tmp_path, source_df):
        good = _write(tmp_path, source_df, "good.sav")
        p = _patch(good, CASE_COUNT_OFFSET, "<i", 0, tmp_path / "ncases0.sav")
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            meta = am.read_sav_meta(p)          # data not read: nothing to compare
            lazy = am.scan_sav(p)               # header-level checks only
            assert lazy.data.collect().height == 5
        assert meta.warnings == [] and lazy.warnings == []

    def test_n_rows_limit_suppresses_row_count_warning(self, tmp_path, source_df):
        good = _write(tmp_path, source_df, "good.sav")
        p = _patch(good, CASE_COUNT_OFFSET, "<i", 0, tmp_path / "ncases0.sav")
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            sav = am.read_sav(p, n_rows=2)
        assert sav.shape == (2, 3) and sav.warnings == []


class TestBias:
    @pytest.mark.parametrize("ext", ["sav", "zsav"])
    def test_non_standard_bias_warns_everywhere_and_reads_literally(self, tmp_path, source_df, ext):
        good = _write(tmp_path, source_df, f"good.{ext}")
        p = _patch(good, BIAS_OFFSET, "<d", GARBAGE_BIAS, tmp_path / f"bias.{ext}")
        with pytest.warns(am.CorruptFileWarning, match="bias") as rec:
            sav = am.read_sav(p)
        assert len(rec) == 1
        assert "assuming 100" in str(rec[0].message)
        with pytest.warns(am.CorruptFileWarning, match="bias"):
            lazy = am.scan_sav(p)
        with pytest.warns(am.CorruptFileWarning, match="bias"):
            meta = am.read_sav_meta(p)
        assert len(sav.warnings) == len(lazy.warnings) == len(meta.warnings) == 1
        assert meta.schema["warnings"] == meta.warnings
        # Literal read: code = value + 100 was stored; decoding with bias ~0 yields value + 100.
        assert sav.data["age"].to_list() == [120.0, 121.0, None, 123.0, 124.0]
        assert_frame_equal(lazy.data.collect(), sav.data)

    def test_bias_is_inert_for_uncompressed_files(self, tmp_path, source_df):
        good = tmp_path / "good_raw.sav"
        am.write_sav(source_df, str(good), meta=META, compression="uncompressed")
        p = _patch(good, BIAS_OFFSET, "<d", GARBAGE_BIAS, tmp_path / "bias_raw.sav")
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            sav = am.read_sav(p)
        assert sav.warnings == []
        assert sav.data["age"].to_list() == [20.0, 21.0, None, 23.0, 24.0]

    def test_filter_silences(self, tmp_path, source_df):
        good = _write(tmp_path, source_df, "good.sav")
        p = _patch(good, BIAS_OFFSET, "<d", GARBAGE_BIAS, tmp_path / "bias.sav")
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            warnings.filterwarnings("ignore", category=am.CorruptFileWarning)
            sav = am.read_sav(p)
        assert len(sav.warnings) == 1  # still recorded, just not emitted


class TestReporterFile:
    PATH = "test_data/github_issues/issue2_oob-write.sav"

    @pytest.mark.skipif(not os.path.exists(PATH), reason="reporter-supplied fuzzer file not present")
    def test_issue2_file_reports_two_findings(self):
        with pytest.warns(am.CorruptFileWarning) as rec:
            sav = am.read_sav(self.PATH)
        assert len(rec) == 1
        msg = str(rec[0].message)
        assert "(1)" in msg and "(2)" in msg
        assert sav.shape == (5, 7)
        assert sorted(w.split(" ")[0] for w in sav.warnings) == ["compression", "header"]
        # Read literally, as Q does; SPSS would show 1/2 because it assumes bias 100.
        assert sav.data["mylabl"].to_list() == [101.0, 102.0, 101.0, 102.0, 101.0]
