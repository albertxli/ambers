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


def _layout(data: bytes):
    """Walk the dictionary: returns (type2 records as [(offset, short_name)], data_start)."""
    i32 = lambda o: struct.unpack_from("<i", data, o)[0]
    pos, type2 = 176, []
    while True:
        rt = i32(pos)
        if rt == 2:
            has_label, nmiss = i32(pos + 8), i32(pos + 12)
            type2.append((pos, data[pos + 24:pos + 32].decode("latin1").rstrip()))
            pos += 32
            if has_label == 1:
                ll = i32(pos); pos += 4 + (ll + 3) // 4 * 4
            pos += 8 * abs(nmiss)
        elif rt == 3:
            n = i32(pos + 4); pos += 8
            for _ in range(n):
                pos += 8; ll = data[pos]; pos += (ll + 1 + 7) // 8 * 8
        elif rt == 4:
            pos += 8 + 4 * i32(pos + 4)
        elif rt == 6:
            pos += 8 + 80 * i32(pos + 4)
        elif rt == 7:
            pos += 16 + i32(pos + 8) * i32(pos + 12)
        elif rt == 999:
            return type2, pos + 8
        else:
            raise AssertionError(f"unexpected record type {rt} at {pos}")
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


class TestImpossibleStructures:
    """Corruptions that cannot be read at all fail fast with a clear OSError."""

    def test_invalid_string_width_fails_fast(self, tmp_path, source_df):
        good = _write(tmp_path, source_df, "good.sav")
        data = bytearray(good.read_bytes())
        type2, _ = _layout(data)
        off = next(o for o, n in type2 if n.upper() == "NAME")
        struct.pack_into("<i", data, off + 4, 905_969_664)
        p = tmp_path / "wide.sav"; p.write_bytes(data)
        import time
        t0 = time.perf_counter()
        with pytest.raises(OSError, match="declares type 905969664"):
            am.read_sav(str(p))
        assert time.perf_counter() - t0 < 1.0

    def test_duplicate_variable_names_fail(self, tmp_path, source_df):
        good = _write(tmp_path, source_df, "good.sav")
        data = bytearray(good.read_bytes())
        type2, _ = _layout(data)
        first = next(o for o, n in type2 if n.upper() == "AGE")
        second = next(o for o, n in type2 if n.upper() == "D")
        data[second + 24:second + 32] = data[first + 24:first + 32]
        p = tmp_path / "dupe.sav"; p.write_bytes(data)
        with pytest.raises(OSError, match="duplicate variable name"):
            am.read_sav(str(p))


class TestTemporalRange:
    def test_out_of_range_dates_become_null_with_finding(self, tmp_path):
        df = pl.DataFrame({"d": [date(2024, 1, 1) for _ in range(5)], "x": [1.0, 2.0, 3.0, 4.0, 5.0]})
        p = tmp_path / "dates.sav"
        am.write_sav(df, str(p), compression="uncompressed")
        data = bytearray(p.read_bytes())
        _, start = _layout(data)
        row_bytes = 16  # two 8-byte slots; "d" is slot 0
        struct.pack_into("<d", data, start + 1 * row_bytes, 1e300)
        struct.pack_into("<d", data, start + 3 * row_bytes, float("nan"))
        bad = tmp_path / "dates_bad.sav"; bad.write_bytes(data)
        with pytest.warns(am.CorruptFileWarning, match="2 date/time values were outside the representable range") as rec:
            sav = am.read_sav(str(bad))
        assert len(rec) == 1
        assert sav.data["d"].null_count() == 2
        assert sav.data["d"].is_null().to_list() == [False, True, False, True, False]
        assert sav.data["x"].to_list() == [1.0, 2.0, 3.0, 4.0, 5.0]
        # Polars can display and summarise the frame (the old saturated values made it panic).
        assert sav.data.describe().height > 0
