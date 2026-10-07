"""Malformed-file handling from Python: a header/dictionary slot-count mismatch
must raise a clean error from read_sav, read_sav_meta and scan_sav (GitHub issue #1),
and a non-positive header slot count must be tolerated. Synthetic files only."""

from __future__ import annotations

import struct
from datetime import date

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import ambers as am

# magic (4) + product name (60) + layout_code (4) -> nominal_case_size i32 (little-endian)
CASE_SIZE_OFFSET = 68
# ... + case size (4) + compression (4) + weight index (4) -> ncases i32
CASE_COUNT_OFFSET = 80


@pytest.fixture
def source_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "age": [20.0, 21.0, None, 23.0, 24.0],
            "d": [date(2024, 1, 1), date(2024, 6, 15), None, date(2020, 2, 29), date(1999, 7, 4)],
            "name": ["Alice", "Bob", "Carol-Louise", "Dan", "Eve"],  # A12 -> 2 slots
        }
    )


@pytest.fixture
def good_path(tmp_path, source_df):
    p = tmp_path / "good.sav"
    # Without an explicit format the writer infers A255 (32 slots); pin A12 so the
    # row is exactly 4 slots: age(1) + d(1) + name(2).
    am.write_sav(source_df, str(p), meta=am.SpssMetadata(variable_formats={"name": "A12"}))
    return p


def _patched(tmp_path, good_path, value: int, name: str, offset: int = CASE_SIZE_OFFSET):
    data = bytearray(good_path.read_bytes())
    declared = struct.unpack_from("<i", data, offset)[0]
    struct.pack_into("<i", data, offset, value)
    p = tmp_path / name
    p.write_bytes(data)
    return str(p), declared


@pytest.mark.filterwarnings("ignore::ambers.CorruptFileWarning")
class TestSlotCountMismatch:
    """The header's row width is unreliable (SPSS ignores it): a mismatch is a finding,
    the variable list is used, and the data reads correctly."""

    @pytest.mark.parametrize("bogus", [7, 3, 1_895_825_415])
    def test_header_mismatch_warns_and_reads(self, tmp_path, good_path, bogus):
        expected = am.read_sav(str(good_path)).data
        path, declared = _patched(tmp_path, good_path, bogus, f"case_size_{bogus}.sav")
        assert declared == 4  # age(1) + d(1) + name A12 (2)
        with pytest.warns(am.CorruptFileWarning, match=f"header declares {bogus} values per row"):
            sav = am.read_sav(path)
        assert_frame_equal(sav.data, expected)
        assert len(sav.warnings) == 1
        with pytest.warns(am.CorruptFileWarning, match="values per row"):
            am.read_sav_meta(path)
        with pytest.warns(am.CorruptFileWarning, match="values per row"):
            am.scan_sav(path)

    @pytest.mark.parametrize("bogus", [0, -1])
    def test_non_positive_header_is_tolerated(self, tmp_path, good_path, bogus):
        expected = am.read_sav(str(good_path)).data
        path, _ = _patched(tmp_path, good_path, bogus, f"bogus_{bogus}.sav")
        sav = am.read_sav(path)
        assert_frame_equal(sav.data, expected)
        assert sav.shape == expected.shape
        lazy = am.scan_sav(path).data.collect()
        assert_frame_equal(lazy, expected)

    def test_good_file_unchanged(self, good_path, source_df):
        df = am.read_sav(str(good_path)).data
        expected = source_df.with_columns(pl.col("name").fill_null(""))
        assert_frame_equal(df, expected)


@pytest.mark.filterwarnings("ignore::ambers.CorruptFileWarning")
class TestZeroCaseCount:
    """GitHub issue #2: a header declaring 0 cases with data behind it used to make the
    reader write past a zero-length buffer and kill the process. The header count is a
    hint; the rows in the file are read, as SPSS does."""

    @pytest.mark.parametrize("bogus", [0, -1])
    def test_sav_reads_all_rows(self, tmp_path, good_path, bogus):
        expected = am.read_sav(str(good_path)).data
        path, declared = _patched(tmp_path, good_path, bogus, f"ncases_{bogus}.sav", CASE_COUNT_OFFSET)
        assert declared == 5
        sav = am.read_sav(path)
        assert sav.shape == (5, 3)
        assert_frame_equal(sav.data, expected)
        assert_frame_equal(am.scan_sav(path).data.collect(), expected)
        # Header value is reported as-is; a negative count reads as None (unknown).
        assert am.read_sav_meta(path).number_rows == (bogus if bogus >= 0 else None)

    def test_zsav_reads_all_rows(self, tmp_path, source_df):
        good = tmp_path / "good.zsav"
        am.write_sav(source_df, str(good), meta=am.SpssMetadata(variable_formats={"name": "A12"}))
        expected = am.read_sav(str(good)).data
        path, declared = _patched(tmp_path, good, 0, "ncases_0.zsav", CASE_COUNT_OFFSET)
        assert declared == 5
        assert_frame_equal(am.read_sav(path).data, expected)
        assert_frame_equal(am.scan_sav(path).data.collect(), expected)

    def test_truly_empty_file(self, tmp_path, source_df):
        path = tmp_path / "empty.sav"
        am.write_sav(source_df.clear(), str(path), meta=am.SpssMetadata(variable_formats={"name": "A12"}))
        sav = am.read_sav(str(path))
        assert sav.shape == (0, 3)
        assert am.scan_sav(str(path)).data.collect().shape == (0, 3)
