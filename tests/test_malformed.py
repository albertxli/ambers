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


def _patched(tmp_path, good_path, value: int, name: str):
    data = bytearray(good_path.read_bytes())
    declared = struct.unpack_from("<i", data, CASE_SIZE_OFFSET)[0]
    struct.pack_into("<i", data, CASE_SIZE_OFFSET, value)
    p = tmp_path / name
    p.write_bytes(data)
    return str(p), declared


class TestSlotCountMismatch:
    def test_header_larger_than_dictionary_raises(self, tmp_path, good_path):
        path, declared = _patched(tmp_path, good_path, 7, "larger.sav")
        assert declared == 4  # age(1) + d(1) + name A12 (2)
        with pytest.raises(OSError, match="7 slots per case but the dictionary defines 4"):
            am.read_sav(path)
        with pytest.raises(OSError, match="slots per case"):
            am.read_sav_meta(path)
        with pytest.raises(OSError, match="slots per case"):
            am.scan_sav(path)

    def test_header_smaller_than_dictionary_raises(self, tmp_path, good_path):
        # GitHub issue #1 shape: columns beyond the header row width.
        path, _ = _patched(tmp_path, good_path, 3, "smaller.sav")
        with pytest.raises(OSError, match="3 slots per case but the dictionary defines 4"):
            am.read_sav(path)

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
