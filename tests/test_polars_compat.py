"""Polars compatibility: Arrow stream -> DataFrame conversion must stay warning-free.

Regression tests for the ``pl.from_arrow(<ArrowStreamExportable>)`` FutureWarning
(Polars >= 1.44) and its Polars 2.0 behavior change (returns a Series). ambers must
convert its PyCapsule streams with ``pl.DataFrame(...)`` in both the eager and the
lazy/batch paths. Synthetic data only; safe for CI.
"""

from __future__ import annotations

import warnings
from datetime import date

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import ambers as am
from ambers._ambers import _SavBatchReader


@pytest.fixture
def source_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "num": [1.0, None, 3.0, 4.0, 5.0, 6.0],
            "code": [0.0, 1.0, 1.0, 0.0, None, 1.0],
            "d": [date(2024, 1, 1), None, date(2024, 6, 15), date(2025, 12, 31),
                  date(2020, 2, 29), date(1999, 7, 4)],
            "short": ["a", "bb", None, "dddd", "e", "ff"],
            "long": ["x" * 300, "y" * 260, None, "z" * 500, "", "w" * 256],
        }
    )


@pytest.fixture
def expected_df(source_df) -> pl.DataFrame:
    """What a faithful SPSS roundtrip yields: string nulls become empty strings."""
    return source_df.with_columns(pl.col("short", "long").fill_null(""))


# Strings wider than 255 bytes need an explicit VLS format; the inferred default is A255.
_META = am.SpssMetadata(variable_formats={"long": "A500"})


@pytest.fixture
def sav_path(tmp_path, source_df):
    path = tmp_path / "compat.sav"
    am.write_sav(source_df, str(path), meta=_META)
    return str(path)


@pytest.fixture
def empty_sav_path(tmp_path, source_df):
    path = tmp_path / "empty.sav"
    am.write_sav(source_df.clear(), str(path), meta=_META)
    return str(path)


class _NoFutureWarning:
    """Context manager: any FutureWarning or DeprecationWarning fails the test."""

    def __enter__(self):
        self._cm = warnings.catch_warnings()
        self._cm.__enter__()
        warnings.simplefilter("error", FutureWarning)
        warnings.simplefilter("error", DeprecationWarning)
        return self

    def __exit__(self, *exc):
        return self._cm.__exit__(*exc)


class TestEagerRead:
    def test_returns_dataframe_without_warning(self, sav_path, expected_df):
        with _NoFutureWarning():
            sav = am.read_sav(sav_path)
        assert isinstance(sav.data, pl.DataFrame)
        assert sav.shape == expected_df.shape
        assert_frame_equal(sav.data, expected_df)

    def test_schema(self, sav_path):
        with _NoFutureWarning():
            df = am.read_sav(sav_path).data
        assert df.schema["num"] == pl.Float64
        assert df.schema["d"] == pl.Date
        assert df.schema["short"] == pl.String
        assert df.schema["long"] == pl.String

    def test_single_column(self, sav_path, source_df):
        with _NoFutureWarning():
            df = am.read_sav(sav_path, columns=["d"]).data
        assert isinstance(df, pl.DataFrame)
        assert_frame_equal(df, source_df.select("d"))

    def test_n_rows(self, sav_path, expected_df):
        with _NoFutureWarning():
            df = am.read_sav(sav_path, n_rows=2).data
        assert_frame_equal(df, expected_df.head(2))

    def test_row_index(self, sav_path):
        with _NoFutureWarning():
            df = am.read_sav(sav_path, row_index_name="idx", row_index_offset=10).data
        assert df.columns[0] == "idx"
        assert df["idx"].to_list() == [10, 11, 12, 13, 14, 15]

    def test_zero_rows(self, empty_sav_path, source_df):
        with _NoFutureWarning():
            sav = am.read_sav(empty_sav_path)
        assert isinstance(sav.data, pl.DataFrame)
        assert sav.shape == (0, source_df.width)
        assert sav.data.columns == source_df.columns


class TestLazyScan:
    def test_collect_matches_eager(self, sav_path, expected_df):
        with _NoFutureWarning():
            lf = am.scan_sav(sav_path).data
            assert isinstance(lf, pl.LazyFrame)
            df = lf.collect()
        assert_frame_equal(df, expected_df)

    def test_projection_and_head(self, sav_path, expected_df):
        with _NoFutureWarning():
            df = am.scan_sav(sav_path).data.select(["short", "num"]).head(3).collect()
        assert_frame_equal(df, expected_df.select(["short", "num"]).head(3))

    def test_predicate(self, sav_path, expected_df):
        with _NoFutureWarning():
            df = am.scan_sav(sav_path).data.filter(pl.col("code") == 1.0).collect()
        assert_frame_equal(df, expected_df.filter(pl.col("code") == 1.0))

    def test_zero_rows(self, empty_sav_path, source_df):
        with _NoFutureWarning():
            df = am.scan_sav(empty_sav_path).data.collect()
        assert df.shape == (0, source_df.width)


class TestBatchConversion:
    """Exercises the per-batch conversion used inside scan_sav's IO source."""

    def test_multiple_batches_concat_to_source(self, sav_path, expected_df):
        reader = _SavBatchReader(sav_path, batch_size=2)
        parts = []
        with _NoFutureWarning():
            while (batch := reader.next_batch()) is not None:
                df = pl.DataFrame(batch)
                assert isinstance(df, pl.DataFrame)
                parts.append(df)
        assert len(parts) == 3
        assert all(p.height == 2 for p in parts)
        assert_frame_equal(pl.concat(parts), expected_df)

    def test_batch_with_select_and_limit(self, sav_path, expected_df):
        reader = _SavBatchReader(sav_path, batch_size=4)
        reader.select(["num", "long"])
        reader.limit(5)
        parts = []
        with _NoFutureWarning():
            while (batch := reader.next_batch()) is not None:
                parts.append(pl.DataFrame(batch))
        assert_frame_equal(pl.concat(parts), expected_df.select(["num", "long"]).head(5))
