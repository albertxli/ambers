"""The committed synthetic fixtures in tests/data/ (see make_fixtures.py).

Unlike test_data/, these are always present, so nothing here skips.
"""

from pathlib import Path

import polars as pl
import pytest

import ambers as am

DATA = Path(__file__).resolve().parent.parent.parent / "tests" / "data"


@pytest.mark.parametrize(
    ("name", "compression"),
    [
        ("survey.sav", "bytecode"),
        ("survey_uncompressed.sav", "uncompressed"),
        ("survey.zsav", "zlib"),
    ],
)
def test_survey_reads_in_every_compression(name, compression):
    sav = am.read_sav(DATA / name)
    df, meta = sav.data, sav.meta
    assert df.shape == (20, 13)
    assert meta.compression == compression
    assert sav.warnings == []


def test_survey_metadata():
    meta = am.read_sav_meta(DATA / "survey.sav")
    assert meta.file_label == "ambers synthetic survey fixture"
    assert meta.weight_variable == "weight"
    assert meta.label("satisfaction") == "Overall satisfaction"
    assert meta.variable_value_labels["satisfaction"][99] == "Refused"
    assert meta.variable_missing_values["income"] == {"type": "range", "low": 999990.0, "high": 999999.0}
    assert meta.mr_sets["brands"]["variables"] == ["brand_a", "brand_b", "brand_c"]


def test_survey_temporal_columns():
    df = am.read_sav(DATA / "survey.sav").data
    assert df.schema["interview_date"] == pl.Date
    assert df.schema["interview_start"] == pl.Datetime("us")
    assert df.schema["duration"] == pl.Duration("us")
    assert df["satisfaction"].null_count() == 2


def test_survey_scan_matches_read():
    lf = am.scan_sav(DATA / "survey.sav").data
    assert lf.select("id", "city").head(5).collect().shape == (5, 2)
    assert lf.collect().equals(am.read_sav(DATA / "survey.sav").data)


def test_long_strings_keep_their_full_width():
    sav = am.read_sav(DATA / "long_strings.sav")
    df, meta = sav.data, sav.meta
    assert meta.format("comment") == "A1000"
    assert df["comment"].to_list() == ["x" * 600, "Çok güzel — très bien — とても良い", ""]
