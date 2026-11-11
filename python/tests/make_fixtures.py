"""Regenerate the small synthetic fixtures in tests/data/.

The files are written with ambers' own writer from made-up survey data, so
they are safe to commit and available in CI. Real files stay in the
gitignored test_data/ directory.

Usage:
    uv run python/tests/make_fixtures.py

The writer stamps the creation time into each file, so regenerated files
differ from the committed ones by those bytes only.
"""

from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl

import ambers as am

OUT = Path(__file__).resolve().parent.parent.parent / "tests" / "data"
N = 20


def survey() -> tuple[pl.DataFrame, am.SpssMetadata]:
    """A 20-row survey touching every metadata feature the reader exposes."""
    idx = list(range(N))
    df = pl.DataFrame(
        {
            "id": [float(i + 1) for i in idx],
            "gender": [float(i % 2 + 1) for i in idx],
            # 99 is a user-missing code; every seventh answer is system-missing.
            "satisfaction": [None if i % 7 == 6 else (99.0 if i % 5 == 4 else float(i % 5 + 1)) for i in idx],
            "age": [float(18 + (i * 3) % 50) for i in idx],
            "income": [999999.0 if i % 9 == 8 else 20000.0 + 1500.5 * i for i in idx],
            "city": [["Lisbon", "Osaka", "Lima", "N/A"][i % 4] for i in idx],
            "brand_a": [float(i % 2) for i in idx],
            "brand_b": [float(i % 3 == 0) for i in idx],
            "brand_c": [float(i % 4 == 0) for i in idx],
            "interview_date": [date(2024, 1, 1) + timedelta(days=i) for i in idx],
            "interview_start": [datetime(2024, 1, 1, 9, 0) + timedelta(minutes=37 * i) for i in idx],
            "duration": [timedelta(seconds=300 + 11 * i) for i in idx],
            "weight": [0.5 + (i % 4) * 0.25 for i in idx],
        }
    )
    yes_no = {0: "No", 1: "Yes"}
    meta = am.SpssMetadata(
        file_label="ambers synthetic survey fixture",
        notes=["Synthetic data written by python/tests/make_fixtures.py."],
        weight_variable="weight",
        variable_labels={
            "id": "Respondent ID",
            "gender": "Gender",
            "satisfaction": "Overall satisfaction",
            "age": "Age in years",
            "income": "Annual income",
            "city": "City of residence",
            "brand_a": "Brands mentioned: A",
            "brand_b": "Brands mentioned: B",
            "brand_c": "Brands mentioned: C",
            "interview_date": "Interview date",
            "interview_start": "Interview start",
            "duration": "Interview length",
            "weight": "Sampling weight",
        },
        variable_value_labels={
            "gender": {1: "Male", 2: "Female"},
            "satisfaction": {
                1: "Very dissatisfied",
                2: "Dissatisfied",
                3: "Neutral",
                4: "Satisfied",
                5: "Very satisfied",
                99: "Refused",
            },
            "city": {"N/A": "Not answered"},
            "brand_a": yes_no,
            "brand_b": yes_no,
            "brand_c": yes_no,
        },
        variable_formats={"id": "F8.0", "age": "F3.0", "income": "F10.2", "city": "A12"},
        variable_measures={
            "id": "scale",
            "gender": "nominal",
            "satisfaction": "ordinal",
            "age": "scale",
            "city": "nominal",
        },
        variable_missing_values={
            "satisfaction": {"type": "discrete", "values": [99]},
            "income": {"type": "range", "low": 999990, "high": 999999},
            "city": {"type": "discrete", "values": ["N/A"]},
        },
        variable_roles={"id": "none", "satisfaction": "target"},
        variable_attributes={"satisfaction": {"Section": ["Core"]}},
        mr_sets={
            "brands": {
                "label": "Brands mentioned",
                "type": "dichotomy",
                "counted_value": "1",
                "variables": ["brand_a", "brand_b", "brand_c"],
            }
        },
    )
    return df, meta


def long_strings() -> tuple[pl.DataFrame, am.SpssMetadata]:
    """Strings wider than 255 bytes (very long string segments) and non-ASCII text."""
    df = pl.DataFrame(
        {
            "id": [1.0, 2.0, 3.0],
            "comment": ["x" * 600, "Çok güzel — très bien — とても良い", ""],
            "short": ["a", "bb", "ccc"],
        }
    )
    meta = am.SpssMetadata(
        file_label="ambers long string fixture",
        variable_labels={"comment": "Open-ended comment"},
        variable_formats={"comment": "A1000", "short": "A8"},
    )
    return df, meta


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    df, meta = survey()
    am.write_sav(df, OUT / "survey.sav", meta=meta)
    am.write_sav(df, OUT / "survey_uncompressed.sav", meta=meta, compression="uncompressed")
    am.write_sav(df, OUT / "survey.zsav", meta=meta)
    df, meta = long_strings()
    am.write_sav(df, OUT / "long_strings.sav", meta=meta)
    for path in sorted(OUT.glob("*sav")):
        print(f"{path.name}: {path.stat().st_size} bytes")


if __name__ == "__main__":
    main()
