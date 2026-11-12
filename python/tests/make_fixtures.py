"""Regenerate the small synthetic fixtures in tests/data/.

The files are written with ambers' own writer from made-up survey data, so
they are safe to commit and available in CI. Real files stay in the
gitignored test_data/ directory.

Usage:
    uv run python/tests/make_fixtures.py                    # every fixture
    uv run python/tests/make_fixtures.py imperfect_labels   # only the named ones

The writer stamps the creation time into each file, so regenerated files
differ from the committed ones by those bytes, and the CLI snapshots that
show the creation time change with them. Regenerate only what you changed.
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


def imperfect_labels() -> tuple[pl.DataFrame, am.SpssMetadata]:
    """Value labels the way real files have them: incomplete and ambiguous.

    - ``q1``: codes 7 and 8 appear in the data but have no label
    - ``scale``: a 0-10 scale with only the endpoints labelled
    - ``brand``: codes 97 and 98 share the label "Other"
    - ``city``: a labelled string variable that also holds free text
    - ``q1`` and ``scale`` declare user-missing values (99, and 90-99)
    - ``age``: no value labels at all
    """
    df = pl.DataFrame(
        {
            "id": [float(i) for i in range(1, 13)],
            "q1": [1.0, 2.0, 3.0, 7.0, 1.0, 99.0, None, 8.0, 2.0, 3.0, 7.0, 1.0],
            "scale": [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 95.0],
            "brand": [1.0, 97.0, 98.0, 1.0, 2.0, 97.0, 2.0, 98.0, 1.0, None, 2.0, 1.0],
            "city": ["NYC", "LA", "Springfield", "NYC", "", "Paris", "LA", "NYC", "LA", "Lyon", "NYC", "LA"],
            "age": [18.0, 25.5, 31.0, 44.0, 52.0, 67.0, None, 23.0, 38.0, 41.0, 59.0, 70.0],
        }
    )
    meta = am.SpssMetadata(
        file_label="ambers imperfect value labels fixture",
        variable_labels={
            "id": "Respondent ID",
            "q1": "Agreement",
            "scale": "Likelihood to recommend",
            "brand": "Brand used most",
            "city": "City",
            "age": "Age in years",
        },
        variable_value_labels={
            "q1": {1: "Agree", 2: "Neutral", 3: "Disagree", 99: "Refused"},
            "scale": {0: "Not at all likely", 10: "Extremely likely"},
            "brand": {1: "Brand A", 2: "Brand B", 97: "Other", 98: "Other"},
            "city": {"NYC": "New York", "LA": "Los Angeles"},
        },
        variable_formats={"id": "F8.0", "q1": "F8.0", "scale": "F8.0", "brand": "F8.0", "city": "A20", "age": "F8.1"},
        variable_missing_values={
            "q1": {"type": "discrete", "values": [99]},
            "scale": {"type": "range", "low": 90, "high": 99},
        },
    )
    return df, meta


# Each fixture: the function that builds it and the files it writes.
FIXTURES = {
    "survey": (
        survey,
        [("survey.sav", {}), ("survey_uncompressed.sav", {"compression": "uncompressed"}), ("survey.zsav", {})],
    ),
    "long_strings": (long_strings, [("long_strings.sav", {})]),
    "imperfect_labels": (imperfect_labels, [("imperfect_labels.sav", {})]),
}


def main(names: list[str]) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for name in names or FIXTURES:
        build, files = FIXTURES[name]
        df, meta = build()
        for file_name, options in files:
            am.write_sav(df, OUT / file_name, meta=meta, **options)
    for path in sorted(OUT.glob("*sav")):
        print(f"{path.name}: {path.stat().st_size} bytes")


if __name__ == "__main__":
    import sys

    main(sys.argv[1:])
