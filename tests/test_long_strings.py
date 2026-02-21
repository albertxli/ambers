"""Pressure test: Long string variable reading (pyreadstat issue #119).

Verifies ambers correctly reads VLS (very long string) variables without
splitting them into ghost segment columns. Tests boundary cases at 255,
256, 504, 505, and 1000 characters.

Usage: .venv/Scripts/python tests/test_long_strings.py
"""

import os
import tempfile

import polars as pl
import pyreadstat
import ambers

PASSED = 0
FAILED = 0


def check(name, condition, detail=""):
    global PASSED, FAILED
    if condition:
        PASSED += 1
        print(f"  PASS: {name}")
    else:
        FAILED += 1
        print(f"  FAIL: {name} — {detail}")


def test_single_long_string(length, label):
    """Test a single string column with the given length."""
    print(f"\n--- {label}: single column, {length} chars ---")

    value = "A" * length
    df_write = pl.DataFrame({"LongStr": [value, value[:length // 2], "short"]})

    with tempfile.NamedTemporaryFile(suffix=".sav", delete=False) as f:
        path = f.name

    try:
        pyreadstat.write_sav(df_write, path)

        # Read with ambers
        df_ambers, meta_ambers = ambers.read_sav(path)

        # Check column count (should be 1, not split)
        check(
            "ambers column count",
            df_ambers.width == 1,
            f"expected 1 column, got {df_ambers.width}: {df_ambers.columns}",
        )

        # Check row count
        check("ambers row count", df_ambers.height == 3, f"got {df_ambers.height}")

        # Check full string preserved (row 0)
        ambers_val = df_ambers["LongStr"][0]
        check(
            f"ambers string length (row 0)",
            len(ambers_val) == length,
            f"expected {length}, got {len(ambers_val)}",
        )
        check(
            "ambers string content (row 0)",
            ambers_val == value,
            f"first 50 chars: {ambers_val[:50]}...",
        )

        # Check half-length string (row 1)
        ambers_val1 = df_ambers["LongStr"][1]
        expected1 = value[:length // 2]
        check(
            f"ambers string length (row 1)",
            len(ambers_val1) == len(expected1),
            f"expected {len(expected1)}, got {len(ambers_val1)}",
        )

        # Check short string (row 2)
        ambers_val2 = df_ambers["LongStr"][2]
        check(
            "ambers short string (row 2)",
            ambers_val2 == "short",
            f"got: '{ambers_val2}'",
        )

    finally:
        os.unlink(path)


def test_multiple_long_strings():
    """Test multiple long string columns mixed with numerics."""
    print("\n--- Multiple long string columns + numerics ---")

    df_write = pl.DataFrame({
        "id": [1.0, 2.0, 3.0],
        "score": [99.5, 88.3, 77.1],
        "short_str": ["hello", "world", "test"],
        "str_255": ["B" * 255] * 3,
        "str_500": ["C" * 500] * 3,
        "str_1000": ["D" * 1000] * 3,
        "another_num": [10.0, 20.0, 30.0],
    })

    with tempfile.NamedTemporaryFile(suffix=".sav", delete=False) as f:
        path = f.name

    try:
        pyreadstat.write_sav(df_write, path)

        # Read with ambers
        df_ambers, meta = ambers.read_sav(path)

        # Column count — should be 7 visible columns, not more
        check(
            "ambers column count (mixed)",
            df_ambers.width == 7,
            f"expected 7, got {df_ambers.width}: {df_ambers.columns}",
        )

        # Check column names match what we wrote
        expected_cols = sorted(["id", "score", "short_str", "str_255", "str_500", "str_1000", "another_num"])
        ambers_cols = sorted(df_ambers.columns)
        check(
            "column names match",
            ambers_cols == expected_cols,
            f"ambers={ambers_cols}, expected={expected_cols}",
        )

        # Verify numeric columns
        check(
            "numeric id preserved",
            list(df_ambers["id"]) == [1.0, 2.0, 3.0],
            f"got {list(df_ambers['id'])}",
        )
        check(
            "numeric score preserved",
            list(df_ambers["score"]) == [99.5, 88.3, 77.1],
            f"got {list(df_ambers['score'])}",
        )

        # Verify string columns
        for col, char, length in [
            ("short_str", None, None),
            ("str_255", "B", 255),
            ("str_500", "C", 500),
            ("str_1000", "D", 1000),
        ]:
            if length:
                val = df_ambers[col][0]
                check(
                    f"{col} length",
                    len(val) == length,
                    f"expected {length}, got {len(val)}",
                )
                check(
                    f"{col} content",
                    val == char * length,
                    f"first 30 chars: {val[:30]}",
                )
            else:
                check(
                    f"{col} value",
                    df_ambers[col][0] == "hello",
                    f"got: '{df_ambers[col][0]}'",
                )

    finally:
        os.unlink(path)


def test_issue_119_reproduction():
    """Reproduce the exact scenario from pyreadstat issue #119."""
    print("\n--- Issue #119 reproduction (reading only) ---")

    lorem = (
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do "
        "eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim "
        "ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut "
        "aliquip ex ea commodo consequat. Duis aute irure dolor in "
        "reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla "
        "pariatur. Excepteur sint occaecat cupidatat non proident, sunt in "
        "culpa qui officia deserunt mollit anim id est laborum."
    )

    df_write = pl.DataFrame({
        "LongString1": [lorem],
        "LongString2": [lorem + " " + lorem],
    })

    with tempfile.NamedTemporaryFile(suffix=".sav", delete=False) as f:
        path = f.name

    try:
        pyreadstat.write_sav(df_write, path)

        # Read with ambers
        df_ambers, meta = ambers.read_sav(path)

        # Should have exactly 2 columns (not 5 as SPSS might show)
        check(
            "ambers: 2 columns (not split)",
            df_ambers.width == 2,
            f"got {df_ambers.width}: {df_ambers.columns}",
        )

        # Verify LongString1
        val1 = df_ambers["LongString1"][0]
        check(
            "LongString1 matches",
            val1 == lorem,
            f"len={len(val1)}, expected={len(lorem)}",
        )

        # Verify LongString2
        expected2 = lorem + " " + lorem
        val2 = df_ambers["LongString2"][0]
        check(
            "LongString2 matches",
            val2 == expected2,
            f"len={len(val2)}, expected={len(expected2)}",
        )

    finally:
        os.unlink(path)


def test_boundary_504_505():
    """Test the exact 504/505 char boundary from issue #119 comments."""
    print("\n--- Boundary test: 504 vs 505 chars (issue #119 comment) ---")

    # Build columns with None defaults, then set long string values
    columns = [
        "so3_10_9_1", "so3_10_10_1", "so3_10_11_1", "so3_10_12_1",
        "so3_10_13_1", "so3_10_14_1", "so3_10_15_1", "so3_10_16_1",
        "so3_10_17_1", "so3_10_18_1", "so3_10_19_1", "so3_10_20_1",
        "so3_10_96opn", "so3_10_97opn", "so3_10_98opn",
    ]

    data = {}
    for col in columns:
        data[col] = [""]

    # Set long string values
    data["so3_10_98opn"] = ["a" * 505]
    data["so3_10_97opn"] = ["a" * 504]
    data["so3_10_96opn"] = ["a" * 503]

    df_write = pl.DataFrame(data)

    with tempfile.NamedTemporaryFile(suffix=".sav", delete=False) as f:
        path = f.name

    try:
        pyreadstat.write_sav(df_write, path)

        # Read with ambers
        df_ambers, meta = ambers.read_sav(path)

        # Column count should be 15 (not more from splitting)
        check(
            "ambers: 15 columns",
            df_ambers.width == len(columns),
            f"got {df_ambers.width}: {df_ambers.columns}",
        )

        # Verify the long string values
        for col, expected_len in [
            ("so3_10_96opn", 503),
            ("so3_10_97opn", 504),
            ("so3_10_98opn", 505),
        ]:
            val = df_ambers[col][0]
            if val is None:
                check(f"{col} not null", False, "got None")
                continue
            check(
                f"{col} length={expected_len}",
                len(val) == expected_len,
                f"got {len(val)}",
            )
            check(
                f"{col} content",
                val == "a" * expected_len,
                f"first 30: {val[:30]}",
            )

    finally:
        os.unlink(path)


def main():
    global PASSED, FAILED

    print("=" * 60)
    print("LONG STRING VARIABLE PRESSURE TEST")
    print("(pyreadstat issue #119)")
    print("=" * 60)

    # Single column at various lengths
    test_single_long_string(255, "255 chars (max normal)")
    test_single_long_string(256, "256 chars (min VLS)")
    test_single_long_string(504, "504 chars (issue boundary)")
    test_single_long_string(505, "505 chars (issue boundary)")
    test_single_long_string(1000, "1000 chars (multi-segment)")

    # Multiple columns mixed
    test_multiple_long_strings()

    # Issue #119 exact reproduction
    test_issue_119_reproduction()

    # 504/505 boundary from comments
    test_boundary_504_505()

    # Summary
    total = PASSED + FAILED
    print(f"\n{'=' * 60}")
    print(f"RESULTS: {PASSED}/{total} passed, {FAILED} failed")
    print(f"{'=' * 60}")

    if FAILED > 0:
        exit(1)


if __name__ == "__main__":
    main()
