"""Stress test: Very wide files (3 million columns).

Verifies ambers can read .sav files far beyond ReadStat's 16 MB allocation
limit (pyreadstat issue #79). Tests 3,000,000 Float64 columns × 1 row.

Usage: .venv/Scripts/python tests/test_wide_columns.py
"""

import os
import sys
import time
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


def test_wide_columns(n_cols):
    """Test reading a file with n_cols Float64 columns."""
    print(f"\n--- {n_cols:,} columns × 1 row ---")

    # Generate column names and data
    print(f"  Generating {n_cols:,} columns...")
    t0 = time.perf_counter()
    col_names = [f"V{i}" for i in range(1, n_cols + 1)]
    # Use a dict of lists for Polars DataFrame
    data = {name: [float(i)] for i, name in enumerate(col_names, 1)}
    df_write = pl.DataFrame(data)
    t_gen = time.perf_counter() - t0
    print(f"  DataFrame generated in {t_gen:.1f}s ({df_write.width:,} cols)")

    with tempfile.NamedTemporaryFile(suffix=".sav", delete=False) as f:
        path = f.name

    try:
        # Write with pyreadstat
        print(f"  Writing .sav file...")
        t0 = time.perf_counter()
        pyreadstat.write_sav(df_write, path)
        t_write = time.perf_counter() - t0
        file_size = os.path.getsize(path)
        print(f"  Written in {t_write:.1f}s ({file_size / 1024 / 1024:.1f} MB)")

        # Read with ambers
        print(f"  Reading with ambers...")
        t0 = time.perf_counter()
        df_ambers, meta = ambers.read_sav(path)
        t_read = time.perf_counter() - t0
        print(f"  Read in {t_read:.1f}s")

        # Verify column count
        check(
            f"ambers column count = {n_cols:,}",
            df_ambers.width == n_cols,
            f"got {df_ambers.width:,}",
        )

        # Verify row count
        check("ambers row count = 1", df_ambers.height == 1, f"got {df_ambers.height}")

        # Spot-check first, middle, and last columns
        check(
            "first column (V1) = 1.0",
            df_ambers["V1"][0] == 1.0,
            f"got {df_ambers['V1'][0]}",
        )

        mid = n_cols // 2
        mid_name = f"V{mid}"
        check(
            f"middle column ({mid_name}) = {float(mid)}",
            df_ambers[mid_name][0] == float(mid),
            f"got {df_ambers[mid_name][0]}",
        )

        last_name = f"V{n_cols}"
        check(
            f"last column ({last_name}) = {float(n_cols)}",
            df_ambers[last_name][0] == float(n_cols),
            f"got {df_ambers[last_name][0]}",
        )

        # Verify metadata
        check(
            "metadata column count",
            meta.number_columns == n_cols,
            f"got {meta.number_columns:,}",
        )

        print(f"\n  Summary: {t_write:.1f}s write, {t_read:.1f}s read, {file_size / 1024 / 1024:.1f} MB file")

    finally:
        os.unlink(path)


def main():
    global PASSED, FAILED

    print("=" * 60)
    print("WIDE COLUMN STRESS TEST")
    print("(pyreadstat issue #79 — ReadStat 16 MB limit)")
    print("=" * 60)

    # Warm up with a smaller test first
    test_wide_columns(100_000)

    # The big one: 3 million columns
    test_wide_columns(3_000_000)

    # Summary
    total = PASSED + FAILED
    print(f"\n{'=' * 60}")
    print(f"RESULTS: {PASSED}/{total} passed, {FAILED} failed")
    print(f"{'=' * 60}")

    if FAILED > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
