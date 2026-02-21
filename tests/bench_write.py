"""Write benchmark: ambers.write_sav vs pyreadstat.write_sav.

Compares write speed across test files for both bytecode (.sav) and
zlib (.zsav) compression modes.

Usage: python tests/bench_write.py
"""

import gc
import os
import tempfile
import time

import ambers
import pyreadstat

RUNS = 5
TEST_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "test_data")

FILES = [
    ("test_1", "test_1_small.sav"),
    ("test_2", "test_2_medium.sav"),
    ("test_3", "test_3_medium.sav"),
    ("test_4", "test_4_small.sav"),
    ("test_5", "test_5_small.sav"),
    ("test_6", "test_6_large.sav"),
]


def fmt_size(path):
    size_mb = os.path.getsize(path) / 1024 / 1024
    return f"{size_mb / 1024:.1f} GB" if size_mb >= 1024 else f"{size_mb:.0f} MB"


def fmt_ratio(a_best, p_best):
    if a_best <= p_best:
        return f"\033[32m{p_best / a_best:.1f}x faster\033[0m"
    else:
        return f"\033[31m{a_best / p_best:.1f}x SLOWER\033[0m"


def prepare_pyreadstat_args(meta):
    """Extract metadata fields into pyreadstat.write_sav kwargs."""
    kwargs = {}

    if meta.file_label:
        kwargs["file_label"] = meta.file_label

    if meta.variable_labels:
        kwargs["column_labels"] = meta.variable_labels

    if meta.variable_value_labels:
        kwargs["variable_value_labels"] = meta.variable_value_labels

    if meta.variable_display_widths:
        kwargs["variable_display_widths"] = meta.variable_display_widths

    if meta.variable_measures:
        kwargs["variable_measures"] = meta.variable_measures

    if meta.notes:
        kwargs["note"] = meta.notes[0] if len(meta.notes) == 1 else meta.notes

    return kwargs


def bench_sav(label, path):
    """Benchmark bytecode .sav writing."""
    size_str = fmt_size(path)

    # Read with ambers
    df_polars, meta = ambers.read_sav(path)
    rows, cols = df_polars.height, df_polars.width

    # Pre-convert to pandas for pyreadstat (exclude conversion from timing)
    df_pandas = df_polars.to_pandas()
    pyr_kwargs = prepare_pyreadstat_args(meta)

    with tempfile.TemporaryDirectory() as tmp:
        am_path = os.path.join(tmp, "ambers.sav")
        pyr_path = os.path.join(tmp, "pyreadstat.sav")

        # Warmup
        ambers.write_sav(df_polars, am_path, meta=meta)
        pyreadstat.write_sav(df_pandas, pyr_path, row_compress=True, **pyr_kwargs)

        # Bench ambers
        a_times = []
        for _ in range(RUNS):
            gc.collect()
            t0 = time.perf_counter()
            ambers.write_sav(df_polars, am_path, meta=meta)
            a_times.append(time.perf_counter() - t0)

        # Bench pyreadstat
        p_times = []
        for _ in range(RUNS):
            gc.collect()
            t0 = time.perf_counter()
            pyreadstat.write_sav(df_pandas, pyr_path, row_compress=True, **pyr_kwargs)
            p_times.append(time.perf_counter() - t0)

    a_best = min(a_times)
    p_best = min(p_times)
    ratio = fmt_ratio(a_best, p_best)

    # Output sizes
    am_sz = os.path.getsize(am_path) if os.path.exists(am_path) else 0
    pyr_sz = os.path.getsize(pyr_path) if os.path.exists(pyr_path) else 0

    print(
        f"  .sav   {label:<8} {size_str:>8} {rows:>8,} x {cols:<5} | "
        f"ambers {a_best:.3f}s  pyr {p_best:.3f}s  -> {ratio}"
    )
    return a_best, p_best


def bench_zsav(label, path):
    """Benchmark zlib .zsav writing."""
    size_str = fmt_size(path)

    # Read with ambers
    df_polars, meta = ambers.read_sav(path)
    rows, cols = df_polars.height, df_polars.width

    # Pre-convert to pandas for pyreadstat
    df_pandas = df_polars.to_pandas()
    pyr_kwargs = prepare_pyreadstat_args(meta)

    with tempfile.TemporaryDirectory() as tmp:
        am_path = os.path.join(tmp, "ambers.zsav")
        pyr_path = os.path.join(tmp, "pyreadstat.zsav")

        # Warmup
        ambers.write_sav(df_polars, am_path, meta=meta)
        pyreadstat.write_sav(df_pandas, pyr_path, compress=True, **pyr_kwargs)

        # Bench ambers
        a_times = []
        for _ in range(RUNS):
            gc.collect()
            t0 = time.perf_counter()
            ambers.write_sav(df_polars, am_path, meta=meta)
            a_times.append(time.perf_counter() - t0)

        # Bench pyreadstat
        p_times = []
        for _ in range(RUNS):
            gc.collect()
            t0 = time.perf_counter()
            pyreadstat.write_sav(df_pandas, pyr_path, compress=True, **pyr_kwargs)
            p_times.append(time.perf_counter() - t0)

    a_best = min(a_times)
    p_best = min(p_times)
    ratio = fmt_ratio(a_best, p_best)

    print(
        f"  .zsav  {label:<8} {size_str:>8} {rows:>8,} x {cols:<5} | "
        f"ambers {a_best:.3f}s  pyr {p_best:.3f}s  -> {ratio}"
    )
    return a_best, p_best


def main():
    print(f"Write Benchmark: ambers vs pyreadstat ({RUNS} runs, best time)")
    print("=" * 80)

    print(f"\n{'  Mode':<8} {'File':<8} {'Size':>8} {'Shape':>16} | {'Results'}")
    print("-" * 80)

    for label, filename in FILES:
        path = os.path.join(TEST_DIR, filename)
        if not os.path.exists(path):
            print(f"  {label}: SKIPPED (not found)")
            continue

        bench_sav(label, path)
        bench_zsav(label, path)
        print()

    print("Done.")


if __name__ == "__main__":
    main()
