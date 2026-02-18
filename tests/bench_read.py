"""Benchmark: ambers (Python/Rust) vs pyreadstat (Python/C) on the same .sav file.

Usage:
    python tests/bench_read.py [path_to_sav_file] [num_runs]
"""

import time
import statistics
import sys

import ambers
import pyreadstat

from test_paths import BENCH_READ_FILE as DEFAULT_FILE
DEFAULT_RUNS = 5


def bench_ambers(file_path, runs):
    """Time ambers.read_sav (Rust via PyO3 -> Polars DataFrame)."""
    times = []
    rows = cols = None
    for i in range(runs):
        t0 = time.perf_counter()
        df, meta = ambers.read_sav(file_path)
        elapsed = time.perf_counter() - t0
        times.append(elapsed)
        rows = df.height
        cols = df.width
        print(f"  ambers     run {i+1}: {elapsed:.3f}s")
    return times, rows, cols


def bench_pyreadstat(file_path, runs):
    """Time pyreadstat.read_sav with polars output."""
    times = []
    rows = cols = None
    for i in range(runs):
        t0 = time.perf_counter()
        df, meta = pyreadstat.read_sav(file_path, output_format="polars")
        elapsed = time.perf_counter() - t0
        times.append(elapsed)
        rows = df.height
        cols = df.width
        print(f"  pyreadstat run {i+1}: {elapsed:.3f}s")
    return times, rows, cols


def main():
    file_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_FILE
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_RUNS

    print(f"Benchmarking: {file_path}")
    print(f"Runs per tool: {runs}\n")

    print("--- ambers (Python/Rust via PyO3) ---")
    a_times, a_rows, a_cols = bench_ambers(file_path, runs)

    print("\n--- pyreadstat (Python/C) ---")
    p_times, p_rows, p_cols = bench_pyreadstat(file_path, runs)

    # Results
    a_min = min(a_times)
    a_mean = statistics.mean(a_times)
    p_min = min(p_times)
    p_mean = statistics.mean(p_times)

    print("\n" + "=" * 60)
    print(f"{'':30} {'ambers':>12} {'pyreadstat':>12}")
    print("-" * 60)
    print(f"{'Rows':30} {a_rows:>12,} {p_rows:>12,}")
    print(f"{'Columns':30} {a_cols:>12,} {p_cols:>12,}")
    print(f"{'Best time (s)':30} {a_min:>12.3f} {p_min:>12.3f}")
    print(f"{'Mean time (s)':30} {a_mean:>12.3f} {p_mean:>12.3f}")
    print("-" * 60)

    if a_min < p_min:
        ratio = p_min / a_min
        print(f"ambers is {ratio:.1f}x faster (best of {runs})")
    else:
        ratio = a_min / p_min
        print(f"pyreadstat is {ratio:.1f}x faster (best of {runs})")

    print("=" * 60)

    # Shape sanity check
    if a_rows != p_rows or a_cols != p_cols:
        print(f"\nWARNING: Shape mismatch! ambers=({a_rows},{a_cols}) vs pyreadstat=({p_rows},{p_cols})")


if __name__ == "__main__":
    main()
