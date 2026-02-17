"""Benchmark: ambers (Python/Rust) vs pyreadstat (Python/C) on the same .sav file."""

import time
import statistics
import sys

import ambers
import pyreadstat

FILE = r"C:\Users\lipov\SynologyDrive\_PMI\Multi-Wave\RPM\2025\Data\251001.sav"
RUNS = 5


def bench_ambers():
    """Time ambers.read_sav (Rust via PyO3 -> Polars DataFrame)."""
    times = []
    rows = cols = None
    for i in range(RUNS):
        t0 = time.perf_counter()
        df, meta = ambers.read_sav(FILE)
        elapsed = time.perf_counter() - t0
        times.append(elapsed)
        rows = df.height
        cols = df.width
        print(f"  ambers     run {i+1}: {elapsed:.3f}s")
    return times, rows, cols


def bench_pyreadstat():
    """Time pyreadstat.read_sav with polars output."""
    times = []
    rows = cols = None
    for i in range(RUNS):
        t0 = time.perf_counter()
        df, meta = pyreadstat.read_sav(FILE, output_format="polars")
        elapsed = time.perf_counter() - t0
        times.append(elapsed)
        rows = df.height
        cols = df.width
        print(f"  pyreadstat run {i+1}: {elapsed:.3f}s")
    return times, rows, cols


def main():
    print(f"Benchmarking: {FILE}")
    print(f"Runs per tool: {RUNS}\n")

    print("--- ambers (Python/Rust via PyO3) ---")
    a_times, a_rows, a_cols = bench_ambers()

    print("\n--- pyreadstat (Python/C) ---")
    p_times, p_rows, p_cols = bench_pyreadstat()

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
        print(f"ambers is {ratio:.1f}x faster (best of {RUNS})")
    else:
        ratio = a_min / p_min
        print(f"pyreadstat is {ratio:.1f}x faster (best of {RUNS})")

    print("=" * 60)

    # Shape sanity check
    if a_rows != p_rows or a_cols != p_cols:
        print(f"\nWARNING: Shape mismatch! ambers=({a_rows},{a_cols}) vs pyreadstat=({p_rows},{p_cols})")


if __name__ == "__main__":
    main()
