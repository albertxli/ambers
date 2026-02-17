# ambers vs pyreadstat Benchmark Results

**Date:** 2026-02-17
**Platform:** Windows 10, MSYS2
**Runs per tool:** 3 (best-of-3 reported)

ambers is a pure Rust SPSS reader with rayon parallelism and fat LTO. pyreadstat is a Python/C wrapper around ReadStat. Both output Polars-compatible Arrow data.

Note: ambers timing includes subprocess spawn overhead (~200-400ms). In-process PyO3 bindings would be even faster.

---

## Test 1: 251001.sav (147 MB)

| | ambers (Rust) | pyreadstat (Python/C) |
|---|---:|---:|
| Rows | 22,070 | 22,070 |
| Columns | 677 | 677 |
| Best time (s) | **1.268** | 3.074 |
| Mean time (s) | 1.282 | 3.187 |

**ambers is 2.4x faster**

---

## Test 2: rpm_2025_data_tracking_partial_uam (1.1 GB)

| | ambers (Rust) | pyreadstat (Python/C) |
|---|---:|---:|
| Rows | 79,066 | 79,066 |
| Columns | 915 | 915 |
| Best time (s) | **2.415** | 6.397 |
| Mean time (s) | 2.470 | 6.841 |

**ambers is 2.6x faster**

---

## Summary

| File | Size | Rows | Cols | ambers | pyreadstat | Speedup |
|---|---|---:|---:|---:|---:|---:|
| 251001.sav | 147 MB | 22,070 | 677 | 1.27s | 3.07s | **2.4x** |
| rpm_2025_tracking | 1.1 GB | 79,066 | 915 | 2.42s | 6.40s | **2.6x** |

Speedup improves at scale as fixed overhead becomes proportionally smaller.
