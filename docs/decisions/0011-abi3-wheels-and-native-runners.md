# 0011: One wheel per platform (abi3), each platform built on a runner of its own kind

Date: 2026-11-17
Status: accepted

## Decision

- The Python bindings are built against Python's stable interface (PyO3 feature `abi3-py312`). A release has **one wheel per platform**, tagged `cp312-abi3`, that serves Python 3.12 and every later version.
- Wheels and the standalone program are built for five platforms, each on a GitHub runner of that platform, with nothing cross-compiled: Linux x86-64 (`ubuntu-latest`), Linux ARM 64-bit (`ubuntu-24.04-arm`), Windows x86-64 (`windows-latest`), macOS Apple Silicon (`macos-latest`), macOS Intel (`macos-15-intel`).
- Before publishing, each platform's wheel is installed on the oldest and the newest supported Python (3.12 and 3.14) and the tests are run, and `abi3audit --strict` checks that the module uses nothing outside the stable interface.

This follows how Polars releases, without its two x86-64 variants (a compatible build and an AVX2 build chosen at import): ambers ships one x86-64 build at the x86-64-v2 baseline.

## Alternatives considered

- **One wheel per Python version**, as before: 9 builds for three platforms, 15 for five, and a new ambers release for every new Python. On the day this was decided GitHub's macOS queue cancelled a macOS job four times; a release needed three macOS wheel builds, now one per Mac architecture.
- **Cross-compiling** the Intel Mac build on an Apple Silicon runner and the ARM Linux build under emulation or with zig. It builds, but the result cannot be run where it was built, so the smoke tests and the install tests would not cover it. Native runners for both exist and are what Polars and DuckDB use.
- **Polars' two x86-64 variants.** A real speed gain for a query engine; for a file reader it is a second set of wheels and a CPU check at import for little.
- **musl wheels and Windows on ARM**, which Polars also builds. Not asked for; each is one more matrix entry when it is.

## Why

The bindings only use what the stable interface has (dictionaries, lists, tuples, capsules, exceptions, classes); the data crosses as Arrow capsules, which the choice of interface does not touch. So the cost of abi3 is small and was measured, and the gain is a third of the builds and no release per Python version.

## Consequences

- Measured before and after on the same machine, portable builds, alternating runs: `read_sav` 311 vs 313 ms (146 MB), 381 vs 381 ms (1.1 GB), 1.856 vs 1.850 s (5.4 GB), 3.21 vs 3.19 s (zsav); `read_sav_meta`, `write_sav` and small-file reads equal, all within run-to-run noise. A lazy `scan_sav(...).collect()` on a 0.6 MB file measured about 0.03 ms slower (1.04 vs 1.00 ms), a fixed cost that does not grow with the file. Accepted by the project owner on 2026-11-17 against the rule that no change may make reading slower.
- A Python built without the global lock (3.13t, 3.14t) cannot load abi3 wheels and falls back to the source package. It had no ambers wheel before either.
- The instruction check for ARM Linux is looser than that platform's baseline: it looks for what is newer than Apple M1, and would not notice ARMv8.1 to 8.4 instructions. Those builds set no CPU flags, so the compilers' ARMv8.0 default applies.
- `macos-15-intel` and `ubuntu-24.04-arm` are pinned images; the other three runners are `-latest` and can change under the build (as `ubuntu-latest` did: see the changelog for 0.6.0).
