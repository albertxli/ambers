# 0001: A Cargo workspace with one crate per concern

Date: 2026-11-11 (extended 2026-11-12)
Status: accepted

## Decision

The repository is a Cargo workspace. Each concern is its own crate, and dependencies run one way only:

```
ambers  <-  ambers-codebook  <-  ambers-dataset  <-  ambers-web  <-  ambers-cli  <-  ambers-py
```

| Crate | Concern |
|---|---|
| `ambers` | The engine: read and write `.sav` / `.zsav`, Arrow output. Published on crates.io |
| `ambers-codebook` | Variable types, counts, label checks; later the codebook model and export |
| `ambers-dataset` | A loaded file ready to look at: background loading, memory budget, row filters, cell formatting, value tables. No user interface code |
| `ambers-web` | The local server, JSON API and embedded page behind `am open` |
| `ambers-cli` | The `ambers` / `am` binary: parses arguments and calls the crates above; holds the terminal viewer |
| `ambers-py` | PyO3 bindings; the only crate that may depend on pyo3 |

`ambers-dataset` and `ambers-web` were added on 2026-11-12, when the browser viewer was planned.

This reverses commit `f5d41c5` (2026-02-17), which had folded the first two crates into a single crate with feature flags.

## Alternatives considered

- **One crate with feature flags** (the layout of `f5d41c5`, after the fastexcel model). Fine while ambers was a reader with Python bindings. It stopped fitting when a command-line binary was added: the binary must run on a machine with no Python, and the engine must stay free of everything that is not parsing and writing.
- **Shared viewer code inside `ambers-cli`** (as modules `dataset/` and `web/`). Rejected: the command line is meant to stay thin, and the formatting and filtering rules are needed by the terminal viewer, the browser, a future codebook export and Python alike. A module inside the CLI crate cannot be depended on without the CLI.

## Why

- The engine depends on no other workspace crate and never on pyo3, so `cargo tree -p ambers` stays small and the published crate stays lean.
- The standalone binary cannot link Python by accident: only `ambers-py` names pyo3.
- "How does this value show, which rows pass this filter" has exactly one implementation, in `ambers-dataset`.
- Heavy dependencies (the HTTP server) sit in one crate behind a feature of the CLI.
- `ambers-py` is outside `default-members`, so `cargo build`, `cargo test` and `cargo clippy` need no Python.
