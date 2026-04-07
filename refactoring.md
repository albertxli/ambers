# Modularity Refactoring Plan

Branch: `refactor/modularity-split`

## Phase 1: Split `src/python/mod.rs` (2,299 LOC → 5 files)

- [x] 1.1 Create `src/python/conversions.rs` (~310 LOC) — type conversion helpers
- [x] 1.2 Create `src/python/metadata.rs` (~990 LOC) — PySpssMetadata + apply_kwargs
- [x] 1.3 Create `src/python/diff.rs` (~450 LOC) — PyMetaDiff + diff helpers
- [x] 1.4 Create `src/python/io.rs` (~220 LOC) — PyArrowData, PySavBatchReader, read/write functions
- [x] 1.5 Rewrite `src/python/mod.rs` as thin module root (~40 LOC)
- [x] 1.6 Verify: `cargo build --features python` ✓
- [x] 1.7 Verify: `cargo test` — 107/107 passed (3 diagnostic test failures are pre-existing)
- [x] 1.8 Clippy: same 19 warnings as original code (pre-existing collapsible_if, no new warnings)

## Phase 2: Slim `python/ambers/__init__.py` (432 LOC → thin re-exports)

- [x] 2.1 Create `python/ambers/_containers.py` (~110 LOC) — SavFile, _format_size, _get_dtype_map, _resolve_columns
- [x] 2.2 Create `python/ambers/_io.py` (~280 LOC) — read_sav, scan_sav, write_sav, read_sav_meta
- [x] 2.3 Rewrite `python/ambers/__init__.py` as thin re-export layer (~18 LOC)
- [x] 2.4 Verify: `uv run maturin develop --release` ✓
- [x] 2.5 Verify: `uv run pytest tests/ -v` — 119/119 passed ✓

## Final Verification

- [x] 3.1 Smoke test: full API exercise (read_sav, scan_sav, write_sav, read_sav_meta, apply_labels, SpssMetadata, MetaDiff) ✓
- [x] 3.2 Broad test: roundtrip + metadata tests on 8 test files — 154 passed, 3 pre-existing failures (identical to main) ✓
- [ ] 3.3 Commit on refactor branch
