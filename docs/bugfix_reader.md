# Reader Bug Fixes

Bugs found by comparing ambers output against pyreadstat for `251001.sav` (677 columns, 22,070 rows, 85 VLS variables with width 512). Root causes confirmed via ReadStat source analysis.

---

## Bug A — Subtype 11 display_idx misalignment

**Symptom:** VLS variables got wrong `measure`, `display_width`, and `alignment` values. All display properties after the first VLS variable were shifted/misassigned.

**Root cause:** Subtype 11 (variable display info) has one entry per non-continuation variable record (`raw_type != -1`), including the named VLS segment variables that get marked as ghosts in step 3. The old code skipped ALL ghosts when iterating, causing `display_idx` to fall behind the actual subtype 11 entry index.

**Example:** If variable #10 is a VLS with 3 segments (1 primary + 2 ghost segments), subtype 11 has 3 entries for those segments. The old code skipped the 2 ghost segments, so `display_idx` was 2 behind for every subsequent variable.

**Fix** (`dictionary.rs`): Rewrote step 4 to iterate all variables, skipping only type=-1 continuation records (which have no subtype 11 entry). Named ghost segments consume their display entry without applying it:

```rust
// Before (broken):
for var in &mut variables {
    if var.is_ghost { continue; }  // skipped ghost segments — misaligns index
    // apply display entry...
    display_idx += 1;
}

// After (fixed):
while var_idx < variables.len() {
    if variables[var_idx].raw_type == -1 {
        var_idx += 1;  // continuation records have no subtype 11 entry
        continue;
    }
    // consume one display entry (apply only if not ghost)
    if !variables[var_idx].is_ghost {
        // apply measure, display_width, alignment
    }
    display_idx += 1;
    var_idx += 1;
}
```

---

## Bug B — SpssFormat.width is u8 (max 255)

**Symptom:** 85 VLS variables with true width 512 showed `A0` format string (512 mod 256 = 0), `display_width=0`, and `storage_width=255`.

**Root cause:** `SpssFormat.width` is a `u8`, parsed from the packed i32 format spec `(type << 16) | (width << 8) | decimals` where width occupies one byte. For VLS variables with width > 255, the packed format truncates to `width % 256`. SPSS stores the true width in subtype 14 (very long strings), but ambers never propagated it to the format string or display width.

pyreadstat/ReadStat solves this by overriding `print_format.width` with the VLS true width after subtype 14 resolution.

**Fix** (`dictionary.rs`): Two changes in the metadata-building loop:

1. **Format string override** — For VLS variables, generate the format string using the true width from `VarType::String(w)`:
```rust
let format_str = match &var.var_type {
    VarType::String(w) if *w > 255 => format!("{}{}", fmt.format_type.prefix(), w),
    _ => fmt.to_spss_string(),
};
```

2. **Display width fallback** — When subtype 11 gives `display_width=0` (from the truncated packed format), default to the true storage width:
```rust
let display_width = match &var.var_type {
    VarType::String(w) if *w > 255 && var.display_width == 0 => *w as u32,
    _ => var.display_width,
};
```

**Note:** `storage_width` was already correct because it reads from `VarType::String(w)` which gets the true width from subtype 14 resolution.

---

## Bug C — mr_sets always empty

**Symptom:** `meta.mr_sets` was always an empty HashMap. pyreadstat showed 7 MR sets for `251001.sav`.

**Root cause:** Subtype 7 (multiple response sets) was silently skipped by the catch-all `_ =>` branch in `info_records/mod.rs`. The `MrSet` struct existed in `metadata.rs` but was never populated.

**Fix:** Implemented full subtype 7 parsing:

- **`constants.rs`** — Added `INFO_MR_SETS: i32 = 7`
- **`info_records/mr_sets.rs`** (new file) — Parser for the newline-separated text format:
  ```
  $NAME=Dn counted_value label_len label var1 var2 ...\n
  $NAME=C label_len label var1 var2 ...\n
  ```
  Where `n` = length of counted_value, `label_len` = length of label string.
- **`info_records/mod.rs`** — Added `InfoRecord::MrSets` variant and dispatch
- **`dictionary.rs`** — Added `mr_sets` field to `RawDictionary`, wired up parsing in the dispatch loop, and resolves SHORT variable names to long names using the `short_to_long` map during dictionary resolution

**Result:** 7 MR sets correctly parsed (AD6, vctq1, S12, LIV, CN_Q1, conditions, DEFENDER_RESULTS) with proper labels and resolved variable name lists.

---

## Previously Fixed Bugs (from Milestone 1 development)

### Bug 1 — Column count wrong (847 vs 677)
VLS segment variables (e.g., SURVE0, SURVE1) weren't marked as ghosts. Fix: walk forward from each VLS variable and mark the next `n_segments - 1` non-ghost named records as ghosts.

### Bug 2 — Row count wrong (87 vs 1500)
Bytecode decompressor was stateless — control block state lost between row calls. Fix: made `BytecodeDecompressor` stateful, preserving `pos`, `control_bytes`, `control_idx`, `eof` across `decompress_row()` calls.

### Bug 3 — SYSMIS is not NaN
SYSMIS is `-DBL_MAX` (most negative finite double, bits `0xFFEFFFFFFFFFFFFF`), not a NaN. Test assertions needed `is_finite()` and `== -f64::MAX`.
