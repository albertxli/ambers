"""Diagnostic: isolate SPSS zsav crash.

Generates several zsav files to determine if the crash is:
(a) ALL ambers zsav files
(b) Only VLS + zsav
(c) Something specific about A500 + zsav

Each file is also written as .sav (bytecode) for comparison.
Open each pair in SPSS and report which ones crash.

Usage: source .venv/Scripts/activate && python tests/diag_zsav.py
"""

import os
import pathlib
import polars as pl
import ambers as am

OUT = pathlib.Path(__file__).resolve().parent.parent / "test_data" / "zsav_diagnostic"
OUT.mkdir(parents=True, exist_ok=True)


def write_pair(df, meta, basename):
    """Write both .sav (bytecode) and .zsav (zlib) for comparison."""
    sav_path = str(OUT / f"{basename}.sav")
    zsav_path = str(OUT / f"{basename}.zsav")
    am.write_sav(df, sav_path, meta=meta)
    am.write_sav(df, zsav_path, meta=meta)
    # Verify both readable by ambers
    df1 = am.read_sav(sav_path).data
    df2 = am.read_sav(zsav_path).data
    assert df1.shape == df2.shape, f"Shape mismatch: {df1.shape} vs {df2.shape}"
    sav_size = os.path.getsize(sav_path)
    zsav_size = os.path.getsize(zsav_path)
    print(f"  {basename}.sav  = {sav_size:>10,} bytes")
    print(f"  {basename}.zsav = {zsav_size:>10,} bytes")


# ── Test 1: Numeric only (no strings at all) ──
print("\n[1] Numeric only (3 cols, 5 rows)")
df = pl.DataFrame({
    "x": [1.0, 2.0, 3.0, 4.0, 5.0],
    "y": [10, 20, 30, 40, 50],
    "z": [3.14, 2.72, 1.41, 0.0, -1.5],
})
meta = am.SpssMetadata(variable_formats={"x": "F8.2", "y": "F8.0", "z": "F8.2"})
write_pair(df, meta, "diag_numeric_only")

# ── Test 2: Short strings only (≤255 bytes, no VLS) ──
print("\n[2] Short strings (A50, no VLS)")
df = pl.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "city": ["New York", "London", "Tokyo"],
})
meta = am.SpssMetadata(variable_formats={"id": "F8.0", "name": "A50", "city": "A50"})
write_pair(df, meta, "diag_short_string")

# ── Test 3: A single VLS column (A300, minimal VLS) ──
print("\n[3] Single VLS A300")
df = pl.DataFrame({"text": ["A" * 250, "B" * 200]})
meta = am.SpssMetadata(variable_formats={"text": "A300"})
write_pair(df, meta, "diag_vls_a300")

# ── Test 4: A single VLS column (A500, same as crashing file) ──
print("\n[4] Single VLS A500 (matches crashing file params)")
rows = ["X" * 400 + str(i) for i in range(5)]
df = pl.DataFrame({"text": rows})
meta = am.SpssMetadata(variable_formats={"text": "A500"})
write_pair(df, meta, "diag_vls_a500")

# ── Test 5: VLS A500 with more rows (to create bigger compressed block) ──
print("\n[5] VLS A500 with 100 rows")
rows = ["X" * 400 + f"{i:04d}" for i in range(100)]
df = pl.DataFrame({"text": rows})
meta = am.SpssMetadata(variable_formats={"text": "A500"})
write_pair(df, meta, "diag_vls_a500_100rows")

# ── Test 6: Mixed numeric + VLS ──
print("\n[6] Mixed: numeric + A500 VLS")
df = pl.DataFrame({
    "id": list(range(1, 6)),
    "text": ["X" * 400 + str(i) for i in range(5)],
    "score": [3.14, 2.72, 1.41, 0.0, -1.5],
})
meta = am.SpssMetadata(
    variable_formats={"id": "F8.0", "text": "A500", "score": "F8.2"}
)
write_pair(df, meta, "diag_mixed_vls")

# ── Test 7: Large VLS A2000 ──
print("\n[7] VLS A2000")
df = pl.DataFrame({"text": ["P" * 1900, "Q" * 1000]})
meta = am.SpssMetadata(variable_formats={"text": "A2000"})
write_pair(df, meta, "diag_vls_a2000")

print("\n" + "=" * 60)
print("All files written to:", OUT)
print("=" * 60)
print()
print("SPSS TEST PLAN:")
print("-" * 60)
print("Open each .sav AND .zsav pair in SPSS.")
print("Report which files crash/freeze vs open OK.")
print()
print("This tells us if the bug is:")
print("  [1-2] both crash  -> ALL ambers zsav files broken")
print("  [1-2] OK, [3+] crash -> VLS + zsav interaction bug")
print("  [1-4] OK, [5+] crash -> size/row-count specific issue")
print("  Only [4] crashes -> exact reproduction of original bug")
