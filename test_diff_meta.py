"""Compare ambers vs pyreadstat metadata for regression testing.

Usage:
    python test_diff_meta.py [path_to_sav_file]

Compares all comparable metadata fields between the two libraries and reports
PASS/FAIL for each. Exit code 0 if all pass, 1 if any diffs.
"""

import sys
import io

# Force UTF-8 output on Windows consoles
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
from collections.abc import Mapping, Sequence

import ambers
import pyreadstat


DEFAULT_FILE = (
    r"C:\Users\lipov\SynologyDrive\_PMI\Multi-Wave\RPM\2025\Data"
    r"\rpm_2025_data_tracking_partial_uam_2026_02_16.sav"
)


# ---------------------------------------------------------------------------
# deep_diff utility
# ---------------------------------------------------------------------------

def deep_diff(a, b, path=""):
    """Recursively diff two nested structures (dicts, lists, scalars)."""
    diffs = []

    if isinstance(a, Mapping) and isinstance(b, Mapping):
        a_keys, b_keys = set(a.keys()), set(b.keys())
        for k in sorted(a_keys - b_keys):
            diffs.append((f"{path}.{k}" if path else str(k), "removed", a[k], None))
        for k in sorted(b_keys - a_keys):
            diffs.append((f"{path}.{k}" if path else str(k), "added", None, b[k]))
        for k in sorted(a_keys & b_keys):
            p = f"{path}.{k}" if path else str(k)
            diffs.extend(deep_diff(a[k], b[k], p))
        return diffs

    if (
        isinstance(a, Sequence) and isinstance(b, Sequence)
        and not isinstance(a, (str, bytes))
        and not isinstance(b, (str, bytes))
    ):
        n = min(len(a), len(b))
        for i in range(n):
            diffs.extend(deep_diff(a[i], b[i], f"{path}[{i}]"))
        for i in range(n, len(a)):
            diffs.append((f"{path}[{i}]", "removed", a[i], None))
        for i in range(n, len(b)):
            diffs.append((f"{path}[{i}]", "added", None, b[i]))
        return diffs

    if a != b:
        diffs.append((path, "changed", a, b))
    return diffs


# ---------------------------------------------------------------------------
# Individual field comparisons
# ---------------------------------------------------------------------------

def compare_dict(label, pyreadstat_val, ambers_val):
    """Deep-diff two dicts and return (passed, n_diffs, sample_diffs)."""
    diffs = deep_diff(pyreadstat_val, ambers_val)
    return len(diffs) == 0, len(diffs), diffs[:5]


def compare_list(label, pyreadstat_val, ambers_val):
    """Compare two lists for equality."""
    if pyreadstat_val == ambers_val:
        return True, 0, []
    diffs = deep_diff(pyreadstat_val, ambers_val)
    return False, len(diffs), diffs[:5]


def compare_key_set(label, pyreadstat_val, ambers_val):
    """Compare only the key sets of two dicts."""
    p_keys = set(pyreadstat_val.keys())
    a_keys = set(ambers_val.keys())
    removed = sorted(p_keys - a_keys)
    added = sorted(a_keys - p_keys)
    diffs = []
    for k in removed:
        diffs.append((k, "in pyreadstat only", None, None))
    for k in added:
        diffs.append((k, "in ambers only", None, None))
    return len(diffs) == 0, len(diffs), diffs[:5]


def compare_scalar(label, pyreadstat_val, ambers_val, normalize=None):
    """Compare two scalar values, with optional normalization."""
    p = normalize(pyreadstat_val) if normalize else pyreadstat_val
    a = normalize(ambers_val) if normalize else ambers_val
    if p == a:
        return True, 0, []
    return False, 1, [(label, "changed", pyreadstat_val, ambers_val)]


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

def normalize_file_label(val):
    """pyreadstat returns None for empty, ambers returns empty string."""
    if val is None:
        return ""
    return val


def normalize_file_format(val):
    """pyreadstat returns 'sav/zsav', ambers returns 'sav' or 'zsav'."""
    if val == "sav/zsav":
        return "sav"
    return val


# ---------------------------------------------------------------------------
# Main comparison
# ---------------------------------------------------------------------------

def run_all(file_path):
    """Run all metadata comparisons and print results."""
    print(f"File: {file_path}")
    print(f"{'=' * 80}\n")

    print("Loading with ambers...", end=" ", flush=True)
    _, am = ambers.read_sav(file_path)
    print(f"OK ({am.number_columns} cols, {am.number_rows} rows)")

    print("Loading with pyreadstat...", end=" ", flush=True)
    _, pr = pyreadstat.read_sav(file_path)
    print(f"OK ({pr.number_columns} cols, {pr.number_rows} rows)")
    print()

    results = []

    # --- Dict fields (deep_diff) ---
    dict_fields = [
        ("column_names_to_labels",  "variable_labels",
         pr.column_names_to_labels, am.variable_labels),
        ("variable_value_labels",   "variable_value_labels",
         pr.variable_value_labels,  am.variable_value_labels),
        ("original_variable_types", "spss_variable_types",
         pr.original_variable_types, am.spss_variable_types),
        ("variable_measure",        "variable_measure",
         pr.variable_measure,       am.variable_measure),
        ("variable_alignment",      "variable_alignment",
         pr.variable_alignment,     am.variable_alignment),
        ("variable_storage_width",  "variable_storage_width",
         pr.variable_storage_width, am.variable_storage_width),
        ("variable_display_width",  "variable_display_width",
         pr.variable_display_width, am.variable_display_width),
    ]

    for pr_name, am_name, pr_val, am_val in dict_fields:
        passed, n, sample = compare_dict(am_name, pr_val, am_val)
        results.append((f"{pr_name} vs {am_name}", passed, n, sample))

    # --- List fields ---
    passed, n, sample = compare_list(
        "column_names", pr.column_names, am.variable_names
    )
    results.append(("column_names vs variable_names", passed, n, sample))

    passed, n, sample = compare_list("notes", pr.notes, am.notes)
    results.append(("notes vs notes", passed, n, sample))

    # --- Key-set-only fields ---
    passed, n, sample = compare_key_set(
        "missing_ranges", pr.missing_ranges, am.variable_missing
    )
    results.append(("missing_ranges vs variable_missing (keys only)", passed, n, sample))

    passed, n, sample = compare_key_set(
        "mr_sets", pr.mr_sets, am.mr_sets
    )
    results.append(("mr_sets vs mr_sets (keys only)", passed, n, sample))

    # --- Scalar fields ---
    passed, n, sample = compare_scalar(
        "file_label", pr.file_label, am.file_label, normalize_file_label
    )
    results.append(("file_label", passed, n, sample))

    passed, n, sample = compare_scalar(
        "file_encoding", pr.file_encoding, am.file_encoding
    )
    results.append(("file_encoding", passed, n, sample))

    passed, n, sample = compare_scalar(
        "number_rows", pr.number_rows, am.number_rows
    )
    results.append(("number_rows", passed, n, sample))

    passed, n, sample = compare_scalar(
        "number_columns", pr.number_columns, am.number_columns
    )
    results.append(("number_columns", passed, n, sample))

    passed, n, sample = compare_scalar(
        "file_format", pr.file_format, am.file_format, normalize_file_format
    )
    results.append(("file_format", passed, n, sample))

    # --- Skipped fields (report only) ---
    skipped = [
        ("creation_time", pr.creation_time, am.creation_time),
        ("modification_time", pr.modification_time, am.modification_time),
    ]

    # --- Print results ---
    print(f"{'Field':<55} {'Result':>8}  {'Diffs':>6}")
    print("-" * 80)

    n_pass = n_fail = 0
    for name, passed, n_diffs, sample in results:
        status = "PASS" if passed else "FAIL"
        if passed:
            n_pass += 1
        else:
            n_fail += 1
        print(f"  {name:<53} {status:>8}  {n_diffs:>6}")
        if not passed:
            for d in sample:
                print(f"    {d}")
            if n_diffs > len(sample):
                print(f"    ... and {n_diffs - len(sample)} more")

    print("-" * 80)
    for name, pr_val, am_val in skipped:
        print(f"  {name:<53} {'SKIP':>8}  pyreadstat={pr_val}  ambers={am_val}")

    print("-" * 80)

    # --- Not comparable (info only) ---
    print(f"\n  Not comparable by design:")
    print(f"    readstat_variable_types vs rust_variable_types (different type systems)")
    print(f"    compression (pyreadstat does not expose separately)")

    # --- Summary ---
    total = n_pass + n_fail
    print(f"\n{'=' * 80}")
    print(f"  {n_pass}/{total} PASSED, {n_fail}/{total} FAILED")
    if n_fail == 0:
        print("  ALL METADATA FIELDS MATCH!")
    print(f"{'=' * 80}")

    return n_fail == 0


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_FILE
    ok = run_all(path)
    sys.exit(0 if ok else 1)
