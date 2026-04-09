"""Variable type detection for codebook generation.

Two-phase approach ported from ultrasav's _detect_variable_type.py:
- Phase 1: Pure Polars dtype detection (always runs)
- Phase 2: Metadata refinement (when meta provided)

Returns one of 5 internal types:
- 'text'          : String data
- 'date'          : Date/Datetime/Duration
- 'numeric'       : Continuous numeric (no value labels)
- 'single-select' : Coded categorical with value labels
- 'multi-select'  : Binary indicator (0/1, from MR sets or heuristics)

The public codebook output merges single-select/multi-select → 'categorical'.
"""

from __future__ import annotations

import re
from typing import Any

import polars as pl


# ---------------------------------------------------------------------------
# Constants for multi-select detection
# ---------------------------------------------------------------------------

SELECTION_PAIRS = [
    ("not selected", "selected"),
    ("unchecked", "checked"),
    ("no", "yes"),
    ("0", "1"),
    ("not mentioned", "mentioned"),
    ("not chosen", "chosen"),
    ("exclude", "include"),
]

GENERIC_BINARY_LABELS = [
    ("no", "yes"),
    ("false", "true"),
    ("disagree", "agree"),
    ("male", "female"),
    ("off", "on"),
    ("absent", "present"),
]

MULTI_SELECT_NAME_PATTERNS = [
    r"[_\-]?\d+$",        # ends with number (Q1_1, Q1-2)
    r"Q\d+[A-Z]$",        # Q1A pattern
    r"r\d+$",             # r1 pattern
    r"_[A-Z]$",           # _A pattern
    r"[A-Z]\d+[A-Z]\d+$", # A1B1 pattern
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def create_mr_set_lookup(meta) -> set[str]:
    """Flatten meta.mr_sets into a set of all variable names in MR sets."""
    mr_vars: set[str] = set()
    mr_sets = meta.mr_sets
    if not mr_sets:
        return mr_vars
    for _set_name, info in mr_sets.items():
        if isinstance(info, dict) and "variables" in info:
            mr_vars.update(info["variables"])
    return mr_vars


def _normalize_value_keys(keys: set[Any]) -> set[Any]:
    """Normalize 0/1 variations (0, 0.0, '0', 1, 1.0, '1') to integers."""
    normalized: set[Any] = set()
    for k in keys:
        if isinstance(k, (int, float)) and k in (0, 1, 0.0, 1.0):
            normalized.add(int(k))
        elif isinstance(k, str) and k in ("0", "1"):
            normalized.add(int(k))
        else:
            normalized.add(k)
    return normalized


def _is_binary_value_dict(value_dict: dict[Any, str]) -> bool:
    """Check if value labels represent a 0/1 binary variable (exactly 2 keys)."""
    if len(value_dict) != 2:
        return False
    normalized = _normalize_value_keys(set(value_dict.keys()))
    return normalized <= {0, 1}


def _labels_lower_pair(value_dict: dict[Any, str]) -> tuple[str, str]:
    """Get lowercase labels for keys 0 and 1."""
    label_0 = str(
        value_dict.get(0, value_dict.get(0.0, value_dict.get("0", "")))
    ).lower().strip()
    label_1 = str(
        value_dict.get(1, value_dict.get(1.0, value_dict.get("1", "")))
    ).lower().strip()
    return label_0, label_1


def _is_generic_binary_labels(label_0: str, label_1: str) -> bool:
    """Check if labels match generic binary patterns like (no, yes)."""
    labels_set = {label_0.lower(), label_1.lower()}
    for pair in GENERIC_BINARY_LABELS:
        if labels_set == {p.lower() for p in pair}:
            return True
    return False


def _match_multi_name_pattern(var_name: str) -> bool:
    """Check if variable name matches multi-select naming patterns."""
    for pattern in MULTI_SELECT_NAME_PATTERNS:
        if re.search(pattern, var_name, re.IGNORECASE):
            return True
    return False


def _build_sibling_map(variable_names: list[str]) -> dict[str, list[str]]:
    """Precompute sibling groups for all variables.

    Returns dict mapping each variable to its siblings (same base prefix).
    Computed once, replaces per-variable _get_sibling_vars calls.
    """
    # Group by base prefix
    base_to_vars: dict[str, list[str]] = {}
    for var in variable_names:
        match = re.match(r"(.+?)([A-Z]|\d+)$", var, re.IGNORECASE)
        if match:
            base = match.group(1)
            base_to_vars.setdefault(base, []).append(var)

    # Build sibling map: each var → list of siblings (excluding self)
    sibling_map: dict[str, list[str]] = {}
    for base, vars_in_group in base_to_vars.items():
        if len(vars_in_group) >= 2:
            for v in vars_in_group:
                sibling_map[v] = [s for s in vars_in_group if s != v]

    return sibling_map


# ---------------------------------------------------------------------------
# Phase 1: Pure Polars dtype detection
# ---------------------------------------------------------------------------

def _detect_from_dtype(dtype: pl.DataType) -> str:
    """Classify column type from Polars dtype alone."""
    if dtype in (pl.String, pl.Utf8):
        return "text"
    if dtype in (pl.Date, pl.Datetime, pl.Duration):
        return "date"
    if dtype == pl.Categorical:
        return "categorical"
    if dtype == pl.Boolean:
        return "boolean"
    return "numeric"


# ---------------------------------------------------------------------------
# Phase 2: Metadata refinement
# ---------------------------------------------------------------------------

def _refine_with_metadata(
    var_name: str,
    phase1_type: str,
    meta,
    mr_set_variables: set[str],
    binary_info: dict[str, str],
    sibling_map: dict[str, list[str]],
) -> str:
    """Refine Phase 1 type using metadata. All 7 multi-select tiers.

    binary_info: pre-computed {col: "strong"|"weak"} from batch detection.
    sibling_map: pre-computed {col: [sibling1, sibling2, ...]} from _build_sibling_map.
    """
    value_labels_all = meta.variable_value_labels
    measures = meta.variable_measures
    formats = meta.variable_formats

    var_measure = measures.get(var_name, "unknown")
    var_format = formats.get(var_name, "")

    # --- Text: confirm ---
    if phase1_type == "text":
        return "text"

    # --- Date: confirm ---
    if phase1_type == "date":
        return "date"

    # --- Categorical (explicit dtype) ---
    if phase1_type == "categorical":
        if var_name in value_labels_all and value_labels_all[var_name]:
            return "single-select"
        return "categorical"

    # --- Boolean ---
    if phase1_type == "boolean":
        if var_name in mr_set_variables:
            return "multi-select"
        if var_name in value_labels_all and value_labels_all[var_name]:
            return "single-select"
        return "numeric"

    # --- Numeric: the complex path ---
    if phase1_type == "numeric":

        # TIER 1: SPSS Multi-Response Sets (definitive)
        if var_name in mr_set_variables:
            return "multi-select"

        # TIER 2: Binary pattern analysis with metadata gating
        pattern = binary_info.get(var_name)
        is_strong = (pattern == "strong")
        is_weak = (pattern == "weak")

        if is_strong or is_weak:
            metadata_confirms_01 = False
            series_confirms_01 = False

            if var_name in value_labels_all:
                keys = set(value_labels_all[var_name].keys())
                if _normalize_value_keys(keys) <= {0, 1}:
                    metadata_confirms_01 = True

            if not metadata_confirms_01:
                siblings = sibling_map.get(var_name, [])
                if len(siblings) >= 2:
                    count_01 = 0
                    for sib in siblings[:5]:
                        if sib in value_labels_all:
                            sib_keys = set(value_labels_all[sib].keys())
                            if _normalize_value_keys(sib_keys) <= {0, 1}:
                                count_01 += 1
                    if count_01 >= 2:
                        series_confirms_01 = True

            # Gating
            if is_strong:
                gated_ok = (
                    metadata_confirms_01
                    or series_confirms_01
                    or var_name not in value_labels_all
                )
            else:  # is_weak
                gated_ok = metadata_confirms_01 or series_confirms_01

            if gated_ok:
                # Guard: generic binary labels → fall through to single-select
                if (var_name in value_labels_all
                        and _is_binary_value_dict(value_labels_all[var_name])):
                    l0, l1 = _labels_lower_pair(value_labels_all[var_name])
                    if _is_generic_binary_labels(l0, l1):
                        pass  # Fall through to label tiers
                    else:
                        return "multi-select"
                else:
                    return "multi-select"

        # TIER 3-7: Value label analysis
        has_labels = (
            var_name in value_labels_all
            and bool(value_labels_all[var_name])
        )

        if has_labels:
            value_dict = value_labels_all[var_name]
            is_binary = _is_binary_value_dict(value_dict)

            if is_binary:
                label_0, label_1 = _labels_lower_pair(value_dict)

                # TIER 3: Descriptive label on 1
                if not label_0 or label_0 in ("null", "none", "not selected", ""):
                    if label_1 and label_1 not in ("yes", "selected", "true", "1"):
                        return "multi-select"

                # TIER 4: Selection pair + naming pattern
                labels_set = {label_0, label_1}
                for pair in SELECTION_PAIRS:
                    if labels_set == {p.lower() for p in pair}:
                        if _match_multi_name_pattern(var_name):
                            return "multi-select"

                # TIER 5: Binary sibling series
                siblings = sibling_map.get(var_name, [])
                if len(siblings) >= 2:
                    all_binary = True
                    for sib in siblings[:3]:
                        if sib in value_labels_all:
                            if not _is_binary_value_dict(value_labels_all[sib]):
                                all_binary = False
                                break
                    if all_binary:
                        return "multi-select"

                # TIER 6: Generic binary → single-select
                if _is_generic_binary_labels(label_0, label_1):
                    return "single-select"

            # TIER 7: Non-binary with labels → single-select
            return "single-select"

        # Measurement level fallback
        if var_measure == "scale":
            return "numeric"

        return "numeric"

    return phase1_type


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def detect_types(
    df: pl.DataFrame,
    meta,
) -> dict[str, str]:
    """Classify all columns in df using dtype + metadata refinement.

    Uses batch Polars lazy expressions for binary pattern detection
    instead of per-column unique() calls.
    """
    schema = df.schema
    mr_set_variables = create_mr_set_lookup(meta)
    variable_names = meta.variable_names if meta.variable_names else df.columns
    value_labels_all = meta.variable_value_labels or {}

    # Phase 1: classify all columns by dtype
    phase1 = {c: _detect_from_dtype(schema[c]) for c in df.columns}

    # Precompute sibling map (replaces per-column _get_sibling_vars)
    sibling_map = _build_sibling_map(variable_names)

    # Identify numeric columns that need binary pattern check
    numeric_cols = [c for c, t in phase1.items() if t == "numeric"]
    cols_skip: set[str] = set()
    for c in numeric_cols:
        if c in mr_set_variables:
            cols_skip.add(c)
        elif c in value_labels_all and value_labels_all[c]:
            keys = set(value_labels_all[c].keys())
            if len(keys) > 2 or not (_normalize_value_keys(keys) <= {0, 1}):
                cols_skip.add(c)

    cols_to_check = [c for c in numeric_cols if c not in cols_skip]

    # Batch binary detection — one lazy().select().collect() call
    binary_info: dict[str, str] = {}
    if cols_to_check:
        exprs = (
            [pl.col(c).drop_nulls().is_in([0.0, 1.0]).all().alias(f"{c}::bin")
             for c in cols_to_check]
            + [pl.col(c).drop_nulls().n_unique().alias(f"{c}::nu")
               for c in cols_to_check]
        )
        result = df.lazy().select(exprs).collect().row(0, named=True)

        for c in cols_to_check:
            is_binary = result[f"{c}::bin"]
            nu = result[f"{c}::nu"]
            if is_binary and nu > 0:
                binary_info[c] = "strong" if nu >= 2 else "weak"

    # Phase 2: refine with metadata (per-column, no data scanning)
    out: dict[str, str] = {}
    for c in df.columns:
        out[c] = _refine_with_metadata(
            var_name=c,
            phase1_type=phase1[c],
            meta=meta,
            mr_set_variables=mr_set_variables,
            binary_info=binary_info,
            sibling_map=sibling_map,
        )
    return out
