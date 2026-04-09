"""Value counting engine for codebook generation.

Polars-native vectorized implementation. Produces raw codebook rows:
one row per (variable, value) for categorical, one summary row for
numeric/text/date, plus MISSING rows for nulls.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from . import MISSING_LABEL


# ---------------------------------------------------------------------------
# Merge metadata labels with actual data values (per-column)
# ---------------------------------------------------------------------------

def merge_meta_and_data_values(
    meta_values: dict[Any, str],
    actual_value_counts: dict[Any, int],
) -> list[tuple[Any, str | None, int, bool]]:
    """Merge metadata value labels with actual value counts from data.

    Returns sorted list of (code, label, count, is_unlabeled) tuples.
    Handles type mismatches (int meta keys vs float data keys).
    """
    meta_keys = set(meta_values.keys())
    actual_keys = set(actual_value_counts.keys())

    should_normalize = False
    normalization_map: dict[Any, Any] = {}

    if meta_keys and actual_keys:
        meta_all_numeric = all(isinstance(k, (int, float)) for k in meta_keys)
        actual_all_numeric = all(isinstance(k, (int, float)) for k in actual_keys)
        actual_all_strings = all(isinstance(k, str) for k in actual_keys)

        if meta_all_numeric and actual_all_strings:
            temp_map = {}
            can_normalize = True
            for key in actual_keys:
                try:
                    float_val = float(key)
                    normalized = int(float_val) if float_val.is_integer() else float_val
                    temp_map[key] = normalized
                except (ValueError, TypeError):
                    can_normalize = False
                    break
            if can_normalize:
                if set(temp_map.values()) & meta_keys:
                    should_normalize = True
                    normalization_map = temp_map

        elif meta_all_numeric and actual_all_numeric:
            if meta_keys != actual_keys:
                temp_map = {}
                for key in actual_keys:
                    if isinstance(key, float) and key == int(key):
                        temp_map[key] = int(key)
                    else:
                        temp_map[key] = key
                if set(temp_map.values()) & meta_keys:
                    should_normalize = True
                    normalization_map = temp_map

    if should_normalize:
        normalized_actual: dict[Any, int] = {}
        for key, count in actual_value_counts.items():
            normalized_key = normalization_map.get(key, key)
            if normalized_key in normalized_actual:
                normalized_actual[normalized_key] += count
            else:
                normalized_actual[normalized_key] = count
    else:
        normalized_actual = actual_value_counts

    all_codes: set[Any] = set(meta_values.keys()) | set(normalized_actual.keys())

    try:
        sorted_codes = sorted(all_codes)
    except TypeError:
        sorted_codes = sorted(all_codes, key=lambda x: (type(x).__name__, str(x)))

    result: list[tuple[Any, str | None, int, bool]] = []
    for code in sorted_codes:
        label: str | None = meta_values.get(code, None)
        count: int = normalized_actual.get(code, 0)
        is_unlabeled: bool = code not in meta_values
        result.append((code, label, count, is_unlabeled))

    return result


# ---------------------------------------------------------------------------
# Build raw codebook rows — vectorized
# ---------------------------------------------------------------------------

def build_rows(
    df: pl.DataFrame,
    meta,
    type_map: dict[str, str],
    target_columns: list[str],
) -> pl.DataFrame:
    """Build raw codebook rows using vectorized Polars operations.

    Returns DataFrame with columns: variable, variable_label,
    variable_type, variable_measure, variable_format,
    value_code (dynamic dtype), value_label, value_n.
    """
    var_labels: dict[str, str] = meta.variable_labels if meta.variable_labels else {}
    value_labels_all: dict = meta.variable_value_labels if meta.variable_value_labels else {}
    measures: dict[str, str] = meta.variable_measures if meta.variable_measures else {}
    formats: dict[str, str] = meta.variable_formats if meta.variable_formats else {}
    schema = df.schema

    # Separate columns by type
    cat_cols_set = set()
    cat_cols = []
    non_cat_cols = []
    for c in target_columns:
        if type_map[c] in ("single-select", "multi-select", "categorical"):
            cat_cols.append(c)
            cat_cols_set.add(c)
        else:
            non_cat_cols.append(c)

    parts: list[pl.DataFrame] = []

    # ---- Non-categorical: batch vectorized stats ----
    if non_cat_cols:
        string_cols = [c for c in non_cat_cols if schema[c] in (pl.String, pl.Utf8)]
        non_string_cols = [c for c in non_cat_cols if c not in set(string_cols)]

        col_stats: dict[str, tuple[int, int]] = {}

        # Batch null counts — single lazy().select().collect() for all non-cat columns
        all_exprs: list[pl.Expr] = [pl.len().alias("::len")]

        for c in non_string_cols:
            all_exprs.append(pl.col(c).null_count().alias(f"{c}::nc"))

        for c in string_cols:
            all_exprs.append(pl.col(c).null_count().alias(f"{c}::nc"))
            all_exprs.append(pl.col(c).drop_nulls().eq("").sum().alias(f"{c}::ec"))

        stats = df.lazy().select(all_exprs).collect().row(0, named=True)
        total_len = stats["::len"]

        for c in non_string_cols:
            nc = stats[f"{c}::nc"]
            col_stats[c] = (nc, total_len - nc)

        for c in string_cols:
            nc = stats[f"{c}::nc"] + stats[f"{c}::ec"]
            col_stats[c] = (nc, total_len - nc)

        # Build rows from stats (O(n_cols) Python, not O(n_rows))
        non_cat_rows: list[dict] = []
        for c in non_cat_cols:
            null_count, valid_count = col_stats[c]
            vl = var_labels.get(c, "") or ""
            vt = type_map[c]
            vm = measures.get(c) or "unknown"
            vf = formats.get(c) or "unknown"
            base = {
                "variable": c, "variable_label": vl,
                "variable_type": vt, "variable_measure": vm,
                "variable_format": vf,
            }

            # MISSING row first (if nulls exist)
            if null_count > 0:
                non_cat_rows.append({
                    **base, "value_code": None,
                    "value_label": MISSING_LABEL, "value_n": null_count,
                })
            # Summary row
            non_cat_rows.append({
                **base, "value_code": None,
                "value_label": None, "value_n": valid_count,
            })

        if non_cat_rows:
            parts.append(_rows_to_df(non_cat_rows))

    # ---- Categorical: per-column (label merging needs per-column logic) ----
    if cat_cols:
        cat_rows: list[dict] = []
        for c in cat_cols:
            vl = var_labels.get(c, "") or ""
            vt = type_map[c]
            vm = measures.get(c) or "unknown"
            vf = formats.get(c) or "unknown"
            base = {
                "variable": c, "variable_label": vl,
                "variable_type": vt, "variable_measure": vm,
                "variable_format": vf,
            }

            s = df[c]
            is_string = schema[c] in (pl.String, pl.Utf8)

            # Null count
            if is_string:
                null_count = int(s.null_count()) + int(s.drop_nulls().eq("").sum())
                vc_df = s.filter(s.is_not_null() & (s != "")).value_counts()
            else:
                null_count = int(s.null_count())
                vc_df = s.drop_nulls().value_counts()

            # Value counts as dict
            if vc_df.height > 0:
                cols = vc_df.columns
                values = vc_df[cols[0]].to_list()
                counts = vc_df[cols[1]].to_list() if len(cols) > 1 else [1] * len(values)
                vc_dict = dict(zip(values, counts))
            else:
                vc_dict = {}

            # MISSING row first
            if null_count > 0:
                cat_rows.append({
                    **base, "value_code": None,
                    "value_label": MISSING_LABEL, "value_n": null_count,
                })

            # Merge with meta labels
            meta_values = value_labels_all.get(c, {})
            merged = merge_meta_and_data_values(meta_values, vc_dict)
            for code, label, count, _is_unlabeled in merged:
                cat_rows.append({
                    **base, "value_code": code,
                    "value_label": label, "value_n": count,
                })

        if cat_rows:
            parts.append(_rows_to_df(cat_rows))

    if not parts:
        return _empty_engine_df()

    # Concat all parts
    combined = pl.concat(parts, how="diagonal_relaxed")

    # Sort by target_columns order (preserve input variable order)
    col_order = {c: i for i, c in enumerate(target_columns)}
    combined = combined.with_columns(
        pl.col("variable").replace_strict(col_order, return_dtype=pl.UInt32).alias("::order")
    ).sort("::order").drop("::order")

    return combined


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rows_to_df(rows: list[dict]) -> pl.DataFrame:
    """Convert list of row dicts to DataFrame with proper value_code dtype."""
    codes = [r["value_code"] for r in rows if r["value_code"] is not None]

    if codes:
        try:
            numeric = [float(v) for v in codes]
            if all(v.is_integer() for v in numeric):
                vc_dtype = pl.Int64
                for r in rows:
                    if r["value_code"] is not None:
                        r["value_code"] = int(float(r["value_code"]))
            else:
                vc_dtype = pl.Float64
                for r in rows:
                    if r["value_code"] is not None:
                        r["value_code"] = float(r["value_code"])
        except (ValueError, TypeError):
            vc_dtype = pl.Utf8
            for r in rows:
                if r["value_code"] is not None:
                    r["value_code"] = str(r["value_code"])
    else:
        vc_dtype = pl.Float64

    return pl.DataFrame(rows, schema={
        "variable": pl.String,
        "variable_label": pl.String,
        "variable_type": pl.String,
        "variable_measure": pl.String,
        "variable_format": pl.String,
        "value_code": vc_dtype,
        "value_label": pl.String,
        "value_n": pl.UInt32,
    })


def _empty_engine_df() -> pl.DataFrame:
    return pl.DataFrame(schema={
        "variable": pl.String, "variable_label": pl.String,
        "variable_type": pl.String, "variable_measure": pl.String,
        "variable_format": pl.String, "value_code": pl.Float64,
        "value_label": pl.String, "value_n": pl.UInt32,
    })
