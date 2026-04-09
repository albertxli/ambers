"""Codebook generation for SPSS data.

Produces a Polars DataFrame documenting every variable and its values.

Two views:

- **Detail** (default): one row per (variable, value) for categorical
  variables, one summary row for numeric/text/date. Columns:
  ``variable``, ``variable_label``, ``variable_type``,
  ``value_code``, ``value_label``, ``value_n``,
  ``n_valid``, ``pct_valid``, ``n_total``, ``pct_total``.

- **Summary**: one row per variable. Columns:
  ``variable``, ``variable_label``, ``variable_type``,
  ``n_valid``, ``n_missing``, ``n_total``, ``n_unique``,
  ``n_labeled``, ``n_unlabeled``, ``values``
  (List[Struct{value_code, value_label}] — explodable + unnestable).
"""

from __future__ import annotations

import polars as pl

MISSING_LABEL = "MISSING"


def codebook(
    df: pl.DataFrame | pl.LazyFrame,
    meta,
    *,
    view: str = "detail",
    columns: list[str] | None = None,
    exclude: list[str] | None = None,
    include_meta: bool = False,
) -> pl.DataFrame:
    """Generate a codebook from data and metadata.

    Args:
        df: A Polars DataFrame or LazyFrame.
        meta: An ``SpssMetadata`` object.
        view: ``"detail"`` (one row per value) or ``"summary"``
            (one row per variable).
        columns: Columns to include. ``None`` includes all.
            Can be combined with ``exclude``.
        exclude: Columns to skip. Applied after ``columns``.
        include_meta: If True, add ``variable_measure`` and
            ``variable_format`` columns to detail view.

    Returns:
        A Polars DataFrame with the codebook.

        **Detail view columns:**
        ``variable``, ``variable_label``, ``variable_type``,
        ``value_code``, ``value_label``, ``value_n``,
        ``n_valid``, ``pct_valid``, ``n_total``, ``pct_total``.

        **Summary view columns:**
        ``variable``, ``variable_label``, ``variable_type``,
        ``n_valid``, ``n_missing``, ``n_total``, ``n_unique``,
        ``n_labeled``, ``n_unlabeled``, ``values``
        (List[Struct{value_code, value_label}]).
    """
    from ._engine import build_rows
    from ._types import detect_types

    if not isinstance(df, (pl.DataFrame, pl.LazyFrame)):
        raise TypeError(
            f"df must be a polars DataFrame or LazyFrame, got {type(df).__name__}"
        )

    # Collect LazyFrame
    if isinstance(df, pl.LazyFrame):
        df = df.collect()

    # Determine target columns
    target = list(df.columns) if columns is None else [c for c in columns if c in df.columns]
    if exclude is not None:
        exclude_set = set(exclude)
        target = [c for c in target if c not in exclude_set]

    if not target:
        return _empty_detail(include_meta) if view == "detail" else _empty_summary()

    # Phase 1: type detection
    type_map = detect_types(df.select(target), meta)

    # Phase 2: build raw rows
    raw = build_rows(df, meta, type_map, target)

    # Phase 3: computed columns
    detail = _compute_detail(raw)

    if view == "summary":
        return _build_summary(detail)

    # Select final columns for detail view
    cols = [
        "variable", "variable_label", "variable_type",
    ]
    if include_meta:
        cols += ["variable_measure", "variable_format"]
    cols += [
        "value_code", "value_label", "value_n",
        "n_valid", "pct_valid", "n_total", "pct_total",
    ]

    return detail.select([c for c in cols if c in detail.columns])


def _compute_detail(raw: pl.DataFrame) -> pl.DataFrame:
    """Add computed columns: n_valid, pct_valid, n_total, pct_total."""
    # Merge single-select/multi-select → categorical in output
    result = raw.with_columns(
        pl.when(pl.col("variable_type").is_in(["single-select", "multi-select"]))
        .then(pl.lit("categorical"))
        .otherwise(pl.col("variable_type"))
        .alias("variable_type")
    )

    # Internal flag: is this a MISSING row? (used for computation, not in output)
    is_missing = pl.col("value_label").eq(MISSING_LABEL).fill_null(False)

    # n_valid: sum of value_n for non-missing rows per variable
    # For MISSING rows: n_valid = its own value_n (ultrasav behavior)
    result = result.with_columns(
        pl.when(is_missing)
        .then(pl.col("value_n"))
        .otherwise(
            pl.col("value_n")
            .filter(~is_missing)
            .sum()
            .over("variable")
        )
        .cast(pl.UInt32)
        .alias("n_valid")
    )

    # n_total: sum of ALL value_n per variable
    result = result.with_columns(
        pl.col("value_n").sum().over("variable").cast(pl.UInt32).alias("n_total")
    )

    # pct_valid: value_n / n_valid (null for missing rows, null if n_valid=0)
    result = result.with_columns(
        pl.when(is_missing)
        .then(pl.lit(None, dtype=pl.Float64))
        .otherwise(pl.col("value_n").cast(pl.Float64) / pl.col("n_valid").cast(pl.Float64))
        .alias("pct_valid")
    ).with_columns(
        pl.when(pl.col("pct_valid").is_nan())
        .then(pl.lit(None, dtype=pl.Float64))
        .otherwise(pl.col("pct_valid"))
        .alias("pct_valid")
    )

    # pct_total: value_n / n_total (null if n_total=0)
    result = result.with_columns(
        (pl.col("value_n").cast(pl.Float64) / pl.col("n_total").cast(pl.Float64))
        .alias("pct_total")
    ).with_columns(
        pl.when(pl.col("pct_total").is_nan())
        .then(pl.lit(None, dtype=pl.Float64))
        .otherwise(pl.col("pct_total"))
        .alias("pct_total")
    )

    return result


def _build_summary(detail: pl.DataFrame) -> pl.DataFrame:
    """Aggregate detail view into one row per variable.

    Uses single group_by().agg() instead of per-variable Python loop.
    """
    if detail.height == 0:
        return _empty_summary()

    is_missing = pl.col("value_label").eq(MISSING_LABEL).fill_null(False)
    is_unlabeled = pl.col("value_code").is_not_null() & pl.col("value_label").is_null()
    has_code_and_label = (
        ~is_missing
        & pl.col("value_code").is_not_null()
        & pl.col("value_label").is_not_null()
    )

    summary = detail.group_by("variable", maintain_order=True).agg(
        pl.col("variable_label").first(),
        pl.col("variable_type").first(),

        # n_valid: first n_valid from non-missing rows (or 0)
        pl.col("n_valid").filter(~is_missing).first().fill_null(0).alias("n_valid"),

        # n_missing: value_n of the MISSING row (or 0)
        pl.col("value_n").filter(is_missing).first().fill_null(0).alias("n_missing"),

        # n_unique: count of rows with non-null value_code
        # (for categorical: includes meta-only labels with count=0)
        # (for non-categorical: equals value_n — preserved quirk)
        pl.col("value_code").drop_nulls().len().alias("n_unique"),

        # n_labeled: rows with value_code AND value_label (non-missing)
        pl.col("value_label").filter(has_code_and_label).len().alias("n_labeled"),

        # n_unlabeled: rows where unlabeled (value_code not null, value_label null)
        is_unlabeled.sum().cast(pl.UInt32).alias("n_unlabeled"),

        # values: List[Struct{value_code, value_label}] — native dtypes preserved
        pl.struct([
            pl.col("value_code"),
            pl.col("value_label"),
        ])
        .filter(has_code_and_label)
        .alias("values"),
    )

    # Add n_total = n_valid + n_missing
    summary = summary.with_columns(
        (pl.col("n_valid") + pl.col("n_missing")).cast(pl.UInt32).alias("n_total")
    )

    # For non-categorical: set n_labeled, n_unlabeled, value_labels to null
    # Also fix n_unique for non-categorical (should be value_n from summary row, not 0)
    is_cat = pl.col("variable_type") == "categorical"
    summary = summary.with_columns(
        pl.when(~is_cat).then(pl.lit(None, dtype=pl.UInt32)).otherwise(pl.col("n_labeled")).alias("n_labeled"),
        pl.when(~is_cat).then(pl.lit(None, dtype=pl.UInt32)).otherwise(pl.col("n_unlabeled")).alias("n_unlabeled"),
        pl.when(~is_cat).then(pl.lit(None)).otherwise(pl.col("values")).alias("values"),
        # Non-cat n_unique: should equal n_valid (preserved quirk from original)
        pl.when(~is_cat).then(pl.col("n_valid")).otherwise(pl.col("n_unique")).alias("n_unique"),
    )

    # Cast to proper dtypes
    summary = summary.with_columns(
        pl.col("n_valid").cast(pl.UInt32),
        pl.col("n_missing").cast(pl.UInt32),
        pl.col("n_unique").cast(pl.UInt32),
    )

    # Reorder columns
    return summary.select([
        "variable", "variable_label", "variable_type",
        "n_valid", "n_missing", "n_total",
        "n_unique", "n_labeled", "n_unlabeled", "values",
    ])


def _empty_detail(include_meta: bool = False) -> pl.DataFrame:
    schema = {
        "variable": pl.String, "variable_label": pl.String,
        "variable_type": pl.String,
    }
    if include_meta:
        schema["variable_measure"] = pl.String
        schema["variable_format"] = pl.String
    schema.update({
        "value_code": pl.Int64, "value_label": pl.String,
        "value_n": pl.UInt32, "n_valid": pl.UInt32,
        "pct_valid": pl.Float64, "n_total": pl.UInt32,
        "pct_total": pl.Float64,
    })
    return pl.DataFrame(schema=schema)


def _empty_summary() -> pl.DataFrame:
    return pl.DataFrame(schema={
        "variable": pl.String, "variable_label": pl.String,
        "variable_type": pl.String, "n_valid": pl.UInt32,
        "n_missing": pl.UInt32, "n_total": pl.UInt32,
        "n_unique": pl.UInt32, "n_labeled": pl.UInt32,
        "n_unlabeled": pl.UInt32,
        "values": pl.List(pl.Struct({"value_code": pl.Int64, "value_label": pl.String})),
    })
