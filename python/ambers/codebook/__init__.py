"""Codebook generation for SPSS data.

Produces a Polars DataFrame documenting every variable and its values.

Two views, named after the granularity of each row:

- **variables** (default): one row per variable. Columns:
  ``variable``, ``variable_label``, ``variable_type``, ``values``,
  ``n_valid``, ``n_missing``, ``n_total``, ``n_unique``,
  ``n_labeled``, ``n_unlabeled``.

  The ``values`` column has two formats controlled by ``values_format=``:

  - ``"string"`` (default): newline-separated ``"1=Low\\n2=Medium\\n5=High"``,
    renders cleanly in marimo HTML and Excel.
  - ``"struct"``: ``List[Struct{value_code, value_label}]``, native dtypes,
    explodable + unnestable for programmatic work.

- **values**: one row per (variable, value) for categorical variables,
  one summary row for numeric/text/date. Columns:
  ``variable``, ``variable_label``, ``variable_type``,
  ``value_code``, ``value_label``, ``value_n``,
  ``n_valid``, ``pct_valid``, ``n_total``, ``pct_total``.
"""

from __future__ import annotations

import polars as pl

MISSING_LABEL = "MISSING"


def codebook(
    df: pl.DataFrame | pl.LazyFrame,
    meta,
    *,
    view: str = "variables",
    columns: list[str] | None = None,
    exclude: list[str] | None = None,
    include_meta: bool = False,
    values_format: str = "string",
) -> pl.DataFrame:
    """Generate a codebook from data and metadata.

    Args:
        df: A Polars DataFrame or LazyFrame.
        meta: An ``SpssMetadata`` object.
        view: ``"variables"`` (default, one row per variable) or
            ``"values"`` (one row per value).
        columns: Columns to include. ``None`` includes all.
            Can be combined with ``exclude``.
        exclude: Columns to skip. Applied after ``columns``.
        include_meta: If True, add ``variable_measure`` and
            ``variable_format`` columns to the values view.
        values_format: ``view="variables"`` only — ignored in the values
            view (and passing a non-default value with ``view="values"``
            raises ``ValueError`` to flag the misuse). ``"string"``
            (default) emits ``values`` as newline-separated
            ``"1=Low\\n2=Medium"``. ``"struct"`` emits
            ``List[Struct{value_code, value_label}]`` for
            ``.explode().unnest()`` workflows.

    Returns:
        A Polars DataFrame with the codebook.

        **Variables view columns:**
        ``variable``, ``variable_label``, ``variable_type``, ``values``,
        ``n_valid``, ``n_missing``, ``n_total``, ``n_unique``,
        ``n_labeled``, ``n_unlabeled``.

        **Values view columns:**
        ``variable``, ``variable_label``, ``variable_type``,
        ``value_code``, ``value_label``, ``value_n``,
        ``n_valid``, ``pct_valid``, ``n_total``, ``pct_total``.
    """
    from ._engine import build_rows
    from ._types import detect_types

    if not isinstance(df, (pl.DataFrame, pl.LazyFrame)):
        raise TypeError(
            f"df must be a polars DataFrame or LazyFrame, got {type(df).__name__}"
        )

    if view not in ("variables", "values"):
        raise ValueError(
            f"view must be 'variables' or 'values', got {view!r}"
        )

    if values_format not in ("string", "struct"):
        raise ValueError(
            f"values_format must be 'string' or 'struct', got {values_format!r}"
        )

    if view == "values" and values_format != "string":
        raise ValueError(
            "values_format only applies to view='variables'; "
            "omit values_format when using view='values'"
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
        if view == "values":
            return _empty_detail(include_meta)
        return _empty_summary(values_format)

    # Phase 1: type detection
    type_map = detect_types(df.select(target), meta)

    # Phase 2: build raw rows
    raw = build_rows(df, meta, type_map, target)

    # Phase 3: computed columns
    detail = _compute_detail(raw)

    if view == "variables":
        return _build_summary(detail, values_format)

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


def _build_summary(detail: pl.DataFrame, values_format: str = "string") -> pl.DataFrame:
    """Aggregate detail view into one row per variable.

    Uses single group_by().agg() instead of per-variable Python loop.
    """
    if detail.height == 0:
        return _empty_summary(values_format)

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

    # Format the values column for human-readable rendering when requested.
    # The struct list was filtered to has_code_and_label upstream, so empty
    # lists here mean "no labeled values" (e.g. non-categorical) — the
    # ~is_cat masking below converts those to nulls.
    if values_format == "string":
        # Cast value_code to string and strip a trailing ".0" so Float64
        # codes (1.0) render as "1" while genuine fractional codes (1.5) are
        # preserved.
        formatted_pair = pl.format(
            "{}={}",
            pl.element().struct.field("value_code")
                .cast(pl.String)
                .str.replace(r"\.0$", ""),
            pl.element().struct.field("value_label"),
        )
        summary = summary.with_columns(
            pl.col("values")
            .list.eval(formatted_pair)
            .list.join("\n")
            .alias("values")
        )

    # For non-categorical: set n_labeled, n_unlabeled, values to null
    # Also fix n_unique for non-categorical (should be value_n from summary row, not 0)
    is_cat = pl.col("variable_type") == "categorical"
    values_null = pl.lit(None, dtype=pl.String) if values_format == "string" else pl.lit(None)
    summary = summary.with_columns(
        pl.when(~is_cat).then(pl.lit(None, dtype=pl.UInt32)).otherwise(pl.col("n_labeled")).alias("n_labeled"),
        pl.when(~is_cat).then(pl.lit(None, dtype=pl.UInt32)).otherwise(pl.col("n_unlabeled")).alias("n_unlabeled"),
        pl.when(~is_cat).then(values_null).otherwise(pl.col("values")).alias("values"),
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
        "variable", "variable_label", "variable_type", "values",
        "n_valid", "n_missing", "n_total",
        "n_unique", "n_labeled", "n_unlabeled",
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


def _empty_summary(values_format: str = "string") -> pl.DataFrame:
    if values_format == "string":
        values_dtype = pl.String
    else:
        values_dtype = pl.List(pl.Struct({"value_code": pl.Int64, "value_label": pl.String}))
    return pl.DataFrame(schema={
        "variable": pl.String, "variable_label": pl.String,
        "variable_type": pl.String,
        "values": values_dtype,
        "n_valid": pl.UInt32, "n_missing": pl.UInt32,
        "n_total": pl.UInt32, "n_unique": pl.UInt32,
        "n_labeled": pl.UInt32, "n_unlabeled": pl.UInt32,
    })
