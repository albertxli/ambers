"""Apply SPSS user-defined missing value specs to DataFrames."""

from __future__ import annotations

import polars as pl


def apply_missing(
    df: pl.DataFrame | pl.LazyFrame,
    meta,
    *,
    columns: list[str] | None = None,
) -> pl.DataFrame | pl.LazyFrame:
    """Replace SPSS user-defined missing value codes with null.

    Converts values that match the missing value specifications in
    ``meta.variable_missing_values`` to null. This handles discrete
    values, ranges, and range-plus-discrete combinations as defined
    in the SPSS file.

    **Scope:** User-defined missing values only (discrete, range,
    range+discrete). SPSS system missing (SYSMIS) is already read
    as null by the reader and is not affected by this function.

    Args:
        df: A Polars DataFrame or LazyFrame with raw SPSS data.
        meta: An ``SpssMetadata`` object with missing value definitions.
        columns: Columns to apply missing values to. ``None`` applies
            to all columns that have missing value specs in metadata.
            Columns not found in the DataFrame or without specs are
            silently skipped.

    Returns:
        DataFrame or LazyFrame (same type as input) with missing value
        codes replaced by null. Existing nulls are preserved.
    """
    if not isinstance(df, (pl.DataFrame, pl.LazyFrame)):
        raise TypeError(
            f"df must be a polars DataFrame or LazyFrame, got {type(df).__name__}"
        )

    missing_specs = meta.variable_missing_values
    if not missing_specs:
        return df

    # Determine which columns to process
    schema = df.collect_schema() if isinstance(df, pl.LazyFrame) else df.schema
    df_columns = set(schema.names())

    if columns is not None:
        target_cols = [c for c in columns if c in df_columns and c in missing_specs]
    else:
        target_cols = [c for c in missing_specs if c in df_columns]

    if not target_cols:
        return df

    # Build one expression per column
    exprs = []
    for name in target_cols:
        spec = missing_specs[name]
        condition = _build_condition(name, spec)
        if condition is not None:
            exprs.append(
                pl.when(condition)
                .then(pl.lit(None))
                .otherwise(pl.col(name))
                .alias(name)
            )

    if not exprs:
        return df

    return df.with_columns(exprs)


def _build_condition(name: str, spec: dict) -> pl.Expr | None:
    """Build a Polars boolean expression for a missing value spec."""
    spec_type = spec.get("type")
    col = pl.col(name)

    if spec_type == "discrete":
        values = spec.get("values", [])
        if not values:
            return None
        return col.is_in(values)

    elif spec_type == "range":
        lo = spec["low"]
        hi = spec["high"]
        condition = col.is_between(lo, hi, closed="both")
        # Range + discrete combination
        discrete = spec.get("discrete")
        if discrete is not None:
            condition = condition | (col == discrete)
        return condition

    return None
