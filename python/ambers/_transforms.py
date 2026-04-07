"""Transform functions for ambers DataFrames."""

from __future__ import annotations

import polars as pl

# output= mode mapping to internal (as_enum, unmapped) logic
_OUTPUT_MODES = {
    "enum": (True, "error"),
    "string": (False, "stringify"),
    "enum_null": (True, "null"),
}


def apply_labels(
    df: pl.DataFrame | pl.LazyFrame,
    meta,
    *,
    columns: list[str] | None = None,
    exclude: list[str] | None = None,
    output: str = "enum",
) -> pl.DataFrame | pl.LazyFrame:
    """Replace numeric/string codes with value labels from SPSS metadata.

    Converts categorical columns from raw codes (1.0, 2.0) to their string
    labels ("Male", "Female"). By default, produces Polars ``Enum`` columns
    that preserve SPSS value label ordering — crucial for Likert scales and
    survey analysis. Enum columns auto-cast to String on ``write_excel()``
    and ``write_csv()``.

    **Dtype-aware behavior:**

    - **Numeric columns** (Float64/Integer) with value labels are treated as
      categorical variables. The ``output`` mode controls dtype and unmapped
      value handling.
    - **String columns** with value labels receive partial label replacement.
      Unmapped text always passes through unchanged, since open-ended text
      responses are legitimate data.
    - Columns without value labels are skipped entirely.

    See `apply_labels.md <apply_labels.md>`_ for full documentation.

    Args:
        df: A Polars DataFrame or LazyFrame with raw SPSS codes.
        meta: An ``SpssMetadata`` object with value labels.
        columns: Columns to apply labels to. ``None`` applies to all
            columns that have value labels in the metadata. When specified
            explicitly, raises if columns are missing from data or have
            no value labels defined. Mutually exclusive with ``exclude``.
        exclude: Columns to skip. When set, all columns with value
            labels are processed except those listed here. Mutually
            exclusive with ``columns``.
        output: Output mode for labeled numeric columns.

            - ``"enum"`` (default) — ``pl.Enum`` with ordered categories.
              Raises if any non-null value has no label. Best for analysis.
            - ``"string"`` — ``pl.String``. Unmapped values become their
              string representation (``3.0`` → ``"3"``). Best for export.
            - ``"enum_null"`` — ``pl.Enum`` with ordered categories.
              Unmapped values become null. Best for analysis where you
              want to exclude unknowns.

    Returns:
        A DataFrame or LazyFrame (same type as input) with labeled columns.

    Raises:
        ValueError: If ``output`` is invalid, or if explicit ``columns``
            are missing or have no labels, or if ``output="enum"`` and
            unmapped values are found in the data.
        TypeError: If ``df`` is not a DataFrame or LazyFrame.

    Examples:
        >>> sav = am.read_sav("survey.sav")
        >>> df, meta = sav.data, sav.meta
        >>> labeled = am.apply_labels(df, meta)
        >>> labeled.write_excel("survey.xlsx")

        >>> # String output for quick export
        >>> labeled = am.apply_labels(df, meta, output="string")

        >>> # Enum with nulls for unmapped values
        >>> labeled = am.apply_labels(df, meta, output="enum_null")
    """
    if not isinstance(df, (pl.DataFrame, pl.LazyFrame)):
        raise TypeError(
            f"Expected DataFrame or LazyFrame, got {type(df).__name__}."
        )

    if output not in _OUTPUT_MODES:
        raise ValueError(
            f"output must be 'enum', 'string', or 'enum_null', "
            f"got {output!r}."
        )
    as_enum, unmapped = _OUTPUT_MODES[output]

    if columns is not None and exclude is not None:
        raise ValueError("columns and exclude are mutually exclusive")

    # Get schema + determine target columns
    schema = (
        df.collect_schema() if isinstance(df, pl.LazyFrame) else df.schema
    )
    data_cols = set(schema.names())
    all_labels = meta.variable_value_labels

    if columns is not None:
        missing = [c for c in columns if c not in data_cols]
        if missing:
            raise ValueError(f"Columns not in data: {missing}")
        no_labels = [c for c in columns if not all_labels.get(c)]
        if no_labels:
            raise ValueError(f"No value labels for: {no_labels}")
        target = columns
    else:
        target = [c for c in all_labels if c in data_cols]
        if exclude is not None:
            exclude_set = set(exclude)
            target = [c for c in target if c not in exclude_set]

    if not target:
        return df

    # Classify columns by dtype
    numeric_targets = []
    string_targets = []
    for col_name in target:
        labels = all_labels[col_name]
        if not labels:
            continue
        col_dtype = schema[col_name]
        if col_dtype in (pl.Float32, pl.Float64) or col_dtype.is_integer():
            numeric_targets.append(col_name)
        else:
            string_targets.append(col_name)

    if not numeric_targets and not string_targets:
        return df

    # Pre-check: unmapped values + duplicate labels on numeric columns
    if numeric_targets and (unmapped == "error" or as_enum):
        _validate_labels(df, all_labels, numeric_targets, unmapped, as_enum,
                         output)

    # Build expressions
    exprs = []
    for col_name in numeric_targets:
        labels = all_labels[col_name]
        c = pl.col(col_name)
        col_dtype = schema[col_name]

        if as_enum:
            categories = list(dict.fromkeys(labels.values()))
            return_dtype = pl.Enum(categories)
        else:
            return_dtype = pl.String

        if unmapped == "error":
            expr = c.replace_strict(labels, return_dtype=return_dtype)
        elif unmapped == "null":
            expr = c.replace_strict(
                labels,
                default=pl.lit(None, dtype=pl.String),
                return_dtype=return_dtype,
            )
        else:  # "stringify"
            if col_dtype in (pl.Float32, pl.Float64):
                default = (
                    pl.when(c == c.floor())
                    .then(c.cast(pl.Int64).cast(pl.String))
                    .otherwise(c.cast(pl.String))
                )
            else:
                default = c.cast(pl.String)
            expr = c.replace_strict(
                labels, default=default, return_dtype=return_dtype
            )
        exprs.append(expr.alias(col_name))

    for col_name in string_targets:
        labels = all_labels[col_name]
        c = pl.col(col_name)
        expr = c.replace_strict(labels, default=c, return_dtype=pl.String)
        exprs.append(expr.alias(col_name))

    if not exprs:
        return df

    # Single with_columns() — all expressions parallel (Rust)
    return df.with_columns(exprs)


def _validate_labels(
    df: pl.DataFrame | pl.LazyFrame,
    all_labels: dict,
    numeric_targets: list[str],
    unmapped: str,
    as_enum: bool,
    output: str,
) -> None:
    """Pre-check for unmapped values and duplicate labels on numeric columns.

    Raises ValueError with a structured diagnostic if issues are found.
    Only called when unmapped="error" or as_enum=True.
    """
    unmapped_issues: list[str] = []
    duplicate_issues: list[str] = []

    for col_name in numeric_targets:
        labels = all_labels[col_name]
        label_keys = set(labels.keys())

        # Check unmapped values
        if unmapped == "error":
            col_expr = pl.col(col_name).drop_nulls().unique()
            if isinstance(df, pl.LazyFrame):
                unique_vals = set(
                    df.select(col_expr).collect()[col_name].to_list()
                )
            else:
                unique_vals = set(df.select(col_expr)[col_name].to_list())
            n_unique = len(unique_vals)
            n_labeled = len(label_keys)
            missing = sorted(unique_vals - label_keys)
            if missing:
                vals_str = ", ".join(str(v) for v in missing[:10])
                if len(missing) > 10:
                    vals_str += f", ... ({len(missing)} total)"
                unmapped_issues.append(
                    f"  {col_name}: {len(missing)} unmapped "
                    f"{'value' if len(missing) == 1 else 'values'}: "
                    f"[{vals_str}]  "
                    f"({n_unique} unique, {n_labeled} labeled)"
                )

        # Check duplicate label values
        if as_enum:
            seen: dict[str, list] = {}
            for k, v in labels.items():
                seen.setdefault(v, []).append(k)
            dupes = {v: keys for v, keys in seen.items() if len(keys) > 1}
            if dupes:
                parts = []
                for label_val, keys in dupes.items():
                    keys_str = ", ".join(str(k) for k in keys)
                    parts.append(f"[{keys_str}] -> {label_val!r}")
                duplicate_issues.append(
                    f"  {col_name}: {len(dupes)} duplicate "
                    f"{'label' if len(dupes) == 1 else 'labels'}: "
                    + "; ".join(parts)
                )

    if not unmapped_issues and not duplicate_issues:
        return

    # Build structured error message
    msg_parts: list[str] = []

    if unmapped_issues:
        n = len(unmapped_issues)
        msg_parts.append(
            f"{n} {'column has' if n == 1 else 'columns have'} "
            f"values without labels (output={output!r})\n\n"
        )
        msg_parts.append("\n".join(unmapped_issues))
        msg_parts.append(
            "\n\nTo fix, either:\n"
            "  - Add missing labels: "
            "meta = meta.with_variable_value_labels({...})\n"
            '  - Use output="enum_null" to set unmapped values to null\n'
            '  - Use output="string" to keep unmapped as strings'
        )

    if duplicate_issues:
        if unmapped_issues:
            msg_parts.append("\n\n")
        n = len(duplicate_issues)
        msg_parts.append(
            f"{n} {'column has' if n == 1 else 'columns have'} "
            f"duplicate label values (multiple codes -> same label)\n\n"
        )
        msg_parts.append("\n".join(duplicate_issues))
        msg_parts.append(
            "\n\nWith Enum output, duplicate labels collapse distinct "
            "codes into the same category.\n"
            "If intentional, this is fine. "
            "If not, fix the labels in metadata."
        )

    raise ValueError("apply_labels: " + "".join(msg_parts))
