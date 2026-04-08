"""Validate SPSS value label quality: unlabeled values, duplicate labels."""

from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl

# ---------------------------------------------------------------------------
# Repr constants
# ---------------------------------------------------------------------------

_REPR_MAX_ISSUES = 10
_REPR_MAX_BOX_WIDTH = 80


# ---------------------------------------------------------------------------
# Pure check helpers — no Polars dependency, no raises, return dict or None
# ---------------------------------------------------------------------------

def check_unlabeled_values(
    col_name: str,
    data_values: set,
    label_keys: set,
) -> dict | None:
    """Check if data contains values without labels.

    Args:
        col_name: Variable name (for context only).
        data_values: Set of unique non-null values from the data.
        label_keys: Set of keys from meta.variable_value_labels[col].

    Returns:
        Dict with unlabeled_values, unique_in_data, labeled_in_data,
        or None if all values are labeled.
    """
    unlabeled = data_values - label_keys
    if not unlabeled:
        return None
    labeled_in_data = len(data_values & label_keys)
    return {
        "unlabeled_values": sorted(unlabeled, key=lambda x: (type(x).__name__, x)),
        "unique_in_data": len(data_values),
        "labeled_in_data": labeled_in_data,
    }


def check_duplicate_labels(
    col_name: str,
    labels: dict,
) -> dict | None:
    """Check if multiple codes map to the same label string.

    Args:
        col_name: Variable name (for context only).
        labels: Value label dict {code: label_string}.

    Returns:
        Dict with duplicates mapping, or None if no duplicates.
    """
    seen: dict[str, list] = {}
    for code, label_str in labels.items():
        seen.setdefault(label_str, []).append(code)
    duplicates = {
        label_str: sorted(codes, key=lambda x: (type(x).__name__, x))
        for label_str, codes in seen.items()
        if len(codes) > 1
    }
    if not duplicates:
        return None
    return {"duplicates": duplicates}


# ---------------------------------------------------------------------------
# ValidationIssue + ValidationReport
# ---------------------------------------------------------------------------

@dataclass
class ValidationIssue:
    """A single validation finding."""

    severity: str
    """``"error"`` or ``"warning"``."""

    column: str
    """Variable name."""

    check: str
    """Check type: ``"unlabeled_values"`` or ``"duplicate_labels"``."""

    message: str
    """Human-readable summary (full, untruncated)."""

    details: dict
    """Structured data for programmatic access."""


@dataclass
class ValidationReport:
    """Result of ``validate(df, meta)``.

    Contains a list of issues found. Use ``is_valid`` to check if
    there are errors (warnings don't count). Use ``raise_if_invalid()``
    to raise in strict mode.
    """

    issues: list[ValidationIssue] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        """True if no errors (warnings are OK)."""
        return not any(i.severity == "error" for i in self.issues)

    @property
    def errors(self) -> list[ValidationIssue]:
        """Issues with severity='error'."""
        return [i for i in self.issues if i.severity == "error"]

    @property
    def warnings(self) -> list[ValidationIssue]:
        """Issues with severity='warning'."""
        return [i for i in self.issues if i.severity == "warning"]

    def raise_if_invalid(self) -> None:
        """Raise ValueError if any errors exist."""
        errs = self.errors
        if not errs:
            return
        parts = [f"validate: {len(errs)} error(s) found\n"]
        for e in errs:
            parts.append(f"  {e.column}: {e.message}")
        raise ValueError("\n".join(parts))

    def to_frame(self) -> pl.DataFrame:
        """Convert issues to a Polars DataFrame.

        Returns full untruncated messages. Schema: severity, column,
        check, message. Use ``report.issues`` for structured details.
        """
        if not self.issues:
            return pl.DataFrame(
                schema={"severity": pl.String, "column": pl.String,
                        "check": pl.String, "message": pl.String}
            )
        return pl.DataFrame([
            {
                "severity": i.severity,
                "column": i.column,
                "check": i.check,
                "message": i.message,
            }
            for i in self.issues
        ])

    def __repr__(self) -> str:
        n_err = len(self.errors)
        n_warn = len(self.warnings)

        if not self.issues:
            status = "VALID (0 errors, 0 warnings)"
        elif self.is_valid:
            status = f"VALID (0 errors, {n_warn} warning{'s' if n_warn != 1 else ''})"
        else:
            status = f"INVALID ({n_err} error{'s' if n_err != 1 else ''}, {n_warn} warning{'s' if n_warn != 1 else ''})"

        max_content = _REPR_MAX_BOX_WIDTH - 4  # "| " + content + " |"
        lines: list[str] = [f"Status   {status}"[:max_content]]

        if self.errors:
            lines.append("")
            lines.append("Errors")
            shown = self.errors[:_REPR_MAX_ISSUES]
            for e in shown:
                lines.append(f"  [x] {e.column}"[:max_content])
                lines.append(f"      {e.message}"[:max_content])
            if n_err > _REPR_MAX_ISSUES:
                lines.append("")
                lines.append(
                    f"  ... and {n_err - _REPR_MAX_ISSUES} more errors"
                )
                lines.append(
                    "  See report.to_frame() or report.errors"
                )

        if self.warnings:
            lines.append("")
            lines.append("Warnings")
            shown = self.warnings[:_REPR_MAX_ISSUES]
            for w in shown:
                lines.append(f"  [!] {w.column}"[:max_content])
                lines.append(f"      {w.message}"[:max_content])
            if n_warn > _REPR_MAX_ISSUES:
                lines.append("")
                lines.append(
                    f"  ... and {n_warn - _REPR_MAX_ISSUES} more warnings"
                )
                lines.append(
                    "  See report.to_frame() or report.warnings"
                )

        # Build box with width cap
        inner_w = min(max(len(line) for line in lines), max_content)
        prefix = "\u250c\u2500 ValidationReport "
        box_w = inner_w + 4
        header = prefix + "\u2500" * (box_w - len(prefix) - 1) + "\u2510"
        footer = "\u2514" + "\u2500" * (box_w - 2) + "\u2518"
        rows = [header]
        for line in lines:
            truncated = line[:inner_w]
            rows.append(f"\u2502 {truncated:<{inner_w}} \u2502")
        rows.append(footer)
        return "\n".join(rows)


# ---------------------------------------------------------------------------
# Message formatting helpers
# ---------------------------------------------------------------------------

def _build_unlabeled_message(result: dict) -> str:
    """Build message for unlabeled values check.

    <=5 unlabeled: show all values.
    >5 unlabeled: show first 5 + '... and N more'.
    """
    unlabeled = result["unlabeled_values"]
    n = len(unlabeled)
    if n <= 5:
        preview = ", ".join(_fmt_value(v) for v in unlabeled)
    else:
        preview = ", ".join(_fmt_value(v) for v in unlabeled[:5])
        preview += f", ... and {n - 5} more"
    return (
        f"{n} unlabeled value{'s' if n != 1 else ''}: "
        f"[{preview}] "
        f"({result['labeled_in_data']} of {result['unique_in_data']} "
        f"unique values labeled)"
    )


def _build_duplicate_message(result: dict) -> str:
    """Build message for duplicate labels check.

    <=3 duplicates: show all.
    >3 duplicates: short summary (count only) to avoid blowing up box width.
    Full details available via issue.details.
    """
    dupes = result["duplicates"]
    n = len(dupes)
    if n <= 3:
        parts = []
        for label_str, codes in dupes.items():
            codes_str = ", ".join(_fmt_value(c) for c in codes)
            parts.append(f"'{label_str}' (codes: {codes_str})")
        return (
            f"{n} duplicate label{'s' if n != 1 else ''}: "
            + ", ".join(parts)
        )
    else:
        return (
            f"{n} duplicate labels (see report.issues for details)"
        )


def _fmt_value(v) -> str:
    """Format a value for display: 3.0 -> '3', 2.5 -> '2.5'."""
    if isinstance(v, float) and v == int(v) and not (v != v):  # not NaN
        return str(int(v))
    return str(v)


# ---------------------------------------------------------------------------
# validate() — top-level function
# ---------------------------------------------------------------------------

def validate(
    df: pl.DataFrame | pl.LazyFrame,
    meta,
    *,
    columns: list[str] | None = None,
    exclude: list[str] | None = None,
) -> ValidationReport:
    """Validate SPSS value label quality.

    Checks numeric columns with value labels for:

    1. **Unlabeled values** (error) — data values without labels in metadata.
    2. **Duplicate labels** (warning) — multiple codes mapping to the same label.

    Only checks columns that have value labels in metadata. Columns
    without value labels (continuous numeric, string) are skipped.

    **Scope:** User-defined value labels only. Does not check missing
    value specs, measure levels, or column presence — those have too
    many legitimate edge cases.

    Args:
        df: A Polars DataFrame or LazyFrame.
        meta: An ``SpssMetadata`` object.
        columns: Columns to check. ``None`` checks all numeric columns
            with value labels. Can be combined with ``exclude``.
        exclude: Columns to skip. Applied after ``columns`` filtering.

    Returns:
        A ``ValidationReport`` with issues found. Use ``report.is_valid``
        to check, ``report.raise_if_invalid()`` for strict mode.
    """
    if not isinstance(df, (pl.DataFrame, pl.LazyFrame)):
        raise TypeError(
            f"df must be a polars DataFrame or LazyFrame, got {type(df).__name__}"
        )

    all_labels = meta.variable_value_labels
    if not all_labels:
        return ValidationReport()

    # Determine schema and target columns
    schema = df.collect_schema() if isinstance(df, pl.LazyFrame) else df.schema
    df_columns = set(schema.names())

    # Build target list: numeric columns with value labels
    if columns is not None:
        target = [c for c in columns if c in df_columns and c in all_labels]
    else:
        target = [c for c in all_labels if c in df_columns]

    if exclude is not None:
        exclude_set = set(exclude)
        target = [c for c in target if c not in exclude_set]

    # Filter to numeric columns only
    numeric_target = []
    for col_name in target:
        col_dtype = schema[col_name]
        if col_dtype in (pl.Float32, pl.Float64) or col_dtype.is_integer():
            numeric_target.append(col_name)

    if not numeric_target:
        return ValidationReport()

    # Collect unique values per column
    if isinstance(df, pl.LazyFrame):
        unique_data = {}
        for c in numeric_target:
            vals = df.select(pl.col(c).drop_nulls().unique()).collect()[c].to_list()
            unique_data[c] = set(vals)
    else:
        unique_data = {}
        for c in numeric_target:
            unique_data[c] = set(df[c].drop_nulls().unique().to_list())

    # Run checks
    issues: list[ValidationIssue] = []

    for col_name in numeric_target:
        labels = all_labels[col_name]
        if not labels:
            continue

        data_values = unique_data[col_name]
        label_keys = set(labels.keys())

        # Check 1: Unlabeled values
        result = check_unlabeled_values(col_name, data_values, label_keys)
        if result is not None:
            issues.append(ValidationIssue(
                severity="error",
                column=col_name,
                check="unlabeled_values",
                message=_build_unlabeled_message(result),
                details=result,
            ))

        # Check 2: Duplicate labels
        result = check_duplicate_labels(col_name, labels)
        if result is not None:
            issues.append(ValidationIssue(
                severity="warning",
                column=col_name,
                check="duplicate_labels",
                message=_build_duplicate_message(result),
                details=result,
            ))

    return ValidationReport(issues=issues)
