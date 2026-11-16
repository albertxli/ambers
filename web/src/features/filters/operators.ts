// How the filter bar names and draws each condition. Which conditions a
// variable offers, and which rows a condition keeps, is decided by the
// server (ambers-dataset); this file only holds words and symbols.

import type { FilterKind, FilterOperator, KindOperators } from "../../api/client";

export type OperatorGroup = "Comparison" | "Text matching" | "Empty values";

export interface OperatorText {
  label: string;
  /** What a chip shows between the variable and the value. */
  symbol: string;
  group: OperatorGroup;
  /** Whether the condition is incomplete without a value. */
  needsValue: boolean;
  /** The value must be a number. */
  numeric: boolean;
}

const text = (
  label: string,
  symbol: string,
  group: OperatorGroup,
  extra: Partial<OperatorText> = {},
): OperatorText => ({ label, symbol, group, needsValue: true, numeric: false, ...extra });

// Typed by the generated `FilterOperator`: an operator added on the server
// does not compile here until it has its words.
export const OPERATORS: Record<FilterOperator, OperatorText> = {
  equals: text("Equals", "=", "Comparison"),
  not_equals: text("Not equal", "≠", "Comparison"),
  greater_than: text("Greater than", ">", "Comparison", { numeric: true }),
  less_than: text("Less than", "<", "Comparison", { numeric: true }),
  greater_or_equal: text("Greater or equal", "≥", "Comparison", { numeric: true }),
  less_or_equal: text("Less or equal", "≤", "Comparison", { numeric: true }),
  contains: text("Contains", "contains", "Text matching"),
  not_contains: text("Does not contain", "not contains", "Text matching"),
  starts_with: text("Starts with", "starts with", "Text matching"),
  ends_with: text("Ends with", "ends with", "Text matching"),
  like: text("Like", "LIKE", "Text matching"),
  ilike: text("iLike (ignore case)", "ILIKE", "Text matching"),
  is_empty: text("Is empty", "is empty", "Empty values", { needsValue: false }),
  is_not_empty: text("Is not empty", "is not empty", "Empty values", { needsValue: false }),
};

/** The short tag that says what a variable is to a filter. */
export const KIND_TAGS: Record<FilterKind, string> = {
  number: "123",
  text: "ABC",
  categorical: "CAT",
  date: "DATE",
};

/** The conditions a variable of `kind` offers, in menu order. */
export function operatorsFor(kind: FilterKind, offered: readonly KindOperators[]): FilterOperator[] {
  return offered.find((one) => one.kind === kind)?.operators ?? [];
}

/** Whether `value` reads as a number. */
function isNumber(value: string): boolean {
  return value.trim() !== "" && Number.isFinite(Number(value));
}

/** What is wrong with a condition, or `null` when it can be applied. */
export function validate(kind: FilterKind, operator: FilterOperator, value: string): string | null {
  const how = OPERATORS[operator];
  if (!how.needsValue) return null;
  if (!value.trim()) return "Enter a value to continue.";
  // A number without labels can only be compared with a number.
  const numeric = how.numeric || (kind === "number" && how.group === "Comparison");
  if (numeric && !isNumber(value)) return "Enter a valid number.";
  return null;
}

/** What to say under the value box while nothing is wrong. */
export function valueHelp(kind: FilterKind, operator: FilterOperator): string {
  if (operator === "like" || operator === "ilike") {
    return "% matches any text; _ matches one character.";
  }
  if (OPERATORS[operator].numeric || kind === "number") {
    return "Use a numeric value, including decimals or negatives.";
  }
  if (kind === "categorical") return "A label or a code; text matching is case-insensitive.";
  return "Text matching is case-insensitive.";
}
