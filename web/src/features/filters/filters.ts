// The filter the page holds: a list of conditions and whether a row must
// pass all of them or any. It is sent to the server with every request
// for rows; the server decides which rows pass.

import type { Filter, FilterMode, FilterOperator, FilterProblem } from "../../api/client";
import { OPERATORS } from "./operators";

export interface FilterRule {
  /** Names the rule in the page; the server never sees it. */
  id: string;
  /** The variable's position in the file. */
  variable: number;
  operator: FilterOperator;
  value: string;
}

export interface FilterGroup {
  mode: FilterMode;
  rules: FilterRule[];
}

export const NO_FILTERS: FilterGroup = { mode: "all", rules: [] };

/** A name for a new rule. */
export function ruleId(): string {
  return Math.random().toString(36).slice(2);
}

/** The filters as the server takes them, in the order of the rules. */
export function toFilters(group: FilterGroup): Filter[] {
  return group.rules.map((rule) => ({
    variable: rule.variable,
    operator: rule.operator,
    // A condition about missing values carries no value.
    value: OPERATORS[rule.operator].needsValue ? rule.value.trim() : "",
  }));
}

/** Put a rule in the group: in place of the one it edits, or at the end. */
export function withRule(group: FilterGroup, rule: FilterRule): FilterGroup {
  const editing = group.rules.some((one) => one.id === rule.id);
  return {
    ...group,
    rules: editing
      ? group.rules.map((one) => (one.id === rule.id ? rule : one))
      : [...group.rules, rule],
  };
}

export function withoutRule(group: FilterGroup, id: string): FilterGroup {
  const rules = group.rules.filter((one) => one.id !== id);
  // With nothing left to match, the mode goes back to the usual one.
  return rules.length === 0 ? NO_FILTERS : { ...group, rules };
}

/** What the server said about each rule it could not use, by rule id. */
export function problemsByRule(
  group: FilterGroup,
  problems: readonly FilterProblem[],
): Map<string, string> {
  const said = new Map<string, string>();
  for (const problem of problems) {
    const rule = group.rules[problem.filter];
    if (rule) said.set(rule.id, problem.message);
  }
  return said;
}
