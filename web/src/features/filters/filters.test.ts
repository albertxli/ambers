import { describe, expect, it } from "vitest";

import type { FilterKind, FilterOperator, KindOperators } from "../../api/client";
import {
  type FilterGroup,
  NO_FILTERS,
  problemsByRule,
  toFilters,
  withRule,
  withoutRule,
} from "./filters";
import { OPERATORS, operatorsFor, validate, valueHelp } from "./operators";

const city = { id: "a", variable: 8, operator: "contains" as const, value: " lis " };
const age = { id: "b", variable: 2, operator: "greater_than" as const, value: "30" };
const empty = { id: "c", variable: 5, operator: "is_empty" as const, value: "left over" };

describe("toFilters", () => {
  it("sends the rules in their order, without the page's ids", () => {
    const group: FilterGroup = { mode: "any", rules: [city, age] };
    expect(toFilters(group)).toEqual([
      { variable: 8, operator: "contains", value: "lis" },
      { variable: 2, operator: "greater_than", value: "30" },
    ]);
  });

  it("sends no value with a condition that takes none", () => {
    expect(toFilters({ mode: "all", rules: [empty] })).toEqual([
      { variable: 5, operator: "is_empty", value: "" },
    ]);
  });
});

describe("withRule and withoutRule", () => {
  it("adds a new rule at the end", () => {
    expect(withRule({ mode: "all", rules: [city] }, age).rules).toEqual([city, age]);
  });

  it("puts an edited rule where it was", () => {
    const edited = { ...city, value: "osaka" };
    const group = withRule({ mode: "any", rules: [city, age] }, edited);
    expect(group).toEqual({ mode: "any", rules: [edited, age] });
  });

  it("removes a rule, and forgets the mode with the last one", () => {
    const group: FilterGroup = { mode: "any", rules: [city, age] };
    expect(withoutRule(group, "a")).toEqual({ mode: "any", rules: [age] });
    expect(withoutRule({ mode: "any", rules: [age] }, "b")).toBe(NO_FILTERS);
  });
});

describe("problemsByRule", () => {
  it("gives each refused rule what the server said about it", () => {
    const group: FilterGroup = { mode: "all", rules: [city, age, empty] };
    const said = problemsByRule(group, [
      { filter: 1, message: "enter a number" },
      { filter: 9, message: "no such rule" },
    ]);
    expect([...said]).toEqual([["b", "enter a number"]]);
  });
});

describe("the conditions", () => {
  const offered: KindOperators[] = [
    { kind: "number", operators: ["equals", "greater_than", "is_empty"] },
    { kind: "text", operators: ["equals", "contains", "like", "is_empty"] },
  ];

  it("offers what the server offers for a kind of variable", () => {
    expect(operatorsFor("number", offered)).toEqual(["equals", "greater_than", "is_empty"]);
    expect(operatorsFor("date", offered)).toEqual([]);
  });

  it("has words for every operator", () => {
    for (const [operator, how] of Object.entries(OPERATORS)) {
      expect(how.label, operator).not.toBe("");
      expect(how.symbol, operator).not.toBe("");
    }
    expect(OPERATORS.is_empty.needsValue).toBe(false);
    expect(OPERATORS.is_not_empty.needsValue).toBe(false);
  });

  const check = (kind: FilterKind, operator: FilterOperator, value: string) =>
    validate(kind, operator, value);

  it("wants a value, except for conditions about missing values", () => {
    expect(check("text", "contains", "  ")).toBe("Enter a value to continue.");
    expect(check("text", "contains", "lis")).toBeNull();
    expect(check("text", "is_empty", "")).toBeNull();
    expect(check("number", "is_not_empty", "")).toBeNull();
  });

  it("wants a number where only a number can match", () => {
    expect(check("number", "greater_than", "abc")).toBe("Enter a valid number.");
    expect(check("number", "equals", "abc")).toBe("Enter a valid number.");
    expect(check("number", "equals", "-3.5")).toBeNull();
    expect(check("categorical", "less_or_equal", "male")).toBe("Enter a valid number.");
    // A labelled number is found by its label too.
    expect(check("categorical", "equals", "Female")).toBeNull();
    expect(check("text", "equals", "abc")).toBeNull();
  });

  it("explains the value box", () => {
    expect(valueHelp("text", "like")).toMatch(/%/);
    expect(valueHelp("number", "equals")).toMatch(/numeric/);
    expect(valueHelp("categorical", "contains")).toMatch(/label or a code/);
    expect(valueHelp("text", "contains")).toMatch(/case-insensitive/);
  });
});
