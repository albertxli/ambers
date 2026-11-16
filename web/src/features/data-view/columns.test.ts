import { describe, expect, it } from "vitest";

import type { Variable } from "../../api/client";
import { MIN_WIDTH, defaultWidth, gutterWidth, typeTag } from "./columns";

function variable(name: string, kind: string, width: number): Variable {
  return {
    index: 0,
    name,
    label: "",
    kind,
    format: "F8.2",
    measure: "",
    role: "",
    numeric: kind !== "text",
    width,
    values: [],
    missing: "",
    valid: 0,
    missing_count: 0,
    label_check: "",
    filter_kind: kind === "text" ? "text" : "number",
  };
}

describe("typeTag", () => {
  it("names the kind of variable", () => {
    expect(typeTag(variable("city", "text", 12))).toBe("ABC");
    expect(typeTag(variable("age", "numeric", 8))).toBe("123");
    expect(typeTag(variable("gender", "categorical", 6))).toBe("CAT");
    expect(typeTag(variable("day", "date", 10))).toBe("DATE");
  });
});

describe("defaultWidth", () => {
  it("grows with the variable's width, within limits", () => {
    const narrow = defaultWidth(variable("q", "numeric", 1));
    const wide = defaultWidth(variable("q", "text", 30));
    expect(narrow).toBeGreaterThanOrEqual(MIN_WIDTH);
    expect(wide).toBeGreaterThan(narrow);
    expect(defaultWidth(variable("q", "text", 5000))).toBe(420);
  });

  it("never cuts the variable's name", () => {
    const long = defaultWidth(variable("interview_start_datetime", "date", 4));
    const short = defaultWidth(variable("d", "date", 4));
    expect(long).toBeGreaterThan(short);
  });
});

describe("gutterWidth", () => {
  it("makes room for the largest row number", () => {
    expect(gutterWidth(20)).toBe(52);
    expect(gutterWidth(395_330)).toBeGreaterThan(gutterWidth(999));
  });
});
