import { describe, expect, it } from "vitest";

import type { Variable } from "../../api/client";
import { MIN_COLUMN } from "../../components/grid/useGrid";
import { variable as make } from "../../test/variable";
import { defaultWidth, gutterWidth, typeTag } from "./columns";

function variable(name: string, kind: string, width: number): Variable {
  return make({ name, kind, width });
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
    expect(narrow).toBeGreaterThanOrEqual(MIN_COLUMN);
    expect(wide).toBeGreaterThan(narrow);
    expect(defaultWidth(variable("q", "text", 5000))).toBe(420);
  });

  it("makes room for the code beside a label in Both mode", () => {
    const gender = make({
      name: "gender",
      kind: "categorical",
      width: 6,
      values: [
        { code: "1.00", label: "Male", user_missing: false },
        { code: "2.00", label: "Female", user_missing: false },
      ],
    });
    expect(defaultWidth(gender, "both")).toBeGreaterThan(defaultWidth(gender, "labels"));
    expect(defaultWidth(gender, "codes")).toBe(defaultWidth(gender, "labels"));
    // A variable without labels shows the same in every mode.
    const age = variable("age", "numeric", 8);
    expect(defaultWidth(age, "both")).toBe(defaultWidth(age, "labels"));
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
