import { describe, expect, it } from "vitest";

import type { ValueRow, ValueTotals, Variable } from "../../api/client";
import { widthForKey } from "../../components/grid/ColumnResizeHandle";
import { MAX_COLUMN, MIN_COLUMN } from "../../components/grid/useGrid";
import { auditText, countsScope, missingShare } from "../../lib/variable";
import { variable } from "../../test/variable";
import { labelOf, tableText } from "./ValueDetails";
import {
  FIELDS,
  NO_FILTERS,
  compareBy,
  fieldText,
  filterVariables,
  isFiltered,
  valuesText,
} from "./variables";

const audited = { audited: true, unlabeled: 0, many: false, duplicates: 0 };
const gender = variable({
  index: 0,
  name: "gender",
  label: "Gender of the respondent",
  kind: "categorical",
  values: [
    { code: "1", label: "Male", user_missing: false },
    { code: "2", label: "Female", user_missing: false },
  ],
  valid: 18,
  missing_count: 2,
  label_check: "ok",
  label_audit: audited,
});
const q1 = variable({
  index: 1,
  name: "Q1",
  label: "",
  kind: "categorical",
  values: [
    { code: "1", label: "Agree", user_missing: false },
    { code: "99", label: "Refused", user_missing: true },
  ],
  missing: "99",
  valid: 20,
  label_check: "2 unlabelled: 7, 8; duplicate: Other (97, 98)",
  label_audit: { ...audited, unlabeled: 2, duplicates: 1 },
});
const q10 = variable({ index: 2, name: "Q10", label: "Age in years", kind: "numeric", valid: 19 });
const q2 = variable({ index: 3, name: "Q2", label: "Home town", kind: "text", valid: 5 });
const all = [gender, q1, q10, q2];

const names = (variables: Variable[]) => variables.map((one) => one.name);

describe("filterVariables", () => {
  it("keeps everything, in file order, without filters", () => {
    expect(filterVariables(all, NO_FILTERS)).toEqual(all);
    expect(isFiltered(NO_FILTERS)).toBe(false);
    expect(isFiltered({ ...NO_FILTERS, search: "  " })).toBe(false);
  });

  it("searches names, variable labels and value labels, ignoring case", () => {
    const search = (text: string) => names(filterVariables(all, { ...NO_FILTERS, search: text }));
    expect(search("q1")).toEqual(["Q1", "Q10"]);
    expect(search("TOWN")).toEqual(["Q2"]);
    expect(search("female")).toEqual(["gender"]);
    expect(search("nothing like it")).toEqual([]);
  });

  it("lists one type, or what a label check found", () => {
    const by = (filters: object) => names(filterVariables(all, { ...NO_FILTERS, ...filters }));
    expect(by({ kind: "categorical" })).toEqual(["gender", "Q1"]);
    expect(by({ kind: "date" })).toEqual([]);
    expect(by({ check: "unlabelled" })).toEqual(["Q1"]);
    expect(by({ check: "duplicates" })).toEqual(["Q1"]);
    expect(by({ check: "no_label" })).toEqual(["Q1"]);
    // Filters narrow each other.
    expect(by({ kind: "text", check: "unlabelled" })).toEqual([]);
    expect(isFiltered({ ...NO_FILTERS, check: "duplicates" })).toBe(true);
  });
});

describe("compareBy", () => {
  const sorted = (field: Parameters<typeof compareBy>[0]) => names([...all].sort(compareBy(field)));

  it("reads names as people do: Q2 before Q10", () => {
    expect(sorted("name")).toEqual(["gender", "Q1", "Q2", "Q10"]);
  });

  it("orders counts as numbers", () => {
    expect(sorted("valid")).toEqual(["Q2", "gender", "Q10", "Q1"]);
    expect(sorted("missing")[3]).toBe("gender");
  });

  it("puts the label checks with findings last", () => {
    const order = sorted("check");
    expect(order[order.length - 1]).toBe("Q1");
    // Not checked, then checked and fine.
    expect(order.indexOf("Q10")).toBeLessThan(order.indexOf("gender"));
  });
});

describe("what a cell says", () => {
  it("writes value labels on one line", () => {
    expect(valuesText(gender)).toBe("1 = Male; 2 = Female");
    expect(valuesText(q10)).toBe("");
  });

  it("has a text for every column", () => {
    const line = FIELDS.map((field) => fieldText(q1, field.id));
    expect(line).toEqual([
      "Q1",
      "Categorical",
      "",
      "1 = Agree; 99 = Refused",
      "20",
      "0",
      "2 unlabelled · 1 duplicate",
      "F8.2",
      "scale",
      "99",
    ]);
    // A variable that is not checked copies as an empty cell, not a dash.
    expect(fieldText(q10, "check")).toBe("");
  });

  it("words the label check", () => {
    expect(auditText(gender)).toBe("OK");
    expect(auditText(q10)).toBe("—");
    expect(auditText(q1)).toBe("2 unlabelled · 1 duplicate");
    const many = variable({ label_audit: { ...audited, unlabeled: 40, many: true, duplicates: 2 } });
    expect(auditText(many)).toBe("many unlabelled · 2 duplicates");
  });

  it("gives missing cases as a count and a share", () => {
    expect(missingShare(gender, 20)).toBe("2 (10.0%)");
    expect(missingShare(gender, 0)).toBe("2 (0.0%)");
  });
});

describe("countsScope", () => {
  const info = {
    id: "d1",
    name: "survey.sav",
    path: "survey.sav",
    label: "",
    rows: 120_000,
    total_rows: 395_330,
    variables: 916,
    load: "loading" as const,
    load_error: null,
    counted_rows: 120_000,
    unlabeled_variables: 0,
    compression: "bytecode",
    encoding: "UTF-8",
    created: "",
    warnings: [],
  };

  it("says which rows the counts cover", () => {
    expect(countsScope(info, 120_000)).toBe("first 120,000 of 395,330 rows, still loading");
    expect(countsScope({ ...info, load: "done" }, 395_330)).toBe("395,330 rows");
    expect(countsScope({ ...info, load: "memory" }, 200_000)).toBe(
      "first 200,000 of 395,330 rows (memory budget)",
    );
    expect(countsScope({ ...info, load: "done", total_rows: null }, 1)).toBe("1 row");
    expect(countsScope(info, 0)).toBe("no rows yet, still loading");
  });
});

describe("a column's width from the keyboard", () => {
  it("steps by 10 pixels, by 50 with Shift, within the limits", () => {
    expect(widthForKey("ArrowRight", false, 120)).toBe(130);
    expect(widthForKey("ArrowLeft", true, 120)).toBe(70);
    expect(widthForKey("ArrowLeft", true, 60)).toBe(MIN_COLUMN);
    expect(widthForKey("ArrowRight", false, MAX_COLUMN)).toBe(MAX_COLUMN);
    expect(widthForKey("Home", false, 120)).toBe(MIN_COLUMN);
    expect(widthForKey("End", false, 120)).toBe(MAX_COLUMN);
  });

  it("resets on Enter or Space, and leaves other keys alone", () => {
    expect(widthForKey("Enter", false, 120)).toBeNull();
    expect(widthForKey(" ", false, 120)).toBeNull();
    expect(widthForKey("a", false, 120)).toBeUndefined();
    expect(widthForKey("Tab", false, 120)).toBeUndefined();
  });
});

describe("the value distribution as text", () => {
  const totals: ValueTotals = {
    valid: 18,
    missing: 2,
    total: 20,
    values: 3,
    labeled: 2,
    unlabeled: 1,
  };
  const row = (code: string, label: string, n: number, kind: ValueRow["kind"]): ValueRow => ({
    code,
    label,
    value_label: kind === "unlabeled" ? "" : label,
    count: n,
    share_valid: n / 18,
    share_total: n / 20,
    kind,
  });
  const rows = [
    row("1", "Agree", 12, "plain"),
    row("7", "(no label)", 3, "unlabeled"),
    row("99", "Refused", 3, "user_missing"),
  ];

  it("names a code without a label", () => {
    expect(labelOf(rows[0] as ValueRow)).toBe("Agree");
    expect(labelOf(rows[1] as ValueRow)).toBe("Unlabelled code");
  });

  it("has a header, the missing row first, and marks user-missing values", () => {
    const text = tableText(q1, totals, rows, { categorical: true, labels: true });
    expect(text.split("\n")).toEqual([
      "Code\tLabel\tN\t% valid\t% total",
      ".\tSystem missing\t2\t—\t10.0%",
      "1\tAgree\t12\t66.7%\t60.0%",
      "7\tUnlabelled code\t3\t16.7%\t15.0%",
      "99\tRefused (user-missing)\t3\t16.7%\t15.0%",
    ]);
  });

  it("leaves the label column out for a variable without labels", () => {
    const town = [row("Lisbon", "", 5, "plain")];
    const text = tableText(q2, { ...totals, missing: 0 }, town, {
      categorical: false,
      labels: false,
    });
    expect(text.split("\n")).toEqual(["Value\tN\t% valid\t% total", "Lisbon\t5\t27.8%\t25.0%"]);
    // A text variable's missing cases are empty strings.
    const withMissing = tableText(q2, totals, town, { categorical: false, labels: false });
    expect(withMissing.split("\n")[1]).toBe("(missing)\t2\t—\t10.0%");
  });
});
