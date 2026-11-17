import { describe, expect, it } from "vitest";

import { cellText, copyWindows } from "./copy";

describe("copyWindows", () => {
  it("asks for a small range in one request", () => {
    expect(copyWindows(10, 29, 5)).toEqual([{ offset: 10, limit: 20 }]);
  });

  it("asks for a long range 500 rows at a time", () => {
    expect(copyWindows(0, 1199, 3)).toEqual([
      { offset: 0, limit: 500 },
      { offset: 500, limit: 500 },
      { offset: 1000, limit: 200 },
    ]);
  });

  it("asks for fewer rows at a time when the range is wide", () => {
    // 916 variables: 54 rows hold just under 50,000 cells.
    const windows = copyWindows(0, 99, 916);
    expect(windows[0]).toEqual({ offset: 0, limit: 54 });
    expect(windows).toHaveLength(2);
  });
});

describe("cellText", () => {
  it("is the cell's text, with the stored value beside a label in Both mode", () => {
    expect(cellText({ text: "Male" })).toBe("Male");
    expect(cellText({ text: "Male", code: "1" })).toBe("Male (1)");
    expect(cellText({ text: "7", flag: "unlabeled" })).toBe("7");
  });
});
