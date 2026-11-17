import { describe, expect, it } from "vitest";

import {
  FIRST_CELL,
  cellsIn,
  clampSelection,
  columnsIn,
  contains,
  describe as describeRange,
  rowsIn,
  selectionBounds,
  single,
  tabSeparated,
} from "./selection";

describe("selectionBounds", () => {
  it("is the rectangle between the two ends, whichever way it was dragged", () => {
    const bounds = selectionBounds({ anchor: { row: 9, column: 2 }, focus: { row: 4, column: 6 } });
    expect(bounds).toEqual({ top: 4, bottom: 9, left: 2, right: 6 });
    expect([rowsIn(bounds), columnsIn(bounds), cellsIn(bounds)]).toEqual([6, 5, 30]);
    expect(contains(bounds, 4, 2)).toBe(true);
    expect(contains(bounds, 9, 6)).toBe(true);
    expect(contains(bounds, 3, 2)).toBe(false);
    expect(contains(bounds, 5, 7)).toBe(false);
  });

  it("is one cell for a selection that was not extended", () => {
    expect(cellsIn(selectionBounds(single({ row: 3, column: 3 })))).toBe(1);
  });
});

describe("clampSelection", () => {
  it("keeps a selection that fits as it is", () => {
    const selection = { anchor: { row: 1, column: 1 }, focus: { row: 4, column: 2 } };
    expect(clampSelection(selection, 10, 5)).toBe(selection);
  });

  it("pulls a selection back into a grid that got smaller", () => {
    const selection = { anchor: { row: 50, column: 8 }, focus: { row: 2, column: 1 } };
    expect(clampSelection(selection, 10, 5)).toEqual({
      anchor: { row: 9, column: 4 },
      focus: { row: 2, column: 1 },
    });
  });

  it("is the first cell in a grid with nothing in it", () => {
    const selection = { anchor: { row: 3, column: 3 }, focus: { row: 5, column: 5 } };
    expect(clampSelection(selection, 0, 0)).toEqual(FIRST_CELL);
  });
});

describe("tabSeparated", () => {
  it("is one line per row, one tab between cells", () => {
    expect(tabSeparated([["1", "Lisbon"], ["2", "Osaka"]])).toBe("1\tLisbon\n2\tOsaka");
  });

  it("quotes a cell that would break the grid when pasted", () => {
    expect(tabSeparated([["a\tb", 'say "hi"', "two\nlines", "plain"]])).toBe(
      '"a\tb"\t"say ""hi"""\t"two\nlines"\tplain',
    );
  });
});

describe("describe", () => {
  const names = { first: "rpmID", last: "Q1" };

  it("names a range by its variables and rows", () => {
    const bounds = { top: 11, bottom: 19, left: 0, right: 14 };
    expect(describeRange(bounds, names, { first: 12, last: 20 })).toBe("rpmID:Q1 · rows 12–20");
    expect(describeRange(bounds, names, { first: 1200, last: 2000 })).toBe(
      "rpmID:Q1 · rows 1,200–2,000",
    );
  });

  it("names one variable or one row once", () => {
    const column = { top: 0, bottom: 4, left: 2, right: 2 };
    expect(describeRange(column, { first: "S1", last: "S1" }, { first: 1, last: 5 })).toBe(
      "S1 · rows 1–5",
    );
    const row = { top: 6, bottom: 6, left: 0, right: 14 };
    expect(describeRange(row, names, { first: 7, last: 7 })).toBe("rpmID:Q1 · row 7");
  });

  it("leaves the rows out when their numbers are not known", () => {
    expect(describeRange({ top: 0, bottom: 3, left: 0, right: 14 }, names, null)).toBe("rpmID:Q1");
  });
});
