import { describe, expect, it } from "vitest";

import type { RowsPage } from "../../api/client";
import {
  COLUMN_BLOCK,
  ROW_BLOCK,
  type Shown,
  blockKey,
  blockRequest,
  blocksFor,
  findCell,
  isPartial,
} from "./blocks";

function page(offset: number, variables: number[], rows: number): RowsPage {
  return {
    offset,
    shown: 1000,
    loaded: 1000,
    load: "done",
    variables,
    problems: [],
    rows: Array.from({ length: rows }, (_, r) => ({
      number: offset + r + 1,
      cells: variables.map((v) => ({ text: `r${offset + r}c${v}` })),
    })),
  };
}

describe("blocksFor", () => {
  it("is empty when nothing is on screen", () => {
    expect(blocksFor(null, [0, 5])).toEqual([]);
    expect(blocksFor([0, 5], [])).toEqual([]);
  });

  it("covers what is on screen, across block edges", () => {
    expect(blocksFor([0, 40], [0, 1, 2, 12])).toEqual([{ row: 0, column: 0 }]);
    const last = ROW_BLOCK - 1;
    expect(blocksFor([last, ROW_BLOCK], [COLUMN_BLOCK - 1, COLUMN_BLOCK])).toEqual([
      { row: 0, column: 0 },
      { row: 0, column: 1 },
      { row: 1, column: 0 },
      { row: 1, column: 1 },
    ]);
  });

  it("skips the blocks whose variables are all hidden", () => {
    // Variables 3 and 60 are next to each other on screen: 4 to 59 are hidden.
    expect(blocksFor([0, 10], [2, 3, 60, 61])).toEqual([
      { row: 0, column: 0 },
      { row: 0, column: 2 },
    ]);
  });
});

describe("blockRequest", () => {
  const codes: Shown = { mode: "codes", filters: [], filterMode: "all" };

  it("asks for the block's rows and variables", () => {
    const request = blockRequest({ row: 3, column: 1 }, 916, codes);
    expect(request.offset).toBe(3 * ROW_BLOCK);
    expect(request.limit).toBe(ROW_BLOCK);
    expect(request.variables?.[0]).toBe(COLUMN_BLOCK);
    expect(request.variables).toHaveLength(COLUMN_BLOCK);
    expect(request.mode).toBe("codes");
  });

  it("stops at the last variable", () => {
    const request = blockRequest({ row: 0, column: 1 }, COLUMN_BLOCK + 3, codes);
    expect(request.variables).toEqual([COLUMN_BLOCK, COLUMN_BLOCK + 1, COLUMN_BLOCK + 2]);
  });

  it("carries the filters and how they combine", () => {
    const filters = [{ variable: 4, operator: "contains" as const, value: "lis" }];
    const request = blockRequest({ row: 0, column: 0 }, 13, {
      mode: "labels",
      filters,
      filterMode: "any",
    });
    expect(request.filters).toEqual(filters);
    expect(request.filter_mode).toBe("any");
  });
});

describe("findCell", () => {
  const variables = Array.from({ length: COLUMN_BLOCK }, (_, i) => COLUMN_BLOCK + i);
  const pages = new Map([[blockKey({ row: 2, column: 1 }), page(2 * ROW_BLOCK, variables, 60)]]);

  it("finds a cell by its row and variable", () => {
    const found = findCell(pages, 2 * ROW_BLOCK + 7, COLUMN_BLOCK + 3);
    expect(found?.cell.text).toBe(`r${2 * ROW_BLOCK + 7}c${COLUMN_BLOCK + 3}`);
    expect(found?.number).toBe(2 * ROW_BLOCK + 8);
  });

  it("is undefined for a block that has not arrived, or a row past its end", () => {
    expect(findCell(pages, 5, 3)).toBeUndefined();
    expect(findCell(pages, 2 * ROW_BLOCK + 60, COLUMN_BLOCK)).toBeUndefined();
  });
});

describe("isPartial", () => {
  it("is true for a block with fewer rows than a full one", () => {
    expect(isPartial(page(0, [0], 60))).toBe(true);
    expect(isPartial(page(0, [0], ROW_BLOCK))).toBe(false);
    expect(isPartial(undefined)).toBe(false);
  });
});
