// Copying a selection. The cells are on the server, so a range is asked
// for (in as few requests as the server's limits allow) and written to the
// clipboard as tab-separated text, which a spreadsheet pastes as cells.

import { api } from "../../api/client";
import type { Shown } from "./blocks";
import { type Bounds, cellsIn, columnsIn, tabSeparated } from "./selection";

/** The most cells one copy takes. */
export const COPY_LIMIT = 50_000;
/** What the server gives in one answer. */
const MAX_ROWS = 500;
const MAX_CELLS = 50_000;

export type CopyState = "idle" | "copying" | "copied" | "failed" | "too-many";

/** Whether a selection is small enough to copy. */
export function canCopy(bounds: Bounds): boolean {
  return cellsIn(bounds) <= COPY_LIMIT;
}

/** The windows of rows to ask for, to get rows `top..=bottom` of `columns` variables. */
export function copyWindows(
  top: number,
  bottom: number,
  columns: number,
): { offset: number; limit: number }[] {
  const step = Math.max(Math.min(MAX_ROWS, Math.floor(MAX_CELLS / Math.max(columns, 1))), 1);
  const windows = [];
  for (let offset = top; offset <= bottom; offset += step) {
    windows.push({ offset, limit: Math.min(step, bottom - offset + 1) });
  }
  return windows;
}

/**
 * The selected cells as text: rows `bounds.top..=bounds.bottom` (positions
 * among the rows shown) of `variables` (file positions, in the order the
 * grid shows them), as the mode shows them.
 */
export async function rangeText(
  dataset: string,
  bounds: Bounds,
  variables: number[],
  shown: Shown,
): Promise<string> {
  if (variables.length !== columnsIn(bounds)) throw new Error("the selection changed");
  const lines: string[][] = [];
  for (const window of copyWindows(bounds.top, bounds.bottom, variables.length)) {
    const page = await api.rows(dataset, {
      ...window,
      variables,
      mode: shown.mode,
      filters: shown.filters,
      filter_mode: shown.filterMode,
    });
    for (const row of page.rows) lines.push(row.cells.map((cell) => cell.text));
  }
  return tabSeparated(lines);
}
