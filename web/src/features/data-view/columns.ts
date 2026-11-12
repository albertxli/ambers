// Column sizes of the grid. A width comes from the file's metadata (the
// server sends it in characters), never from the rows on screen, so
// columns do not move while scrolling. The user can drag an edge.

import type { Variable } from "../../api/client";

/** The width of one character of the data font at its size, in pixels. */
const CHARACTER = 6.7;
/** Space inside a cell left and right of its text. */
const PADDING = 20;
export const MIN_WIDTH = 48;
const MAX_WIDTH = 420;

/** The short tag in a column header that says what kind of variable it is. */
export function typeTag(variable: Variable): string {
  switch (variable.kind) {
    case "text":
      return "ABC";
    case "numeric":
      return "123";
    case "date":
      return "DATE";
    default:
      return "CAT";
  }
}

/** A column's starting width in pixels. */
export function defaultWidth(variable: Variable): number {
  // The header holds the name and the type tag; the name is never cut.
  const header = variable.name.length + typeTag(variable).length + 2;
  const characters = Math.max(variable.width, header);
  return Math.min(Math.max(Math.round(characters * CHARACTER + PADDING), MIN_WIDTH), MAX_WIDTH);
}

/** The width of the row-number column, for a file with this many rows. */
export function gutterWidth(rows: number): number {
  const digits = String(Math.max(rows, 1)).length;
  // Thousands separators take room too.
  const separators = Math.floor((digits - 1) / 3);
  return Math.max(Math.round((digits + separators) * CHARACTER + 24), 52);
}
