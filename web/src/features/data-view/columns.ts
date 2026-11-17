// Column sizes of the grid. A width comes from the file's metadata (the
// server sends it in characters), never from the rows on screen, so
// columns do not move while scrolling. The user can drag an edge.

import type { CellMode, Variable } from "../../api/client";
import { MIN_COLUMN } from "../../components/grid/useGrid";

/** The width of one character of the data font at its size, in pixels. */
const CHARACTER = 6.7;
/** Space inside a cell left and right of its text. */
const PADDING = 20;
/** The widest a column starts at. */
const MAX_WIDTH = 420;
/** The width of one character of a code tag (Both mode), and the tag's own room. */
const TAG_CHARACTER = 5.6;
const TAG_ROOM = 20;

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

/**
 * A column's starting width in pixels. In Both mode a labelled variable
 * gets room for the code shown beside each label.
 */
export function defaultWidth(variable: Variable, mode: CellMode = "labels"): number {
  // The header holds the name and the type tag; the name is never cut.
  const header = variable.name.length + typeTag(variable).length + 2;
  const characters = Math.max(variable.width, header);
  const longestCode = Math.max(0, ...variable.values.map((value) => value.code.length));
  const tag = mode === "both" && longestCode > 0 ? longestCode * TAG_CHARACTER + TAG_ROOM : 0;
  const width = Math.round(characters * CHARACTER + PADDING + tag);
  return Math.min(Math.max(width, MIN_COLUMN), MAX_WIDTH);
}

/** The width of the row-number column, for a file with this many rows. */
export function gutterWidth(rows: number): number {
  const digits = String(Math.max(rows, 1)).length;
  // Thousands separators take room too.
  const separators = Math.floor((digits - 1) / 3);
  return Math.max(Math.round((digits + separators) * CHARACTER + 24), 52);
}
