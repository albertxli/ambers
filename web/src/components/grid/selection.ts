// The cells the user has selected: a rectangle between the cell where the
// selection started (the anchor, which is the highlighted cell) and the
// cell it was extended to (the focus). Rows are positions among the rows
// shown, columns are positions among the variables shown.

export interface CellAddress {
  row: number;
  column: number;
}

export interface CellSelection {
  anchor: CellAddress;
  focus: CellAddress;
}

export interface Bounds {
  top: number;
  bottom: number;
  left: number;
  right: number;
}

export const FIRST_CELL: CellSelection = {
  anchor: { row: 0, column: 0 },
  focus: { row: 0, column: 0 },
};

/** A selection of one cell. */
export function single(address: CellAddress): CellSelection {
  return { anchor: address, focus: address };
}

export function selectionBounds({ anchor, focus }: CellSelection): Bounds {
  return {
    top: Math.min(anchor.row, focus.row),
    bottom: Math.max(anchor.row, focus.row),
    left: Math.min(anchor.column, focus.column),
    right: Math.max(anchor.column, focus.column),
  };
}

export function rowsIn(bounds: Bounds): number {
  return bounds.bottom - bounds.top + 1;
}

export function columnsIn(bounds: Bounds): number {
  return bounds.right - bounds.left + 1;
}

export function cellsIn(bounds: Bounds): number {
  return rowsIn(bounds) * columnsIn(bounds);
}

export function contains(bounds: Bounds, row: number, column: number): boolean {
  return row >= bounds.top && row <= bounds.bottom && column >= bounds.left && column <= bounds.right;
}

function clamp(value: number, last: number): number {
  return Math.min(Math.max(value, 0), Math.max(last, 0));
}

/** An address inside a grid of `rows` × `columns`. */
export function clampAddress(address: CellAddress, rows: number, columns: number): CellAddress {
  return { row: clamp(address.row, rows - 1), column: clamp(address.column, columns - 1) };
}

/**
 * A selection inside a grid of `rows` × `columns`: after a filter or a
 * hidden variable made the grid smaller, the selection must not point
 * outside it.
 */
export function clampSelection(
  selection: CellSelection,
  rows: number,
  columns: number,
): CellSelection {
  const anchor = clampAddress(selection.anchor, rows, columns);
  const focus = clampAddress(selection.focus, rows, columns);
  const same = (a: CellAddress, b: CellAddress) => a.row === b.row && a.column === b.column;
  return same(anchor, selection.anchor) && same(focus, selection.focus)
    ? selection
    : { anchor, focus };
}

/** One field of tab-separated text. */
function field(text: string): string {
  // A tab or a line break inside a value would start a new cell or row
  // when pasted into a spreadsheet; quoted, it stays one cell.
  return /["\t\n\r]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

/** Rows of cells as text to paste into a spreadsheet. */
export function tabSeparated(rows: readonly (readonly string[])[]): string {
  return rows.map((row) => row.map(field).join("\t")).join("\n");
}

/**
 * How a selection is named: by variable and row, as people talk about
 * survey data. `rows` are the file's row numbers of the first and last
 * selected row, when known.
 */
export function describe(
  bounds: Bounds,
  names: { first: string; last: string },
  rows: { first: number; last: number } | null,
): string {
  const variables = bounds.left === bounds.right ? names.first : `${names.first}:${names.last}`;
  if (!rows) return variables;
  const numbers = (n: number) => n.toLocaleString("en-US");
  return bounds.top === bounds.bottom
    ? `${variables} · row ${numbers(rows.first)}`
    : `${variables} · rows ${numbers(rows.first)}–${numbers(rows.last)}`;
}
