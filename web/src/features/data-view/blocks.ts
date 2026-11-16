// The grid asks the server for cells in blocks: a fixed number of rows by
// a fixed number of variables. Blocks are cached by their position, so
// scrolling back shows cells at once, and a wide file (hundreds of
// variables) never asks for more than what is near the screen.
//
// Rows are positions among the rows the filters keep. Columns are
// variables by their position in the file, whatever is hidden: a block
// holds 24 neighbouring variables of the file, and is asked for only when
// one of them is on screen.

import type {
  Cell,
  CellMode,
  Filter,
  FilterMode,
  RowsPage,
  RowsRequest,
} from "../../api/client";

/** Rows in a block. */
export const ROW_BLOCK = 100;
/** Variables in a block. */
export const COLUMN_BLOCK = 24;

/** A block by its place: `row` 2 holds rows 200 to 299. */
export interface Block {
  row: number;
  column: number;
}

/** An inclusive range of rows on screen; `null` when none is. */
export type Span = readonly [first: number, last: number] | null;

/** The blocks that hold every cell of `rows` × `variables` (file positions). */
export function blocksFor(rows: Span, variables: readonly number[]): Block[] {
  if (!rows || variables.length === 0) return [];
  const columns = [...new Set(variables.map((index) => Math.floor(index / COLUMN_BLOCK)))];
  columns.sort((a, b) => a - b);
  const blocks: Block[] = [];
  const lastRow = Math.floor(rows[1] / ROW_BLOCK);
  for (let row = Math.floor(rows[0] / ROW_BLOCK); row <= lastRow; row++) {
    for (const column of columns) blocks.push({ row, column });
  }
  return blocks;
}

/** What the rows shown depend on, besides the file. */
export interface Shown {
  mode: CellMode;
  filters: Filter[];
  filterMode: FilterMode;
}

/** What to ask the server for one block. */
export function blockRequest(block: Block, variableCount: number, shown: Shown): RowsRequest {
  const first = block.column * COLUMN_BLOCK;
  const last = Math.min(first + COLUMN_BLOCK, variableCount);
  const variables = Array.from({ length: Math.max(last - first, 0) }, (_, i) => first + i);
  return {
    offset: block.row * ROW_BLOCK,
    limit: ROW_BLOCK,
    variables,
    mode: shown.mode,
    filters: shown.filters,
    filter_mode: shown.filterMode,
  };
}

/** The name a block is cached under. */
export function blockKey(block: Block): string {
  return `${block.row}:${block.column}`;
}

/** A block that may gain rows while the file is still loading. */
export function isPartial(page: RowsPage | undefined): boolean {
  return page !== undefined && page.rows.length < ROW_BLOCK;
}

/** A cell of the grid, found in the blocks that have arrived. */
export interface Found {
  cell: Cell;
  /** The row's number in the file, from 1. */
  number: number;
}

/** The cell at `row` (a position among the rows shown) of a variable. */
export function findCell(
  pages: ReadonlyMap<string, RowsPage>,
  row: number,
  variable: number,
): Found | undefined {
  const block = { row: Math.floor(row / ROW_BLOCK), column: Math.floor(variable / COLUMN_BLOCK) };
  const page = pages.get(blockKey(block));
  const line = page?.rows[row - page.offset];
  const cell = line?.cells[variable - block.column * COLUMN_BLOCK];
  return line && cell ? { cell, number: line.number } : undefined;
}
