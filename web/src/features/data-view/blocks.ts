// The grid asks the server for cells in blocks: a fixed number of rows by
// a fixed number of variables. Blocks are cached by their position, so
// scrolling back shows cells at once, and a wide file (hundreds of
// variables) never asks for more than what is near the screen.

import type { Cell, CellMode, Filter, RowsPage, RowsRequest } from "../../api/client";

/** Rows in a block. */
export const ROW_BLOCK = 100;
/** Variables in a block. */
export const COLUMN_BLOCK = 24;

/** A block by its place: `row` 2 holds rows 200 to 299. */
export interface Block {
  row: number;
  column: number;
}

/** An inclusive range of rows or columns on screen; `null` when none is. */
export type Span = readonly [first: number, last: number] | null;

/** The blocks that hold every cell of `rows` × `columns`. */
export function blocksFor(rows: Span, columns: Span): Block[] {
  if (!rows || !columns) return [];
  const blocks: Block[] = [];
  const lastRow = Math.floor(rows[1] / ROW_BLOCK);
  const lastColumn = Math.floor(columns[1] / COLUMN_BLOCK);
  for (let row = Math.floor(rows[0] / ROW_BLOCK); row <= lastRow; row++) {
    for (let column = Math.floor(columns[0] / COLUMN_BLOCK); column <= lastColumn; column++) {
      blocks.push({ row, column });
    }
  }
  return blocks;
}

/** What to ask the server for one block. */
export function blockRequest(
  block: Block,
  variableCount: number,
  mode: CellMode,
  filters: Filter[],
): RowsRequest {
  const first = block.column * COLUMN_BLOCK;
  const last = Math.min(first + COLUMN_BLOCK, variableCount);
  const variables = Array.from({ length: Math.max(last - first, 0) }, (_, i) => first + i);
  return { offset: block.row * ROW_BLOCK, limit: ROW_BLOCK, variables, mode, filters };
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

export function findCell(
  pages: ReadonlyMap<string, RowsPage>,
  row: number,
  column: number,
): Found | undefined {
  const block = { row: Math.floor(row / ROW_BLOCK), column: Math.floor(column / COLUMN_BLOCK) };
  const page = pages.get(blockKey(block));
  const line = page?.rows[row - page.offset];
  const cell = line?.cells[column - block.column * COLUMN_BLOCK];
  return line && cell ? { cell, number: line.number } : undefined;
}
