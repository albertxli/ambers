import { useVirtualizer } from "@tanstack/react-virtual";
import { type KeyboardEvent, type PointerEvent, useEffect, useRef } from "react";

import type { CellMode, Variable } from "../../api/client";
import { count } from "../../lib/format";
import type { Span } from "./blocks";
import { MIN_WIDTH, gutterWidth, typeTag } from "./columns";
import { useRowBlocks } from "./useRowBlocks";

/** The highlighted cell: a row (its position among the rows shown) and a variable. */
export interface Selection {
  row: number;
  variable: number;
}

interface Props {
  dataset: string;
  variables: Variable[];
  /** Rows that can be shown now. */
  rowCount: number;
  mode: CellMode;
  compact: boolean;
  selection: Selection;
  onSelect: (selection: Selection) => void;
  /** Column widths in pixels, by variable. */
  widths: number[];
  onResize: (variable: number, width: number | null) => void;
}

const HEADER_HEIGHT = 35;

/**
 * The data grid. Only the rows and columns near the screen exist in the
 * page, whatever the size of the file; their cells come from the server
 * in blocks (`useRowBlocks`).
 */
export function Grid(props: Props) {
  const { dataset, variables, rowCount, mode, compact, selection, onSelect, widths, onResize } =
    props;
  const scroller = useRef<HTMLDivElement>(null);
  const rowHeight = compact ? 27 : 35;
  const gutter = gutterWidth(rowCount);

  const rows = useVirtualizer({
    count: rowCount,
    getScrollElement: () => scroller.current,
    estimateSize: () => rowHeight,
    overscan: 8,
    paddingStart: HEADER_HEIGHT,
    scrollPaddingStart: HEADER_HEIGHT,
  });
  const columns = useVirtualizer({
    horizontal: true,
    count: variables.length,
    getScrollElement: () => scroller.current,
    estimateSize: (index) => widths[index] ?? MIN_WIDTH,
    overscan: 3,
    paddingStart: gutter,
    scrollPaddingStart: gutter,
  });
  // Sizes are estimates the virtualizer keeps: tell it when they change.
  useEffect(() => rows.measure(), [rows, rowHeight]);
  useEffect(() => columns.measure(), [columns, widths, gutter]);

  const shownRows = rows.getVirtualItems();
  const shownColumns = columns.getVirtualItems();
  const span = (items: { index: number }[]): Span => {
    const first = items[0];
    const last = items[items.length - 1];
    return first && last ? [first.index, last.index] : null;
  };
  const cellAt = useRowBlocks({
    dataset,
    variableCount: variables.length,
    mode,
    filters: [],
    rows: span(shownRows),
    columns: span(shownColumns),
    loaded: rowCount,
  });

  const select = (row: number, variable: number) => {
    const next = {
      row: Math.min(Math.max(row, 0), Math.max(rowCount - 1, 0)),
      variable: Math.min(Math.max(variable, 0), Math.max(variables.length - 1, 0)),
    };
    onSelect(next);
    rows.scrollToIndex(next.row, { align: "auto" });
    columns.scrollToIndex(next.variable, { align: "auto" });
  };

  const onKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    const { row, variable } = selection;
    const page = Math.max(Math.floor((scroller.current?.clientHeight ?? 0) / rowHeight) - 2, 1);
    const jump = event.ctrlKey || event.metaKey;
    const moves: Record<string, () => void> = {
      ArrowUp: () => select(jump ? 0 : row - 1, variable),
      ArrowDown: () => select(jump ? rowCount - 1 : row + 1, variable),
      ArrowLeft: () => select(row, jump ? 0 : variable - 1),
      ArrowRight: () => select(row, jump ? variables.length - 1 : variable + 1),
      PageUp: () => select(row - page, variable),
      PageDown: () => select(row + page, variable),
      Home: () => (jump ? select(0, variable) : select(row, 0)),
      End: () => (jump ? select(rowCount - 1, variable) : select(row, variables.length - 1)),
    };
    const move = moves[event.key];
    if (move) {
      event.preventDefault();
      move();
    } else if (jump && event.key.toLowerCase() === "c") {
      const found = cellAt(row, variable);
      if (found) void navigator.clipboard?.writeText(found.cell.text);
    }
  };

  // Dragging the right edge of a column header resizes the column.
  const drag = useRef<{ variable: number; startX: number; startWidth: number } | null>(null);
  const onGrab = (event: PointerEvent<HTMLSpanElement>, variable: number) => {
    event.preventDefault();
    event.stopPropagation();
    event.currentTarget.setPointerCapture(event.pointerId);
    drag.current = { variable, startX: event.clientX, startWidth: widths[variable] ?? MIN_WIDTH };
  };
  const onDrag = (event: PointerEvent<HTMLSpanElement>) => {
    const held = drag.current;
    if (!held) return;
    onResize(held.variable, Math.max(held.startWidth + event.clientX - held.startX, MIN_WIDTH));
  };
  const onRelease = () => {
    drag.current = null;
  };

  return (
    <div
      ref={scroller}
      className={`grid ${compact ? "is-compact" : "is-comfortable"}`}
      tabIndex={0}
      role="grid"
      aria-label="Data"
      aria-rowcount={rowCount}
      aria-colcount={variables.length}
      onKeyDown={onKeyDown}
    >
      <div
        className="grid-canvas"
        style={{ width: columns.getTotalSize(), height: rows.getTotalSize() }}
      >
        <div className="grid-head" style={{ height: HEADER_HEIGHT }} role="row">
          <div className="grid-corner" style={{ width: gutter }}>
            #
          </div>
          {shownColumns.map((column) => {
            const variable = variables[column.index];
            if (!variable) return null;
            return (
              <div
                key={column.key}
                className={`grid-th ${column.index === selection.variable ? "is-current" : ""}`}
                style={{ left: column.start, width: column.size }}
                role="columnheader"
                title={variable.label || variable.name}
                onClick={() => select(selection.row, column.index)}
              >
                <span className="grid-th-name">{variable.name}</span>
                <span className="grid-th-type">{typeTag(variable)}</span>
                <span
                  className="grid-th-edge"
                  onPointerDown={(event) => onGrab(event, column.index)}
                  onPointerMove={onDrag}
                  onPointerUp={onRelease}
                  onPointerCancel={onRelease}
                  onDoubleClick={() => onResize(column.index, null)}
                  onClick={(event) => event.stopPropagation()}
                />
              </div>
            );
          })}
        </div>

        {shownRows.map((row) => {
          const current = row.index === selection.row;
          const first = cellAt(row.index, shownColumns[0]?.index ?? 0);
          return (
            <div
              key={row.key}
              className={`grid-row ${row.index % 2 ? "is-odd" : ""} ${current ? "is-current" : ""}`}
              style={{ top: row.start, height: row.size }}
              role="row"
              aria-rowindex={row.index + 1}
            >
              <div className="grid-number" style={{ width: gutter }}>
                {count(first?.number ?? row.index + 1)}
              </div>
              {shownColumns.map((column) => {
                const found = cellAt(row.index, column.index);
                const selected = current && column.index === selection.variable;
                const flag = found?.cell.flag ? `is-${found.cell.flag}` : "";
                const text = found?.cell.text;
                return (
                  <div
                    key={column.key}
                    className={`grid-cell ${selected ? "is-selected" : ""} ${flag} ${text === "." ? "is-missing" : ""}`}
                    style={{ left: column.start, width: column.size }}
                    role="gridcell"
                    aria-selected={selected}
                    onMouseDown={() => select(row.index, column.index)}
                  >
                    {found ? text : <span className="grid-wait" />}
                  </div>
                );
              })}
            </div>
          );
        })}
      </div>
    </div>
  );
}
