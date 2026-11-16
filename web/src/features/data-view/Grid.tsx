import { useVirtualizer } from "@tanstack/react-virtual";
import {
  type KeyboardEvent,
  type PointerEvent,
  useCallback,
  useEffect,
  useMemo,
  useRef,
} from "react";

import { count } from "../../lib/format";
import type { Shown, Span } from "./blocks";
import { MIN_WIDTH, gutterWidth, typeTag } from "./columns";
import {
  type CellAddress,
  type CellSelection,
  clampAddress,
  contains,
  selectionBounds,
  single,
} from "./selection";
import { type DataTable, variableOf } from "./useDataTable";
import { useRowBlocks } from "./useRowBlocks";

interface Props {
  dataset: string;
  /** The columns: which variables are shown, and how wide. */
  table: DataTable;
  /** Variables in the file, shown or not. */
  variableCount: number;
  /** Rows that can be shown now: those loaded that the filters keep. */
  rowCount: number;
  /** Rows loaded so far. */
  loaded: number;
  shown: Shown;
  selection: CellSelection;
  onSelect: (selection: CellSelection) => void;
  /** The user asked for the selection to be copied. */
  onCopy: () => void;
}

const HEADER_HEIGHT = 35;
export const ROW_HEIGHT = 27;
/** How near an edge the pointer scrolls the grid while cells are selected. */
const EDGE = 28;

/** A selection being dragged out with the pointer. */
interface Drag {
  pointerId: number;
  anchor: CellAddress;
  focus: CellAddress;
  x: number;
  y: number;
}

/**
 * The data grid. Only the rows and columns near the screen exist in the
 * page, whatever the size of the file; their cells come from the server
 * in blocks (`useRowBlocks`). The columns come from the table.
 */
export function Grid(props: Props) {
  const { dataset, table, variableCount, rowCount, loaded, shown, selection, onSelect, onCopy } =
    props;
  const scroller = useRef<HTMLDivElement>(null);
  const gutter = gutterWidth(loaded);

  const leaf = table.getVisibleLeafColumns();
  const headerGroup = table.getHeaderGroups()[0];
  // A column's header, for its resize handle.
  const headers = useMemo(
    () => new Map((headerGroup?.headers ?? []).map((header) => [header.column.id, header])),
    [headerGroup],
  );

  const rows = useVirtualizer({
    count: rowCount,
    getScrollElement: () => scroller.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 8,
    paddingStart: HEADER_HEIGHT,
    scrollPaddingStart: HEADER_HEIGHT,
  });
  const columns = useVirtualizer({
    horizontal: true,
    count: leaf.length,
    getScrollElement: () => scroller.current,
    estimateSize: (index) => leaf[index]?.getSize() ?? MIN_WIDTH,
    getItemKey: (index) => leaf[index]?.id ?? index,
    overscan: 3,
    paddingStart: gutter,
    scrollPaddingStart: gutter,
  });
  // Sizes are estimates the virtualizer keeps: tell it when they change.
  const { columnSizing, columnVisibility } = table.state;
  useEffect(() => columns.measure(), [columns, columnSizing, columnVisibility, gutter]);

  const shownRows = rows.getVirtualItems();
  const shownColumns = columns.getVirtualItems();
  const firstRow = shownRows[0];
  const lastRow = shownRows[shownRows.length - 1];
  const rowSpan: Span = firstRow && lastRow ? [firstRow.index, lastRow.index] : null;
  /** The file position of the variable in each column on screen. */
  const variables = shownColumns.map((item) => Number(leaf[item.index]?.id ?? 0));
  const cellAt = useRowBlocks({ dataset, variableCount, shown, rows: rowSpan, variables, loaded });

  // Without filters a row's number is its position, known before it arrives.
  const filtered = shown.filters.length > 0;
  const bounds = selectionBounds(selection);
  const range = bounds.top !== bounds.bottom || bounds.left !== bounds.right;

  // What the pointer and the keyboard handlers need of the latest render.
  const live = useRef({ selection, rowCount, columnCount: leaf.length, shownColumns, onSelect });
  live.current = { selection, rowCount, columnCount: leaf.length, shownColumns, onSelect };

  const reveal = (address: CellAddress) => {
    rows.scrollToIndex(address.row, { align: "auto" });
    columns.scrollToIndex(address.column, { align: "auto" });
  };

  /** Move the far end of the selection; without `extend`, the whole selection. */
  const move = (to: CellAddress, extend: boolean) => {
    const focus = clampAddress(to, rowCount, leaf.length);
    onSelect(extend ? { anchor: selection.anchor, focus } : single(focus));
    reveal(focus);
  };

  const onKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (rowCount === 0 || leaf.length === 0) return;
    const { row, column } = selection.focus;
    const page = Math.max(Math.floor((scroller.current?.clientHeight ?? 0) / ROW_HEIGHT) - 2, 1);
    const jump = event.ctrlKey || event.metaKey;
    const [lastRowIndex, lastColumn] = [rowCount - 1, leaf.length - 1];
    const targets: Record<string, CellAddress> = {
      ArrowUp: { row: jump ? 0 : row - 1, column },
      ArrowDown: { row: jump ? lastRowIndex : row + 1, column },
      ArrowLeft: { row, column: jump ? 0 : column - 1 },
      ArrowRight: { row, column: jump ? lastColumn : column + 1 },
      PageUp: { row: row - page, column },
      PageDown: { row: row + page, column },
      // With Ctrl: the first and the last cell of the grid.
      Home: { row: jump ? 0 : row, column: 0 },
      End: { row: jump ? lastRowIndex : row, column: lastColumn },
    };
    const target = targets[event.key];
    if (target) {
      event.preventDefault();
      move(target, event.shiftKey);
    } else if (jump && event.key.toLowerCase() === "a") {
      event.preventDefault();
      onSelect({ anchor: { row: 0, column: 0 }, focus: { row: lastRowIndex, column: lastColumn } });
    } else if (jump && event.key.toLowerCase() === "c") {
      event.preventDefault();
      onCopy();
    } else if (event.key === "Escape" && range) {
      event.preventDefault();
      onSelect(single(selection.anchor));
    }
  };

  // --- selecting with the pointer ------------------------------------------

  const drag = useRef<Drag | null>(null);
  const frame = useRef(0);

  /** The cell under a point of the window, kept inside the cells drawn. */
  const addressAt = useCallback(
    (clientX: number, clientY: number): CellAddress | null => {
      const element = scroller.current;
      const { rowCount, shownColumns } = live.current;
      const [first, last] = [shownColumns[0], shownColumns[shownColumns.length - 1]];
      if (!element || rowCount === 0 || !first || !last) return null;
      const box = element.getBoundingClientRect();
      // Not on the header, the row numbers or a scroll bar.
      const within = (value: number, low: number, high: number) =>
        Math.min(Math.max(value, low), Math.max(high, low));
      const x =
        within(clientX, box.left + gutter + 1, box.left + element.clientWidth - 1) -
        box.left +
        element.scrollLeft;
      const y =
        within(clientY, box.top + HEADER_HEIGHT + 1, box.top + element.clientHeight - 1) -
        box.top +
        element.scrollTop;
      const row = within(Math.floor((y - HEADER_HEIGHT) / ROW_HEIGHT), 0, rowCount - 1);
      const hit =
        shownColumns.find((item) => x >= item.start && x < item.end) ??
        (x < first.start ? first : last);
      return { row, column: hit.index };
    },
    [gutter],
  );

  const stopDragging = useCallback(() => {
    const held = drag.current;
    drag.current = null;
    cancelAnimationFrame(frame.current);
    if (held && scroller.current?.hasPointerCapture(held.pointerId)) {
      scroller.current.releasePointerCapture(held.pointerId);
    }
  }, []);

  const extendToPointer = useCallback(() => {
    const held = drag.current;
    if (!held) return;
    const focus = addressAt(held.x, held.y);
    if (focus && (focus.row !== held.focus.row || focus.column !== held.focus.column)) {
      held.focus = focus;
      live.current.onSelect({ anchor: held.anchor, focus });
    }
  }, [addressAt]);

  // While the pointer is held near an edge, the grid scrolls under it.
  const scrollWhileDragging = useCallback(() => {
    const held = drag.current;
    const element = scroller.current;
    if (!held || !element) return;
    const box = element.getBoundingClientRect();
    const speed = (point: number, start: number, end: number) => {
      if (point < start + EDGE) return -Math.min(20, Math.max(1, (start + EDGE - point) / 3));
      if (point > end - EDGE) return Math.min(20, Math.max(1, (point - end + EDGE) / 3));
      return 0;
    };
    element.scrollLeft += speed(held.x, box.left + gutter, box.left + element.clientWidth);
    element.scrollTop += speed(held.y, box.top + HEADER_HEIGHT, box.top + element.clientHeight);
    extendToPointer();
    frame.current = requestAnimationFrame(scrollWhileDragging);
  }, [extendToPointer, gutter]);

  useEffect(() => {
    window.addEventListener("blur", stopDragging);
    return () => {
      window.removeEventListener("blur", stopDragging);
      stopDragging();
    };
  }, [stopDragging]);

  const onPointerDown = (event: PointerEvent<HTMLDivElement>) => {
    const element = scroller.current;
    if (!element || event.button !== 0) return;
    // The header has its own clicks; a scroll bar is not a cell.
    if ((event.target as HTMLElement).closest(".grid-head")) return;
    const box = element.getBoundingClientRect();
    if (
      event.clientX - box.left >= element.clientWidth ||
      event.clientY - box.top >= element.clientHeight
    ) {
      return;
    }
    const address = addressAt(event.clientX, event.clientY);
    if (!address) return;
    // No text selection, and the keyboard stays with the grid.
    event.preventDefault();
    element.focus({ preventScroll: true });
    stopDragging();
    const anchor = event.shiftKey ? selection.anchor : address;
    drag.current = {
      pointerId: event.pointerId,
      anchor,
      focus: address,
      x: event.clientX,
      y: event.clientY,
    };
    element.setPointerCapture(event.pointerId);
    onSelect({ anchor, focus: address });
    frame.current = requestAnimationFrame(scrollWhileDragging);
  };

  const onPointerMove = (event: PointerEvent<HTMLDivElement>) => {
    const held = drag.current;
    if (!held || event.pointerId !== held.pointerId) return;
    if (event.buttons === 0) {
      stopDragging();
      return;
    }
    held.x = event.clientX;
    held.y = event.clientY;
    extendToPointer();
  };

  return (
    <div
      ref={scroller}
      className="grid"
      tabIndex={0}
      role="grid"
      aria-label="Data"
      aria-multiselectable="true"
      aria-rowcount={rowCount}
      aria-colcount={leaf.length}
      onKeyDown={onKeyDown}
      onPointerDown={onPointerDown}
      onPointerMove={onPointerMove}
      onPointerUp={stopDragging}
      onPointerCancel={stopDragging}
      onLostPointerCapture={stopDragging}
    >
      <div
        className="grid-canvas"
        style={{ width: columns.getTotalSize(), height: rows.getTotalSize() }}
      >
        <div className="grid-head" style={{ height: HEADER_HEIGHT }} role="row">
          <div className="grid-corner" style={{ width: gutter }}>
            #
          </div>
          {shownColumns.map((item) => {
            const column = leaf[item.index];
            const header = column && headers.get(column.id);
            if (!column || !header) return null;
            const variable = variableOf(column);
            const selected = item.index >= bounds.left && item.index <= bounds.right;
            const resize = header.getResizeHandler();
            return (
              <div
                key={item.key}
                className={`grid-th ${selected && rowCount > 0 ? "is-selected" : ""}`}
                style={{ left: item.start, width: item.size }}
                role="columnheader"
                title={variable.label || variable.name}
                onClick={() => move({ row: selection.anchor.row, column: item.index }, false)}
              >
                <span className="grid-th-name">
                  <table.FlexRender header={header} />
                </span>
                <span className="grid-th-type">{typeTag(variable)}</span>
                <span
                  className={`grid-th-edge ${column.getIsResizing() ? "is-dragged" : ""}`}
                  onMouseDown={resize}
                  onTouchStart={resize}
                  onDoubleClick={() => column.resetSize()}
                  onClick={(event) => event.stopPropagation()}
                />
              </div>
            );
          })}
        </div>

        {shownRows.map((row) => {
          const inRows = row.index >= bounds.top && row.index <= bounds.bottom;
          const current = row.index === selection.anchor.row;
          // The band across the highlighted row helps reading one cell; a
          // selected block has its own tint.
          const band = current && !range;
          const first = cellAt(row.index, variables[0] ?? 0);
          return (
            <div
              key={row.key}
              className={`grid-row ${row.index % 2 ? "is-odd" : ""} ${band ? "is-current" : ""}`}
              style={{ top: row.start, height: row.size }}
              role="row"
              aria-rowindex={row.index + 1}
            >
              <div
                className={`grid-number ${inRows ? "is-selected" : ""}`}
                style={{ width: gutter }}
              >
                {first ? count(first.number) : filtered ? "" : count(row.index + 1)}
              </div>
              {shownColumns.map((item, place) => {
                const found = cellAt(row.index, variables[place] ?? 0);
                const active = current && item.index === selection.anchor.column;
                const inside = contains(bounds, row.index, item.index);
                const text = found?.cell.text;
                const classes = [
                  "grid-cell",
                  active && "is-active",
                  range && inside && "in-range",
                  range && inside && row.index === bounds.top && "range-top",
                  range && inside && row.index === bounds.bottom && "range-bottom",
                  range && inside && item.index === bounds.left && "range-left",
                  range && inside && item.index === bounds.right && "range-right",
                  found?.cell.flag && `is-${found.cell.flag}`,
                  text === "." && "is-missing",
                ];
                return (
                  <div
                    key={item.key}
                    className={classes.filter(Boolean).join(" ")}
                    style={{ left: item.start, width: item.size }}
                    role="gridcell"
                    aria-selected={inside}
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
