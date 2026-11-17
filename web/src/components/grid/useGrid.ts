// What every grid of the page does, whatever it shows: only the rows and
// columns near the screen exist, a cell or a block of cells is selected
// with the pointer or the keyboard, and the selection can be copied.
//
// The Data View and the Variable View both use it. Each brings its own
// columns and draws its own cells (`GridView`); this hook knows nothing
// about variables or the server.

import { type VirtualItem, useVirtualizer } from "@tanstack/react-virtual";
import {
  type KeyboardEvent,
  type PointerEvent,
  type RefObject,
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";

import {
  type Bounds,
  type CellAddress,
  type CellSelection,
  clampAddress,
  selectionBounds,
  single,
} from "./selection";

/** A column of a grid: what it is called, and how wide it is now. */
export interface GridColumn {
  id: string;
  size: number;
}

export const HEADER_HEIGHT = 35;
export const ROW_HEIGHT = 27;
/** The narrowest and the widest a column can be made. */
export const MIN_COLUMN = 48;
export const MAX_COLUMN = 1600;
/** How near an edge the pointer scrolls the grid while cells are selected. */
const EDGE = 28;

interface Options {
  /** The columns shown, in order. */
  columns: readonly GridColumn[];
  rowCount: number;
  /** The width of the row-number column. */
  gutter: number;
  rowHeight?: number;
  selection: CellSelection;
  onSelect: (selection: CellSelection) => void;
  /** The user asked for the selection to be copied. */
  onCopy: () => void;
  /** False while the grid's view is hidden behind another one. */
  active?: boolean;
}

/** A selection being dragged out with the pointer. */
interface Drag {
  pointerId: number;
  anchor: CellAddress;
  focus: CellAddress;
  x: number;
  y: number;
}

export interface GridState {
  scroller: RefObject<HTMLDivElement | null>;
  columns: readonly GridColumn[];
  rowCount: number;
  gutter: number;
  rowHeight: number;
  /** The rows and columns near the screen. */
  shownRows: VirtualItem[];
  shownColumns: VirtualItem[];
  /** The first and last row near the screen; `null` when none is. */
  rowSpan: readonly [number, number] | null;
  width: number;
  height: number;
  selection: CellSelection;
  bounds: Bounds;
  /** More than one cell is selected. */
  range: boolean;
  /** Move the far end of the selection; without `extend`, all of it. */
  move: (to: CellAddress, extend: boolean) => void;
  /** The column whose edge is being dragged. */
  resizing: string | null;
  setResizing: (id: string | null) => void;
  /** A block of cells is being dragged out. */
  dragging: boolean;
  handlers: {
    onKeyDown: (event: KeyboardEvent<HTMLDivElement>) => void;
    onPointerDown: (event: PointerEvent<HTMLDivElement>) => void;
    onPointerMove: (event: PointerEvent<HTMLDivElement>) => void;
    onPointerUp: () => void;
    onPointerCancel: () => void;
    onLostPointerCapture: () => void;
  };
}

export function useGrid(options: Options): GridState {
  const { columns, rowCount, gutter, selection, onSelect, onCopy } = options;
  const rowHeight = options.rowHeight ?? ROW_HEIGHT;
  const active = options.active ?? true;
  const scroller = useRef<HTMLDivElement>(null);
  const [resizing, setResizing] = useState<string | null>(null);
  const [dragging, setDragging] = useState(false);

  const rows = useVirtualizer({
    count: rowCount,
    getScrollElement: () => scroller.current,
    estimateSize: () => rowHeight,
    overscan: 8,
    paddingStart: HEADER_HEIGHT,
    scrollPaddingStart: HEADER_HEIGHT,
  });
  const across = useVirtualizer({
    horizontal: true,
    count: columns.length,
    getScrollElement: () => scroller.current,
    estimateSize: (index) => columns[index]?.size ?? MIN_COLUMN,
    getItemKey: (index) => columns[index]?.id ?? index,
    overscan: 3,
    paddingStart: gutter,
    scrollPaddingStart: gutter,
  });
  // Sizes are estimates the virtualizer keeps: tell it when they change.
  const sizes = useMemo(
    () => columns.map((column) => `${column.id}:${column.size}`).join(),
    [columns],
  );
  useEffect(() => across.measure(), [across, sizes, gutter]);

  // A hidden view has no size and so no scroll position: where it was is
  // remembered, and put back when the view is shown again.
  const place = useRef({ left: 0, top: 0 });
  useEffect(() => {
    const element = scroller.current;
    if (!element) return;
    const remember = () => {
      if (element.clientHeight > 0) {
        place.current = { left: element.scrollLeft, top: element.scrollTop };
      }
    };
    element.addEventListener("scroll", remember, { passive: true });
    return () => element.removeEventListener("scroll", remember);
  }, []);
  useLayoutEffect(() => {
    const element = scroller.current;
    if (!active || !element) return;
    if (element.scrollTop !== place.current.top) element.scrollTop = place.current.top;
    if (element.scrollLeft !== place.current.left) element.scrollLeft = place.current.left;
  }, [active]);

  const shownRows = rows.getVirtualItems();
  const shownColumns = across.getVirtualItems();
  const firstRow = shownRows[0];
  const lastRow = shownRows[shownRows.length - 1];
  const rowSpan = firstRow && lastRow ? ([firstRow.index, lastRow.index] as const) : null;

  const bounds = selectionBounds(selection);
  const range = bounds.top !== bounds.bottom || bounds.left !== bounds.right;

  // What the pointer and the keyboard handlers need of the latest render.
  const live = useRef({ selection, rowCount, columnCount: columns.length, shownColumns, onSelect });
  live.current = { selection, rowCount, columnCount: columns.length, shownColumns, onSelect };

  const move = (to: CellAddress, extend: boolean) => {
    const focus = clampAddress(to, rowCount, columns.length);
    onSelect(extend ? { anchor: selection.anchor, focus } : single(focus));
    rows.scrollToIndex(focus.row, { align: "auto" });
    across.scrollToIndex(focus.column, { align: "auto" });
  };

  const onKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    // A key pressed in a control inside the grid (a header button) is its own.
    if (event.target !== event.currentTarget) return;
    if (rowCount === 0 || columns.length === 0) return;
    const { row, column } = selection.focus;
    const page = Math.max(Math.floor((scroller.current?.clientHeight ?? 0) / rowHeight) - 2, 1);
    const jump = event.ctrlKey || event.metaKey;
    const [lastRowIndex, lastColumn] = [rowCount - 1, columns.length - 1];
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
      const row = within(Math.floor((y - HEADER_HEIGHT) / rowHeight), 0, rowCount - 1);
      const hit =
        shownColumns.find((item) => x >= item.start && x < item.end) ??
        (x < first.start ? first : last);
      return { row, column: hit.index };
    },
    [gutter, rowHeight],
  );

  const stopDragging = useCallback(() => {
    const held = drag.current;
    drag.current = null;
    cancelAnimationFrame(frame.current);
    setDragging(false);
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
  useEffect(() => {
    if (!active) stopDragging();
  }, [active, stopDragging]);

  const onPointerDown = (event: PointerEvent<HTMLDivElement>) => {
    const element = scroller.current;
    if (!element || event.button !== 0) return;
    const target = event.target as HTMLElement;
    // The header has its own clicks; a scroll bar is not a cell.
    if (target.closest(".grid-head")) return;
    const box = element.getBoundingClientRect();
    if (
      event.clientX - box.left >= element.clientWidth ||
      event.clientY - box.top >= element.clientHeight
    ) {
      return;
    }
    const address = addressAt(event.clientX, event.clientY);
    if (!address) return;
    // A button inside a cell is pressed, not dragged from: its cell is
    // selected and the button gets its click.
    if (target.closest("button")) {
      onSelect(single(address));
      return;
    }
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
    setDragging(true);
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

  return {
    scroller,
    columns,
    rowCount,
    gutter,
    rowHeight,
    shownRows,
    shownColumns,
    rowSpan,
    width: across.getTotalSize(),
    height: rows.getTotalSize(),
    selection,
    bounds,
    range,
    move,
    resizing,
    setResizing,
    dragging,
    handlers: {
      onKeyDown,
      onPointerDown,
      onPointerMove,
      onPointerUp: stopDragging,
      onPointerCancel: stopDragging,
      onLostPointerCapture: stopDragging,
    },
  };
}
