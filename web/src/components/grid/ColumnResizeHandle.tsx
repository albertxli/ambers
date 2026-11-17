import { type PointerEvent, useCallback, useEffect, useRef } from "react";

import { MAX_COLUMN, MIN_COLUMN } from "./useGrid";

interface Props {
  /** The column's name, for assistive technology. */
  name: string;
  size: number;
  /** The id of the grid the handle belongs to. */
  controls: string;
  /** A new width in pixels; `null` puts the column back to its own width. */
  onResize: (width: number | null) => void;
  /** The handle is being dragged, or was let go. */
  onDragging: (dragging: boolean) => void;
}

/** A width a column can have. */
export function columnWidth(width: number): number {
  return Math.max(MIN_COLUMN, Math.min(MAX_COLUMN, Math.round(width)));
}

/** The width a key press on the handle asks for; `null` resets, `undefined` is not a resize key. */
export function widthForKey(key: string, shift: boolean, size: number): number | null | undefined {
  switch (key) {
    case "ArrowLeft":
      return columnWidth(size - (shift ? 50 : 10));
    case "ArrowRight":
      return columnWidth(size + (shift ? 50 : 10));
    case "Home":
      return MIN_COLUMN;
    case "End":
      return MAX_COLUMN;
    case "Enter":
    case " ":
      return null;
    default:
      return undefined;
  }
}

/**
 * The right edge of a column header. Drag it to resize the column; a
 * double-click or Enter puts the column back to its own width; with the
 * keyboard, the arrow keys resize by 10 pixels (50 with Shift).
 */
export function ColumnResizeHandle({ name, size, controls, onResize, onDragging }: Props) {
  const element = useRef<HTMLDivElement>(null);
  const drag = useRef<{ pointerId: number; startX: number; startSize: number } | null>(null);
  const dragging = useRef(onDragging);
  dragging.current = onDragging;

  const finish = useCallback(() => {
    const held = drag.current;
    if (!held) return;
    drag.current = null;
    if (element.current?.hasPointerCapture(held.pointerId)) {
      element.current.releasePointerCapture(held.pointerId);
    }
    document.body.classList.remove("is-resizing-columns");
    element.current?.classList.remove("is-resizing");
    dragging.current(false);
  }, []);

  useEffect(() => {
    window.addEventListener("blur", finish);
    return () => {
      window.removeEventListener("blur", finish);
      finish();
    };
  }, [finish]);

  const follow = (event: PointerEvent<HTMLDivElement>) => {
    const held = drag.current;
    if (!held || event.pointerId !== held.pointerId) return;
    event.preventDefault();
    event.stopPropagation();
    onResize(columnWidth(held.startSize + event.clientX - held.startX));
  };

  return (
    <div
      ref={element}
      role="separator"
      aria-orientation="vertical"
      aria-label={`Resize ${name} column`}
      aria-controls={controls}
      aria-valuemin={MIN_COLUMN}
      aria-valuemax={MAX_COLUMN}
      aria-valuenow={Math.round(size)}
      aria-valuetext={`${Math.round(size)} pixels`}
      tabIndex={0}
      className="column-resize-handle"
      title="Drag to resize · Double-click or press Enter to reset"
      onPointerDown={(event) => {
        event.preventDefault();
        event.stopPropagation();
        if (event.button !== 0 || !event.isPrimary || drag.current) return;
        event.currentTarget.focus({ preventScroll: true });
        event.currentTarget.setPointerCapture(event.pointerId);
        event.currentTarget.classList.add("is-resizing");
        drag.current = { pointerId: event.pointerId, startX: event.clientX, startSize: size };
        document.body.classList.add("is-resizing-columns");
        onDragging(true);
      }}
      onPointerMove={follow}
      onPointerUp={(event) => {
        follow(event);
        finish();
      }}
      onPointerCancel={finish}
      onLostPointerCapture={finish}
      onBlur={finish}
      onClick={(event) => event.stopPropagation()}
      onDoubleClick={(event) => {
        event.preventDefault();
        event.stopPropagation();
        onResize(null);
      }}
      onKeyDown={(event) => {
        const width = widthForKey(event.key, event.shiftKey, size);
        if (width === undefined) return;
        event.preventDefault();
        event.stopPropagation();
        onResize(width);
      }}
    />
  );
}
