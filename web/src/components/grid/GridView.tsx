import { type AriaAttributes, type ReactNode, useId } from "react";

import { ColumnResizeHandle } from "./ColumnResizeHandle";
import { contains } from "./selection";
import { type GridColumn, type GridState, HEADER_HEIGHT } from "./useGrid";
import "./grid.css";

/** What a view puts in one cell. */
export interface CellView {
  content: ReactNode;
  /** Extra class names of the cell. */
  className?: string | false | null;
  /** The whole text, for a cell that may be cut at its edge. */
  title?: string;
}

interface Props {
  grid: GridState;
  /** What the grid is, for assistive technology. */
  label: string;
  className?: string;
  /** The content of a column's header. */
  renderHeader: (column: GridColumn, index: number) => ReactNode;
  /** A column's name, said by its resize handle. */
  columnName: (column: GridColumn) => string;
  headerSort?: (column: GridColumn) => AriaAttributes["aria-sort"];
  /** A click on a header, outside its controls. */
  onHeaderClick?: (index: number) => void;
  renderNumber: (row: number) => ReactNode;
  renderCell: (row: number, column: GridColumn, index: number) => CellView;
  /** A new width for a column; `null` puts it back to its own width. */
  onResize: (column: GridColumn, width: number | null) => void;
}

/**
 * The grid as it is drawn: a sticky header with resize handles, sticky
 * row numbers, and the cells near the screen with the selection on them.
 */
export function GridView(props: Props) {
  const { grid, renderHeader, renderNumber, renderCell } = props;
  const id = useId();
  const { bounds, range, selection, gutter } = grid;

  return (
    <div
      ref={grid.scroller}
      id={id}
      className={`grid ${props.className ?? ""} ${grid.resizing ? "is-column-resizing" : ""}`}
      tabIndex={0}
      role="grid"
      aria-label={props.label}
      aria-multiselectable="true"
      aria-rowcount={grid.rowCount}
      aria-colcount={grid.columns.length}
      {...grid.handlers}
    >
      <div className="grid-canvas" style={{ width: grid.width, height: grid.height }}>
        <div className="grid-head" style={{ height: HEADER_HEIGHT }} role="row">
          <div className="grid-corner" style={{ width: gutter }}>
            #
          </div>
          {grid.shownColumns.map((item) => {
            const column = grid.columns[item.index];
            if (!column) return null;
            const selected =
              grid.rowCount > 0 && item.index >= bounds.left && item.index <= bounds.right;
            const onHeaderClick = props.onHeaderClick;
            return (
              <div
                key={item.key}
                className={`grid-th ${selected ? "is-selected" : ""}`}
                style={{ left: item.start, width: item.size }}
                role="columnheader"
                aria-sort={props.headerSort?.(column)}
                onClick={onHeaderClick && (() => onHeaderClick(item.index))}
              >
                {renderHeader(column, item.index)}
                <ColumnResizeHandle
                  name={props.columnName(column)}
                  size={column.size}
                  controls={id}
                  onResize={(width) => props.onResize(column, width)}
                  onDragging={(dragging) => grid.setResizing(dragging ? column.id : null)}
                />
              </div>
            );
          })}
        </div>

        {grid.shownRows.map((row) => {
          const inRows = row.index >= bounds.top && row.index <= bounds.bottom;
          const current = row.index === selection.anchor.row;
          // The band across the highlighted row helps reading one cell; a
          // selected block has its own tint.
          const band = current && !range;
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
                role="rowheader"
              >
                {renderNumber(row.index)}
              </div>
              {grid.shownColumns.map((item) => {
                const column = grid.columns[item.index];
                if (!column) return null;
                const cell = renderCell(row.index, column, item.index);
                const active = current && item.index === selection.anchor.column;
                const inside = contains(bounds, row.index, item.index);
                const classes = [
                  "grid-cell",
                  active && "is-active",
                  range && inside && "in-range",
                  range && inside && row.index === bounds.top && "range-top",
                  range && inside && row.index === bounds.bottom && "range-bottom",
                  range && inside && item.index === bounds.left && "range-left",
                  range && inside && item.index === bounds.right && "range-right",
                  cell.className,
                ];
                return (
                  <div
                    key={item.key}
                    className={classes.filter(Boolean).join(" ")}
                    style={{ left: item.start, width: item.size }}
                    role="gridcell"
                    aria-selected={inside}
                    aria-colindex={item.index + 1}
                    title={cell.title}
                  >
                    {cell.content}
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
