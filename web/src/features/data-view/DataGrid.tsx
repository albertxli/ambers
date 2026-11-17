import { useMemo } from "react";

import { useVariableCard } from "../../components/VariableCard";
import { type CellView, GridView } from "../../components/grid/GridView";
import type { CellSelection } from "../../components/grid/selection";
import { type GridColumn, useGrid } from "../../components/grid/useGrid";
import type { Variable } from "../../api/client";
import { count } from "../../lib/format";
import type { Shown } from "./blocks";
import { gutterWidth, typeTag } from "./columns";
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
  /** False while the Data View is hidden behind another view. */
  active: boolean;
  /** Every variable of the file, by position: what a header's card describes. */
  variables: readonly Variable[];
  /** Rows the counts cover, and the same in words. */
  countedRows: number;
  scope: string;
}

/** Below this width a header shows the name only. */
const COMPACT = 80;

/**
 * The Data View's grid. Only the rows and columns near the screen exist
 * in the page, whatever the size of the file; their cells come from the
 * server in blocks (`useRowBlocks`). The columns come from the table.
 */
export function DataGrid(props: Props) {
  const { dataset, table, variableCount, rowCount, loaded, shown, selection } = props;
  const leaf = table.getVisibleLeafColumns();
  const { columnSizing } = table.state;
  const columns = useMemo<GridColumn[]>(
    () => leaf.map((column) => ({ id: column.id, size: column.getSize() })),
    // `columnSizing` is not read, but sizes change with it and `leaf` does not.
    [leaf, columnSizing],
  );

  const grid = useGrid({
    columns,
    rowCount,
    gutter: gutterWidth(loaded),
    selection,
    onSelect: props.onSelect,
    onCopy: props.onCopy,
    active: props.active,
  });

  /** The file position of the variable in each column on screen. */
  const variables = grid.shownColumns.map((item) => Number(columns[item.index]?.id ?? 0));
  const cellAt = useRowBlocks({
    dataset,
    variableCount,
    shown,
    rows: grid.rowSpan,
    variables,
    loaded,
  });

  // Without filters a row's number is its position, known before it arrives.
  const filtered = shown.filters.length > 0;
  const both = shown.mode === "both";
  const firstVariable = variables[0] ?? 0;

  // One card for all the headers; not while an edge or a block is dragged.
  const card = useVariableCard({
    variables: props.variables,
    countedRows: props.countedRows,
    scope: props.scope,
    enabled: props.active && grid.resizing === null && !grid.dragging,
  });
  const variableAt = (column: GridColumn) => {
    const found = table.getColumn(column.id);
    return found && variableOf(found);
  };

  return (
    <>
    <GridView
      grid={grid}
      label="Data"
      columnName={(column) => variableAt(column)?.name ?? column.id}
      onHeaderClick={(index) => grid.move({ row: selection.anchor.row, column: index }, false)}
      onResize={(column, width) => {
        const found = table.getColumn(column.id);
        if (width === null) found?.resetSize();
        else table.setColumnSizing((sizes) => ({ ...sizes, [column.id]: width }));
      }}
      renderHeader={(column) => {
        const variable = variableAt(column);
        if (!variable) return null;
        return (
          <button
            type="button"
            // Reached with the pointer; the keyboard stays with the cells.
            tabIndex={-1}
            className={`grid-th-button has-card ${column.size < COMPACT ? "is-compact" : ""}`}
            aria-label={`${variable.name}: variable information`}
            {...card.trigger(variable)}
          >
            <span className="grid-th-name">{variable.name}</span>
            <span className="grid-th-type">{typeTag(variable)}</span>
          </button>
        );
      }}
      renderNumber={(row) => {
        const first = cellAt(row, firstVariable);
        return first ? count(first.number) : filtered ? "" : count(row + 1);
      }}
      renderCell={(row, column): CellView => {
        const found = cellAt(row, Number(column.id));
        if (!found) return { content: <span className="grid-wait" /> };
        const { text, code, flag } = found.cell;
        return {
          content:
            both && code !== undefined ? (
              <span className="cell-value-both">
                <span>{text}</span>
                <span className="cell-value-code">{code}</span>
              </span>
            ) : (
              text
            ),
          className: [flag && `is-${flag}`, text === "." && "is-missing"].filter(Boolean).join(" "),
        };
      }}
    />
    {card.card}
    </>
  );
}
