// The table behind the Data View's grid: TanStack Table holds the columns
// and what the user did to them (sizes, which are shown). It does not hold
// rows. A file can have hundreds of thousands of them, so they stay on the
// server and the grid asks for the ones near the screen (`useRowBlocks`);
// filtering happens there too.

import {
  type ColumnDef,
  columnSizingFeature,
  columnVisibilityFeature,
  metaHelper,
  tableFeatures,
  useTable,
} from "@tanstack/react-table";
import { useMemo } from "react";

import type { CellMode, Variable } from "../../api/client";
import { MAX_COLUMN, MIN_COLUMN } from "../../components/grid/useGrid";
import { defaultWidth } from "./columns";

/** What a column carries besides its id: the variable it shows. */
interface ColumnMeta {
  variable: Variable;
}

const features = tableFeatures({
  columnSizingFeature,
  columnVisibilityFeature,
  columnMeta: metaHelper<ColumnMeta>(),
});

/** Rows never enter the table. */
type NoRow = Record<string, never>;
const NO_ROWS: NoRow[] = [];

const DEFAULT_COLUMN = { minSize: MIN_COLUMN, maxSize: MAX_COLUMN };

/**
 * One column per variable, in file order; a column's id is the variable's
 * index. A column's own width depends on the mode (Both needs room for
 * the code beside a label); a width the user set stays whatever the mode.
 */
export function useDataTable(variables: Variable[], mode: CellMode) {
  // Labels and Codes share their widths, so switching between them moves nothing.
  const wide = mode === "both";
  const columns = useMemo(
    () =>
      variables.map(
        (variable): ColumnDef<typeof features, NoRow> => ({
          id: String(variable.index),
          header: variable.name,
          // From the file's metadata, never from the rows on screen.
          size: defaultWidth(variable, wide ? "both" : "labels"),
          meta: { variable },
        }),
      ),
    [variables, wide],
  );
  return useTable({ features, columns, data: NO_ROWS, defaultColumn: DEFAULT_COLUMN });
}

export type DataTable = ReturnType<typeof useDataTable>;
export type DataColumn = ReturnType<DataTable["getAllLeafColumns"]>[number];

/** The variable a column shows. */
export function variableOf(column: DataColumn): Variable {
  // Every column is built above with its variable.
  return (column.columnDef.meta as ColumnMeta).variable;
}
