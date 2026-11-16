// The table behind the grid: TanStack Table holds the columns and what the
// user did to them (sizes, which are shown). It does not hold rows. A file
// can have hundreds of thousands of them, so they stay on the server and
// the grid asks for the ones near the screen (`useRowBlocks`); filtering
// happens there too.

import {
  type ColumnDef,
  columnResizingFeature,
  columnSizingFeature,
  columnVisibilityFeature,
  metaHelper,
  tableFeatures,
  useTable,
} from "@tanstack/react-table";
import { useMemo } from "react";

import type { Variable } from "../../api/client";
import { MAX_DRAGGED_WIDTH, MIN_WIDTH, defaultWidth } from "./columns";

/** What a column carries besides its id: the variable it shows. */
interface ColumnMeta {
  variable: Variable;
}

const features = tableFeatures({
  columnSizingFeature,
  columnResizingFeature,
  columnVisibilityFeature,
  columnMeta: metaHelper<ColumnMeta>(),
});

/** Rows never enter the table. */
type NoRow = Record<string, never>;
const NO_ROWS: NoRow[] = [];

const DEFAULT_COLUMN = { minSize: MIN_WIDTH, maxSize: MAX_DRAGGED_WIDTH };

/** One column per variable, in file order; a column's id is the variable's index. */
export function useDataTable(variables: Variable[]) {
  const columns = useMemo(
    () =>
      variables.map(
        (variable): ColumnDef<typeof features, NoRow> => ({
          id: String(variable.index),
          header: variable.name,
          // From the file's metadata, never from the rows on screen.
          size: defaultWidth(variable),
          meta: { variable },
        }),
      ),
    [variables],
  );
  return useTable({
    features,
    columns,
    data: NO_ROWS,
    defaultColumn: DEFAULT_COLUMN,
    // A column follows the pointer while its edge is dragged.
    columnResizeMode: "onChange",
  });
}

export type DataTable = ReturnType<typeof useDataTable>;
export type DataColumn = ReturnType<DataTable["getAllLeafColumns"]>[number];

/** The variable a column shows. */
export function variableOf(column: DataColumn): Variable {
  // Every column is built above with its variable.
  return (column.columnDef.meta as ColumnMeta).variable;
}
