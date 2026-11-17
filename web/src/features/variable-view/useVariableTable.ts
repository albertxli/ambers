// The table behind the Variable View. Here TanStack Table does hold rows:
// they are the file's variables, which the page has anyway for the column
// headers of the Data View (a few hundred, not the data). It keeps the
// sort order and the column sizes; which variables are listed is decided
// before (`filterVariables`).

import {
  type ColumnDef,
  type SortingState,
  columnSizingFeature,
  createSortedRowModel,
  rowSortingFeature,
  tableFeatures,
  useTable,
} from "@tanstack/react-table";
import { useMemo } from "react";

import type { Variable } from "../../api/client";
import { MAX_COLUMN, MIN_COLUMN } from "../../components/grid/useGrid";
import { FIELDS, type FieldId, compareBy, fieldText } from "./variables";

const features = tableFeatures({
  columnSizingFeature,
  rowSortingFeature,
  sortedRowModel: createSortedRowModel(),
});

const DEFAULT_COLUMN = { minSize: MIN_COLUMN, maxSize: MAX_COLUMN };

const COLUMNS = FIELDS.map((field): ColumnDef<typeof features, Variable> => {
  const compare = compareBy(field.id);
  return {
    id: field.id,
    header: field.label,
    accessorFn: (variable) => fieldText(variable, field.id),
    size: field.width,
    // Ascending; the table turns it round for a descending sort.
    sortFn: (a, b) => compare(a.original, b.original),
    // A count is looked at from its largest.
    sortDescFirst: field.numeric === true,
  };
});

interface Options {
  /** The variables to list, in file order. */
  variables: Variable[];
  sorting: SortingState;
  onSorting: (sorting: SortingState) => void;
}

export function useVariableTable({ variables, sorting, onSorting }: Options) {
  const state = useMemo(() => ({ sorting }), [sorting]);
  return useTable({
    features,
    columns: COLUMNS,
    data: variables,
    defaultColumn: DEFAULT_COLUMN,
    getRowId: (variable) => String(variable.index),
    state,
    onSortingChange: (updater) =>
      onSorting(typeof updater === "function" ? updater(sorting) : updater),
  });
}

export type VariableTable = ReturnType<typeof useVariableTable>;

/** The field a column of the table shows. */
export function fieldOf(id: string): FieldId {
  return id as FieldId;
}
