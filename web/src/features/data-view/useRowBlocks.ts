import { useQueries, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo } from "react";

import { api } from "../../api/client";
import type { CellMode, Filter, RowsPage } from "../../api/client";
import {
  type Found,
  type Span,
  blockKey,
  blockRequest,
  blocksFor,
  findCell,
  isPartial,
} from "./blocks";

interface Wanted {
  dataset: string;
  variableCount: number;
  mode: CellMode;
  filters: Filter[];
  /** The rows and variables on screen (with their overscan). */
  rows: Span;
  columns: Span;
  /** Rows loaded so far: when it grows, short blocks are asked for again. */
  loaded: number;
}

/**
 * The cells near the screen, fetched block by block and kept while the
 * page is open. Returns how to look a cell up; a cell that has not
 * arrived yet is `undefined`.
 */
export function useRowBlocks(wanted: Wanted): (row: number, column: number) => Found | undefined {
  const { dataset, variableCount, mode, filters, rows, columns, loaded } = wanted;
  const filterKey = JSON.stringify(filters);
  const scope = useMemo(() => ["rows", dataset, mode, filterKey] as const, [dataset, mode, filterKey]);
  const blocks = useMemo(
    () => blocksFor(rows, columns),
    // The spans are new arrays on every render; their ends are what matter.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [rows?.[0], rows?.[1], columns?.[0], columns?.[1]],
  );

  const pages = useQueries({
    queries: blocks.map((block) => ({
      queryKey: [...scope, block.row, block.column],
      queryFn: () => api.rows(dataset, blockRequest(block, variableCount, mode, filters)),
      // A loaded row never changes: a block is good until rows are added.
      staleTime: Number.POSITIVE_INFINITY,
      gcTime: 120_000,
    })),
    combine: (results) => {
      const found = new Map<string, RowsPage>();
      results.forEach((result, i) => {
        const block = blocks[i];
        if (result.data && block) found.set(blockKey(block), result.data);
      });
      return found;
    },
  });

  // Rows arrive in file order, so only a block that was short can change.
  const client = useQueryClient();
  useEffect(() => {
    void client.invalidateQueries({
      predicate: (query) =>
        query.queryKey[0] === "rows" &&
        query.queryKey[1] === dataset &&
        isPartial(query.state.data as RowsPage | undefined),
    });
  }, [client, dataset, loaded]);

  return useMemo(() => (row: number, column: number) => findCell(pages, row, column), [pages]);
}
