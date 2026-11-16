import { useQueries, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo } from "react";

import { api } from "../../api/client";
import type { RowsPage } from "../../api/client";
import {
  type Found,
  type Shown,
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
  shown: Shown;
  /** The rows on screen (with their overscan), as positions among the rows shown. */
  rows: Span;
  /** The variables on screen, by position in the file. */
  variables: readonly number[];
  /** Rows loaded so far: when it grows, short blocks are asked for again. */
  loaded: number;
}

/** What blocks of rows are cached under, for one way of showing a dataset. */
export function rowsScope(dataset: string, shown: Shown) {
  return ["rows", dataset, shown.mode, shown.filterMode, JSON.stringify(shown.filters)] as const;
}

/**
 * The cells near the screen, fetched block by block and kept while the
 * page is open. Returns how to look a cell up by its row (a position
 * among the rows shown) and its variable (a position in the file); a cell
 * that has not arrived yet is `undefined`.
 */
export function useRowBlocks(wanted: Wanted): (row: number, variable: number) => Found | undefined {
  const { dataset, variableCount, shown, rows, variables, loaded } = wanted;
  const filterKey = JSON.stringify(shown.filters);
  const scope = useMemo(
    () => rowsScope(dataset, shown),
    // The filters are compared by what they say, not by their array.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [dataset, shown.mode, shown.filterMode, filterKey],
  );
  const variableKey = variables.join(",");
  const blocks = useMemo(
    () => blocksFor(rows, variables),
    // The spans are new arrays on every render; their content is what matters.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [rows?.[0], rows?.[1], variableKey],
  );

  const pages = useQueries({
    queries: blocks.map((block) => ({
      queryKey: [...scope, block.row, block.column],
      queryFn: () => api.rows(dataset, blockRequest(block, variableCount, shown)),
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

  return useMemo(() => (row: number, variable: number) => findCell(pages, row, variable), [pages]);
}
