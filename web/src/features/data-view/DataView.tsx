import { useQuery } from "@tanstack/react-query";
import { useMemo, useRef, useState } from "react";

import { api } from "../../api/client";
import type { CellMode, DatasetInfo } from "../../api/client";
import type { ViewProps } from "../../app/views";
import { Icon } from "../../components/Icon";
import {
  type CellSelection,
  FIRST_CELL,
  cellsIn,
  clampSelection,
  describe,
  selectionBounds,
} from "../../components/grid/selection";
import { count } from "../../lib/format";
import { countsScope } from "../../lib/variable";
import { FilterBar } from "../filters/FilterBar";
import { type FilterGroup, NO_FILTERS, problemsByRule, toFilters } from "../filters/filters";
import type { Shown } from "./blocks";
import { ColumnsMenu } from "./ColumnsMenu";
import { type CopyState, canCopy, rangeText } from "./copy";
import { DataGrid } from "./DataGrid";
import { Inspector } from "./Inspector";
import { useDataTable, variableOf } from "./useDataTable";
import { ValueDisplaySwitch } from "./ValueDisplaySwitch";
import "./data-view.css";

/** What the status bar says about loading. */
function loadText(info: DatasetInfo): { text: string; tone: "ok" | "busy" | "warn" } {
  const rows = count(info.rows);
  const total = info.total_rows === null ? "" : ` of ${count(info.total_rows)}`;
  switch (info.load) {
    case "loading":
      return { text: `Loading ${rows}${total} rows…`, tone: "busy" };
    case "limit":
      return { text: `First ${rows}${total} rows (row limit)`, tone: "warn" };
    case "memory":
      return { text: `First ${rows}${total} rows: memory budget reached`, tone: "warn" };
    case "failed":
      return { text: `Stopped at row ${rows}: ${info.load_error ?? "read error"}`, tone: "warn" };
    default:
      return { text: "Ready", tone: "ok" };
  }
}

/**
 * Data View: one row per respondent, one column per variable, like SPSS's
 * Data View. The data stays on the server; this screen asks for what it
 * shows, and for the rows its filters keep.
 */
export function DataView({ dataset, info, variables, active, onShortcuts }: ViewProps) {
  const [mode, setMode] = useState<CellMode>("labels");
  const [filters, setFilters] = useState<FilterGroup>(NO_FILTERS);
  const [selection, setSelection] = useState<CellSelection>(FIRST_CELL);
  const [inspector, setInspector] = useState(true);
  const [copyState, setCopyState] = useState<CopyState>("idle");

  const loading = info.load === "loading";
  const list = variables.variables;
  const table = useDataTable(list, mode);
  // Without value labels the three modes show the same.
  const labelled = useMemo(() => list.some((variable) => variable.values.length > 0), [list]);

  // What the rows shown depend on: the mode and the filters.
  const shown = useMemo<Shown>(
    () => ({ mode, filters: toFilters(filters), filterMode: filters.mode }),
    [mode, filters],
  );
  const filtered = filters.rules.length > 0;
  const loaded = info.rows;
  // How many rows the filters keep, asked again as rows load.
  const filterKey = JSON.stringify(shown.filters);
  const keptRows = useQuery({
    queryKey: ["kept", dataset, shown.filterMode, filterKey, loaded],
    queryFn: () =>
      api.rows(dataset, {
        offset: 0,
        limit: 0,
        variables: [0],
        mode: "codes",
        filters: shown.filters,
        filter_mode: shown.filterMode,
      }),
    enabled: filtered,
    // The last count stays while only the number of loaded rows changed.
    placeholderData: (previous, query) =>
      query?.queryKey[2] === shown.filterMode && query?.queryKey[3] === filterKey
        ? previous
        : undefined,
  });
  const kept = filtered ? (keptRows.data?.shown ?? null) : loaded;
  const rowCount = kept ?? 0;
  const problems = useMemo(
    () => problemsByRule(filters, keptRows.data?.problems ?? []),
    [filters, keptRows.data],
  );

  const leaf = table.getVisibleLeafColumns();
  // A filter or a hidden variable may have made the grid smaller.
  const selected = clampSelection(selection, rowCount, leaf.length);
  const bounds = selectionBounds(selected);

  const copied = useRef(0);
  const select = (next: CellSelection) => {
    setSelection(next);
    setInspector(true);
    setCopyState("idle");
  };
  const changeFilters = (next: FilterGroup) => {
    setFilters(next);
    setSelection(FIRST_CELL);
    setCopyState("idle");
  };
  const copy = async () => {
    if (rowCount === 0 || leaf.length === 0) return;
    if (!canCopy(bounds)) {
      setCopyState("too-many");
      return;
    }
    const columns = leaf.slice(bounds.left, bounds.right + 1).map((column) => Number(column.id));
    setCopyState("copying");
    window.clearTimeout(copied.current);
    try {
      const text = await rangeText(dataset, bounds, columns, shown);
      await navigator.clipboard.writeText(text);
      setCopyState("copied");
    } catch {
      setCopyState("failed");
    }
    copied.current = window.setTimeout(() => setCopyState("idle"), 1600);
  };

  const status = loadText(info);
  const anchorColumn = leaf[selected.anchor.column];
  const current = anchorColumn ? variableOf(anchorColumn) : undefined;
  const firstColumn = leaf[bounds.left];
  const lastColumn = leaf[bounds.right];
  const cells = rowCount > 0 && leaf.length > 0 ? cellsIn(bounds) : 0;
  const rangeName =
    firstColumn && lastColumn
      ? describe(
          bounds,
          { first: variableOf(firstColumn).name, last: variableOf(lastColumn).name },
          // Without filters a row's position is its number in the file.
          filtered ? null : { first: bounds.top + 1, last: bounds.bottom + 1 },
        )
      : "";

  return (
    <>
      <div className="toolbar">
        <FilterBar
          dataset={dataset}
          variables={list}
          offered={variables.operators}
          active={active}
          filters={filters}
          onChange={changeFilters}
          kept={kept}
          loaded={loaded}
          problems={problems}
        />
        <div className="toolbar-actions">
          <ValueDisplaySwitch
            value={mode}
            disabled={!labelled}
            onChange={(next) => {
              setMode(next);
              setCopyState("idle");
            }}
          />
          <ColumnsMenu table={table} />
        </div>
      </div>

      {info.rows === 0 ? (
        <div className="notice">
          <p>{loading ? "Loading…" : "This file has no rows."}</p>
        </div>
      ) : leaf.length === 0 ? (
        <div className="grid-filter-empty" role="status">
          <Icon name="columns" size={25} />
          <strong>No columns are shown</strong>
          <p>Every variable of this file is hidden.</p>
          <button onClick={() => table.toggleAllColumnsVisible(true)}>
            Show all columns
            <Icon name="arrow-right" size={14} />
          </button>
        </div>
      ) : kept === null ? (
        <div className="notice">
          <p>Filtering…</p>
        </div>
      ) : rowCount === 0 ? (
        <div className="grid-filter-empty" role="status">
          <Icon name="filter" size={25} />
          <strong>No matching rows</strong>
          <p>
            {loading
              ? "None of the rows loaded so far match; the file is still loading."
              : "Try another condition or clear your filters to start again."}
          </p>
          <button onClick={() => changeFilters(NO_FILTERS)}>
            Clear filters
            <Icon name="arrow-right" size={14} />
          </button>
        </div>
      ) : (
        <DataGrid
          dataset={dataset}
          table={table}
          variableCount={list.length}
          rowCount={rowCount}
          loaded={loaded}
          shown={shown}
          selection={selected}
          onSelect={select}
          onCopy={copy}
          active={active}
          variables={list}
          countedRows={variables.counted_rows}
          scope={countsScope(info, variables.counted_rows)}
        />
      )}

      {inspector && current && cells > 0 && (
        <Inspector
          dataset={dataset}
          variable={current}
          row={selected.anchor.row}
          bounds={bounds}
          rangeName={rangeName}
          shown={shown}
          countedRows={variables.counted_rows}
          copyState={copyState}
          onCopy={copy}
          onClose={() => setInspector(false)}
        />
      )}

      <footer className="statusbar">
        <div className={`status-group is-${status.tone}`}>
          <span className="status-dot" /> {status.text}
        </div>
        <div className="status-group">
          Row <strong>{count(rowCount === 0 ? 0 : selected.anchor.row + 1)}</strong> of{" "}
          <strong>{count(rowCount)}</strong>
          {filtered && " filtered"}
        </div>
        <div className="status-group">
          Variable <strong>{count(current ? current.index + 1 : 0)}</strong> of{" "}
          <strong>{count(info.variables)}</strong>
        </div>
        {info.unlabeled_variables > 0 && (
          <div className="status-group is-warn">
            {count(info.unlabeled_variables)}{" "}
            {info.unlabeled_variables === 1 ? "variable has" : "variables have"} unlabelled values
          </div>
        )}
        {cells > 1 && (
          <div className="selection-summary" aria-live="polite">
            <strong>{rangeName}</strong>
            <span>{count(cells)} cells</span>
          </div>
        )}
        <div className="status-spacer" />
        <button onClick={onShortcuts}>Keyboard shortcuts</button>
      </footer>
    </>
  );
}
