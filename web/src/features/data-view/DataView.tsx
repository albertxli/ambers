import { useQuery } from "@tanstack/react-query";
import { type MouseEvent, useEffect, useMemo, useRef, useState } from "react";

import { ApiFailure, api } from "../../api/client";
import type { CellMode, DatasetInfo, Variable } from "../../api/client";
import { Icon } from "../../components/Icon";
import { count } from "../../lib/format";
import { FilterBar } from "../filters/FilterBar";
import { type FilterGroup, NO_FILTERS, problemsByRule, toFilters } from "../filters/filters";
import type { Shown } from "./blocks";
import { ColumnsMenu } from "./ColumnsMenu";
import { type CopyState, canCopy, rangeText } from "./copy";
import { Grid } from "./Grid";
import { Inspector } from "./Inspector";
import {
  type CellSelection,
  FIRST_CELL,
  cellsIn,
  clampSelection,
  describe,
  selectionBounds,
} from "./selection";
import { ShortcutsPanel } from "./ShortcutsPanel";
import { useDataTable, variableOf } from "./useDataTable";
import "./data-view.css";

const MODES: { value: CellMode; label: string; hint: string }[] = [
  { value: "labels", label: "Labels", hint: "Value labels; the value itself when it has none" },
  { value: "codes", label: "Codes", hint: "Stored values" },
  { value: "both", label: "Both", hint: "The stored value, then its label" },
];

/** Until the variables have arrived. */
const NO_VARIABLES: Variable[] = [];

/**
 * For a toolbar button pressed with the mouse: the keyboard focus stays
 * where it was, so the arrow keys keep moving in the grid after a click
 * on Labels / Codes / Both. The buttons are still reached with Tab.
 */
function keepFocus(event: MouseEvent) {
  event.preventDefault();
}

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

/** Whether a key press is being typed into a field. */
function typing(target: EventTarget | null): boolean {
  return target instanceof HTMLElement && target.closest("input, textarea, select") !== null;
}

/**
 * Data View: one row per respondent, one column per variable, like SPSS's
 * Data View. The data stays on the server; this screen asks for what it
 * shows, and for the rows its filters keep.
 */
export function DataView({ dataset }: { dataset: string }) {
  const [mode, setMode] = useState<CellMode>("labels");
  const [filters, setFilters] = useState<FilterGroup>(NO_FILTERS);
  const [selection, setSelection] = useState<CellSelection>(FIRST_CELL);
  const [inspector, setInspector] = useState(true);
  const [copyState, setCopyState] = useState<CopyState>("idle");
  const [shortcuts, setShortcuts] = useState(false);

  const info = useQuery({
    queryKey: ["dataset", dataset],
    queryFn: () => api.dataset(dataset),
    // While the file loads, the row count is followed closely.
    refetchInterval: (query) => (query.state.data?.load === "loading" ? 400 : false),
  });
  const loading = info.data?.load === "loading";
  const variables = useQuery({
    queryKey: ["variables", dataset],
    queryFn: () => api.variables(dataset),
    // Types and counts settle as rows arrive.
    refetchInterval: loading ? 2500 : false,
  });
  // Once more when loading ends, for the final counts.
  const finished = info.data ? info.data.load !== "loading" : false;
  const refetchVariables = variables.refetch;
  useEffect(() => {
    if (finished) void refetchVariables();
  }, [finished, refetchVariables]);

  const list = variables.data?.variables ?? NO_VARIABLES;
  const table = useDataTable(list);

  // What the rows shown depend on: the mode and the filters.
  const shown = useMemo<Shown>(
    () => ({ mode, filters: toFilters(filters), filterMode: filters.mode }),
    [mode, filters],
  );
  const filtered = filters.rules.length > 0;
  const loaded = info.data?.rows ?? 0;
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

  // `?` shows the keys, wherever the focus is, unless something is typed.
  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "?" && !event.ctrlKey && !event.metaKey && !typing(event.target)) {
        event.preventDefault();
        setShortcuts((was) => !was);
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, []);

  const failure = info.error ?? variables.error;
  if (failure) {
    const gone = failure instanceof ApiFailure && failure.status === 404;
    return (
      <div className="notice is-error">
        <h1>{gone ? "This file is no longer open" : "The file could not be shown"}</h1>
        <p>{failure.message}</p>
      </div>
    );
  }
  if (!info.data || !variables.data) {
    return (
      <div className="notice">
        <p>Opening…</p>
      </div>
    );
  }

  const data = info.data;
  const status = loadText(data);
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
      <header className="topbar">
        <div className="file-identity">
          <a className="viewer-back" href="#/files" aria-label="Back to the data files">
            <Icon name="arrow-left" size={17} />
            <span>Files</span>
          </a>
          <div className="file-icon" aria-hidden="true">
            S
          </div>
          <div>
            <div className="title-row">
              <h1 title={data.path}>{data.name}</h1>
              <span className="file-badge">
                {data.name.toLowerCase().endsWith(".zsav") ? "ZSAV" : "SAV"}
              </span>
            </div>
            <p>
              {count(data.total_rows ?? data.rows)} rows <span>·</span> {count(data.variables)}{" "}
              variables
              {data.label && (
                <>
                  {" "}
                  <span>·</span> {data.label}
                </>
              )}
            </p>
          </div>
        </div>
        {data.warnings.length > 0 && (
          <span className="file-warning" title={data.warnings.join("\n")}>
            {data.warnings.length} {data.warnings.length === 1 ? "warning" : "warnings"}
          </span>
        )}
      </header>

      <div className="toolbar">
        <FilterBar
          dataset={dataset}
          variables={list}
          offered={variables.data.operators}
          filters={filters}
          onChange={changeFilters}
          kept={kept}
          loaded={loaded}
          problems={problems}
        />
        <div className="segmented" role="group" aria-label="What cells show">
          {MODES.map((choice) => (
            <button
              key={choice.value}
              aria-pressed={mode === choice.value}
              title={choice.hint}
              onMouseDown={keepFocus}
              onClick={() => {
                setMode(choice.value);
                setCopyState("idle");
              }}
            >
              {choice.label}
            </button>
          ))}
        </div>
        <ColumnsMenu table={table} />
        <span className="grid-hint">
          Drag to select <span>·</span> Shift + click to extend
        </span>
      </div>

      {data.rows === 0 ? (
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
        <Grid
          dataset={dataset}
          table={table}
          variableCount={list.length}
          rowCount={rowCount}
          loaded={loaded}
          shown={shown}
          selection={selected}
          onSelect={select}
          onCopy={copy}
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
          countedRows={variables.data.counted_rows}
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
          <strong>{count(data.variables)}</strong>
        </div>
        {data.unlabeled_variables > 0 && (
          <div className="status-group is-warn">
            {count(data.unlabeled_variables)}{" "}
            {data.unlabeled_variables === 1 ? "variable has" : "variables have"} unlabelled values
          </div>
        )}
        {cells > 1 && (
          <div className="selection-summary" aria-live="polite">
            <strong>{rangeName}</strong>
            <span>{count(cells)} cells</span>
          </div>
        )}
        <div className="status-spacer" />
        <button onClick={() => setShortcuts(true)}>Keyboard shortcuts</button>
      </footer>

      {shortcuts && <ShortcutsPanel onClose={() => setShortcuts(false)} />}
    </>
  );
}
