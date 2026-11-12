import { useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";

import { ApiFailure, api } from "../../api/client";
import type { CellMode, DatasetInfo } from "../../api/client";
import { count } from "../../lib/format";
import { Grid, type Selection } from "./Grid";
import { Inspector } from "./Inspector";
import { defaultWidth } from "./columns";
import "./data-view.css";

const MODES: { value: CellMode; label: string; hint: string }[] = [
  { value: "labels", label: "Labels", hint: "Value labels; the value itself when it has none" },
  { value: "codes", label: "Codes", hint: "Stored values" },
  { value: "both", label: "Both", hint: "The stored value, then its label" },
];

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
 * shows.
 */
export function DataView({ dataset }: { dataset: string }) {
  const [mode, setMode] = useState<CellMode>("labels");
  const [compact, setCompact] = useState(true);
  const [selection, setSelection] = useState<Selection>({ row: 0, variable: 0 });
  const [inspector, setInspector] = useState(true);
  /** Widths the user set, by variable. */
  const [resized, setResized] = useState<Record<number, number>>({});

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

  const list = variables.data?.variables;
  const widths = useMemo(
    () => (list ?? []).map((variable) => resized[variable.index] ?? defaultWidth(variable)),
    [list, resized],
  );

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
  if (!info.data || !list) {
    return (
      <div className="notice">
        <p>Opening…</p>
      </div>
    );
  }

  const data = info.data;
  const current = list[selection.variable];
  const status = loadText(data);
  const onResize = (variable: number, width: number | null) =>
    setResized((previous) => {
      const next = { ...previous };
      if (width === null) delete next[variable];
      else next[variable] = width;
      return next;
    });

  return (
    <>
      <header className="topbar">
        <div className="file-identity">
          <div className="file-icon" aria-hidden="true">
            S
          </div>
          <div>
            <div className="title-row">
              <h1 title={data.path}>{data.name}</h1>
              <span className="file-badge">{data.name.toLowerCase().endsWith(".zsav") ? "ZSAV" : "SAV"}</span>
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
        <div className="segmented" role="group" aria-label="What cells show">
          {MODES.map((choice) => (
            <button
              key={choice.value}
              aria-pressed={mode === choice.value}
              title={choice.hint}
              onClick={() => setMode(choice.value)}
            >
              {choice.label}
            </button>
          ))}
        </div>
        <div className="toolbar-spacer" />
        <div className="segmented" role="group" aria-label="Row density">
          <button aria-pressed={compact} onClick={() => setCompact(true)}>
            Compact
          </button>
          <button aria-pressed={!compact} onClick={() => setCompact(false)}>
            Comfortable
          </button>
        </div>
      </div>

      {data.rows === 0 ? (
        <div className="notice">
          <p>{loading ? "Loading…" : "This file has no rows."}</p>
        </div>
      ) : (
        <Grid
          dataset={dataset}
          variables={list}
          rowCount={data.rows}
          mode={mode}
          compact={compact}
          selection={selection}
          onSelect={(next) => {
            setSelection(next);
            setInspector(true);
          }}
          widths={widths}
          onResize={onResize}
        />
      )}

      {inspector && current && data.rows > 0 && (
        <Inspector
          dataset={dataset}
          variable={current}
          selection={selection}
          mode={mode}
          countedRows={variables.data?.counted_rows ?? 0}
          onClose={() => setInspector(false)}
        />
      )}

      <footer className="statusbar">
        <div className={`status-group is-${status.tone}`}>
          <span className="status-dot" /> {status.text}
        </div>
        <div className="status-group">
          Row <strong>{count(data.rows === 0 ? 0 : selection.row + 1)}</strong> of{" "}
          <strong>{count(data.rows)}</strong>
        </div>
        <div className="status-group">
          Variable <strong>{count(selection.variable + 1)}</strong> of{" "}
          <strong>{count(data.variables)}</strong>
        </div>
        {data.unlabeled_variables > 0 && (
          <div className="status-group is-warn">
            {count(data.unlabeled_variables)}{" "}
            {data.unlabeled_variables === 1 ? "variable has" : "variables have"} unlabelled values
          </div>
        )}
      </footer>
    </>
  );
}
