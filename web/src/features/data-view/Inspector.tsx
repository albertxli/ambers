import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { useState } from "react";

import { api } from "../../api/client";
import type { CellMode, Variable } from "../../api/client";
import { count, percent } from "../../lib/format";
import type { Selection } from "./Grid";

interface Props {
  dataset: string;
  variable: Variable;
  selection: Selection;
  mode: CellMode;
  /** Rows the counts cover. */
  countedRows: number;
  onClose: () => void;
}

/**
 * Under the grid: the highlighted cell in full. The grid cuts long text
 * at the column's edge; an open-ended answer is read here.
 */
export function Inspector({ dataset, variable, selection, mode, countedRows, onClose }: Props) {
  const [copied, setCopied] = useState(false);
  // One cell, asked for by itself: independent of what the grid has loaded.
  const cell = useQuery({
    queryKey: ["cell", dataset, mode, selection.row, variable.index],
    queryFn: () =>
      api.rows(dataset, {
        offset: selection.row,
        limit: 1,
        variables: [variable.index],
        mode,
        filters: [],
      }),
    staleTime: Number.POSITIVE_INFINITY,
    placeholderData: keepPreviousData,
  });
  const row = cell.data?.rows[0];
  const text = row?.cells[0]?.text ?? "";
  // The previous cell's text stays, dimmed, until this one's arrives.
  const pending = cell.isPlaceholderData || cell.isPending;

  const copy = async () => {
    await navigator.clipboard?.writeText(text);
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1200);
  };
  const missing = countedRows > 0 ? variable.missing_count / countedRows : 0;

  return (
    <aside className="inspector" aria-label="Selected value">
      <div className="inspector-heading">
        <div>
          <span className="eyebrow">Selected value{row ? ` · row ${count(row.number)}` : ""}</span>
          <h2>{variable.name}</h2>
          {variable.label && <p title={variable.label}>{variable.label}</p>}
        </div>
        <button className="icon-button" onClick={onClose} aria-label="Close details">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" aria-hidden="true">
            <path
              d="m7 7 10 10M17 7 7 17"
              stroke="currentColor"
              strokeWidth="1.8"
              strokeLinecap="round"
            />
          </svg>
        </button>
      </div>
      <div className={`value-card ${pending ? "is-pending" : ""}`}>
        <code className={text === "" ? "is-empty" : ""}>{text === "" ? "(empty)" : text}</code>
        <button onClick={copy} disabled={!row || pending}>
          {copied ? "Copied" : "Copy"}
        </button>
      </div>
      <dl className="metadata">
        <div>
          <dt>Type</dt>
          <dd>{variable.kind}</dd>
        </div>
        <div>
          <dt>Format</dt>
          <dd>{variable.format}</dd>
        </div>
        <div>
          <dt>Missing</dt>
          <dd>
            {percent(missing)} <span>({count(variable.missing_count)})</span>
          </dd>
        </div>
      </dl>
    </aside>
  );
}
