import { keepPreviousData, useQuery } from "@tanstack/react-query";

import { api } from "../../api/client";
import type { Variable } from "../../api/client";
import { Icon } from "../../components/Icon";
import { count, percent } from "../../lib/format";
import type { Shown } from "./blocks";
import { COPY_LIMIT, type CopyState } from "./copy";
import { type Bounds, cellsIn, columnsIn, rowsIn } from "./selection";

interface Props {
  dataset: string;
  /** The variable of the highlighted cell. */
  variable: Variable;
  /** The highlighted cell's row, as a position among the rows shown. */
  row: number;
  /** The whole selection. */
  bounds: Bounds;
  /** How the selection is named when it is more than one cell. */
  rangeName: string;
  shown: Shown;
  /** Rows the counts cover. */
  countedRows: number;
  copyState: CopyState;
  onCopy: () => void;
  onClose: () => void;
}

function copyLabel(state: CopyState, range: boolean): string {
  switch (state) {
    case "copying":
      return "Copying…";
    case "copied":
      return "Copied";
    case "failed":
      return "Copy failed";
    case "too-many":
      return "Too many cells";
    default:
      return range ? "Copy range" : "Copy";
  }
}

/**
 * Under the grid: the highlighted cell in full, or what a selected range
 * holds. The grid cuts long text at the column's edge; an open-ended
 * answer is read here.
 */
export function Inspector(props: Props) {
  const { dataset, variable, row, bounds, rangeName, shown, countedRows, copyState } = props;
  const cells = cellsIn(bounds);
  const range = cells > 1;
  // One cell, asked for by itself: independent of what the grid has loaded.
  const cell = useQuery({
    queryKey: [
      "cell",
      dataset,
      shown.mode,
      shown.filterMode,
      JSON.stringify(shown.filters),
      row,
      variable.index,
    ],
    queryFn: () =>
      api.rows(dataset, {
        offset: row,
        limit: 1,
        variables: [variable.index],
        mode: shown.mode,
        filters: shown.filters,
        filter_mode: shown.filterMode,
      }),
    staleTime: Number.POSITIVE_INFINITY,
    placeholderData: keepPreviousData,
    enabled: !range,
  });
  const line = cell.data?.rows[0];
  const text = line?.cells[0]?.text ?? "";
  // The previous cell's text stays, dimmed, until this one's arrives.
  const pending = !range && (cell.isPlaceholderData || cell.isPending);
  const missing = countedRows > 0 ? variable.missing_count / countedRows : 0;
  const tooMany = cells > COPY_LIMIT;

  return (
    <aside className="inspector" aria-label={range ? "Selected range" : "Selected value"}>
      <div className="inspector-heading">
        <div>
          <span className="eyebrow">
            {range ? "Selected range" : `Selected value${line ? ` · row ${count(line.number)}` : ""}`}
          </span>
          <h2 title={range ? rangeName : variable.name}>{range ? rangeName : variable.name}</h2>
          {!range && variable.label && <p title={variable.label}>{variable.label}</p>}
        </div>
        <button className="icon-button" onClick={props.onClose} aria-label="Close details">
          <Icon name="close" size={14} />
        </button>
      </div>
      <div className={`value-card ${pending ? "is-pending" : ""}`}>
        {range ? (
          <code>
            {count(cells)} cells · {count(rowsIn(bounds))} rows × {count(columnsIn(bounds))}{" "}
            {columnsIn(bounds) === 1 ? "variable" : "variables"}
          </code>
        ) : (
          <code className={text === "" ? "is-empty" : ""}>{text === "" ? "(empty)" : text}</code>
        )}
        {/* Pressed with the mouse, it leaves the keyboard with the grid. */}
        <button
          onMouseDown={(event) => event.preventDefault()}
          onClick={props.onCopy}
          disabled={copyState === "copying" || tooMany || (!range && (!line || pending))}
          title={
            tooMany ? `At most ${count(COPY_LIMIT)} cells can be copied at once` : "Ctrl + C"
          }
          aria-live="polite"
        >
          {tooMany ? "Too many cells" : copyLabel(copyState, range)}
        </button>
      </div>
      {range ? (
        <dl className="metadata">
          <div>
            <dt>Cells</dt>
            <dd>{count(cells)}</dd>
          </div>
          <div>
            <dt>Rows</dt>
            <dd>{count(rowsIn(bounds))}</dd>
          </div>
          <div>
            <dt>Variables</dt>
            <dd>{count(columnsIn(bounds))}</dd>
          </div>
        </dl>
      ) : (
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
      )}
    </aside>
  );
}
