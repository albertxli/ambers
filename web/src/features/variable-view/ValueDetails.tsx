import { keepPreviousData, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useId, useRef, useState } from "react";

import { api } from "../../api/client";
import type { DatasetInfo, ValueOrder, ValueRow, ValueTotals, Variable } from "../../api/client";
import { Icon } from "../../components/Icon";
import { tabSeparated } from "../../components/grid/selection";
import { backdropClose, useModal } from "../../components/useModal";
import { count, percent } from "../../lib/format";
import { kindName, missingName } from "../../lib/variable";

/** Values on one page of the table. */
const PAGE = 50;
/** The most values one copy takes, and how many the server gives at once. */
export const COPY_LIMIT = 50_000;
const COPY_STEP = 10_000;

type CopyState = "idle" | "copying" | "copied" | "failed";

interface Props {
  dataset: string;
  variable: Variable;
  info: DatasetInfo;
  onClose: () => void;
}

/** A value that follows `value` once it has been still for `delay` ms. */
function useSettled<T>(value: T, delay: number): T {
  const [settled, setSettled] = useState(value);
  useEffect(() => {
    const timer = window.setTimeout(() => setSettled(value), delay);
    return () => window.clearTimeout(timer);
  }, [value, delay]);
  return settled;
}

function share(value: number | null): string {
  return value === null ? "—" : percent(value);
}

/** What the Label column says of a value. */
export function labelOf(row: ValueRow): string {
  if (row.value_label) return row.value_label;
  return row.kind === "unlabeled" ? "Unlabelled code" : "";
}

/** The row that counts missing cases, as the table shows it. */
export function missingRow(variable: Variable, totals: ValueTotals): string[] {
  const of = totals.total > 0 ? percent(totals.missing / totals.total) : "—";
  return [variable.numeric ? "." : "(missing)", missingName(variable), String(totals.missing), "—", of];
}

/** The table as text to paste into a spreadsheet, with a header row. */
export function tableText(
  variable: Variable,
  totals: ValueTotals,
  rows: readonly ValueRow[],
  options: { categorical: boolean; labels: boolean },
): string {
  const pick = (cells: string[]) => (options.labels ? cells : cells.filter((_, at) => at !== 1));
  const lines = [pick([options.categorical ? "Code" : "Value", "Label", "N", "% valid", "% total"])];
  if (totals.missing > 0) lines.push(pick(missingRow(variable, totals)));
  for (const row of rows) {
    const label = row.kind === "user_missing" ? `${labelOf(row)} (user-missing)` : labelOf(row);
    lines.push(
      pick([row.code, label, String(row.count), share(row.share_valid), share(row.share_total)]),
    );
  }
  return tabSeparated(lines);
}

/**
 * How a variable's values are spread: every code or distinct value with
 * its count and share, over the rows loaded. The server holds the table;
 * this asks for the page shown, searched and sorted there, since a text
 * variable can have hundreds of thousands of values.
 */
export function ValueDetails({ dataset, variable, info, onClose }: Props) {
  const dialog = useModal();
  const title = useId();
  const description = useId();
  const searchField = useRef<HTMLInputElement>(null);
  const tableWrap = useRef<HTMLDivElement>(null);
  const client = useQueryClient();

  const [query, setQuery] = useState("");
  const search = useSettled(query.trim(), 180);
  const [order, setOrder] = useState<ValueOrder>("value");
  const [page, setPage] = useState(0);
  const [copyState, setCopyState] = useState<CopyState>("idle");

  const scope = ["values", dataset, variable.index] as const;
  const table = useQuery({
    queryKey: [...scope, search, order, page],
    queryFn: () =>
      api.valuePage(dataset, variable.index, { offset: page * PAGE, limit: PAGE, search, order }),
    placeholderData: keepPreviousData,
    // The counts of an open dialog stay as they were until Refresh.
    staleTime: Number.POSITIVE_INFINITY,
  });
  useEffect(() => {
    if (tableWrap.current) tableWrap.current.scrollTop = 0;
  }, [page, search, order]);

  const data = table.data;
  const totals = data?.totals;
  const categorical = data?.categorical ?? variable.kind === "categorical";
  const labels = data?.has_labels ?? categorical;
  const found = data?.total ?? 0;
  const pages = Math.max(1, Math.ceil(found / PAGE));
  const columns = labels ? 5 : 4;
  // Rows that have loaded since the table was counted.
  const behind = totals ? info.rows - totals.total : 0;
  const what = categorical ? "codes" : "values";

  const change = (apply: () => void) => {
    apply();
    setPage(0);
    setCopyState("idle");
  };
  const refresh = () => {
    setCopyState("idle");
    void client.invalidateQueries({ queryKey: scope });
  };

  const copy = async () => {
    if (!totals) return;
    setCopyState("copying");
    try {
      const rows: ValueRow[] = [];
      for (let offset = 0; offset < Math.min(found, COPY_LIMIT); offset += COPY_STEP) {
        const part = await api.valuePage(dataset, variable.index, {
          offset,
          limit: COPY_STEP,
          search,
          order,
        });
        rows.push(...part.rows);
      }
      await navigator.clipboard.writeText(tableText(variable, totals, rows, { categorical, labels }));
      setCopyState("copied");
    } catch {
      setCopyState("failed");
    }
  };
  const tooMany = found > COPY_LIMIT;

  return (
    <dialog
      ref={dialog}
      className="value-details-dialog"
      aria-labelledby={title}
      aria-describedby={description}
      onCancel={onClose}
      onClick={backdropClose(onClose)}
      onKeyDown={(event) => {
        if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
          event.preventDefault();
          event.stopPropagation();
          searchField.current?.focus();
          searchField.current?.select();
        }
      }}
    >
      <header className="frequency-dialog-header">
        <div className="frequency-title-row">
          <div>
            <span className="frequency-eyebrow">VALUE DISTRIBUTION</span>
            <h2 id={title}>
              <code>{variable.name}</code>
              <span className={`variable-kind kind-${variable.kind}`}>{kindName(variable)}</span>
              {info.load === "loading" && <span className="frequency-preview-badge">Loading</span>}
            </h2>
          </div>
          <button className="icon-button" aria-label="Close value distribution" onClick={onClose}>
            <Icon name="close" size={18} />
          </button>
        </div>
        <p className="frequency-variable-label" id={description}>
          {variable.label || "This variable has no label."}
        </p>
        <div className="frequency-variable-meta">
          <span>
            Format <code>{variable.format}</code>
          </span>
          <span>
            Measure <strong>{variable.measure}</strong>
          </span>
          {variable.missing && (
            <span>
              User-missing <code>{variable.missing}</code>
            </span>
          )}
          <span className="frequency-scope">
            {totals ? `${count(totals.total)} rows counted` : "Counting…"}
          </span>
        </div>
      </header>

      <div className={`frequency-summary ${categorical ? "is-categorical" : ""}`}>
        <Tile name="VALID" value={totals?.valid} />
        <Tile name="MISSING" value={totals?.missing} warn />
        <Tile name="TOTAL" value={totals?.total} />
        {categorical ? (
          <>
            <Tile name="CODES" value={totals?.values} />
            <Tile name="LABELLED" value={totals?.labeled} />
            <Tile name="UNLABELLED" value={totals?.unlabeled} warn />
          </>
        ) : (
          <Tile name="DISTINCT" value={totals?.values} />
        )}
      </div>
      {behind > 0 && (
        <p className="frequency-source-note">
          {count(behind)} more {behind === 1 ? "row has" : "rows have"} loaded since these were
          counted. <button onClick={refresh}>Refresh</button>
        </p>
      )}

      <div className="frequency-toolbar">
        <label className="frequency-search">
          <Icon name="search" size={16} />
          <input
            ref={searchField}
            aria-label="Search the values"
            placeholder={categorical ? "Search codes or labels…" : "Search individual values…"}
            value={query}
            onChange={(event) => change(() => setQuery(event.target.value))}
          />
          {query && (
            <button aria-label="Clear the search" onClick={() => change(() => setQuery(""))}>
              <Icon name="close" size={13} />
            </button>
          )}
        </label>
        <label className="frequency-sort">
          <span>Sort</span>
          <select
            aria-label="Sort the values"
            value={order}
            onChange={(event) => change(() => setOrder(event.target.value as ValueOrder))}
          >
            <option value="value">
              {categorical ? "Code" : "Value"}: {variable.kind === "text" ? "A–Z" : "low to high"}
            </option>
            <option value="value_desc">
              {categorical ? "Code" : "Value"}: {variable.kind === "text" ? "Z–A" : "high to low"}
            </option>
            <option value="count">Most frequent first</option>
          </select>
          <Icon name="chevron-down" size={13} />
        </label>
        <button
          className="frequency-copy"
          onClick={copy}
          disabled={!totals || tooMany || copyState === "copying"}
          title={tooMany ? `At most ${count(COPY_LIMIT)} values can be copied at once` : undefined}
          aria-live="polite"
        >
          <Icon name={copyState === "copied" ? "check" : "copy"} size={14} />
          {tooMany
            ? "Too many values"
            : copyState === "copying"
              ? "Copying…"
              : copyState === "copied"
                ? "Copied"
                : copyState === "failed"
                  ? "Copy failed"
                  : "Copy table"}
        </button>
      </div>

      <div ref={tableWrap} className={`frequency-table-wrap ${table.isPlaceholderData ? "is-pending" : ""}`}>
        <table
          className={`frequency-table ${labels ? "is-categorical" : ""}`}
          aria-label={`${variable.name}: how often each value occurs`}
        >
          <colgroup>
            <col className="frequency-value-column" />
            {labels && <col className="frequency-label-column" />}
            <col className="frequency-number-column" />
            <col className="frequency-percent-column" />
            <col className="frequency-percent-column" />
          </colgroup>
          <thead>
            <tr>
              <th scope="col">{categorical ? "Code" : "Value"}</th>
              {labels && <th scope="col">Label</th>}
              <th scope="col">N</th>
              <th scope="col" title="Share of the rows that are not missing">
                % valid
              </th>
              <th scope="col" title="Share of all the rows counted">
                % total
              </th>
            </tr>
          </thead>
          <tbody>
            {totals && totals.missing > 0 && (
              <tr className="frequency-missing-row">
                <td>
                  <span className="frequency-code">{missingRow(variable, totals)[0]}</span>
                </td>
                {labels && <td>{missingName(variable)}</td>}
                <td>{count(totals.missing)}</td>
                <td>—</td>
                <td>{missingRow(variable, totals)[4]}</td>
              </tr>
            )}
            {data?.rows.map((row) => (
              <tr key={row.code}>
                <td>
                  <span
                    title={row.code}
                    className={variable.kind === "text" ? "frequency-text" : "frequency-code"}
                  >
                    {row.code}
                  </span>
                </td>
                {labels && (
                  <td>
                    <span className={row.kind === "unlabeled" ? "frequency-unlabelled" : ""}>
                      {labelOf(row)}
                    </span>
                    {row.kind === "user_missing" && (
                      <small className="frequency-user-missing">user-missing</small>
                    )}
                  </td>
                )}
                <td>{count(row.count)}</td>
                <td>{share(row.share_valid)}</td>
                <td>{share(row.share_total)}</td>
              </tr>
            ))}
            {data && data.rows.length === 0 && (
              <tr>
                <td colSpan={columns} className="frequency-empty">
                  <strong>{search ? "No matching values" : "No values that are not missing"}</strong>
                  <span>
                    {search
                      ? "Try another value or label."
                      : "Every row counted is missing for this variable."}
                  </span>
                  {search && <button onClick={() => change(() => setQuery(""))}>Clear search</button>}
                </td>
              </tr>
            )}
            {!data && (
              <tr>
                <td colSpan={columns} className="frequency-empty">
                  {table.error ? (
                    <strong>{table.error.message}</strong>
                  ) : (
                    <span>Counting the values…</span>
                  )}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      <footer className="frequency-footer">
        <div>
          <span>
            {found === 0 ? 0 : count(page * PAGE + 1)}–{count(Math.min((page + 1) * PAGE, found))}{" "}
            of {count(found)} {what}
          </span>
          <small>
            {info.load === "loading"
              ? "Counts and percentages use the rows loaded when they were counted."
              : "Counts and percentages use all loaded rows, whatever is searched for."}
          </small>
        </div>
        <div className="frequency-pagination">
          <button
            aria-label="Previous page of values"
            disabled={page === 0}
            onClick={() => setPage(page - 1)}
          >
            <Icon name="arrow-left" size={15} />
          </button>
          <span>
            {count(page + 1)} / {count(pages)}
          </span>
          <button
            aria-label="Next page of values"
            disabled={page + 1 >= pages}
            onClick={() => setPage(page + 1)}
          >
            <Icon name="arrow-right" size={15} />
          </button>
        </div>
      </footer>
    </dialog>
  );
}

function Tile({ name, value, warn }: { name: string; value: number | undefined; warn?: boolean }) {
  return (
    <div className={warn && value ? "has-missing" : ""}>
      <span>{name}</span>
      <strong>{value === undefined ? "…" : count(value)}</strong>
    </div>
  );
}
