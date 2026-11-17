import type { SortingState } from "@tanstack/react-table";
import { type MouseEvent, type ReactNode, useEffect, useMemo, useRef, useState } from "react";

import type { Variable } from "../../api/client";
import type { ViewProps } from "../../app/views";
import { Icon } from "../../components/Icon";
import { ValueLabels, useVariableCard } from "../../components/VariableCard";
import { type CellView, GridView } from "../../components/grid/GridView";
import {
  type CellSelection,
  cellsIn,
  clampSelection,
  columnsIn,
  rowsIn,
  selectionBounds,
  tabSeparated,
} from "../../components/grid/selection";
import { type GridColumn, useGrid } from "../../components/grid/useGrid";
import { count } from "../../lib/format";
import { auditText, countsScope, kindName, missingShare } from "../../lib/variable";
import { gutterWidth } from "../data-view/columns";
import { fieldOf, useVariableTable } from "./useVariableTable";
import { ValueDetails } from "./ValueDetails";
import {
  type CheckFilter,
  FIELDS,
  type FieldId,
  NO_FILTERS,
  type VariableFilters,
  fieldText,
  filterVariables,
  isFiltered,
} from "./variables";
import "./variable-view.css";

/** Where the selection starts: the first variable's label. */
const FIRST_LABEL: CellSelection = {
  anchor: { row: 0, column: 2 },
  focus: { row: 0, column: 2 },
};

const KINDS = [
  { value: "all", label: "All types" },
  { value: "categorical", label: "Categorical" },
  { value: "numeric", label: "Numeric" },
  { value: "text", label: "Text" },
  { value: "date", label: "Date" },
];

const CHECKS: { value: CheckFilter; label: string }[] = [
  { value: "all", label: "All checks" },
  { value: "unlabelled", label: "Unlabelled values" },
  { value: "duplicates", label: "Duplicate labels" },
  { value: "no_label", label: "No variable label" },
];

const MAC = /Mac|iPhone|iPad/.test(navigator.platform);

/** What a field says when a variable has nothing for it. */
const NOTHING: Partial<Record<FieldId, string>> = {
  label: "This variable has no label.",
  values: "No value labels are defined.",
  check: "Not checked: the check is for numeric variables with value labels.",
  user_missing: "No user-missing values are declared.",
};

/** Pressed with the mouse, a control leaves the keyboard with the grid. */
function keepFocus(event: MouseEvent) {
  event.preventDefault();
}

/**
 * Variable View: one row per variable, like SPSS's Variable View, with
 * what the codebook counts for each (type, valid and missing cases, the
 * label check) over the rows loaded.
 */
export function VariableView({ dataset, info, variables, active, onShortcuts }: ViewProps) {
  const [filters, setFilters] = useState<VariableFilters>(NO_FILTERS);
  const [sorting, setSorting] = useState<SortingState>([]);
  const [selection, setSelection] = useState<CellSelection>(FIRST_LABEL);
  const [inspector, setInspector] = useState(true);
  const [copied, setCopied] = useState<"idle" | "copied" | "failed">("idle");
  /** The variable whose value distribution is open, by its position in the file. */
  const [details, setDetails] = useState<number | null>(null);
  const searchField = useRef<HTMLInputElement>(null);

  const all = variables.variables;
  const listed = useMemo(() => filterVariables(all, filters), [all, filters]);
  const table = useVariableTable({
    variables: listed,
    sorting,
    onSorting: (next) => {
      setSorting(next);
      setSelection(FIRST_LABEL);
      setCopied("idle");
    },
  });
  const sorted = table.getRowModel().rows;
  /** The variables as listed: filtered, in the order chosen. */
  const rows = useMemo(() => sorted.map((row) => row.original), [sorted]);

  const leaf = table.getAllLeafColumns();
  const { columnSizing } = table.state;
  const columns = useMemo<GridColumn[]>(
    () => leaf.map((column) => ({ id: column.id, size: column.getSize() })),
    // `columnSizing` is not read, but sizes change with it and `leaf` does not.
    [leaf, columnSizing],
  );

  const selected = clampSelection(selection, rows.length, columns.length);
  const bounds = selectionBounds(selected);
  const cells = rows.length > 0 ? cellsIn(bounds) : 0;
  const range = cells > 1;
  const current = rows[selected.anchor.row];
  const field = fieldOf(columns[selected.anchor.column]?.id ?? "name");

  const change = (next: VariableFilters) => {
    setFilters(next);
    setSelection(FIRST_LABEL);
    setCopied("idle");
  };
  const select = (next: CellSelection) => {
    setSelection(next);
    setInspector(true);
    setCopied("idle");
  };
  const copy = async () => {
    if (cells === 0) return;
    const lines = rows
      .slice(bounds.top, bounds.bottom + 1)
      .map((variable) =>
        columns
          .slice(bounds.left, bounds.right + 1)
          .map((column) => fieldText(variable, fieldOf(column.id))),
      );
    try {
      await navigator.clipboard.writeText(tabSeparated(lines));
      setCopied("copied");
    } catch {
      setCopied("failed");
    }
  };

  const grid = useGrid({
    columns,
    rowCount: rows.length,
    gutter: gutterWidth(all.length),
    selection: selected,
    onSelect: select,
    onCopy: () => void copy(),
    active,
  });

  const scope = countsScope(info, variables.counted_rows);
  const card = useVariableCard({
    variables: all,
    countedRows: variables.counted_rows,
    scope,
    enabled: active && details === null && grid.resizing === null && !grid.dragging,
  });

  // Ctrl / ⌘ K goes to the search, while this view is the one shown.
  useEffect(() => {
    if (!active) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (document.querySelector("dialog[open]")) return;
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        searchField.current?.focus();
        searchField.current?.select();
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [active]);
  // Another view is shown: a dialog of this one must not stay over it.
  useEffect(() => {
    if (!active) setDetails(null);
  }, [active]);

  const cellOf = (variable: Variable, id: FieldId): CellView => {
    switch (id) {
      case "name":
        return {
          content: (
            <span className="variable-name" {...card.trigger(variable)}>
              {variable.name}
            </span>
          ),
        };
      case "type":
        return {
          content: (
            <span className={`variable-kind kind-${variable.kind}`}>{kindName(variable)}</span>
          ),
        };
      case "label":
        return variable.label
          ? { content: variable.label, title: variable.label }
          : { content: <span className="variable-empty-label">—</span> };
      case "values":
        return { content: <ValuesCell variable={variable} onOpen={() => setDetails(variable.index)} /> };
      case "valid":
        return { content: count(variable.valid), className: "is-number" };
      case "missing":
        return {
          content: (
            <span className={variable.missing_count ? "variable-has-missing" : "variable-zero-missing"}>
              {count(variable.missing_count)}
            </span>
          ),
          className: "is-number",
        };
      case "check": {
        const text = auditText(variable);
        const state = text === "OK" ? "is-ok" : text === "—" ? "is-none" : "has-issues";
        return {
          content: (
            <span className={`variable-label-check ${state}`}>
              {text === "OK" && <Icon name="check" size={12} />}
              {text}
            </span>
          ),
          title: variable.label_check === "ok" ? undefined : variable.label_check || undefined,
        };
      }
      default: {
        const text = fieldText(variable, id);
        return text
          ? { content: text, title: text }
          : { content: <span className="variable-empty-label">—</span> };
      }
    }
  };

  const filtered = isFiltered(filters);
  const opened = details === null ? undefined : all[details];
  const firstField = FIELDS[bounds.left];
  const lastField = FIELDS[bounds.right];

  return (
    <>
      <div className="toolbar variables-toolbar">
        <label className="variables-search">
          <Icon name="search" size={16} />
          <input
            ref={searchField}
            aria-label="Search variables"
            value={filters.search}
            placeholder="Search names, labels, or value labels…"
            onChange={(event) => change({ ...filters, search: event.target.value })}
          />
          <kbd>{MAC ? "⌘ K" : "Ctrl K"}</kbd>
        </label>
        <label className="variables-select">
          <select
            aria-label="Variable type"
            value={filters.kind}
            onChange={(event) => change({ ...filters, kind: event.target.value })}
          >
            {KINDS.map((kind) => (
              <option key={kind.value} value={kind.value}>
                {kind.label}
              </option>
            ))}
          </select>
          <Icon name="chevron-down" size={12} />
        </label>
        <label className="variables-select">
          <select
            aria-label="Label check"
            value={filters.check}
            onChange={(event) => change({ ...filters, check: event.target.value as CheckFilter })}
          >
            {CHECKS.map((check) => (
              <option key={check.value} value={check.value}>
                {check.label}
              </option>
            ))}
          </select>
          <Icon name="chevron-down" size={12} />
        </label>
        {filtered && (
          <button className="variables-clear" onClick={() => change(NO_FILTERS)}>
            Clear filters
          </button>
        )}
        <span className="variables-loaded">
          <strong>{count(rows.length)}</strong> / {count(all.length)} variables
        </span>
      </div>

      {rows.length === 0 ? (
        <div className="grid-filter-empty" role="status">
          <Icon name="filter" size={25} />
          <strong>No matching variables</strong>
          <p>Try a different name, variable type, or label check.</p>
          <button onClick={() => change(NO_FILTERS)}>
            Clear filters
            <Icon name="arrow-right" size={14} />
          </button>
        </div>
      ) : (
        <GridView
          grid={grid}
          label="Variables"
          className="variables-grid"
          columnName={(column) => FIELDS.find((one) => one.id === column.id)?.label ?? column.id}
          headerSort={(column) => {
            const order = table.getColumn(column.id)?.getIsSorted();
            return order === "asc" ? "ascending" : order === "desc" ? "descending" : "none";
          }}
          onResize={(column, width) => {
            if (width === null) table.getColumn(column.id)?.resetSize();
            else table.setColumnSizing((sizes) => ({ ...sizes, [column.id]: width }));
          }}
          renderHeader={(column) => {
            const found = table.getColumn(column.id);
            const one = FIELDS.find((item) => item.id === column.id);
            if (!found || !one) return null;
            const order = found.getIsSorted();
            return (
              <button
                type="button"
                className={`variable-sort-header ${one.numeric ? "is-numeric" : ""}`}
                aria-label={`Sort by ${one.label}`}
                onMouseDown={keepFocus}
                onClick={() => found.toggleSorting()}
              >
                <span>{one.label}</span>
                <span
                  className={`variable-sort-icon ${order ? "is-sorted" : ""} ${order === "asc" ? "is-ascending" : ""}`}
                >
                  <Icon name="chevron-down" size={12} />
                </span>
              </button>
            );
          }}
          renderNumber={(row) => {
            const variable = rows[row];
            return variable ? count(variable.index + 1) : "";
          }}
          renderCell={(row, column) => {
            const variable = rows[row];
            return variable ? cellOf(variable, fieldOf(column.id)) : { content: null };
          }}
        />
      )}

      {inspector && current && (
        <aside className="inspector variable-inspector" aria-label="Selected variable">
          <div className="inspector-heading">
            <div>
              <span className="eyebrow">
                Variable {count(current.index + 1)} of {count(all.length)}
              </span>
              <h2 title={current.name}>{current.name}</h2>
            </div>
            <button
              className="icon-button"
              aria-label="Close variable details"
              onClick={() => setInspector(false)}
            >
              <Icon name="close" size={14} />
            </button>
          </div>
          <div className="variable-inspector-content">
            <div className="variable-inspector-caption">
              <span className="eyebrow">
                {range
                  ? `Selected range · ${firstField?.label ?? ""}${bounds.left === bounds.right ? "" : `:${lastField?.label ?? ""}`}`
                  : field === "values"
                    ? `Value labels · ${count(current.values.length)} defined`
                    : (FIELDS.find((one) => one.id === field)?.label ?? "")}
              </span>
              <button onMouseDown={keepFocus} onClick={() => void copy()} aria-live="polite">
                {copied === "copied"
                  ? "Copied"
                  : copied === "failed"
                    ? "Copy failed"
                    : range
                      ? "Copy range"
                      : "Copy"}
              </button>
            </div>
            <InspectorValue
              variable={current}
              field={field}
              range={
                range
                  ? `${count(cells)} cells · ${count(rowsIn(bounds))} variables × ${count(columnsIn(bounds))} columns`
                  : null
              }
            />
          </div>
          <dl className="metadata">
            <div>
              <dt>Format</dt>
              <dd>{current.format}</dd>
            </div>
            <div>
              <dt>N · valid</dt>
              <dd>{count(current.valid)}</dd>
            </div>
            <div>
              <dt>Missing</dt>
              <dd>{missingShare(current, variables.counted_rows)}</dd>
            </div>
          </dl>
        </aside>
      )}

      <footer className="statusbar variables-statusbar">
        <div className={`status-group ${info.load === "loading" ? "is-busy" : "is-ok"}`}>
          <span className="status-dot" /> Variable view
        </div>
        <div className="status-group">
          <strong>{count(rows.length)}</strong> of <strong>{count(all.length)}</strong> variables
        </div>
        <span className="variable-statistics-scope">Counts: {scope}</span>
        {cells > 1 && (
          <div className="selection-summary" aria-live="polite">
            <span>{count(cells)} cells</span>
          </div>
        )}
        <div className="status-spacer" />
        <button onClick={onShortcuts}>Keyboard shortcuts</button>
      </footer>

      {card.card}
      {opened && (
        <ValueDetails
          key={opened.index}
          dataset={dataset}
          variable={opened}
          info={info}
          onClose={() => setDetails(null)}
        />
      )}
    </>
  );
}

/** The Values cell: the first labels, and the way to all values with their counts. */
function ValuesCell({ variable, onOpen }: { variable: Variable; onOpen: () => void }) {
  const values = variable.values;
  let preview: ReactNode;
  if (values.length === 0) {
    preview = (
      <span className="variable-values-preview variable-individual-values">
        <span>{variable.valid > 0 ? "Individual values" : "No valid values"}</span>
      </span>
    );
  } else {
    preview = (
      <span className="variable-values-preview">
        {values.slice(0, 2).map((value) => (
          <span key={value.code}>
            <code>{value.code}</code>
            {value.label}
          </span>
        ))}
        {values.length > 2 && <small>+{count(values.length - 2)}</small>}
      </span>
    );
  }
  return (
    <span className="variable-values-cell">
      {preview}
      <button
        type="button"
        className="variable-values-more"
        aria-label={`View values for ${variable.name}`}
        aria-haspopup="dialog"
        title={
          variable.kind === "categorical"
            ? "Codes, labels and how often each occurs"
            : "Every value and how often it occurs"
        }
        onMouseDown={keepFocus}
        onClick={onOpen}
      >
        <Icon name="more" size={16} />
      </button>
    </span>
  );
}

/** Under the table: the highlighted cell in full. */
function InspectorValue({
  variable,
  field,
  range,
}: {
  variable: Variable;
  field: FieldId;
  /** What a selected block holds, when more than one cell is selected. */
  range: string | null;
}) {
  if (range !== null) {
    return (
      <div className="value-card">
        <code>{range}</code>
      </div>
    );
  }
  if (field === "values" && (variable.values.length > 0 || variable.missing_count > 0)) {
    return (
      <ValueLabels
        variable={variable}
        limit={Number.POSITIVE_INFINITY}
        heading={false}
        className="variable-inspector-values"
      />
    );
  }
  const text = field === "valid" || field === "missing" ? count(Number(fieldText(variable, field))) : fieldText(variable, field);
  return (
    <>
      <div className="value-card">
        <code className={text === "" ? "is-empty" : ""}>{text === "" ? (NOTHING[field] ?? "(empty)") : text}</code>
      </div>
      {field === "check" && variable.label_check !== "" && variable.label_check !== "ok" && (
        <p className="variable-check-detail">{variable.label_check}</p>
      )}
    </>
  );
}
