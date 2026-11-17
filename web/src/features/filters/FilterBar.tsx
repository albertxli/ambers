import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { useVirtualizer } from "@tanstack/react-virtual";
import { type KeyboardEvent, useEffect, useId, useLayoutEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";

import { api } from "../../api/client";
import type { FilterMode, FilterOperator, KindOperators, Variable } from "../../api/client";
import { Icon } from "../../components/Icon";
import { count } from "../../lib/format";
import { type FilterGroup, type FilterRule, NO_FILTERS, ruleId, withRule, withoutRule } from "./filters";
import { KIND_TAGS, OPERATORS, operatorsFor, validate, valueHelp } from "./operators";
import "./filters.css";

interface Props {
  dataset: string;
  /** Every variable of the file, shown in the grid or not. */
  variables: Variable[];
  /** The conditions each kind of variable offers. */
  offered: KindOperators[];
  filters: FilterGroup;
  onChange: (filters: FilterGroup) => void;
  /** Rows the filters keep; `null` until the server has said. */
  kept: number | null;
  /** Rows loaded so far. */
  loaded: number;
  /** What the server said about rules it could not use, by rule id. */
  problems: ReadonlyMap<string, string>;
  /** False while the Data View is hidden: its shortcut is then another view's. */
  active: boolean;
}

type Stage = "column" | "operator" | "value";

/** The key that opens the builder, as this machine writes it. */
const SHORTCUT = /Mac|iPhone|iPad/.test(navigator.platform) ? "⌘ K" : "Ctrl K";

/** A value that follows `value` once it has been still for `delay` ms. */
function useSettled<T>(value: T, delay: number): T {
  const [settled, setSettled] = useState(value);
  useEffect(() => {
    const timer = window.setTimeout(() => setSettled(value), delay);
    return () => window.clearTimeout(timer);
  }, [value, delay]);
  return settled;
}

/**
 * The filter bar: the conditions as chips, and a builder that adds one in
 * three steps (column, condition, value). The server decides which rows
 * pass; this only says what is asked.
 */
export function FilterBar(props: Props) {
  const { dataset, variables, offered, filters, onChange, kept, loaded, problems, active } = props;
  const menuId = useId();
  const [open, setOpen] = useState(false);
  const [stage, setStage] = useState<Stage>("column");
  const [search, setSearch] = useState("");
  const [activeIndex, setActiveIndex] = useState(0);
  const [draft, setDraft] = useState<FilterRule | null>(null);
  const [editing, setEditing] = useState<string | null>(null);
  const [position, setPosition] = useState({ left: 12, top: 0, width: 370, maxHeight: 420 });
  const rootRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const popoverRef = useRef<HTMLDivElement>(null);
  const columnSearchRef = useRef<HTMLInputElement>(null);
  const valueRef = useRef<HTMLInputElement>(null);

  const listed = useMemo(() => {
    const needle = search.trim().toLowerCase();
    if (!needle) return variables;
    return variables.filter(
      (variable) =>
        variable.name.toLowerCase().includes(needle) ||
        variable.label.toLowerCase().includes(needle),
    );
  }, [variables, search]);
  const selected = draft ? variables[draft.variable] : undefined;
  const operators = selected ? operatorsFor(selected.filter_kind, offered) : [];
  const validation =
    draft && selected
      ? validate(selected.filter_kind, draft.operator, draft.value)
      : "Choose a column.";

  const close = (returnFocus = false) => {
    setOpen(false);
    setDraft(null);
    setEditing(null);
    if (returnFocus) triggerRef.current?.focus();
  };

  const startFilter = () => {
    setSearch("");
    setActiveIndex(0);
    setDraft(null);
    setEditing(null);
    setStage("column");
    setOpen(true);
  };

  useEffect(() => {
    const onPointerDown = (event: PointerEvent) => {
      const target = event.target as Node;
      if (!rootRef.current?.contains(target) && !popoverRef.current?.contains(target)) {
        setOpen(false);
        setDraft(null);
        setEditing(null);
      }
    };
    const onKeyDown = (event: globalThis.KeyboardEvent) => {
      if (!active || document.querySelector("dialog[open]")) return;
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        startFilter();
      }
    };
    document.addEventListener("pointerdown", onPointerDown);
    window.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown);
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [active]);

  // The builder hangs under the bar, inside the window.
  useLayoutEffect(() => {
    if (!open) return;
    const measure = () => {
      const bar = rootRef.current?.getBoundingClientRect();
      if (!bar) return;
      const width = Math.min(370, window.innerWidth - 24);
      const top = window.innerHeight - bar.bottom < 260 ? 12 : bar.bottom + 7;
      setPosition({
        left: Math.max(12, Math.min(bar.left, window.innerWidth - width - 12)),
        top,
        width,
        maxHeight: Math.max(120, Math.min(450, window.innerHeight - top - 12)),
      });
    };
    measure();
    window.addEventListener("resize", measure);
    return () => window.removeEventListener("resize", measure);
  }, [open, stage, filters.rules.length]);

  useEffect(() => {
    if (!open) return;
    if (stage === "column") columnSearchRef.current?.focus();
    else if (stage === "value" && valueRef.current) valueRef.current.focus();
    else popoverRef.current?.focus();
  }, [open, stage, editing]);

  const commit = (rule: FilterRule) => {
    const variable = variables[rule.variable];
    if (!variable || validate(variable.filter_kind, rule.operator, rule.value)) return;
    onChange(withRule(filters, rule));
    close(true);
  };

  const chooseColumn = (variable: Variable) => {
    const first = operatorsFor(variable.filter_kind, offered)[0] ?? "equals";
    setDraft({ id: editing ?? ruleId(), variable: variable.index, operator: first, value: "" });
    setActiveIndex(0);
    setStage("operator");
  };

  const chooseOperator = (operator: FilterOperator) => {
    if (!draft) return;
    const next = { ...draft, operator };
    setDraft(next);
    if (OPERATORS[operator].needsValue) setStage("value");
    else commit(next);
  };

  const onPopoverKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (event.key === "Escape") {
      event.preventDefault();
      event.stopPropagation();
      close(true);
      return;
    }
    if (stage === "value") return;
    const size = stage === "column" ? listed.length : operators.length;
    if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      const step = event.key === "ArrowDown" ? 1 : -1;
      setActiveIndex((current) => (size ? (current + step + size) % size : 0));
    } else if (event.key === "Enter" && size) {
      event.preventDefault();
      if (stage === "column") {
        const variable = listed[activeIndex] ?? listed[0];
        if (variable) chooseColumn(variable);
      } else {
        const operator = operators[activeIndex] ?? operators[0];
        if (operator) chooseOperator(operator);
      }
    }
  };

  const some = filters.rules.length > 0;

  return (
    <div className="filter-builder" ref={rootRef}>
      <div
        className={`filter-bar ${some ? "has-filters" : ""} ${open ? "is-open" : ""}`}
        onClick={(event) => {
          if (event.target === event.currentTarget) startFilter();
        }}
      >
        <button className="filter-bar-icon" aria-label="Choose filter column" onClick={startFilter}>
          <Icon name="filter" size={17} />
        </button>
        {some && (
          <label className="filter-mode">
            <select
              aria-label="Filter match mode"
              value={filters.mode}
              onChange={(event) =>
                onChange({ ...filters, mode: event.target.value as FilterMode })
              }
            >
              <option value="all">Match all</option>
              <option value="any">Match any</option>
            </select>
            <Icon name="chevron-down" size={12} />
          </label>
        )}
        {filters.rules.map((rule, index) => {
          const name = variables[rule.variable]?.name ?? `variable ${rule.variable + 1}`;
          const operator = OPERATORS[rule.operator];
          const problem = problems.get(rule.id);
          const said = `${name} ${operator.label} ${operator.needsValue ? rule.value : ""}`.trim();
          return (
            <div className="filter-condition" key={rule.id}>
              {index > 0 && (
                <span className="filter-connector">{filters.mode === "all" ? "AND" : "OR"}</span>
              )}
              <div
                className={`filter-chip ${editing === rule.id && open ? "is-editing" : ""} ${problem ? "has-problem" : ""}`}
                title={problem ? `Not applied: ${problem}` : undefined}
              >
                <button
                  className="filter-chip-edit"
                  aria-label={`Edit filter ${said}`}
                  onClick={() => {
                    setDraft({ ...rule });
                    setEditing(rule.id);
                    setStage(operator.needsValue ? "value" : "operator");
                    setActiveIndex(0);
                    setOpen(true);
                  }}
                >
                  <span className="filter-chip-column">{name}</span>
                  <span className="filter-chip-operator">{operator.symbol}</span>
                  {operator.needsValue && <span className="filter-chip-value">{rule.value}</span>}
                </button>
                <button
                  className="filter-chip-remove"
                  aria-label={`Remove filter ${said}`}
                  onClick={() => {
                    onChange(withoutRule(filters, rule.id));
                    if (editing === rule.id) close();
                  }}
                >
                  <Icon name="close" size={12} />
                </button>
              </div>
            </div>
          );
        })}
        <button
          ref={triggerRef}
          className="filter-add"
          aria-label="Add filter"
          aria-haspopup="dialog"
          aria-expanded={open}
          aria-controls={open ? menuId : undefined}
          onClick={startFilter}
        >
          {some ? (
            <>
              <Icon name="plus" size={13} />
              Add filter
            </>
          ) : (
            <>
              <span>Filter by column, condition, and value…</span>
              <kbd>{SHORTCUT}</kbd>
            </>
          )}
        </button>
        {some && (
          <button
            className="filter-clear"
            onClick={() => {
              onChange(NO_FILTERS);
              close();
            }}
          >
            Clear all
          </button>
        )}
      </div>
      {some && (
        <div className="filter-context">
          <span className="filter-active-dot" />
          {filters.rules.length} {filters.rules.length === 1 ? "condition" : "conditions"}
          <span>·</span>Matching {filters.mode === "all" ? "all" : "any"}
          <span>·</span>
          <strong aria-live="polite">
            {kept === null ? "…" : count(kept)} of {count(loaded)} rows
          </strong>
        </div>
      )}

      {open &&
        createPortal(
          <div
            id={menuId}
            ref={popoverRef}
            className="filter-popover"
            role="dialog"
            aria-label={editing ? "Edit filter" : "Build filter"}
            tabIndex={-1}
            style={position}
            onKeyDown={onPopoverKeyDown}
          >
            <div className="filter-popover-heading">
              <span className="filter-popover-symbol">
                <Icon name="filter" size={15} />
              </span>
              <strong>{editing ? "Edit condition" : "Add a condition"}</strong>
              <button aria-label="Close filter builder" onClick={() => close(true)}>
                <Icon name="close" size={15} />
              </button>
            </div>
            <div className="filter-steps">
              <button
                className={stage === "column" ? "is-current" : ""}
                onClick={() => {
                  setSearch("");
                  setActiveIndex(0);
                  setStage("column");
                }}
              >
                <span>1</span>
                {stage === "column" || !selected ? "Column" : selected.name}
              </button>
              <Icon name="arrow-right" size={11} />
              <button
                className={stage === "operator" ? "is-current" : ""}
                disabled={!draft}
                onClick={() => {
                  setActiveIndex(0);
                  setStage("operator");
                }}
              >
                <span>2</span>
                {stage === "value" && draft ? OPERATORS[draft.operator].label : "Condition"}
              </button>
              <Icon name="arrow-right" size={11} />
              <span className={stage === "value" ? "is-current" : ""}>
                <span>3</span>Value
              </span>
            </div>

            {stage === "column" && (
              <>
                <label className="filter-column-search">
                  <Icon name="search" size={15} />
                  <input
                    ref={columnSearchRef}
                    role="combobox"
                    aria-expanded="true"
                    aria-controls={`${menuId}-columns`}
                    aria-activedescendant={
                      listed[activeIndex] ? `${menuId}-option-${activeIndex}` : undefined
                    }
                    aria-label="Find a column"
                    placeholder="Find a column…"
                    value={search}
                    onChange={(event) => {
                      setSearch(event.target.value);
                      setActiveIndex(0);
                    }}
                  />
                </label>
                <div className="filter-menu-caption">
                  CHOOSE A COLUMN<span>{count(listed.length)} available</span>
                </div>
                <ColumnList
                  id={menuId}
                  listed={listed}
                  active={activeIndex}
                  onActive={setActiveIndex}
                  onChoose={chooseColumn}
                />
                {listed.length === 0 && (
                  <p className="filter-menu-empty">No columns match “{search}”.</p>
                )}
                <div className="filter-popover-footer">
                  <span>
                    <kbd>↑</kbd>
                    <kbd>↓</kbd> to navigate
                  </span>
                  <span>
                    <kbd>↵</kbd> to select
                  </span>
                </div>
              </>
            )}

            {stage === "operator" && selected && (
              <>
                <div className="filter-menu-list" role="listbox" aria-label="Filter conditions">
                  {operators.map((operator, index) => {
                    const how = OPERATORS[operator];
                    const previous = operators[index - 1];
                    return (
                      <div key={operator}>
                        {(!previous || OPERATORS[previous].group !== how.group) && (
                          <div className="filter-menu-caption">{how.group.toUpperCase()}</div>
                        )}
                        <button
                          id={`${menuId}-option-${index}`}
                          role="option"
                          tabIndex={-1}
                          aria-selected={index === activeIndex}
                          className={`filter-menu-option ${index === activeIndex ? "is-highlighted" : ""}`}
                          onPointerMove={() => setActiveIndex(index)}
                          onClick={() => chooseOperator(operator)}
                        >
                          <span>{how.label}</span>
                          <span className="filter-operator-symbol">{how.symbol}</span>
                        </button>
                      </div>
                    );
                  })}
                </div>
                <div className="filter-popover-footer">
                  <span>
                    {selected.filter_kind === "number"
                      ? "Numeric comparisons use number values"
                      : selected.filter_kind === "categorical"
                        ? "A label or a code both match; text ignores case, except LIKE"
                        : "Text comparisons ignore case, except LIKE"}
                  </span>
                </div>
              </>
            )}

            {stage === "value" && draft && selected && (
              <form
                className="filter-value-form"
                onSubmit={(event) => {
                  event.preventDefault();
                  commit(draft);
                }}
              >
                <label className="filter-value-label" htmlFor={`${menuId}-value`}>
                  Value for {selected.name}
                </label>
                <input
                  ref={valueRef}
                  id={`${menuId}-value`}
                  className="filter-value-input"
                  aria-label="Filter value"
                  aria-describedby={`${menuId}-value-help`}
                  inputMode={OPERATORS[draft.operator].numeric ? "decimal" : "text"}
                  autoComplete="off"
                  spellCheck={false}
                  value={draft.value}
                  placeholder={
                    draft.operator === "like" || draft.operator === "ilike"
                      ? "e.g. %econom%"
                      : OPERATORS[draft.operator].numeric || selected.filter_kind === "number"
                        ? "Enter a number…"
                        : "Enter a value…"
                  }
                  onChange={(event) => setDraft({ ...draft, value: event.target.value })}
                />
                <p
                  id={`${menuId}-value-help`}
                  className={`filter-value-help ${draft.value && validation ? "has-error" : ""}`}
                >
                  {draft.value && validation
                    ? validation
                    : valueHelp(selected.filter_kind, draft.operator)}
                </p>
                <Suggestions
                  dataset={dataset}
                  variable={selected}
                  // A pattern is not a value to look for: show the usual values.
                  text={draft.operator === "like" || draft.operator === "ilike" ? "" : draft.value}
                  onPick={(value) => {
                    setDraft({ ...draft, value });
                    valueRef.current?.focus();
                  }}
                />
                <div className="filter-value-actions">
                  <button type="button" onClick={() => close(true)}>
                    Cancel
                  </button>
                  <button className="filter-apply" type="submit" disabled={Boolean(validation)}>
                    <Icon name="check" size={14} />
                    {editing ? "Save filter" : "Apply filter"}
                  </button>
                </div>
              </form>
            )}
          </div>,
          document.body,
        )}
    </div>
  );
}

const OPTION_HEIGHT = 36;

/**
 * The variables to choose from. A file can have a thousand of them, so
 * only the ones in view are in the page.
 */
function ColumnList(props: {
  id: string;
  listed: Variable[];
  active: number;
  onActive: (index: number) => void;
  onChoose: (variable: Variable) => void;
}) {
  const { id, listed, active, onActive, onChoose } = props;
  const scroller = useRef<HTMLDivElement>(null);
  const rows = useVirtualizer({
    count: listed.length,
    getScrollElement: () => scroller.current,
    estimateSize: () => OPTION_HEIGHT,
    overscan: 6,
  });
  // The highlighted option stays in view while the arrow keys move it.
  useEffect(() => {
    if (listed.length) rows.scrollToIndex(Math.min(active, listed.length - 1), { align: "auto" });
  }, [active, listed.length, rows]);

  return (
    <div
      ref={scroller}
      className="filter-menu-list"
      role="listbox"
      aria-label="Dataset columns"
      id={`${id}-columns`}
    >
      <div className="filter-menu-rows" style={{ height: rows.getTotalSize() }}>
        {rows.getVirtualItems().map((item) => {
          const variable = listed[item.index];
          if (!variable) return null;
          const kind = variable.filter_kind;
          return (
            <button
              key={variable.index}
              id={`${id}-option-${item.index}`}
              role="option"
              tabIndex={-1}
              aria-selected={item.index === active}
              className={`filter-menu-option ${item.index === active ? "is-highlighted" : ""}`}
              style={{ top: item.start, height: item.size }}
              onPointerMove={() => onActive(item.index)}
              onClick={() => onChoose(variable)}
            >
              <span className={`filter-type-icon is-${kind}`}>{KIND_TAGS[kind]}</span>
              <span className="filter-option-name">{variable.name}</span>
              {variable.label && <span className="filter-option-label">{variable.label}</span>}
              <Icon name="chevron-right" size={13} />
            </button>
          );
        })}
      </div>
    </div>
  );
}

/** The most frequent values of the variable that contain what is typed. */
function Suggestions(props: {
  dataset: string;
  variable: Variable;
  text: string;
  onPick: (value: string) => void;
}) {
  const { dataset, variable, onPick } = props;
  const text = useSettled(props.text.trim(), 150);
  const found = useQuery({
    queryKey: ["suggest", dataset, variable.index, text.toLowerCase()],
    queryFn: () => api.suggest(dataset, variable.index, text, 5),
    placeholderData: keepPreviousData,
    staleTime: 5000,
  });
  const rows = found.data?.rows ?? [];
  if (rows.length === 0) return null;
  return (
    <div className="filter-suggestions">
      <span>VALUES IN THIS COLUMN</span>
      {rows.map((row) => (
        // A labelled value is asked for by its label, as the file has it.
        <button
          type="button"
          key={`${row.code}\u0000${row.value_label}`}
          onClick={() => onPick(row.value_label || row.code)}
        >
          <span>
            {row.value_label || row.code}
            {row.value_label && row.code && <em>{row.code}</em>}
          </span>
          <small>
            {count(row.count)} {row.count === 1 ? "row" : "rows"}
          </small>
        </button>
      ))}
    </div>
  );
}
