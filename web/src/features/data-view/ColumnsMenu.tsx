import { useVirtualizer } from "@tanstack/react-virtual";
import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";

import { Icon } from "../../components/Icon";
import { count } from "../../lib/format";
import { type DataTable, variableOf } from "./useDataTable";

const OPTION_HEIGHT = 32;

/**
 * The Columns button and its menu: which variables the grid shows. The
 * choice is the table's; hiding a variable does not touch the file, the
 * filters or the counts.
 */
export function ColumnsMenu({ table }: { table: DataTable }) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const [position, setPosition] = useState({ left: 12, top: 0, width: 330, maxHeight: 420 });
  const button = useRef<HTMLButtonElement>(null);
  const menu = useRef<HTMLDivElement>(null);
  const searchBox = useRef<HTMLInputElement>(null);
  const scroller = useRef<HTMLDivElement>(null);

  const all = table.getAllLeafColumns();
  const shown = table.getVisibleLeafColumns().length;
  const listed = useMemo(() => {
    const needle = search.trim().toLowerCase();
    if (!needle) return all;
    return all.filter((column) => {
      const variable = variableOf(column);
      return (
        variable.name.toLowerCase().includes(needle) || variable.label.toLowerCase().includes(needle)
      );
    });
  }, [all, search]);

  const rows = useVirtualizer({
    count: listed.length,
    getScrollElement: () => scroller.current,
    estimateSize: () => OPTION_HEIGHT,
    overscan: 8,
  });

  useEffect(() => {
    if (!open) return;
    const onPointerDown = (event: PointerEvent) => {
      const target = event.target as Node;
      if (!button.current?.contains(target) && !menu.current?.contains(target)) setOpen(false);
    };
    document.addEventListener("pointerdown", onPointerDown);
    return () => document.removeEventListener("pointerdown", onPointerDown);
  }, [open]);

  // The menu hangs under the button, its right edge on the button's.
  useLayoutEffect(() => {
    if (!open) return;
    const measure = () => {
      const box = button.current?.getBoundingClientRect();
      if (!box) return;
      const width = Math.min(330, window.innerWidth - 24);
      const top = box.bottom + 7;
      setPosition({
        left: Math.max(12, Math.min(box.right - width, window.innerWidth - width - 12)),
        top,
        width,
        maxHeight: Math.max(160, Math.min(460, window.innerHeight - top - 12)),
      });
    };
    measure();
    searchBox.current?.focus();
    window.addEventListener("resize", measure);
    return () => window.removeEventListener("resize", measure);
  }, [open]);

  /** Show or hide every variable the search lists. */
  const setListed = (visible: boolean) => {
    if (!search.trim()) {
      table.toggleAllColumnsVisible(visible);
      return;
    }
    table.setColumnVisibility((previous) => {
      const next = { ...previous };
      for (const column of listed) next[column.id] = visible;
      return next;
    });
  };

  const close = () => {
    setOpen(false);
    button.current?.focus();
  };

  return (
    <>
      <button
        ref={button}
        className={`tool-button ${open ? "is-open" : ""}`}
        aria-haspopup="dialog"
        aria-expanded={open}
        onClick={() => {
          setSearch("");
          setOpen((was) => !was);
        }}
      >
        <Icon name="columns" size={16} /> Columns
        {shown < all.length && (
          <span className="tool-count">
            {count(shown)}/{count(all.length)}
          </span>
        )}
      </button>
      {open &&
        createPortal(
          <div
            ref={menu}
            className="columns-menu"
            role="dialog"
            aria-label="Choose columns"
            style={position}
            onKeyDown={(event) => {
              if (event.key === "Escape") {
                event.preventDefault();
                event.stopPropagation();
                close();
              }
            }}
          >
            <label className="filter-column-search">
              <Icon name="search" size={15} />
              <input
                ref={searchBox}
                aria-label="Find a column"
                placeholder="Find a column…"
                value={search}
                onChange={(event) => setSearch(event.target.value)}
              />
            </label>
            <div className="filter-menu-caption">
              COLUMNS SHOWN
              <span>
                {count(shown)} of {count(all.length)}
              </span>
            </div>
            <div ref={scroller} className="filter-menu-list" role="group" aria-label="Columns">
              <div className="filter-menu-rows" style={{ height: rows.getTotalSize() }}>
                {rows.getVirtualItems().map((item) => {
                  const column = listed[item.index];
                  if (!column) return null;
                  const variable = variableOf(column);
                  return (
                    <label
                      key={column.id}
                      className="columns-option"
                      style={{ top: item.start, height: item.size }}
                    >
                      <input
                        type="checkbox"
                        checked={column.getIsVisible()}
                        onChange={(event) => column.toggleVisibility(event.target.checked)}
                      />
                      <span className="filter-option-name">{variable.name}</span>
                      {variable.label && (
                        <span className="filter-option-label">{variable.label}</span>
                      )}
                    </label>
                  );
                })}
              </div>
            </div>
            {listed.length === 0 && (
              <p className="filter-menu-empty">No columns match “{search}”.</p>
            )}
            <div className="filter-popover-footer">
              <button type="button" onClick={() => setListed(true)}>
                {search.trim() ? `Show these ${count(listed.length)}` : "Show all"}
              </button>
              <button type="button" onClick={() => setListed(false)}>
                {search.trim() ? `Hide these ${count(listed.length)}` : "Hide all"}
              </button>
            </div>
          </div>,
          document.body,
        )}
    </>
  );
}
