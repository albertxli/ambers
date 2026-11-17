import { useEffect, useState } from "react";

import { ApiFailure } from "../../api/client";
import { DATASET_VIEWS, DEFAULT_VIEW } from "../../app/views";
import { Icon } from "../../components/Icon";
import { ShortcutsDialog } from "../../components/ShortcutsDialog";
import { count } from "../../lib/format";
import { ViewSwitch, panelId, tabId } from "./ViewSwitch";
import { useDataset } from "./useDataset";
import "./dataset.css";

interface Props {
  /** The dataset's id. */
  dataset: string;
  /** The view the address names. */
  view: string;
  onView: (view: string) => void;
}

/** Whether a key press is being typed into a field. */
function typing(target: EventTarget | null): boolean {
  return target instanceof HTMLElement && target.closest("input, textarea, select") !== null;
}

/**
 * An open file: the top bar with its name and the Data | Variables tabs,
 * and the view chosen. A view that was shown once stays in the page,
 * hidden, so its filters, selection and place are there on coming back.
 */
export function DatasetShell({ dataset, view, onView }: Props) {
  const { info, variables, failure } = useDataset(dataset);
  const shown = DATASET_VIEWS.some((one) => one.id === view) ? view : DEFAULT_VIEW;
  const [visited, setVisited] = useState<readonly string[]>([shown]);
  if (!visited.includes(shown)) setVisited([...visited, shown]);
  const [shortcuts, setShortcuts] = useState(false);

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

  if (failure) {
    const gone = failure instanceof ApiFailure && failure.status === 404;
    return (
      <div className="notice is-error">
        <h1>{gone ? "This file is no longer open" : "The file could not be shown"}</h1>
        <p>{failure.message}</p>
        <p>
          <a href="#/files">Back to the data files</a>
        </p>
      </div>
    );
  }
  if (!info || !variables) {
    return (
      <div className="notice">
        <p>Opening…</p>
      </div>
    );
  }

  const prefix = `dataset-${dataset}`;
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
              <h1 title={info.path}>{info.name}</h1>
              <span className="file-badge">
                {info.name.toLowerCase().endsWith(".zsav") ? "ZSAV" : "SAV"}
              </span>
            </div>
            <p>
              {count(info.total_rows ?? info.rows)} rows <span>·</span> {count(info.variables)}{" "}
              variables
              {info.label && (
                <>
                  {" "}
                  <span>·</span> {info.label}
                </>
              )}
            </p>
          </div>
        </div>
        <div className="viewer-header-actions">
          {info.warnings.length > 0 && (
            <span className="file-warning" title={info.warnings.join("\n")}>
              {info.warnings.length} {info.warnings.length === 1 ? "warning" : "warnings"}
            </span>
          )}
          <ViewSwitch tabs={DATASET_VIEWS} value={shown} onChange={onView} prefix={prefix} />
        </div>
      </header>

      {DATASET_VIEWS.filter((one) => visited.includes(one.id)).map((one) => {
        const View = one.component;
        const active = one.id === shown;
        return (
          <div
            key={one.id}
            className="viewer-panel"
            hidden={!active}
            role="tabpanel"
            id={panelId(prefix, one.id)}
            aria-labelledby={tabId(prefix, one.id)}
          >
            <View
              dataset={dataset}
              info={info}
              variables={variables}
              active={active}
              onShortcuts={() => setShortcuts(true)}
            />
          </div>
        );
      })}

      {shortcuts && <ShortcutsDialog onClose={() => setShortcuts(false)} />}
    </>
  );
}
