import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import { api } from "../../api/client";
import type { FileEntry } from "../../api/client";
import { Icon, type IconName } from "../../components/Icon";
import { count } from "../../lib/format";
import {
  type Scope,
  type Sort,
  describeFile,
  folderName,
  formatCounts,
  recentFiles,
  visibleFiles,
} from "./files";
import "./launcher.css";

const SCOPES: { value: Scope; icon: IconName; label: string }[] = [
  { value: "all", icon: "files", label: "All files" },
  { value: "recent", icon: "clock", label: "Recently opened" },
  { value: "starred", icon: "star", label: "Starred" },
];

const FORMAT_NAMES: Record<string, string> = { SAV: "SPSS (.sav)", ZSAV: "SPSS compressed (.zsav)" };

function FileIcon({ format }: { format: string }) {
  return <span className={`dataset-file-icon format-${format.toLowerCase()}`}>{format}</span>;
}

/** Rows or variables of a file; a dash while its dictionary is being read. */
function known(value: number | null): string {
  return value === null ? "–" : count(value);
}

/**
 * The launch page: the data files of the folder `am open` was started
 * in, those opened before and those starred. Opening one goes to its
 * Data View. The server decides which files can be listed and opened.
 */
export function Launcher({ onOpen }: { onOpen: (dataset: string) => void }) {
  const [scope, setScope] = useState<Scope>("all");
  const [format, setFormat] = useState("all");
  const [query, setQuery] = useState("");
  const [sort, setSort] = useState<Sort>("name");

  const client = useQueryClient();
  const list = useQuery({
    queryKey: ["files"],
    queryFn: api.files,
    // Rows and variables arrive as the server reads each file's dictionary.
    refetchInterval: (state) => ((state.state.data?.pending ?? 0) > 0 ? 400 : false),
  });
  const open = useMutation({
    mutationFn: (file: FileEntry) =>
      file.dataset ? Promise.resolve(file.dataset) : api.openFile(file.path).then((info) => info.id),
    onSuccess: (dataset) => {
      void client.invalidateQueries({ queryKey: ["files"] });
      void client.invalidateQueries({ queryKey: ["server"] });
      onOpen(dataset);
    },
  });
  const star = useMutation({
    mutationFn: (file: FileEntry) => api.starFile(file.path, !file.starred),
    onSettled: () => client.invalidateQueries({ queryKey: ["files"] }),
  });

  const files = list.data?.files ?? [];
  const roots = list.data?.roots ?? [];
  const visible = visibleFiles(files, { scope, format, query, sort });
  const recent = recentFiles(files);
  const formats = formatCounts(files);
  const counts: Record<Scope, number> = {
    all: files.length,
    recent: files.filter((file) => file.opened !== null).length,
    starred: files.filter((file) => file.starred).length,
  };
  const reset = () => {
    setScope("all");
    setFormat("all");
    setQuery("");
  };
  const narrowed = scope !== "all" || format !== "all" || query !== "";
  const showRecent = !narrowed && recent.length > 0;
  const heading =
    format !== "all"
      ? (FORMAT_NAMES[format] ?? format)
      : (SCOPES.find((one) => one.value === scope)?.label ?? "All files");
  const place = roots[0] ? folderName(roots[0]) : "ambers";

  return (
    <div className="library">
      <aside className="library-sidebar">
        <a className="library-brand" href="#/files" onClick={reset}>
          <span className="brand-mark">
            <Icon name="database" size={20} />
          </span>
          <span>
            ambers
            <small>SPSS DATA FILES</small>
          </span>
        </a>

        <nav aria-label="File library">
          <span className="sidebar-label">Library</span>
          {SCOPES.map((one) => {
            const active = scope === one.value && format === "all";
            return (
              <button
                key={one.value}
                className={`sidebar-link ${active ? "is-active" : ""}`}
                aria-pressed={active}
                onClick={() => {
                  setScope(one.value);
                  setFormat("all");
                }}
              >
                <Icon name={one.icon} size={17} />
                <span>{one.label}</span>
                <small>{counts[one.value]}</small>
              </button>
            );
          })}

          {formats.length > 0 && <span className="sidebar-label formats-label">File formats</span>}
          {formats.map((one) => (
            <button
              key={one.format}
              className={`sidebar-link ${format === one.format ? "is-active" : ""}`}
              aria-pressed={format === one.format}
              onClick={() => {
                setFormat(one.format);
                setScope("all");
              }}
            >
              <span className={`format-dot format-${one.format.toLowerCase()}`} />
              <span>{FORMAT_NAMES[one.format] ?? one.format}</span>
              <small>{one.count}</small>
            </button>
          ))}
        </nav>

        <div className="sidebar-folders">
          <span className="sidebar-label">{roots.length === 1 ? "Folder" : "Folders"}</span>
          {roots.map((root) => (
            <p key={root} title={root}>
              {root}
            </p>
          ))}
          {roots.length === 0 && <p>Only files opened before are listed.</p>}
        </div>
      </aside>

      <div className="library-main">
        <header className="library-topbar">
          <div>
            <Icon name="folder" size={16} />
            <span>{place}</span>
            <span className="path-separator">/</span>
            <strong>Data files</strong>
          </div>
          <span className="local-badge">
            <span className="status-dot" /> Served from this machine only
          </span>
        </header>

        <div className="library-content">
          <div className="library-intro">
            <div className="section-kicker">YOUR DATA FILES</div>
            <h1>
              Data files<span>.</span>
            </h1>
            <p>Open a file to look through its data.</p>
            <span className="library-total">
              <Icon name="database" size={14} />
              {count(files.length)} {files.length === 1 ? "file" : "files"}
            </span>
          </div>

          {showRecent && (
            <section className="recent-section" aria-label="Recently opened files">
              <div className="section-heading">
                <h2>
                  <Icon name="clock" size={15} />
                  Recently opened
                </h2>
                <span>Pick up where you left off</span>
              </div>
              <div className="recent-grid">
                {recent.map((file) => (
                  <button className="recent-card" key={file.path} onClick={() => open.mutate(file)}>
                    <div className="recent-card-top">
                      <FileIcon format={file.format} />
                      <Icon name="arrow-right" size={17} />
                    </div>
                    <strong>{file.name}</strong>
                    <span className="recent-folder" title={file.path}>
                      {file.folder || place}
                    </span>
                    <div className="recent-card-bottom">
                      <span>{known(file.rows)} rows</span>
                      <span>{known(file.variables)} variables</span>
                    </div>
                  </button>
                ))}
              </div>
            </section>
          )}

          <section className="file-section" aria-label="Data files">
            <div className="file-section-heading">
              <div>
                <h2>{heading}</h2>
                <span className="file-count">{visible.length}</span>
              </div>
              {narrowed && (
                <button className="clear-filters" onClick={reset}>
                  Clear filters
                </button>
              )}
            </div>
            <div className="library-toolbar">
              <label className="library-search">
                <Icon name="search" size={17} />
                <input
                  aria-label="Search data files"
                  placeholder="Search files by name, label or folder…"
                  value={query}
                  onChange={(event) => setQuery(event.target.value)}
                />
                {query && (
                  <button aria-label="Clear the search" onClick={() => setQuery("")}>
                    <Icon name="close" size={14} />
                  </button>
                )}
              </label>
              <label className="file-sort">
                <span>Sort by:</span>
                <select
                  aria-label="Sort files"
                  value={sort}
                  disabled={scope === "recent"}
                  onChange={(event) => setSort(event.target.value as Sort)}
                >
                  <option value="name">Name</option>
                  <option value="rows">Most rows</option>
                  <option value="modified">Last changed</option>
                </select>
                <Icon name="chevron-down" size={14} />
              </label>
            </div>

            {open.error && (
              <p className="library-error" role="alert">
                {open.error.message}
              </p>
            )}

            <div className="file-list" role="list" aria-label="Data files">
              <div className="file-list-header" aria-hidden="true">
                <span>File name</span>
                <span>Rows</span>
                <span>Variables</span>
                <span>Format</span>
                <span />
                <span />
              </div>
              {visible.map((file) => (
                <div className="file-list-item" role="listitem" key={file.path}>
                  <button
                    className="file-open-button"
                    aria-label={`Open ${file.name}`}
                    disabled={file.error !== null}
                    onClick={() => open.mutate(file)}
                  >
                    <span className="file-name-cell">
                      <FileIcon format={file.format} />
                      <span>
                        <strong>{file.name}</strong>
                        <span className={`file-description ${file.error ? "is-error" : ""}`}>
                          {describeFile(file)}
                        </span>
                      </span>
                    </span>
                    <span className="file-number">{known(file.rows)}</span>
                    <span className="file-number">{known(file.variables)}</span>
                    <span>
                      <span className={`format-badge format-${file.format.toLowerCase()}`}>
                        {file.format}
                      </span>
                    </span>
                    <span className="file-open-arrow">
                      <Icon name="arrow-right" size={16} />
                    </span>
                  </button>
                  <button
                    className={`file-star ${file.starred ? "is-starred" : ""}`}
                    aria-label={`${file.starred ? "Unstar" : "Star"} ${file.name}`}
                    aria-pressed={file.starred}
                    onClick={() => star.mutate(file)}
                  >
                    <Icon name="star" size={15} filled={file.starred} />
                  </button>
                </div>
              ))}
              {visible.length === 0 && (
                <div className="library-empty">
                  <Icon name={scope === "starred" ? "star" : "search"} size={28} />
                  {list.isPending ? (
                    <h3>Looking for data files…</h3>
                  ) : list.isError ? (
                    <>
                      <h3>The files could not be listed</h3>
                      <p>{list.error.message}</p>
                    </>
                  ) : files.length === 0 ? (
                    <>
                      <h3>No data files here</h3>
                      <p>
                        This folder has no .sav or .zsav files. Open one from a terminal:{" "}
                        <code>am open survey.sav</code>
                      </p>
                    </>
                  ) : (
                    <>
                      <h3>
                        {scope === "starred" && !query
                          ? "Your starred files, all in one place"
                          : "No matching files"}
                      </h3>
                      <p>
                        {scope === "starred" && !query
                          ? "Star a file in All files to find it here."
                          : "Try a different name or clear the filters."}
                      </p>
                      <button onClick={reset}>
                        Show all files
                        <Icon name="arrow-right" size={14} />
                      </button>
                    </>
                  )}
                </div>
              )}
              <div className="file-list-footer">
                <span>
                  {count(visible.length)} of {count(files.length)} files
                </span>
                <span>
                  Click a file to open its data
                  <Icon name="arrow-right" size={13} />
                </span>
              </div>
            </div>
            {list.data?.truncated && (
              <p className="library-note">
                There are more data files in this folder than can be listed. Start{" "}
                <code>am open</code> in a sub-folder to see the rest.
              </p>
            )}
          </section>
        </div>

        <footer className="library-status">
          <span>
            <span className="status-dot" />
            {(list.data?.pending ?? 0) > 0
              ? `Reading file details… ${count(list.data?.pending ?? 0)} to go`
              : "Ready"}
          </span>
          <span>ambers</span>
        </footer>
      </div>
    </div>
  );
}
