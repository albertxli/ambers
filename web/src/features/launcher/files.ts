// Which files the launch page shows, and in what order. The list itself
// comes from the server, which decides what may be listed at all.

import type { FileEntry } from "../../api/client";

export type Scope = "all" | "recent" | "starred";
export type Sort = "name" | "rows" | "modified";

export interface View {
  scope: Scope;
  /** `SAV`, `ZSAV`, or `all`. */
  format: string;
  query: string;
  sort: Sort;
}

/** The line under a file's name: its SPSS label, or where it is. */
export function describeFile(file: FileEntry): string {
  if (file.error) return `Cannot be read: ${file.error}`;
  return file.label || file.folder || "";
}

/** The files a view shows, in its order. */
export function visibleFiles(files: FileEntry[], view: View): FileEntry[] {
  const term = view.query.trim().toLowerCase();
  const shown = files.filter(
    (file) =>
      (view.scope === "all" ||
        (view.scope === "recent" ? file.opened !== null : file.starred)) &&
      (view.format === "all" || file.format === view.format) &&
      `${file.name} ${file.label ?? ""} ${file.folder}`.toLowerCase().includes(term),
  );
  const byName = (a: FileEntry, b: FileEntry) =>
    a.name.localeCompare(b.name, undefined, { sensitivity: "base" }) ||
    a.path.localeCompare(b.path);
  // A file whose count is not known yet goes after those that are.
  const most = (pick: (file: FileEntry) => number | null) => (a: FileEntry, b: FileEntry) =>
    (pick(b) ?? -1) - (pick(a) ?? -1) || byName(a, b);
  if (view.scope === "recent") return shown.sort(most((file) => file.opened));
  if (view.sort === "rows") return shown.sort(most((file) => file.rows));
  if (view.sort === "modified") return shown.sort(most((file) => file.modified));
  return shown.sort(byName);
}

/** The files opened most recently, newest first. */
export function recentFiles(files: FileEntry[], limit = 3): FileEntry[] {
  return files
    .filter((file) => file.opened !== null)
    .sort((a, b) => (b.opened ?? 0) - (a.opened ?? 0))
    .slice(0, limit);
}

/** The formats present, with how many files each has. */
export function formatCounts(files: FileEntry[]): { format: string; count: number }[] {
  const counts = new Map<string, number>();
  for (const file of files) counts.set(file.format, (counts.get(file.format) ?? 0) + 1);
  return [...counts]
    .map(([format, count]) => ({ format, count }))
    .sort((a, b) => a.format.localeCompare(b.format));
}

/** The last part of a folder's path: the name people know it by. */
export function folderName(path: string): string {
  const parts = path.split(/[\\/]/).filter(Boolean);
  return parts[parts.length - 1] ?? path;
}
