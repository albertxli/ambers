import { describe, expect, it } from "vitest";

import type { FileEntry } from "../../api/client";
import {
  type View,
  describeFile,
  folderName,
  formatCounts,
  recentFiles,
  visibleFiles,
} from "./files";

function file(name: string, more: Partial<FileEntry> = {}): FileEntry {
  return {
    path: `/data/${name}`,
    name,
    folder: "",
    format: name.endsWith(".zsav") ? "ZSAV" : "SAV",
    size: 1000,
    modified: 100,
    rows: 10,
    variables: 5,
    label: "",
    described: true,
    error: null,
    starred: false,
    opened: null,
    dataset: null,
    ...more,
  };
}

const files = [
  file("wave2.sav", { rows: 500, opened: 30, label: "Second wave" }),
  file("Archive.zsav", { rows: 9000, starred: true, modified: 900 }),
  file("pilot.sav", { rows: null, described: false, opened: 70, folder: "old/pilot" }),
];
const all: View = { scope: "all", format: "all", query: "", sort: "name" };
const names = (shown: FileEntry[]) => shown.map((one) => one.name);

describe("visibleFiles", () => {
  it("sorts by name, whatever the case", () => {
    expect(names(visibleFiles(files, all))).toEqual(["Archive.zsav", "pilot.sav", "wave2.sav"]);
  });

  it("sorts by rows with unknown counts last", () => {
    const shown = visibleFiles(files, { ...all, sort: "rows" });
    expect(names(shown)).toEqual(["Archive.zsav", "wave2.sav", "pilot.sav"]);
  });

  it("narrows by scope, format and search", () => {
    expect(names(visibleFiles(files, { ...all, scope: "starred" }))).toEqual(["Archive.zsav"]);
    expect(names(visibleFiles(files, { ...all, format: "SAV" }))).toEqual([
      "pilot.sav",
      "wave2.sav",
    ]);
    // The search looks at the name, the label and the folder.
    expect(names(visibleFiles(files, { ...all, query: "SECOND" }))).toEqual(["wave2.sav"]);
    expect(names(visibleFiles(files, { ...all, query: "old/" }))).toEqual(["pilot.sav"]);
    expect(visibleFiles(files, { ...all, query: "nothing" })).toEqual([]);
  });

  it("lists recently opened files newest first", () => {
    const shown = visibleFiles(files, { ...all, scope: "recent" });
    expect(names(shown)).toEqual(["pilot.sav", "wave2.sav"]);
  });

  it("does not reorder the list it was given", () => {
    const before = names(files);
    visibleFiles(files, { ...all, sort: "rows" });
    expect(names(files)).toEqual(before);
  });
});

describe("recentFiles", () => {
  it("returns the newest few", () => {
    expect(names(recentFiles(files))).toEqual(["pilot.sav", "wave2.sav"]);
    expect(names(recentFiles(files, 1))).toEqual(["pilot.sav"]);
  });
});

describe("formatCounts", () => {
  it("counts the files of each format", () => {
    expect(formatCounts(files)).toEqual([
      { format: "SAV", count: 2 },
      { format: "ZSAV", count: 1 },
    ]);
  });
});

describe("describeFile", () => {
  it("prefers the label, then the folder, and says when a file is unreadable", () => {
    expect(describeFile(files[0]!)).toBe("Second wave");
    expect(describeFile(files[2]!)).toBe("old/pilot");
    expect(describeFile(file("x.sav", { error: "not an SPSS file" }))).toBe(
      "Cannot be read: not an SPSS file",
    );
  });
});

describe("folderName", () => {
  it("is the last part of a path, on any system", () => {
    expect(folderName("C:\\Users\\me\\surveys")).toBe("surveys");
    expect(folderName("/home/me/surveys/")).toBe("surveys");
  });
});
