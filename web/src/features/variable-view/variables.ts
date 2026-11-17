// What the Variable View does with the list of variables: which it
// lists (search, type, label check), in what order, and what a cell says
// when it is copied. The variables come whole from the server; they are
// metadata, a few hundred lines even for a large file, so this happens in
// the page.

import type { Variable } from "../../api/client";
import { auditText, kindName } from "../../lib/variable";

export type FieldId =
  | "name"
  | "type"
  | "label"
  | "values"
  | "valid"
  | "missing"
  | "check"
  | "format"
  | "measure"
  | "user_missing";

export interface Field {
  id: FieldId;
  label: string;
  /** The column's own width in pixels. */
  width: number;
  /** A count: right-aligned, largest first when sorted. */
  numeric?: boolean;
}

/** The columns of the Variable View, in order. */
export const FIELDS: readonly Field[] = [
  { id: "name", label: "Name", width: 150 },
  { id: "type", label: "Type", width: 120 },
  { id: "label", label: "Label", width: 330 },
  { id: "values", label: "Values", width: 340 },
  { id: "valid", label: "N", width: 86, numeric: true },
  { id: "missing", label: "Missing", width: 86, numeric: true },
  { id: "check", label: "Label check", width: 130 },
  { id: "format", label: "Format", width: 90 },
  { id: "measure", label: "Measure", width: 100 },
  { id: "user_missing", label: "User-missing", width: 130 },
];

/** A label check to list by. */
export type CheckFilter = "all" | "unlabelled" | "duplicates" | "no_label";

export interface VariableFilters {
  /** Looked for in names, variable labels and value labels. */
  search: string;
  /** A codebook type (`categorical`, `numeric`, `text`, `date`), or `all`. */
  kind: string;
  check: CheckFilter;
}

export const NO_FILTERS: VariableFilters = { search: "", kind: "all", check: "all" };

export function isFiltered(filters: VariableFilters): boolean {
  return filters.search.trim() !== "" || filters.kind !== "all" || filters.check !== "all";
}

function passesCheck(variable: Variable, check: CheckFilter): boolean {
  switch (check) {
    case "unlabelled":
      return variable.label_audit.unlabeled > 0;
    case "duplicates":
      return variable.label_audit.duplicates > 0;
    case "no_label":
      return variable.label.trim() === "";
    default:
      return true;
  }
}

/** The variables the filters keep, in file order. */
export function filterVariables(
  variables: readonly Variable[],
  filters: VariableFilters,
): Variable[] {
  const needle = filters.search.trim().toLowerCase();
  return variables.filter((variable) => {
    if (filters.kind !== "all" && variable.kind !== filters.kind) return false;
    if (!passesCheck(variable, filters.check)) return false;
    if (needle === "") return true;
    return (
      variable.name.toLowerCase().includes(needle) ||
      variable.label.toLowerCase().includes(needle) ||
      variable.values.some((value) => value.label.toLowerCase().includes(needle))
    );
  });
}

/** A variable's value labels on one line: `1 = Male; 2 = Female`. */
export function valuesText(variable: Variable): string {
  return variable.values.map((value) => `${value.code} = ${value.label}`).join("; ");
}

/** What a cell of the Variable View says, as text: for copying and the inspector. */
export function fieldText(variable: Variable, field: FieldId): string {
  switch (field) {
    case "name":
      return variable.name;
    case "type":
      return kindName(variable);
    case "label":
      return variable.label;
    case "values":
      return valuesText(variable);
    case "valid":
      return String(variable.valid);
    case "missing":
      return String(variable.missing_count);
    case "check":
      return variable.label_audit.audited ? auditText(variable) : "";
    case "format":
      return variable.format;
    case "measure":
      return variable.measure;
    case "user_missing":
      return variable.missing;
  }
}

const byText = new Intl.Collator(undefined, { numeric: true, sensitivity: "base" });

/** How serious a label check is: nothing to check, fine, then by findings. */
function checkRank(variable: Variable): number {
  const audit = variable.label_audit;
  if (!audit.audited) return -1;
  return audit.unlabeled + audit.duplicates + (audit.many ? 1_000_000 : 0);
}

/**
 * The ascending order of a column: negative when `a` comes first. Text
 * is compared as people read it (`Q2` before `Q10`), ignoring case; the
 * Values column goes by how many labels a variable has.
 */
export function compareBy(field: FieldId): (a: Variable, b: Variable) => number {
  switch (field) {
    case "valid":
      return (a, b) => a.valid - b.valid;
    case "missing":
      return (a, b) => a.missing_count - b.missing_count;
    case "values":
      return (a, b) => a.values.length - b.values.length || byText.compare(valuesText(a), valuesText(b));
    case "check":
      return (a, b) => checkRank(a) - checkRank(b);
    default:
      return (a, b) => byText.compare(fieldText(a, field), fieldText(b, field));
  }
}
