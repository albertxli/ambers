// How a variable is described in words, wherever it is shown: the column
// headers' card, the Variable View, the value distribution. The facts come
// from the server; only the wording is here.

import type { DatasetInfo, Variable } from "../api/client";
import { count, percent } from "./format";

/** The codebook's type of a variable, capitalised. */
export function kindName(variable: Variable): string {
  switch (variable.kind) {
    case "categorical":
      return "Categorical";
    case "numeric":
      return "Numeric";
    case "date":
      return "Date";
    default:
      return "Text";
  }
}

/** How the file stores the variable. */
export function storageName(variable: Variable): string {
  if (variable.numeric) return "Numeric";
  return variable.kind === "date" ? "Date / time" : "String";
}

/** What a missing case of this variable is. */
export function missingName(variable: Variable): string {
  return variable.numeric || variable.kind === "date" ? "System missing" : "Empty";
}

/** The parts of a label check: `["3 unlabelled", "1 duplicate"]`; empty when all is well. */
export function auditFindings(variable: Variable): string[] {
  const audit = variable.label_audit;
  const findings: string[] = [];
  if (audit.unlabeled > 0) {
    findings.push(`${audit.many ? "many" : count(audit.unlabeled)} unlabelled`);
  }
  if (audit.duplicates > 0) {
    findings.push(`${count(audit.duplicates)} ${audit.duplicates === 1 ? "duplicate" : "duplicates"}`);
  }
  return findings;
}

/** The label check in a word or two: `OK`, `3 unlabelled`, or `—` when not checked. */
export function auditText(variable: Variable): string {
  if (!variable.label_audit.audited) return "—";
  const findings = auditFindings(variable);
  return findings.length === 0 ? "OK" : findings.join(" · ");
}

/** A variable's missing cases as a share of the rows counted: `1,204 (0.3%)`. */
export function missingShare(variable: Variable, countedRows: number): string {
  const share = countedRows > 0 ? variable.missing_count / countedRows : 0;
  return `${count(variable.missing_count)} (${percent(share)})`;
}

/** Which rows the counts cover: all of the file, or what has loaded. */
export function countsScope(info: DatasetInfo, countedRows: number): string {
  const rows = `${count(countedRows)} ${countedRows === 1 ? "row" : "rows"}`;
  const whole = info.total_rows !== null && countedRows >= info.total_rows;
  if (info.load === "done" || whole) return rows;
  const total = info.total_rows === null ? "" : ` of ${count(info.total_rows)}`;
  const first = `first ${count(countedRows)}${total} rows`;
  switch (info.load) {
    case "loading":
      return `${first}, still loading`;
    case "limit":
      return `${first} (row limit)`;
    case "memory":
      return `${first} (memory budget)`;
    default:
      return `${first} (reading stopped)`;
  }
}
