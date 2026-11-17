// A variable as the server describes one, for tests: everything plain
// unless the test says otherwise.

import type { Variable } from "../api/client";

export function variable(overrides: Partial<Variable> = {}): Variable {
  const kind = overrides.kind ?? "numeric";
  return {
    index: 0,
    name: "q1",
    label: "",
    kind,
    format: "F8.2",
    measure: "scale",
    role: "input",
    numeric: kind !== "text" && kind !== "date",
    width: 8,
    values: [],
    missing: "",
    valid: 0,
    missing_count: 0,
    label_check: "",
    label_audit: { audited: false, unlabeled: 0, many: false, duplicates: 0 },
    filter_kind: kind === "text" ? "text" : kind === "date" ? "date" : "number",
    ...overrides,
  };
}
