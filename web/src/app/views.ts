// The screens a dataset can be looked at through. A new screen (a codebook
// diff) is one folder under `features/` and one entry here; the shell
// around them (`features/dataset/DatasetShell`) draws a tab for each.

import type { ComponentType } from "react";

import type { DatasetInfo, Variables } from "../api/client";
import type { IconName } from "../components/Icon";
import { DataView } from "../features/data-view/DataView";
import { VariableView } from "../features/variable-view/VariableView";

/** What every view of a file is given. */
export interface ViewProps {
  /** The dataset's id. */
  dataset: string;
  info: DatasetInfo;
  variables: Variables;
  /** False while another view is shown; this one stays in the page, hidden. */
  active: boolean;
  /** Show the keyboard shortcuts. */
  onShortcuts: () => void;
}

export interface DatasetView {
  /** The name in the address: `#/data/d1`. */
  id: string;
  label: string;
  icon: IconName;
  component: ComponentType<ViewProps>;
}

export const DATASET_VIEWS: DatasetView[] = [
  { id: "data", label: "Data", icon: "table", component: DataView },
  { id: "variables", label: "Variables", icon: "list", component: VariableView },
];

export const DEFAULT_VIEW = "data";

/** The address of the launch page: `#/files`. It is not a view of a dataset. */
export const FILES_VIEW = "files";
