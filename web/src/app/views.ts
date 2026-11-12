// The screens a dataset can be looked at through. A new screen (Variable
// View, a codebook diff) is one folder under `features/` and one entry
// here; nothing else in the app needs to know about it.

import type { ComponentType } from "react";

import { DataView } from "../features/data-view/DataView";

export interface DatasetView {
  /** The name in the address: `#/data/d1`. */
  id: string;
  label: string;
  component: ComponentType<{ dataset: string }>;
}

export const DATASET_VIEWS: DatasetView[] = [{ id: "data", label: "Data", component: DataView }];

export const DEFAULT_VIEW = "data";
