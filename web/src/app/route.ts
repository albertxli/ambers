// Where the page is: which screen, of which dataset. It lives in the part
// of the address after `#`, so the server never sees it and the browser's
// back button works.

import { DEFAULT_VIEW } from "./views";

export interface Route {
  view: string;
  /** The dataset's id, e.g. `d1`; `null` when the address names none. */
  dataset: string | null;
}

/** `#/data/d1` → the Data screen of dataset d1. */
export function parseRoute(hash: string): Route {
  const [view, dataset] = hash.replace(/^#\/?/, "").split("/");
  return { view: view || DEFAULT_VIEW, dataset: dataset || null };
}

export function routeHash(route: Route): string {
  return route.dataset ? `#/${route.view}/${route.dataset}` : `#/${route.view}`;
}

/**
 * The dataset named when the page was opened: `am open file.sav` sends
 * the browser to `/?dataset=d1`.
 */
export function datasetFromQuery(search: string): string | null {
  return new URLSearchParams(search).get("dataset");
}
