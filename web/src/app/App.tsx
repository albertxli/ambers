import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";

import { ApiFailure, api } from "../api/client";
import type { DatasetInfo, FileList, ServerInfo } from "../api/client";
import { ServerNotice } from "../components/ServerNotice";
import { Launcher } from "../features/launcher/Launcher";
import { type Route, datasetFromQuery, parseRoute, routeHash } from "./route";
import { DATASET_VIEWS, DEFAULT_VIEW, FILES_VIEW } from "./views";

/** How often the page tells the server it is still open. */
const HEARTBEAT = 5000;

function useRoute(): Route {
  const [route, setRoute] = useState(() => parseRoute(window.location.hash));
  useEffect(() => {
    const update = () => setRoute(parseRoute(window.location.hash));
    window.addEventListener("hashchange", update);
    return () => window.removeEventListener("hashchange", update);
  }, []);
  return route;
}

/**
 * True while the server does not answer this page: it has stopped, or a
 * new one runs in its place that does not know this page yet. `onBack` is
 * called when it answers again.
 */
function useHeartbeat(onBack: () => void): boolean {
  const [stopped, setStopped] = useState(false);
  const back = useRef(onBack);
  back.current = onBack;
  useEffect(() => {
    let away = false;
    const beat = () =>
      api.heartbeat().then(
        () => {
          if (away) back.current();
          away = false;
          setStopped(false);
        },
        (failure: unknown) => {
          away = failure instanceof ApiFailure && (failure.status === 0 || failure.status === 401);
          setStopped(away);
        },
      );
    void beat();
    const timer = window.setInterval(beat, HEARTBEAT);
    return () => window.clearInterval(timer);
  }, []);
  return stopped;
}

/** A path as a shell takes it, whatever spaces it has. */
function quoted(path: string): string {
  return `"${path.replaceAll('"', '\\"')}"`;
}

export function App() {
  const route = useRoute();
  const client = useQueryClient();
  const server = useQuery({ queryKey: ["server"], queryFn: api.server });

  // A dataset is shown when the address names one, or when the server
  // sent the browser to one (`am open file.sav`). Otherwise: the files.
  const files = route.view === FILES_VIEW;
  const dataset = files ? null : (route.dataset ?? datasetFromQuery(window.location.search));
  useEffect(() => {
    if (dataset && route.dataset !== dataset) {
      window.history.replaceState(null, "", routeHash({ view: route.view, dataset }));
    }
  }, [dataset, route.dataset, route.view]);

  // The server this page was opened from, as last heard of.
  const known = useRef<ServerInfo | null>(null);
  if (server.data && (!known.current || known.current.instance === server.data.instance)) {
    known.current = server.data;
  }
  /** The file this page shows, when it shows one. */
  const openPath = (): string | undefined =>
    known.current?.datasets.find((one) => one.id === dataset)?.path ??
    client.getQueryData<DatasetInfo>(["dataset", dataset])?.path;

  // The server answers again. If it is another server (ambers was started
  // again), this page's dataset ids mean nothing to it, or mean other
  // files: start over, on the same file when the new server has it open.
  const reconnected = async () => {
    const now = await api.server().catch(() => null);
    if (!now || !known.current || now.instance === known.current.instance) return;
    const path = openPath();
    const same = path ? now.datasets.find((one) => one.path === path) : undefined;
    window.location.hash = same
      ? routeHash({ view: route.view, dataset: same.id })
      : routeHash({ view: FILES_VIEW, dataset: null });
    window.location.reload();
  };
  const stopped = useHeartbeat(() => void reconnected());
  const [dismissed, setDismissed] = useState(false);
  useEffect(() => {
    // A later stop is said again.
    if (!stopped) setDismissed(false);
  }, [stopped]);

  const name = server.data?.datasets.find((one) => one.id === dataset)?.name;
  useEffect(() => {
    document.title = dataset ? `${name ?? "Data"} · ambers` : "Data files · ambers";
  }, [dataset, name]);

  const view = DATASET_VIEWS.find((one) => one.id === route.view) ?? DATASET_VIEWS[0];
  const Screen = view?.component;
  const open = (id: string) => {
    window.location.hash = routeHash({ view: DEFAULT_VIEW, dataset: id });
  };

  // What to run to get back here: the file, or the folder that was listed.
  const target = dataset ? openPath() : client.getQueryData<FileList>(["files"])?.roots[0];
  const command = target ? `am open ${quoted(target)}` : "am open";

  return (
    <main className="app-shell">
      <section className="workspace" aria-label="ambers">
        {stopped && !dismissed && (
          <ServerNotice command={command} onDismiss={() => setDismissed(true)} />
        )}
        {dataset && Screen ? <Screen key={dataset} dataset={dataset} /> : <Launcher onOpen={open} />}
      </section>
    </main>
  );
}
