import { useQuery } from "@tanstack/react-query";
import { useEffect, useState } from "react";

import { ApiFailure, api } from "../api/client";
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

/** True once the server has stopped answering. */
function useHeartbeat(): boolean {
  const [stopped, setStopped] = useState(false);
  useEffect(() => {
    const beat = () =>
      api.heartbeat().then(
        () => setStopped(false),
        (failure: unknown) => setStopped(failure instanceof ApiFailure && failure.status === 0),
      );
    void beat();
    const timer = window.setInterval(beat, HEARTBEAT);
    return () => window.clearInterval(timer);
  }, []);
  return stopped;
}

export function App() {
  const route = useRoute();
  const stopped = useHeartbeat();
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

  const name = server.data?.datasets.find((one) => one.id === dataset)?.name;
  useEffect(() => {
    document.title = dataset ? `${name ?? "Data"} · ambers` : "Data files · ambers";
  }, [dataset, name]);

  const view = DATASET_VIEWS.find((one) => one.id === route.view) ?? DATASET_VIEWS[0];
  const Screen = view?.component;
  const open = (id: string) => {
    window.location.hash = routeHash({ view: DEFAULT_VIEW, dataset: id });
  };

  return (
    <main className="app-shell">
      <section className="workspace" aria-label="ambers">
        {stopped && (
          <div className="stopped" role="alert">
            The ambers server has stopped. Run <code>am open</code> again to continue.
          </div>
        )}
        {dataset && Screen ? <Screen key={dataset} dataset={dataset} /> : <Launcher onOpen={open} />}
      </section>
    </main>
  );
}
