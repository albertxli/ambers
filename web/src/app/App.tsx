import { useQuery } from "@tanstack/react-query";
import { useEffect, useState } from "react";

import { ApiFailure, api } from "../api/client";
import { type Route, datasetFromQuery, parseRoute, routeHash } from "./route";
import { DATASET_VIEWS } from "./views";

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

  // Without a dataset in the address: the one the server sent the browser
  // to, or the first that is open.
  const open = server.data?.datasets ?? [];
  const asked = route.dataset ?? datasetFromQuery(window.location.search);
  const dataset = asked ?? open[0]?.id ?? null;
  useEffect(() => {
    if (dataset && route.dataset !== dataset) {
      window.history.replaceState(null, "", routeHash({ view: route.view, dataset }));
    }
  }, [dataset, route.dataset, route.view]);

  const name = open.find((one) => one.id === dataset)?.name;
  useEffect(() => {
    document.title = name ? `${name} · ambers` : "ambers";
  }, [name]);

  const view = DATASET_VIEWS.find((one) => one.id === route.view) ?? DATASET_VIEWS[0];
  const Screen = view?.component;

  return (
    <main className="app-shell">
      <section className="workspace" aria-label="ambers">
        {stopped && (
          <div className="stopped" role="alert">
            The ambers server has stopped. Run <code>am open</code> again to continue.
          </div>
        )}
        {dataset && Screen ? (
          <Screen key={dataset} dataset={dataset} />
        ) : server.isError ? (
          <div className="notice is-error">
            <h1>The server is not answering</h1>
            <p>{server.error.message}</p>
          </div>
        ) : server.isPending ? (
          <div className="notice">
            <p>Connecting…</p>
          </div>
        ) : (
          <div className="notice">
            <h1>No file is open</h1>
            <p>
              Open one from a terminal: <code>am open survey.sav</code>
            </p>
          </div>
        )}
      </section>
    </main>
  );
}
