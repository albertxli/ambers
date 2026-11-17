// What every view of an open file needs to know: the file itself (rows
// loaded, how far loading is) and its variables with their counts.

import { useQuery } from "@tanstack/react-query";
import { useEffect } from "react";

import { api } from "../../api/client";

export function useDataset(dataset: string) {
  const info = useQuery({
    queryKey: ["dataset", dataset],
    queryFn: () => api.dataset(dataset),
    // While the file loads, the row count is followed closely.
    refetchInterval: (query) => (query.state.data?.load === "loading" ? 400 : false),
  });
  const loading = info.data?.load === "loading";
  const variables = useQuery({
    queryKey: ["variables", dataset],
    queryFn: () => api.variables(dataset),
    // Types and counts settle as rows arrive.
    refetchInterval: loading ? 2500 : false,
  });
  // Once more when loading ends, for the final counts.
  const finished = info.data ? info.data.load !== "loading" : false;
  const refetch = variables.refetch;
  useEffect(() => {
    if (finished) void refetch();
  }, [finished, refetch]);

  return { info: info.data, variables: variables.data, failure: info.error ?? variables.error };
}
