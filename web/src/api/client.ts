// The page's only way to the data: the JSON API of the local server.
//
// Every type comes from `generated/`, which is written from the Rust
// structs in crates/ambers-web/src/api.rs. Do not describe an API shape by
// hand here: if a field changes in Rust, this file must stop compiling.

import type { ApiError } from "./generated/ApiError";
import type { DatasetInfo } from "./generated/DatasetInfo";
import type { FileList } from "./generated/FileList";
import type { FileRequest } from "./generated/FileRequest";
import type { RowsPage } from "./generated/RowsPage";
import type { RowsRequest } from "./generated/RowsRequest";
import type { ServerInfo } from "./generated/ServerInfo";
import type { StarRequest } from "./generated/StarRequest";
import type { ValueTable } from "./generated/ValueTable";
import type { Variables } from "./generated/Variables";

export type { Cell } from "./generated/Cell";
export type { FileEntry } from "./generated/FileEntry";
export type { FileList } from "./generated/FileList";
export type { CellMode } from "./generated/CellMode";
export type { DatasetInfo } from "./generated/DatasetInfo";
export type { Filter } from "./generated/Filter";
export type { LoadState } from "./generated/LoadState";
export type { Row } from "./generated/Row";
export type { RowsPage } from "./generated/RowsPage";
export type { RowsRequest } from "./generated/RowsRequest";
export type { ServerInfo } from "./generated/ServerInfo";
export type { ValueTable } from "./generated/ValueTable";
export type { Variable } from "./generated/Variable";
export type { Variables } from "./generated/Variables";

const API = "/api/v1";

/** The server said no, or could not be reached. `message` is for people. */
export class ApiFailure extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
    this.name = "ApiFailure";
  }
}

async function call<T>(path: string, init?: RequestInit): Promise<T> {
  let answer: Response;
  try {
    answer = await fetch(API + path, init);
  } catch {
    throw new ApiFailure("The ambers server is not answering. It may have been stopped.", 0);
  }
  if (!answer.ok) {
    const said = (await answer.json().catch(() => null)) as ApiError | null;
    throw new ApiFailure(said?.error ?? answer.statusText, answer.status);
  }
  return answer.status === 204 ? (undefined as T) : ((await answer.json()) as T);
}

function post<T>(path: string, body: unknown): Promise<T> {
  return call<T>(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export const api = {
  server: () => call<ServerInfo>("/server"),
  dataset: (id: string) => call<DatasetInfo>(`/datasets/${id}`),
  variables: (id: string) => call<Variables>(`/datasets/${id}/variables`),
  rows: (id: string, request: RowsRequest) => post<RowsPage>(`/datasets/${id}/rows`, request),
  values: (id: string, variable: number, offset = 0, limit = 1000) =>
    call<ValueTable>(`/datasets/${id}/variables/${variable}/values?offset=${offset}&limit=${limit}`),
  /** The data files the launch page may show and open. */
  files: () => call<FileList>("/files"),
  /** Open a listed file; the server refuses any other path. */
  openFile: (path: string) => post<DatasetInfo>("/files/open", { path } satisfies FileRequest),
  starFile: (path: string, starred: boolean) =>
    post<void>("/files/star", { path, starred } satisfies StarRequest),
  /** Tells the server a page is still open; it stops when none is. */
  heartbeat: () => post<void>("/heartbeat", {}),
};
