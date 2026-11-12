import { describe, expect, it } from "vitest";

import { datasetFromQuery, parseRoute, routeHash } from "./route";

describe("parseRoute", () => {
  it("reads the screen and the dataset", () => {
    expect(parseRoute("#/data/d1")).toEqual({ view: "data", dataset: "d1" });
    expect(parseRoute("#/variables/d12")).toEqual({ view: "variables", dataset: "d12" });
  });

  it("falls back to the data screen with no dataset", () => {
    expect(parseRoute("")).toEqual({ view: "data", dataset: null });
    expect(parseRoute("#/")).toEqual({ view: "data", dataset: null });
    expect(parseRoute("#/data")).toEqual({ view: "data", dataset: null });
  });
});

describe("routeHash", () => {
  it("is what parseRoute reads back", () => {
    const route = { view: "data", dataset: "d3" };
    expect(parseRoute(routeHash(route))).toEqual(route);
    expect(routeHash({ view: "data", dataset: null })).toBe("#/data");
  });
});

describe("datasetFromQuery", () => {
  it("reads the dataset the server sent the browser to", () => {
    expect(datasetFromQuery("?dataset=d2")).toBe("d2");
    expect(datasetFromQuery("")).toBeNull();
  });
});
