# 0002: API types for the browser are generated from Rust with `ts-rs`

Date: 2026-11-12
Status: accepted

## Decision

The request and response types of the `am open` JSON API are Rust structs in `ambers-web` that derive `ts-rs`. The TypeScript definitions are generated from them into `web/src/api/generated/` and committed. CI regenerates them and fails when the committed files differ. The front end imports only these types for API data.

## Alternatives considered

- **Hand-written TypeScript types.** No tooling, but a field renamed in Rust is found only when the page breaks at run time.
- **`utoipa` → OpenAPI → a generated client.** What marimo does. It also documents the API for outside callers. Rejected for now: the API is private, and both sides live in this repository, so the OpenAPI document would be a layer maintained for no reader.
- **`specta`.** Comparable to `ts-rs`; `ts-rs` is the smaller dependency and needs no runtime registration.

## Why

A change to the API must break the front-end build, not the page. With generated types, the TypeScript compiler finds every place that used the old shape.

If the API is ever opened to other programs, revisit this and add an OpenAPI description.
