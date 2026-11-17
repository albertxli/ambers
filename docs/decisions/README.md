# Decisions

One short record per significant decision about how ambers is built: what was decided, what else was considered, and why. They explain the repository to someone reading it later; they are not user guides.

Add a record when a decision changes the shape of the project (a new crate, a dependency that others build on, a format or protocol, a security rule, something deliberately not done). Do not edit an old record to reverse it: write a new one and mark the old one superseded.

| # | Decision | Date |
|---|---|---|
| [0001](0001-cargo-workspace.md) | A Cargo workspace with one crate per concern | 2026-11-11 |
| [0002](0002-generated-api-types.md) | API types for the browser are generated from Rust with `ts-rs` | 2026-11-12 |
| [0003](0003-front-end-assets-in-sdist.md) | The built front end ships inside the Python sdist | 2026-11-12 |
| [0004](0004-open-dracula-palette.md) | The open Dracula palette, not Dracula Pro | 2026-11-12 |
| [0005](0005-local-server-security.md) | The browser viewer is served from `127.0.0.1` with a token and a Host check | 2026-11-12 |
| [0006](0006-vite-spa-with-bun.md) | The front end is a Vite single-page app built with Bun, not Next.js | 2026-11-12 |
| [0007](0007-server-lifecycle.md) | The server stops by itself, and there is one per ambers version | 2026-11-12 |
| [0008](0008-browser-filter-conditions.md) | The browser filter sends conditions, and a missing value passes only "is empty" | 2026-11-16 |
| [0009](0009-tanstack-table-for-columns.md) | TanStack Table holds the columns and their state; rows stay on the server | 2026-11-16 |
| [0010](0010-value-distributions-on-the-server.md) | A variable's value distribution is searched, sorted and paged on the server | 2026-11-16 |
