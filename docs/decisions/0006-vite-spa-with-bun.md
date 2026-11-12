# 0006: The front end is a Vite single-page app built with Bun, not Next.js

Date: 2026-11-12
Status: accepted

## Decision

The browser viewer's front end lives in `web/`: TypeScript, React, Vite as the bundler, Tailwind for styles, TanStack Table / Virtual / Query for the grid and data fetching. Bun installs packages and runs scripts. The build output is plain static files, which the Rust binary embeds and serves.

Bun is needed to develop and to build releases. People who use ambers never need it.

## Alternatives considered

- **Next.js.** It is a server framework: it renders pages and routes requests on a Node server. ambers already has its server, the Rust binary, and must ship as one file with no Node. Next.js would bring a second server to switch off. (marimo, the model for this viewer, is also a Vite single-page app.)
- **Plain HTML, CSS and JavaScript with no build step.** No toolchain at all, but a large interactive interface (virtualised grid, filters, a file launcher, later a codebook diff) becomes hard to keep correct without types and components.
- **pnpm or npm instead of Bun.** Both work. Bun is one fast tool for installing and running; the choice is easy to change because Vite does the bundling.

## Why

The page must be static files so the binary stays self-contained, and the interface is large enough to need types and components. Vite produces exactly that, and the design mock-ups are already React with Tailwind.
