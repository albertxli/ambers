# ambers: the browser viewer's front end

The page that `am open` serves. It is built into `../crates/ambers-web/dist/` and embedded in the `ambers` binary, so people who use ambers never need anything in this folder.

To work on it you need [Bun](https://bun.sh).

```bash
bun install        # once
bun run build      # type-check, then build into ../crates/ambers-web/dist/
bun run test       # unit tests (vitest)
bun run typecheck
bun run e2e        # the built page against a real server, in a browser (after bun run build)
```

After `bun run build`, rebuild the binary (`cargo build -p ambers-cli`) to embed the new page.

## With hot reload

Start a server with a fixed token and port, then the dev server:

```bash
AMBERS_DEV_TOKEN=dev am open ../tests/data/survey.sav --no-browser --port 2627 --keep-alive
bun run dev        # http://localhost:5173
```

The dev server passes `/api` on to the ambers server the way the command line talks to it, so the server's security checks stay as they are.

## Layout

| Folder | Holds |
|---|---|
| `src/api/` | `client.ts`, the only code that calls the server. `generated/` is written from the Rust structs in `crates/ambers-web/src/api.rs`: do not edit it. After changing `api.rs`, run `AMBERS_UPDATE_BINDINGS=1 cargo test -p ambers-web typescript` |
| `src/app/` | The shell: the address (`#/<view>/<dataset>`) and the list of screens (`views.ts`) |
| `src/features/<name>/` | One folder per screen (`launcher`, `data-view`, `variable-view`), or per part that screens share (`filters`, and `dataset`: the top bar with the Data / Variables tabs around the views of a file). A new view of a file is a folder here and one entry in `views.ts` |
| `src/components/` | Pieces used across features: the grid both views draw (`grid/`), the card that describes a variable, the shortcuts dialog, icons, the server-stopped notice |
| `src/theme/` | `tokens.css`: every colour and font, as roles. Components never name a colour |
| `src/lib/` | Small helpers: number formats, how a variable is worded |
| `src/test/` | What tests share |

The page never holds the data. It asks the server for the cells near the screen, in blocks, and the server does the formatting, filtering and counting. TanStack Table holds the Data View's columns and their state (sizes, which are shown), never data rows: see `docs/decisions/0009-tanstack-table-for-columns.md`. The Variable View's table holds the variables, which are metadata. A variable's value distribution is searched, sorted and paged on the server: `docs/decisions/0010-value-distributions-on-the-server.md`.

Colours are the open Dracula palette (MIT). Do not add Dracula Pro values: see `docs/decisions/0004-open-dracula-palette.md`.
