# ambers: the browser viewer's front end

The page that `am open` serves. It is built into `../crates/ambers-web/dist/` and embedded in the `ambers` binary, so people who use ambers never need anything in this folder.

To work on it you need [Bun](https://bun.sh).

```bash
bun install        # once
bun run build      # type-check, then build into ../crates/ambers-web/dist/
bun run test       # unit tests (vitest)
bun run typecheck
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
| `src/features/<name>/` | One folder per screen. A new screen is a folder here and one entry in `views.ts` |
| `src/theme/` | `tokens.css`: every colour and font, as roles. Components never name a colour |
| `src/lib/` | Small helpers |

The page never holds the data. It asks the server for the cells near the screen, in blocks, and the server does the formatting, filtering and counting.

Colours are the open Dracula palette (MIT). Do not add Dracula Pro values: see `docs/decisions/0004-open-dracula-palette.md`.
