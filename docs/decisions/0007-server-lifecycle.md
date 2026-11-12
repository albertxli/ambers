# 0007: The server stops by itself, and there is one per ambers version

Date: 2026-11-12
Status: accepted

## Decision

1. **It stops when nobody is looking.** The page tells the server it is open every few seconds. When no request with the token has arrived for three minutes, the server exits. `--keep-alive` turns this off; Ctrl+C always works.
2. **One server per ambers version.** A server writes a record (port, token, version) into the user's state folder. A second `am open` reads it:
   - same version and the server answers: it asks that server to open the file, opens the browser on it and returns at once;
   - another version: that server is left alone, and a new one starts on another port, saying so in one line;
   - the server no longer answers: the record is removed and a new server starts.
3. **A file is held open only while it loads.** The loading thread owns the file and ends when the data is read, or the row limit or memory budget is reached.
4. **The Rust binary is the entry point.** The wheel's `ambers open` forwards its arguments to the same function with the GIL released; the server handles Ctrl+C (and Ctrl+Break, SIGTERM) itself.

The state folder is `%LOCALAPPDATA%\ambers` on Windows, `~/Library/Application Support/ambers` on macOS, and `$XDG_STATE_HOME/ambers` (or `~/.local/state/ambers`) elsewhere. `AMBERS_STATE_DIR` overrides it.

## Alternatives considered

- **Run until Ctrl+C only** (what a developer expects). A researcher who opens a file and closes the browser tab is left with a server they do not know about, holding the data in memory.
- **A new server for every `am open`.** Simple, but each takes a port and its own copy of the memory budget, and two files can never be compared in one page.
- **Join any running server, whatever its version.** Its API may differ from what the newer command expects; the failure would be confusing and silent.
- **Keep the file open while it is shown.** On Windows another program that wants the file for itself (SPSS saving over it, for example) is refused while any handle is open, and the user cannot tell why.

## Why

The people who will use this are researchers, not developers. The server has to look after itself: stop when it is no longer needed, not multiply, and not get in the way of the file it was asked to show. One server per version also prepares for showing two files side by side.
