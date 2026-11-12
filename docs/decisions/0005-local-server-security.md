# 0005: The browser viewer is served from `127.0.0.1` with a token and a Host check

Date: 2026-11-12
Status: accepted

## Decision

`am open` starts a server for the browser viewer under these rules:

1. It listens on `127.0.0.1` only, never on all interfaces.
2. It creates a random token at start. The browser is opened with the token in the URL; after that the page carries it. Requests without it are refused.
3. Every request's `Host` header must be `127.0.0.1` or `localhost` (with the port). Anything else is refused before routing. Requests that change state must also carry a matching `Origin`.
4. The page loads nothing from outside: no CDN, no remote fonts or scripts.
5. The server lists and opens only files under the folder it was started in, and files named on the command line. The page cannot ask for an arbitrary path.

## Alternatives considered

- **One static HTML file with the data inside, no server.** The earlier rule for a codebook viewer, and still right for a small codebook. It cannot work for data: a survey file can be several gigabytes, and the page must ask for only the rows on screen.
- **Binding `127.0.0.1` and a token, without the Host check.** Not enough. With DNS rebinding, a web page on the internet can make the browser send requests to a local port under the attacker's host name. Checking `Host` closes that.

## Why

Client survey data passes through this server. It must be reachable by the person at the machine and by nothing else: not another machine on the network, not a web page open in another tab.
