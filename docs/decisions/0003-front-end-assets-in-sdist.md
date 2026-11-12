# 0003: The built front end ships inside the Python sdist

Date: 2026-11-12
Status: accepted

## Decision

The front end is built before anything is packaged, into `crates/ambers-web/dist/` (ignored by git). The Python sdist includes that folder. `build.rs` of `ambers-web` never runs Bun: it embeds `dist/` when it is there, and otherwise the crate still compiles and `am open` says that this build has no browser interface.

The order is always: build the front end, then run cargo or maturin.

## Alternatives considered

- **Leave the assets out of the sdist.** Then a wheel built from the sdist has no browser interface, and the release check "build a wheel from the sdist, then use it" would pass while testing something users do not get.
- **Run Bun from `build.rs`.** Rejected: building from the sdist would need Bun and network access, and a cargo build would silently depend on a JavaScript toolchain.
- **Commit the built assets to git.** Rejected: generated files in history, and a stale copy whenever someone forgets to rebuild. (marimo also builds them in CI and ignores them in git.)

## Why

A wheel built from the sdist must be the same product as the wheels on PyPI, without asking the person building it for Bun. Someone building from a bare git checkout without Bun still gets a working package, and a clear message instead of a build failure.

The release workflow checks that a wheel built from the sdist has the assets.
