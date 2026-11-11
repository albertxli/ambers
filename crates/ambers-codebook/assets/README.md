# Viewer assets

The HTML, CSS and JavaScript of the codebook viewer will live here. They are
compiled into the binary with `include_str!` (see `src/html.rs`), and the
viewer is written out as a single self-contained HTML file.

Rules:

- Nothing is loaded from a CDN or any other network location. Third-party
  JavaScript is vendored into this directory.
- The page must work when opened straight from disk (`file://`), with no
  local server.
