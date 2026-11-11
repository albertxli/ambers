//! The HTML codebook viewer (`html` feature).
//!
//! Will render a codebook as one self-contained HTML file. The page's
//! HTML, CSS and JavaScript live in `assets/` and are compiled into the
//! binary with `include_str!`; the codebook is injected as inline JSON.
//! The page loads nothing from the network and needs no local server.
