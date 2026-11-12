//! The page the server sends: the built front end when it was embedded at
//! compile time, otherwise a small built-in page.

mod embedded {
    include!(concat!(env!("OUT_DIR"), "/embedded.rs"));
}

/// Shown when the front end was not built into this binary. It works on
/// the same API, so the server is usable and testable without it.
const PLACEHOLDER: &str = include_str!("../assets/placeholder.html");

/// Whether the full front end is built in.
pub fn has_interface() -> bool {
    !embedded::FILES.is_empty()
}

/// The page itself.
pub fn index() -> (&'static [u8], &'static str) {
    file("index.html").unwrap_or((PLACEHOLDER.as_bytes(), "text/html; charset=utf-8"))
}

/// An embedded file by its path under `dist/`, with its content type.
pub fn file(path: &str) -> Option<(&'static [u8], &'static str)> {
    let path = path.trim_start_matches('/');
    (embedded::FILES.iter())
        .find(|(name, _)| *name == path)
        .map(|(name, bytes)| (*bytes, content_type(name)))
}

fn content_type(name: &str) -> &'static str {
    match name.rsplit('.').next().unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" | "map" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "ico" => "image/x-icon",
        "woff2" => "font/woff2",
        "woff" => "font/woff",
        "txt" => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn there_is_always_a_page() {
        let (bytes, kind) = index();
        assert!(!bytes.is_empty());
        assert!(kind.starts_with("text/html"));
        assert_eq!(file("no/such/file.js"), None);
    }

    #[test]
    fn content_types_follow_the_extension() {
        assert_eq!(
            content_type("assets/app.3f.js"),
            "text/javascript; charset=utf-8"
        );
        assert_eq!(content_type("assets/app.css"), "text/css; charset=utf-8");
        assert_eq!(content_type("fonts/mono.woff2"), "font/woff2");
        assert_eq!(content_type("unknown"), "application/octet-stream");
    }
}
