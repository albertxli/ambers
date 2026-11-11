//! Test-only helpers for locating files under the gitignored `test_data/`
//! directory at the repo root. Shared by the unit tests and, through
//! `#[path]`, by the integration tests.
#![allow(dead_code)]

use std::path::Path;

/// Absolute path of a repo-root-relative test file, e.g. `test_data/x.sav`.
pub fn data_path(rel: &str) -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(rel)
        .to_string_lossy()
        .into_owned()
}

/// Whether a test file is present. Tests skip when it is not, so with
/// `AMBERS_REQUIRE_TEST_DATA=1` a missing file panics instead: a run in
/// that mode proves no test passed by skipping.
pub fn data_exists(path: &str) -> bool {
    let exists = Path::new(path).exists();
    if !exists && std::env::var_os("AMBERS_REQUIRE_TEST_DATA").is_some_and(|v| v == "1") {
        panic!("AMBERS_REQUIRE_TEST_DATA=1 but test file is missing: {path}");
    }
    exists
}
