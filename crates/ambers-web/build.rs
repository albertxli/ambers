//! Embeds the built front end, when there is one.
//!
//! This script never builds the front end and never runs Bun: it only
//! looks. If `dist/index.html` exists (built beforehand by
//! `bun run build` in `web/`, or shipped inside the sdist) every file
//! under `dist/` is compiled into the crate. If not, the crate still
//! compiles, with an empty list, and the server shows its built-in page.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::{env, fs};

fn main() {
    let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let dist = manifest.join("dist");
    println!("cargo:rerun-if-changed=dist");
    println!("cargo:rerun-if-changed=build.rs");

    let mut files = Vec::new();
    if dist.join("index.html").is_file() {
        collect(&dist, &dist, &mut files);
        files.sort();
    }

    let mut code = String::from("pub static FILES: &[(&str, &[u8])] = &[\n");
    for (name, path) in &files {
        // Debug formatting quotes and escapes both strings.
        writeln!(code, "    ({name:?}, include_bytes!({path:?})),").unwrap();
    }
    code.push_str("];\n");
    let out = PathBuf::from(env::var("OUT_DIR").unwrap()).join("embedded.rs");
    fs::write(out, code).unwrap();
}

/// Every file under `dir`, as (path relative to `root` with `/`, full path).
fn collect(root: &Path, dir: &Path, files: &mut Vec<(String, String)>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect(root, &path, files);
        } else if let Ok(relative) = path.strip_prefix(root) {
            let name = relative
                .components()
                .map(|part| part.as_os_str().to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join("/");
            files.push((name, path.to_string_lossy().into_owned()));
        }
    }
}
