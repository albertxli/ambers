//! End-to-end tests of the `ambers` binary against the committed fixtures
//! in `tests/data/`. Output is compared with `insta` snapshots; after an
//! intended change, review and accept them with `cargo insta review` (or
//! rerun with `INSTA_UPDATE=always`).

use std::path::Path;
use std::process::{Command, Output};

/// Run the built binary from the repo root, so fixture paths in the output
/// are the same on every machine.
fn ambers(args: &[&str]) -> Output {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    Command::new(env!("CARGO_BIN_EXE_ambers"))
        .args(args)
        .current_dir(repo_root)
        .output()
        .expect("failed to run the ambers binary")
}

fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).expect("stdout is not UTF-8")
}

fn stderr(output: &Output) -> String {
    String::from_utf8(output.stderr.clone()).expect("stderr is not UTF-8")
}

/// A successful run: exit code 0 and nothing on stderr.
fn ok_stdout(args: &[&str]) -> String {
    let output = ambers(args);
    assert_eq!(output.status.code(), Some(0), "stderr: {}", stderr(&output));
    assert_eq!(stderr(&output), "");
    stdout(&output)
}

#[test]
fn info_summary() {
    insta::assert_snapshot!(ok_stdout(&["info", "tests/data/survey.sav"]));
}

#[test]
fn info_head_zsav() {
    insta::assert_snapshot!(ok_stdout(&[
        "info",
        "tests/data/survey.zsav",
        "--head",
        "3"
    ]));
}

#[test]
fn info_head_long_strings() {
    insta::assert_snapshot!(ok_stdout(&[
        "info",
        "tests/data/long_strings.sav",
        "--head",
        "2"
    ]));
}

#[test]
fn info_is_the_same_for_every_compression() {
    // Only the path, size, format and compression lines may differ.
    let body = |file: &str| {
        ok_stdout(&["info", file, "--head", "20"])
            .lines()
            .filter(|line| {
                !["File:", "Size:", "Format:", "Compression:", "Created:"]
                    .iter()
                    .any(|prefix| line.starts_with(prefix))
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    let bytecode = body("tests/data/survey.sav");
    assert_eq!(bytecode, body("tests/data/survey_uncompressed.sav"));
    assert_eq!(bytecode, body("tests/data/survey.zsav"));
}

#[test]
fn help() {
    insta::assert_snapshot!(ok_stdout(&["--help"]));
}

#[test]
fn version() {
    assert_eq!(
        ok_stdout(&["--version"]),
        format!("ambers {}\n", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn missing_file_fails_on_stderr() {
    let output = ambers(&["info", "tests/data/does_not_exist.sav"]);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(stdout(&output), "");
    // The rest of the message is the operating system's wording.
    assert!(
        stderr(&output).starts_with("error: cannot read tests/data/does_not_exist.sav: "),
        "stderr: {}",
        stderr(&output)
    );
}

#[test]
fn not_a_sav_file_fails_on_stderr() {
    let output = ambers(&["info", "Cargo.toml"]);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(stdout(&output), "");
    assert!(
        stderr(&output).starts_with("error: cannot read Cargo.toml: invalid magic number"),
        "stderr: {}",
        stderr(&output)
    );
}

#[test]
fn unknown_command_is_a_usage_error() {
    let output = ambers(&["bogus"]);
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(stdout(&output), "");
    insta::assert_snapshot!(stderr(&output));
}
