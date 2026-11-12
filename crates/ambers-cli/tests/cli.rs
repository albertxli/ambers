//! End-to-end tests of the `ambers` binary against the committed fixtures
//! in `tests/data/`. Output is compared with `insta` snapshots; after an
//! intended change, review and accept them with `cargo insta review` (or
//! rerun with `INSTA_UPDATE=always`).
//!
//! The binaries run with their output captured, so nothing here sees
//! colour: that is the piped behaviour, and one test asserts it.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Run a built binary in `dir`. Fixture tests run from the repo root, so
/// the paths in the output are the same on every machine.
fn run_in(dir: &Path, exe: &str, args: &[&str]) -> Output {
    Command::new(exe)
        .args(args)
        .current_dir(dir)
        .output()
        .expect("failed to run the binary")
}

fn ambers(args: &[&str]) -> Output {
    run_in(&repo_root(), env!("CARGO_BIN_EXE_ambers"), args)
}

/// The short alias binary.
fn am(args: &[&str]) -> Output {
    run_in(&repo_root(), env!("CARGO_BIN_EXE_am"), args)
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

/// A fresh directory for files a test generates.
fn scratch_dir(test: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("ambers-cli-{test}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The `#` column of the variables table: the first cell of every row
/// below its header.
fn variable_numbers(output: &str) -> Vec<String> {
    output
        .lines()
        .filter_map(|line| line.strip_prefix("│ "))
        .filter_map(|line| line.split(['┆', '│']).next())
        .map(|cell| cell.trim().to_string())
        .skip_while(|cell| cell != "#")
        .skip(1)
        .collect()
}

// ---------------------------------------------------------------------------
// Layout
// ---------------------------------------------------------------------------

#[test]
fn info_summary() {
    insta::assert_snapshot!(ok_stdout(&["info", "tests/data/survey.sav"]));
}

#[test]
fn info_rows_zsav() {
    insta::assert_snapshot!(ok_stdout(&[
        "info",
        "tests/data/survey.zsav",
        "--rows",
        "3"
    ]));
}

#[test]
fn info_rows_long_strings() {
    insta::assert_snapshot!(ok_stdout(&[
        "info",
        "tests/data/long_strings.sav",
        "--rows",
        "2"
    ]));
}

#[test]
fn info_is_the_same_for_every_compression() {
    // The summary differs (path, size, compression), so compare everything
    // below it: the variables table and all twenty data rows.
    let body = |file: &str| {
        let out = ok_stdout(&["info", file, "--rows", "20"]);
        let (_summary, rest) = out.split_once("╯\n").expect("no summary table");
        rest.to_string()
    };
    let bytecode = body("tests/data/survey.sav");
    assert_eq!(bytecode, body("tests/data/survey_uncompressed.sav"));
    assert_eq!(bytecode, body("tests/data/survey.zsav"));
}

#[test]
fn piped_output_has_no_colour_codes() {
    let output = ambers(&["info", "tests/data/survey.sav", "--rows", "2"]);
    assert!(!stdout(&output).contains('\u{1b}'));
    let failed = ambers(&["info", "tests/data/does_not_exist.sav"]);
    assert!(!stderr(&failed).contains('\u{1b}'));
}

// ---------------------------------------------------------------------------
// Which variables are listed
// ---------------------------------------------------------------------------

#[test]
fn head_and_tail_choose_the_variables() {
    let numbers = |args: &[&str]| variable_numbers(&ok_stdout(args));
    let file = "tests/data/survey.sav"; // 13 variables: short enough to list in full

    let all: Vec<String> = (1..=13).map(|n| n.to_string()).collect();
    assert_eq!(numbers(&["info", file]), all);
    assert_eq!(numbers(&["info", file, "--head", "2"]), ["1", "2"]);
    assert_eq!(numbers(&["info", file, "--tail", "2"]), ["12", "13"]);
    assert_eq!(
        numbers(&["info", file, "--head", "2", "--tail", "1"]),
        ["1", "2", "...", "13"]
    );
    // Together they cover everything: no gap row.
    assert_eq!(
        numbers(&["info", file, "--head", "10", "--tail", "10"]),
        all
    );
}

#[test]
fn head_zero_prints_the_summary_only() {
    let out = ok_stdout(&["info", "tests/data/survey.sav", "--head", "0"]);
    assert!(out.contains("│ Status "));
    assert!(!out.contains("Name"));
}

#[test]
fn long_variable_lists_show_both_ends() {
    // 26 numeric variables v01..v26, written with ambers' own writer.
    let dir = scratch_dir("wide");
    let fields: Vec<Field> = (1..=26)
        .map(|n| Field::new(format!("v{n:02}"), DataType::Float64, true))
        .collect();
    let columns: Vec<ArrayRef> = (1..=26)
        .map(|n| Arc::new(Float64Array::from(vec![n as f64])) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let meta = ambers::SpssMetadata::from_arrow_schema(&batch.schema());
    ambers::write_sav(
        dir.join("wide.sav"),
        &batch,
        &meta,
        ambers::Compression::Bytecode,
        None,
    )
    .unwrap();

    let output = run_in(&dir, env!("CARGO_BIN_EXE_ambers"), &["info", "wide.sav"]);
    assert_eq!(output.status.code(), Some(0), "stderr: {}", stderr(&output));
    let table = stdout(&output);
    let expected: Vec<String> = (1..=10)
        .map(|n| n.to_string())
        .chain(["...".to_string()])
        .chain((22..=26).map(|n| n.to_string()))
        .collect();
    assert_eq!(variable_numbers(&table), expected);
    assert!(table.contains("┆ v10 "));
    assert!(table.contains("┆ v22 "));
    assert!(!table.contains("┆ v11 "));
    std::fs::remove_dir_all(dir).unwrap();
}

// ---------------------------------------------------------------------------
// Status: ok, warnings, error
// ---------------------------------------------------------------------------

#[test]
fn a_damaged_file_reports_warnings() {
    // The compression bias is the f64 at byte 84 of the header; anything
    // but 100 is a finding the reader records without failing.
    let dir = scratch_dir("biased");
    let mut bytes = std::fs::read(repo_root().join("tests/data/survey.sav")).unwrap();
    bytes[84..92].copy_from_slice(&50.0f64.to_le_bytes());
    std::fs::write(dir.join("biased.sav"), bytes).unwrap();

    let output = run_in(&dir, env!("CARGO_BIN_EXE_ambers"), &["info", "biased.sav"]);
    assert_eq!(output.status.code(), Some(0), "stderr: {}", stderr(&output));
    let summary = stdout(&output);
    assert!(summary.contains("│ Status      │ 1 warning"), "{summary}");
    assert!(
        summary.contains("│ Warning     │ compression bias"),
        "{summary}"
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn missing_file_fails_on_stderr() {
    let output = ambers(&["info", "tests/data/does_not_exist.sav"]);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(stdout(&output), "");
    let report = stderr(&output);
    assert!(
        report.contains("│ File   │ tests/data/does_not_exist.sav"),
        "{report}"
    );
    assert!(report.contains("│ Status │ error"), "{report}");
    // The message itself is the operating system's wording.
    assert!(report.contains("│ Error  │ "), "{report}");
}

#[test]
fn not_a_sav_file_fails_on_stderr() {
    let output = ambers(&["info", "Cargo.toml"]);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(stdout(&output), "");
    insta::assert_snapshot!(stderr(&output));
}

// ---------------------------------------------------------------------------
// Command line
// ---------------------------------------------------------------------------

#[test]
fn help() {
    insta::assert_snapshot!(ok_stdout(&["--help"]));
}

#[test]
fn info_help() {
    insta::assert_snapshot!(ok_stdout(&["info", "--help"]));
}

#[test]
fn version() {
    assert_eq!(
        ok_stdout(&["--version"]),
        format!("ambers {}\n", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn unknown_command_is_a_usage_error() {
    let output = ambers(&["bogus"]);
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(stdout(&output), "");
    insta::assert_snapshot!(stderr(&output));
}

#[test]
fn am_is_the_same_program() {
    for args in [
        &["info", "tests/data/survey.sav"][..],
        &["info", "tests/data/does_not_exist.sav"][..],
        &["--version"][..],
    ] {
        let (long, short) = (ambers(args), am(args));
        assert_eq!(long.status.code(), short.status.code(), "{args:?}");
        assert_eq!(stdout(&long), stdout(&short), "{args:?}");
        assert_eq!(stderr(&long), stderr(&short), "{args:?}");
    }
}

#[test]
fn am_usage_names_the_alias() {
    let help = stdout(&am(&["--help"]));
    assert!(help.contains("Usage: am <COMMAND>"), "{help}");
    // Apart from that one word the help is identical.
    assert_eq!(
        help.replace("Usage: am ", "Usage: ambers "),
        ok_stdout(&["--help"])
    );
}

// ---------------------------------------------------------------------------
// view: what can be checked without a terminal
// ---------------------------------------------------------------------------

#[test]
fn view_refuses_to_run_without_a_terminal() {
    // The test harness captures the output, so this is the piped case.
    let output = ambers(&["view", "tests/data/survey.sav"]);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(stdout(&output), "");
    assert_eq!(
        stderr(&output),
        "error: view needs an interactive terminal\n"
    );
}

#[test]
fn view_reports_an_unreadable_file_like_info() {
    let output = ambers(&["view", "tests/data/does_not_exist.sav"]);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(stdout(&output), "");
    let report = stderr(&output);
    assert!(
        report.contains("│ File   │ tests/data/does_not_exist.sav"),
        "{report}"
    );
    assert!(report.contains("│ Status │ error"), "{report}");

    let not_sav = ambers(&["view", "Cargo.toml"]);
    assert_eq!(stderr(&not_sav), stderr(&ambers(&["info", "Cargo.toml"])));
}

#[test]
fn view_help() {
    insta::assert_snapshot!(ok_stdout(&["view", "--help"]));
}

#[test]
fn view_rejects_an_unknown_mode() {
    let output = ambers(&["view", "tests/data/survey.sav", "--mode", "pretty"]);
    assert_eq!(output.status.code(), Some(2));
    assert!(stderr(&output).contains("[possible values: labels, codes, both]"));
}
