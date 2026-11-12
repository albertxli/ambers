//! The Rust codebook must report exactly what Python's `am.codebook()`
//! reports: types, counts and value rows.
//!
//! `python/tests/make_codebook_expected.py` records the Python output as
//! JSON next to the fixtures; this test reads each file with the engine,
//! feeds the profiler in small batches, and compares field by field.
//! After changing a fixture or the Python codebook, rerun that script and
//! review the diff.

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use ambers::Value;
use ambers::scanner::SavScanner;
use ambers_codebook::profile::Profiler;
use ambers_codebook::types::VariableType;
use serde_json::Value as Json;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Profile a file, a few rows per batch so counts cross batch boundaries.
fn profile(path: &Path) -> Profiler {
    let reader = BufReader::new(File::open(path).unwrap());
    let mut scanner = SavScanner::open(reader, 7).unwrap();
    let mut profiler = Profiler::new(scanner.metadata(), &scanner.schema());
    while let Some(batch) = scanner.next_batch().unwrap() {
        profiler.update(&batch);
    }
    profiler
}

/// A code from JSON: Python writes numeric codes as numbers.
fn code(json: &Json) -> Option<Value> {
    match json {
        Json::Null => None,
        Json::Number(number) => Some(Value::Numeric(number.as_f64().unwrap())),
        Json::String(text) => Some(Value::String(text.clone())),
        other => panic!("unexpected code {other}"),
    }
}

fn internal_name(variable_type: VariableType) -> &'static str {
    match variable_type {
        VariableType::Text => "text",
        VariableType::Date => "date",
        VariableType::Numeric => "numeric",
        VariableType::SingleSelect => "single-select",
        VariableType::MultiSelect => "multi-select",
    }
}

fn assert_close(actual: Option<f64>, expected: &Json, what: &str) {
    match (actual, expected.as_f64()) {
        (None, None) => {}
        (Some(a), Some(e)) if (a - e).abs() < 1e-12 => {}
        _ => panic!("{what}: got {actual:?}, Python has {expected}"),
    }
}

/// Compare one recorded Python result with the profiler's.
fn check(sav: &Path, expected_path: &Path) {
    let expected: Json =
        serde_json::from_reader(BufReader::new(File::open(expected_path).unwrap())).unwrap();
    let profiler = profile(sav);
    let file = sav.file_name().unwrap().to_string_lossy();
    assert_eq!(
        profiler.rows(),
        expected["rows"].as_u64().unwrap(),
        "{file}: rows"
    );

    // Variables view.
    let variables = expected["variables"].as_array().unwrap();
    assert_eq!(profiler.len(), variables.len(), "{file}: variable count");
    let mut value_rows = Vec::new();
    for (index, row) in variables.iter().enumerate() {
        let summary = profiler.summary(index);
        let name = &summary.name;
        let what = |field: &str| format!("{file}: {name}.{field}");
        assert_eq!(*name, row["variable"].as_str().unwrap(), "{file}: order");
        assert_eq!(
            summary.label,
            row["variable_label"].as_str().unwrap(),
            "{}",
            what("label")
        );
        assert_eq!(
            summary.variable_type.name(),
            row["variable_type"].as_str().unwrap(),
            "{}",
            what("type")
        );
        assert_eq!(
            internal_name(summary.variable_type),
            expected["internal_types"][name].as_str().unwrap(),
            "{}",
            what("internal type")
        );
        assert_eq!(
            Some(summary.n_valid),
            row["n_valid"].as_u64(),
            "{}",
            what("n_valid")
        );
        assert_eq!(
            Some(summary.n_missing),
            row["n_missing"].as_u64(),
            "{}",
            what("n_missing")
        );
        assert_eq!(
            Some(summary.n_total),
            row["n_total"].as_u64(),
            "{}",
            what("n_total")
        );
        assert_eq!(
            Some(summary.n_unique),
            row["n_unique"].as_u64(),
            "{}",
            what("n_unique")
        );
        assert_eq!(
            summary.n_labeled,
            row["n_labeled"].as_u64(),
            "{}",
            what("n_labeled")
        );
        assert_eq!(
            summary.n_unlabeled,
            row["n_unlabeled"].as_u64(),
            "{}",
            what("n_unlabeled")
        );

        // Python has null for a variable that is not categorical.
        let labelled: Vec<(Value, String)> = (row["values"].as_array().into_iter().flatten())
            .map(|pair| {
                let label = pair["value_label"].as_str().unwrap().to_string();
                (code(&pair["value_code"]).unwrap(), label)
            })
            .collect();
        assert_eq!(summary.values, labelled, "{}", what("values"));
        assert_eq!(
            row["values"].is_null(),
            !summary.variable_type.is_categorical(),
            "{}",
            what("values is null")
        );

        value_rows.extend(
            profiler
                .values(index)
                .into_iter()
                .map(|row| (name.clone(), row)),
        );
    }

    // Values view: the same rows in the same order.
    let values = expected["values"].as_array().unwrap();
    assert_eq!(value_rows.len(), values.len(), "{file}: value row count");
    for ((name, actual), row) in value_rows.iter().zip(values) {
        let shown = actual
            .code
            .as_ref()
            .map(Value::to_string)
            .unwrap_or_default();
        let what = |field: &str| format!("{file}: {name}[{shown}].{field}");
        assert_eq!(
            *name,
            row["variable"].as_str().unwrap(),
            "{file}: value row order"
        );
        assert_eq!(actual.code, code(&row["value_code"]), "{}", what("code"));
        assert_eq!(
            actual.label.as_deref(),
            row["value_label"].as_str(),
            "{}",
            what("label")
        );
        assert_eq!(Some(actual.n), row["value_n"].as_u64(), "{}", what("n"));
        assert_eq!(
            Some(actual.n_valid),
            row["n_valid"].as_u64(),
            "{}",
            what("n_valid")
        );
        assert_eq!(
            Some(actual.n_total),
            row["n_total"].as_u64(),
            "{}",
            what("n_total")
        );
        assert_close(actual.pct_valid, &row["pct_valid"], &what("pct_valid"));
        assert_close(actual.pct_total, &row["pct_total"], &what("pct_total"));
    }
}

/// Every `<name>.codebook.json` in `dir/expected`, with the file it describes.
fn recorded(dir: &Path) -> Vec<(PathBuf, PathBuf)> {
    let Ok(entries) = std::fs::read_dir(dir.join("expected")) else {
        return Vec::new();
    };
    let mut found: Vec<(PathBuf, PathBuf)> = entries
        .map(|entry| entry.unwrap().path())
        .filter_map(|expected| {
            let name = expected
                .file_name()?
                .to_str()?
                .strip_suffix(".codebook.json")?;
            Some((dir.join(name), expected.clone()))
        })
        .collect();
    found.sort();
    found
}

#[test]
fn committed_fixtures_match_python() {
    let recorded = recorded(&repo_root().join("tests/data"));
    let names: Vec<String> = (recorded.iter())
        .map(|(sav, _)| sav.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    // Every fixture has a recording: a new fixture without one fails here.
    assert_eq!(
        names,
        [
            "imperfect_labels.sav",
            "long_strings.sav",
            "survey.sav",
            "survey.zsav",
            "survey_uncompressed.sav",
            "variable_types.sav",
        ]
    );
    for (sav, expected) in recorded {
        check(&sav, &expected);
    }
}

/// Real survey files, when their recordings exist locally
/// (`make_codebook_expected.py --private`). Skipped in CI.
#[test]
fn private_files_match_python() {
    let recorded = recorded(&repo_root().join("test_data"));
    if recorded.is_empty() {
        let required = std::env::var_os("AMBERS_REQUIRE_TEST_DATA").is_some_and(|v| v == "1");
        assert!(
            !required,
            "AMBERS_REQUIRE_TEST_DATA=1 but test_data/expected has no recordings"
        );
        eprintln!("Skipping: no recordings in test_data/expected");
        return;
    }
    for (sav, expected) in recorded {
        check(&sav, &expected);
    }
}
