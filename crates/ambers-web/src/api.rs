//! The shapes that cross between the server and the page.
//!
//! Every type here derives `TS`: the TypeScript definitions the front end
//! uses are generated from these structs into `web/src/api/generated/`,
//! so a changed field breaks the front-end build instead of the page.
//! The test `the_typescript_types_are_up_to_date` fails when the committed
//! files differ from what these structs generate; run it with
//! `AMBERS_UPDATE_BINDINGS=1` to rewrite them.
//!
//! Row and variable numbers are `u32` (a filter's row list is `u32` too);
//! counts are `u64` in Rust and plain numbers in TypeScript.

use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// The version of the API in the URL: `/api/v1/…`.
pub const API: &str = "/api/v1";

/// `GET /api/v1/server`
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct ServerInfo {
    /// The ambers version of the running server.
    pub version: String,
    /// Whether the full front end is built into this server, or only the
    /// small built-in page.
    pub interface: bool,
    pub datasets: Vec<DatasetInfo>,
}

/// How far a dataset's loading has come.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum LoadState {
    Loading,
    Done,
    /// Stopped at the row limit; the file has more rows.
    Limit,
    /// Stopped at the memory budget; the file has more rows.
    Memory,
    /// Reading failed part-way; the rows read so far are kept.
    Failed,
}

/// `GET /api/v1/datasets/{id}`: one open file.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct DatasetInfo {
    pub id: String,
    /// The file's name, without its folder.
    pub name: String,
    pub path: String,
    /// The file label set in SPSS; empty when there is none.
    pub label: String,
    /// Rows loaded so far.
    pub rows: u32,
    /// Rows in the file according to its header.
    pub total_rows: Option<u32>,
    pub variables: u32,
    pub load: LoadState,
    /// Why loading failed, when it did.
    pub load_error: Option<String>,
    /// Rows the counts cover; can trail `rows` slightly while loading.
    #[ts(type = "number")]
    pub counted_rows: u64,
    /// Variables that hold values without a label.
    pub unlabeled_variables: u32,
    pub compression: String,
    pub encoding: String,
    pub created: String,
    /// Damage the reader found in the file; empty for a healthy file.
    pub warnings: Vec<String>,
}

/// A labelled value of a variable.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct ValueLabel {
    pub code: String,
    pub label: String,
}

/// One variable, as Variable View lists it.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct Variable {
    /// Position in the file, from 0.
    pub index: u32,
    pub name: String,
    pub label: String,
    /// `categorical`, `numeric`, `text` or `date`, as the codebook has it.
    pub kind: String,
    /// The SPSS print format, e.g. `F8.2`.
    pub format: String,
    pub measure: String,
    pub role: String,
    /// Stored as a number (labels may still make it categorical).
    pub numeric: bool,
    /// A starting column width in characters, from the print format and
    /// the longest value label.
    pub width: u32,
    /// The labelled values, in code order; empty unless categorical.
    pub values: Vec<ValueLabel>,
    /// The user-missing definition as text; empty when there is none.
    pub missing: String,
    #[ts(type = "number")]
    pub valid: u64,
    #[ts(type = "number")]
    pub missing_count: u64,
    /// `ok`, what is wrong with the value labels, or empty when the
    /// variable has none to check.
    pub label_check: String,
}

/// `GET /api/v1/datasets/{id}/variables`
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct Variables {
    /// Rows the counts cover.
    #[ts(type = "number")]
    pub counted_rows: u64,
    pub variables: Vec<Variable>,
}

/// What cells show.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum CellMode {
    /// The value label, or the value itself when it has none.
    #[default]
    Labels,
    /// The stored value.
    Codes,
    /// The stored value followed by its label.
    Both,
}

/// A row filter on one variable. The text follows the filter grammar:
/// `jap` contains, `=3` exact, `!=3`, `>30`, `=.` missing, `a|b` either.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
pub struct Filter {
    /// The variable's position in the file, from 0.
    pub variable: u32,
    pub text: String,
}

/// `POST /api/v1/datasets/{id}/rows`: a window of rows.
#[derive(Debug, Clone, Default, Serialize, Deserialize, TS)]
pub struct RowsRequest {
    /// The first row wanted, as a position among the rows the filters keep.
    pub offset: u32,
    /// How many rows; the server caps it.
    pub limit: u32,
    /// The variables wanted, by position in the file. Empty: all.
    #[serde(default)]
    pub variables: Vec<u32>,
    #[serde(default)]
    pub mode: CellMode,
    /// A row must pass every filter.
    #[serde(default)]
    pub filters: Vec<Filter>,
}

/// What is special about a cell's value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum CellFlag {
    /// The variable has value labels, and this value has none.
    Unlabeled,
    /// The metadata declares this value user-missing.
    UserMissing,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct Cell {
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub flag: Option<CellFlag>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct Row {
    /// The row's number in the file, from 1, whatever is filtered.
    pub number: u32,
    /// One cell per variable asked for, in that order.
    pub cells: Vec<Cell>,
}

/// A filter that could not be used, and why. The others still apply.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct FilterProblem {
    pub variable: u32,
    pub text: String,
    pub message: String,
}

/// The answer to a `RowsRequest`.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct RowsPage {
    pub offset: u32,
    /// Rows the filters keep, of those loaded; equals `loaded` without
    /// filters.
    pub shown: u32,
    /// Rows loaded so far.
    pub loaded: u32,
    pub load: LoadState,
    /// The variables of each row's cells, by position in the file.
    pub variables: Vec<u32>,
    pub rows: Vec<Row>,
    pub problems: Vec<FilterProblem>,
}

/// What kind of row a value table row is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum ValueKind {
    Plain,
    /// The count of missing cases.
    Missing,
    /// A value the metadata declares user-missing.
    UserMissing,
    /// A code found in the data that has no value label.
    Unlabeled,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct ValueRow {
    /// The stored value as text.
    pub code: String,
    pub label: String,
    #[ts(type = "number")]
    pub count: u64,
    /// Share of valid cases, from 0 to 1; absent on the missing row.
    pub share_valid: Option<f64>,
    /// Share of all cases, from 0 to 1.
    pub share_total: Option<f64>,
    pub kind: ValueKind,
}

/// `GET /api/v1/datasets/{id}/variables/{index}/values`: every value of a
/// variable with its count, over the rows loaded so far.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct ValueTable {
    pub variable: u32,
    /// Rows the counts cover.
    pub rows_loaded: u32,
    /// The rows come from the codebook's value labels and codes; otherwise
    /// they are the distinct values found in the data.
    pub categorical: bool,
    pub has_labels: bool,
    /// Rows in the whole table; `rows` may be one page of it.
    pub total: u32,
    pub offset: u32,
    pub rows: Vec<ValueRow>,
}

/// `POST /api/v1/open`: what a second `am open` sends to a running server.
#[derive(Debug, Clone, Default, Serialize, Deserialize, TS)]
pub struct OpenRequest {
    /// The file to open, as an absolute path.
    pub path: String,
    /// Rows to load at most.
    #[serde(default)]
    pub limit: Option<u32>,
    /// Memory to load at most, in GB.
    #[serde(default)]
    pub memory_gb: Option<f64>,
}

/// What `POST /api/v1/open` answers: the dataset, when a file was given.
/// A folder is added to what the launch page lists, and opens nothing.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct OpenAnswer {
    pub dataset: Option<DatasetInfo>,
}

/// A data file the launch page lists.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct FileEntry {
    /// The file's full path: what to send back to open or star it.
    pub path: String,
    pub name: String,
    /// The folder the file is in, relative to the folder being listed
    /// (empty for that folder itself); a full path for a file elsewhere.
    pub folder: String,
    /// `SAV` or `ZSAV`.
    pub format: String,
    /// Size in bytes.
    pub size: u64,
    /// When the file was last changed, in seconds since 1970.
    pub modified: Option<u64>,
    /// Rows according to the file's header; `null` until it has been read,
    /// and for a file whose header does not say.
    pub rows: Option<u32>,
    pub variables: Option<u32>,
    /// The file label set in SPSS.
    pub label: Option<String>,
    /// The file's dictionary has been read (rows, variables, label).
    pub described: bool,
    /// Why the file cannot be read, when it cannot.
    pub error: Option<String>,
    pub starred: bool,
    /// When the file was last opened here, in seconds since 1970.
    pub opened: Option<u64>,
    /// The id of the dataset, when the file is open now.
    pub dataset: Option<String>,
}

/// `GET /api/v1/files`: the files the launch page may show and open.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct FileList {
    /// The folders being listed.
    pub roots: Vec<String>,
    pub files: Vec<FileEntry>,
    /// Files whose dictionary is still being read; ask again while not 0.
    pub pending: u32,
    /// The folders hold more data files than are listed.
    pub truncated: bool,
}

/// `POST /api/v1/files/open`: open a listed file from the page.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct FileRequest {
    pub path: String,
}

/// `POST /api/v1/files/star`
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct StarRequest {
    pub path: String,
    pub starred: bool,
}

/// The body of every error answer.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct ApiError {
    pub error: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::{Path, PathBuf};

    /// Where the front end keeps the generated types.
    fn generated() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../web/src/api/generated")
    }

    /// The `.ts` files of a folder by name, with `\n` line ends (git may
    /// check them out with `\r\n`).
    fn files(dir: &Path) -> BTreeMap<String, String> {
        let Ok(entries) = fs::read_dir(dir) else {
            return BTreeMap::new();
        };
        (entries.flatten())
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "ts"))
            .map(|entry| {
                let text = fs::read_to_string(entry.path()).unwrap();
                let name = entry.file_name().to_string_lossy().into_owned();
                (name, text.replace("\r\n", "\n"))
            })
            .collect()
    }

    #[test]
    fn the_typescript_types_are_up_to_date() {
        let fresh = tempfile::tempdir().unwrap();
        let config = ts_rs::Config::new()
            .with_out_dir(fresh.path())
            .with_large_int("number");
        // Each of these brings the types it is made of.
        ServerInfo::export_all(&config).unwrap();
        Variables::export_all(&config).unwrap();
        RowsRequest::export_all(&config).unwrap();
        RowsPage::export_all(&config).unwrap();
        ValueTable::export_all(&config).unwrap();
        OpenRequest::export_all(&config).unwrap();
        OpenAnswer::export_all(&config).unwrap();
        FileList::export_all(&config).unwrap();
        FileRequest::export_all(&config).unwrap();
        StarRequest::export_all(&config).unwrap();
        ApiError::export_all(&config).unwrap();
        let wanted = files(fresh.path());
        assert!(wanted.len() >= 15, "{:?}", wanted.keys());

        let committed = generated();
        if std::env::var_os("AMBERS_UPDATE_BINDINGS").is_some() {
            fs::create_dir_all(&committed).unwrap();
            for name in files(&committed).keys() {
                fs::remove_file(committed.join(name)).unwrap();
            }
            for (name, text) in &wanted {
                fs::write(committed.join(name), text).unwrap();
            }
        }
        assert!(
            files(&committed) == wanted,
            "web/src/api/generated/ differs from the structs in api.rs. \
             Rewrite it with: AMBERS_UPDATE_BINDINGS=1 cargo test -p ambers-web typescript"
        );
    }

    #[test]
    fn a_rows_request_needs_only_its_window() {
        let request: RowsRequest = serde_json::from_str(r#"{"offset": 5, "limit": 10}"#).unwrap();
        assert_eq!((request.offset, request.limit), (5, 10));
        assert!(request.variables.is_empty() && request.filters.is_empty());
        assert_eq!(request.mode, CellMode::Labels);
    }

    #[test]
    fn a_cell_without_a_flag_is_only_its_text() {
        let plain = Cell {
            text: "Male".to_string(),
            flag: None,
        };
        assert_eq!(serde_json::to_string(&plain).unwrap(), r#"{"text":"Male"}"#);
        let flagged = Cell {
            text: "7".to_string(),
            flag: Some(CellFlag::Unlabeled),
        };
        assert_eq!(
            serde_json::to_string(&flagged).unwrap(),
            r#"{"text":"7","flag":"unlabeled"}"#
        );
    }
}
