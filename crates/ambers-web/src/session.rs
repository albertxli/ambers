//! One open file on the server: a `Dataset` that a background thread
//! keeps feeding, and the answers the API gives about it.
//!
//! The page never holds the data. It asks for a window of rows, and the
//! work (formatting, filtering, counting) happens here, on the dataset.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard};

use ambers::Compression;
use ambers_dataset::detail::RowKind;
use ambers_dataset::{Dataset, Load, Mode, RowFilter, loader};

use crate::api;

/// Rows one request may ask for.
const MAX_ROWS: usize = 500;
/// Cells one answer may carry: a wide file gets fewer rows per request.
const MAX_CELLS: usize = 50_000;
/// Value-table rows per request when the page does not say.
pub const VALUES_PAGE: usize = 1_000;
/// Filter results kept ready, so scrolling a filtered view does not match
/// the column again for every window.
const KEPT_SETS: usize = 8;

/// The rows a set of filters keeps, brought up to date as rows load.
struct Kept {
    key: Vec<api::Filter>,
    filters: Vec<RowFilter>,
    rows: Vec<u32>,
    /// Rows `0..upto` have been tested.
    upto: usize,
}

pub struct Session {
    pub id: String,
    pub name: String,
    pub path: PathBuf,
    label: String,
    compression: &'static str,
    encoding: String,
    created: String,
    warnings: Vec<String>,
    data: RwLock<Dataset>,
    kept: Mutex<Vec<Kept>>,
}

impl Session {
    /// Open a file and start loading it in the background. The first
    /// rows are available as soon as the first batch arrives.
    ///
    /// The file is held open only while it loads: the loader thread owns
    /// it, and ends when the data is read, the limit or memory budget is
    /// reached, or the session is closed.
    pub fn open(
        id: String,
        path: &Path,
        limit: Option<usize>,
        memory_gb: Option<f64>,
    ) -> ambers::error::Result<Arc<Session>> {
        let (scanner, batch_rows) = loader::open(path, limit)?;
        let meta = scanner.metadata();
        let data = Dataset::new(meta, &scanner.schema(), batch_rows, limit);
        let profiler = data.profiler_handle();
        let name = path.file_name().map_or_else(
            || path.display().to_string(),
            |name| name.to_string_lossy().into_owned(),
        );
        let session = Arc::new(Session {
            id,
            name,
            path: path.to_path_buf(),
            label: meta.file_label.clone(),
            compression: match meta.compression {
                Compression::None => "uncompressed",
                Compression::Bytecode => "bytecode",
                Compression::Zlib => "zlib",
            },
            encoding: meta.file_encoding.clone(),
            created: meta.creation_time.clone(),
            warnings: meta.warnings.clone(),
            data: RwLock::new(data),
            kept: Mutex::new(Vec::new()),
        });

        let receiver = loader::spawn(scanner, profiler, loader::memory_budget(memory_gb));
        // The thread holds the session weakly: once it is closed, the
        // receiver is dropped here and the loader stops by itself.
        let weak = Arc::downgrade(&session);
        std::thread::spawn(move || {
            for message in receiver {
                let Some(session) = weak.upgrade() else {
                    return;
                };
                let mut data = session.data.write().unwrap_or_else(|p| p.into_inner());
                data.receive(message);
            }
        });
        Ok(session)
    }

    fn data(&self) -> RwLockReadGuard<'_, Dataset> {
        // A panic while holding the lock does not make the rows unusable.
        self.data.read().unwrap_or_else(|p| p.into_inner())
    }

    fn kept(&self) -> MutexGuard<'_, Vec<Kept>> {
        self.kept.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub fn info(&self) -> api::DatasetInfo {
        let data = self.data();
        let (load, load_error) = load_state(&data.load);
        api::DatasetInfo {
            id: self.id.clone(),
            name: self.name.clone(),
            path: self.path.display().to_string(),
            label: self.label.clone(),
            rows: data.rows as u32,
            total_rows: data.total_rows.map(|rows| rows as u32),
            variables: data.columns.len() as u32,
            load,
            load_error,
            counted_rows: data.counted_rows(),
            unlabeled_variables: data.unlabeled_variables() as u32,
            compression: self.compression.to_string(),
            encoding: self.encoding.clone(),
            created: self.created.clone(),
            warnings: self.warnings.clone(),
        }
    }

    pub fn variables(&self) -> api::Variables {
        let data = self.data();
        let variables = (data.columns.iter().enumerate())
            .map(|(index, column)| {
                let summary = data.summary(index);
                api::Variable {
                    index: index as u32,
                    name: column.name.clone(),
                    label: column.label.clone(),
                    kind: summary.variable_type.name().to_string(),
                    format: column.format.clone(),
                    measure: column.measure.to_string(),
                    role: column.role.to_string(),
                    numeric: column.numeric,
                    width: column.width(Mode::Labels) as u32,
                    values: (summary.values.iter())
                        .map(|(code, label)| api::ValueLabel {
                            code: column.code_text(code),
                            label: label.clone(),
                        })
                        .collect(),
                    missing: column.missing_text(),
                    valid: summary.n_valid,
                    missing_count: summary.n_missing,
                    label_check: data.label_check(index).to_string(),
                }
            })
            .collect();
        api::Variables {
            counted_rows: data.counted_rows(),
            variables,
        }
    }

    /// A window of rows, as the mode shows them, among those the filters
    /// keep. A filter that cannot be used is reported and left out.
    pub fn rows(&self, request: &api::RowsRequest) -> api::RowsPage {
        let data = self.data();
        let count = data.columns.len();
        let variables: Vec<u32> = if request.variables.is_empty() {
            (0..count as u32).collect()
        } else {
            (request.variables.iter().copied())
                .filter(|&variable| (variable as usize) < count)
                .collect()
        };
        let mode = match request.mode {
            api::CellMode::Labels => Mode::Labels,
            api::CellMode::Codes => Mode::Codes,
            api::CellMode::Both => Mode::Both,
        };
        // Fewer rows for a wide answer, but always at least one.
        let by_cells = (MAX_CELLS / variables.len().max(1)).max(1);
        let limit = (request.limit as usize).min(MAX_ROWS).min(by_cells);
        let offset = request.offset as usize;

        let (shown, window, problems) = self.window(&data, &request.filters, offset, limit);
        let rows = (window.into_iter())
            .map(|row| api::Row {
                number: row + 1,
                cells: (variables.iter())
                    .map(|&variable| {
                        let cell = data.cell(row as usize, variable as usize, mode);
                        api::Cell {
                            text: cell.text,
                            flag: if cell.unlabeled {
                                Some(api::CellFlag::Unlabeled)
                            } else if cell.user_missing {
                                Some(api::CellFlag::UserMissing)
                            } else {
                                None
                            },
                        }
                    })
                    .collect(),
            })
            .collect();
        api::RowsPage {
            offset: offset.min(shown) as u32,
            shown: shown as u32,
            loaded: data.rows as u32,
            load: load_state(&data.load).0,
            variables,
            rows,
            problems,
        }
    }

    /// The file rows at positions `offset..offset + limit` among the rows
    /// the filters keep, how many they keep in all, and the filters that
    /// could not be used.
    fn window(
        &self,
        data: &Dataset,
        filters: &[api::Filter],
        offset: usize,
        limit: usize,
    ) -> (usize, Vec<u32>, Vec<api::FilterProblem>) {
        let mut problems = Vec::new();
        let mut key = Vec::new();
        let mut ready = Vec::new();
        for filter in filters {
            let problem = |message: String| api::FilterProblem {
                variable: filter.variable,
                text: filter.text.clone(),
                message,
            };
            let column = filter.variable as usize;
            if column >= data.columns.len() {
                problems.push(problem("no such variable".to_string()));
                continue;
            }
            match data.matcher(column, &filter.text) {
                // An empty filter keeps every row.
                Ok(None) => {}
                Ok(Some((_, matcher))) => {
                    key.push(filter.clone());
                    ready.push(RowFilter {
                        column,
                        text: filter.text.clone(),
                        matcher,
                    });
                }
                Err(message) => problems.push(problem(message)),
            }
        }
        if key.is_empty() {
            let start = offset.min(data.rows);
            let end = (start + limit).min(data.rows);
            return (data.rows, (start as u32..end as u32).collect(), problems);
        }

        let mut sets = self.kept();
        let place = match sets.iter().position(|kept| kept.key == key) {
            Some(place) => place,
            None => {
                if sets.len() >= KEPT_SETS {
                    sets.remove(0);
                }
                sets.push(Kept {
                    key,
                    filters: ready,
                    rows: Vec::new(),
                    upto: 0,
                });
                sets.len() - 1
            }
        };
        // The most recently used set goes last, so the oldest is dropped.
        let mut kept = sets.remove(place);
        // Only the rows that arrived since the last request are tested.
        if kept.upto < data.rows {
            if let Some(rows) = data.passing(&mut kept.filters, kept.upto..data.rows) {
                kept.rows.extend(rows);
            }
            kept.upto = data.rows;
        }
        let start = offset.min(kept.rows.len());
        let end = (start + limit).min(kept.rows.len());
        let answer = (kept.rows.len(), kept.rows[start..end].to_vec(), problems);
        sets.push(kept);
        answer
    }

    /// One page of a variable's value table; `None` when there is no such
    /// variable.
    pub fn values(&self, variable: usize, offset: usize, limit: usize) -> Option<api::ValueTable> {
        let data = self.data();
        if variable >= data.columns.len() {
            return None;
        }
        let detail = data.detail(variable);
        let total = detail.rows.len();
        let start = offset.min(total);
        let end = (start + limit.max(1)).min(total);
        let rows = (detail.rows[start..end].iter())
            .map(|row| api::ValueRow {
                code: row.code.clone(),
                label: row.label.clone(),
                count: row.n,
                share_valid: row.pct_valid,
                share_total: row.pct_total,
                kind: match row.kind {
                    RowKind::Plain => api::ValueKind::Plain,
                    RowKind::Missing => api::ValueKind::Missing,
                    RowKind::UserMissing => api::ValueKind::UserMissing,
                    RowKind::Unlabeled => api::ValueKind::Unlabeled,
                },
            })
            .collect();
        Some(api::ValueTable {
            variable: variable as u32,
            rows_loaded: detail.rows_loaded as u32,
            categorical: detail.categorical,
            has_labels: detail.has_labels(),
            total: total as u32,
            offset: start as u32,
            rows,
        })
    }
}

fn load_state(load: &Load) -> (api::LoadState, Option<String>) {
    match load {
        Load::Loading => (api::LoadState::Loading, None),
        Load::Done => (api::LoadState::Done, None),
        Load::Limit => (api::LoadState::Limit, None),
        Load::Memory => (api::LoadState::Memory, None),
        Load::Failed(error) => (api::LoadState::Failed, Some(error.clone())),
    }
}
