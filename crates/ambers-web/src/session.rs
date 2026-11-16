//! One open file on the server: a `Dataset` that a background thread
//! keeps feeding, and the answers the API gives about it.
//!
//! The page never holds the data. It asks for a window of rows, and the
//! work (formatting, filtering, counting) happens here, on the dataset.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard};

use ambers::Compression;
use ambers_dataset::detail::{Detail, RowKind};
use ambers_dataset::filter::{Condition, Kind, Operator};
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
/// Value tables kept ready: the filter bar asks for the same variable's
/// values again with every letter typed.
const KEPT_TABLES: usize = 4;

/// The rows a set of filters keeps, brought up to date as rows load.
struct Kept {
    key: (api::FilterMode, Vec<api::Filter>),
    filters: Vec<RowFilter>,
    rows: Vec<u32>,
    /// Rows `0..upto` have been tested.
    upto: usize,
}

/// A variable's value table, good while no more rows have been counted.
struct Table {
    variable: usize,
    rows: usize,
    counted: u64,
    detail: Arc<Detail>,
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
    tables: Mutex<Vec<Table>>,
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
            tables: Mutex::new(Vec::new()),
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
                    filter_kind: filter_kind(Kind::of(column)),
                }
            })
            .collect();
        api::Variables {
            counted_rows: data.counted_rows(),
            variables,
            operators: (Kind::ALL.into_iter())
                .map(|kind| api::KindOperators {
                    kind: filter_kind(kind),
                    operators: (kind.operators().iter())
                        .map(|&operator| from_operator(operator))
                        .collect(),
                })
                .collect(),
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

        let (shown, window, problems) = self.window(&data, request, offset, limit);
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
        request: &api::RowsRequest,
        offset: usize,
        limit: usize,
    ) -> (usize, Vec<u32>, Vec<api::FilterProblem>) {
        let mode = request.filter_mode;
        let mut problems = Vec::new();
        let mut used = Vec::new();
        let mut ready = Vec::new();
        for (place, filter) in request.filters.iter().enumerate() {
            let problem = |message: String| api::FilterProblem {
                filter: place as u32,
                message,
            };
            let column = filter.variable as usize;
            if column >= data.columns.len() {
                problems.push(problem("no such variable".to_string()));
                continue;
            }
            let condition = Condition {
                operator: to_operator(filter.operator),
                value: filter.value.clone(),
            };
            match data.condition(column, &condition) {
                Ok(matcher) => {
                    used.push(filter.clone());
                    ready.push(RowFilter {
                        column,
                        text: String::new(),
                        matcher,
                    });
                }
                Err(message) => problems.push(problem(message)),
            }
        }
        if used.is_empty() {
            let start = offset.min(data.rows);
            let end = (start + limit).min(data.rows);
            return (data.rows, (start as u32..end as u32).collect(), problems);
        }

        let key = (mode, used);
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
            let arrived = kept.upto..data.rows;
            let passed = match mode {
                api::FilterMode::All => data.passing(&mut kept.filters, arrived),
                api::FilterMode::Any => data.passing_any(&mut kept.filters, arrived),
            };
            if let Some(rows) = passed {
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

    /// A variable's value table over the rows loaded so far. Counting the
    /// distinct values of a long text variable takes a moment, so the
    /// table is kept until more rows have been counted.
    fn table(&self, variable: usize) -> Option<Arc<Detail>> {
        let data = self.data();
        if variable >= data.columns.len() {
            return None;
        }
        let (rows, counted) = (data.rows, data.counted_rows());
        let mut tables = self.tables.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(place) = tables.iter().position(|table| table.variable == variable) {
            let table = tables.remove(place);
            if table.rows == rows && table.counted == counted {
                let detail = Arc::clone(&table.detail);
                tables.push(table);
                return Some(detail);
            }
        }
        let detail = Arc::new(data.detail(variable));
        if tables.len() >= KEPT_TABLES {
            tables.remove(0);
        }
        tables.push(Table {
            variable,
            rows,
            counted,
            detail: Arc::clone(&detail),
        });
        Some(detail)
    }

    /// One page of a variable's value table; `None` when there is no such
    /// variable.
    ///
    /// With `search`, only the values whose code or label contains it,
    /// ignoring case. With `top`, the most frequent of them first and no
    /// more than that many, without the row that counts missing cases:
    /// what the filter bar suggests while a value is typed.
    pub fn values(
        &self,
        variable: usize,
        offset: usize,
        limit: usize,
        search: Option<&str>,
        top: Option<usize>,
    ) -> Option<api::ValueTable> {
        let detail = self.table(variable)?;
        let needle = search.map(|text| text.trim().to_lowercase());
        let mut listed: Vec<&ambers_dataset::detail::DetailRow> = (detail.rows.iter())
            .filter(|row| top.is_none() || row.kind != RowKind::Missing)
            .filter(|row| match needle.as_deref() {
                None | Some("") => true,
                Some(needle) => {
                    row.code.to_lowercase().contains(needle)
                        || row.value_label.to_lowercase().contains(needle)
                }
            })
            .collect();
        let (offset, limit) = match top {
            Some(top) => {
                // Stable: values with the same count keep the table's order.
                listed.sort_by_key(|row| std::cmp::Reverse(row.n));
                (0, top)
            }
            None => (offset, limit),
        };
        let total = listed.len();
        let start = offset.min(total);
        let end = (start + limit.max(1)).min(total);
        let rows = (listed[start..end].iter())
            .map(|row| api::ValueRow {
                code: row.code.clone(),
                label: row.label.clone(),
                value_label: row.value_label.clone(),
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

fn filter_kind(kind: Kind) -> api::FilterKind {
    match kind {
        Kind::Number => api::FilterKind::Number,
        Kind::Text => api::FilterKind::Text,
        Kind::Categorical => api::FilterKind::Categorical,
        Kind::Date => api::FilterKind::Date,
    }
}

fn to_operator(operator: api::FilterOperator) -> Operator {
    match operator {
        api::FilterOperator::Equals => Operator::Equals,
        api::FilterOperator::NotEquals => Operator::NotEquals,
        api::FilterOperator::GreaterThan => Operator::Greater,
        api::FilterOperator::LessThan => Operator::Less,
        api::FilterOperator::GreaterOrEqual => Operator::GreaterOrEqual,
        api::FilterOperator::LessOrEqual => Operator::LessOrEqual,
        api::FilterOperator::Contains => Operator::Contains,
        api::FilterOperator::NotContains => Operator::NotContains,
        api::FilterOperator::StartsWith => Operator::StartsWith,
        api::FilterOperator::EndsWith => Operator::EndsWith,
        api::FilterOperator::Like => Operator::Like,
        api::FilterOperator::Ilike => Operator::ILike,
        api::FilterOperator::IsEmpty => Operator::IsEmpty,
        api::FilterOperator::IsNotEmpty => Operator::IsNotEmpty,
    }
}

fn from_operator(operator: Operator) -> api::FilterOperator {
    match operator {
        Operator::Equals => api::FilterOperator::Equals,
        Operator::NotEquals => api::FilterOperator::NotEquals,
        Operator::Greater => api::FilterOperator::GreaterThan,
        Operator::Less => api::FilterOperator::LessThan,
        Operator::GreaterOrEqual => api::FilterOperator::GreaterOrEqual,
        Operator::LessOrEqual => api::FilterOperator::LessOrEqual,
        Operator::Contains => api::FilterOperator::Contains,
        Operator::NotContains => api::FilterOperator::NotContains,
        Operator::StartsWith => api::FilterOperator::StartsWith,
        Operator::EndsWith => api::FilterOperator::EndsWith,
        Operator::Like => api::FilterOperator::Like,
        Operator::ILike => api::FilterOperator::Ilike,
        Operator::IsEmpty => api::FilterOperator::IsEmpty,
        Operator::IsNotEmpty => api::FilterOperator::IsNotEmpty,
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
