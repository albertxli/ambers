//! The viewer's state and what each key does to it. Nothing here touches
//! the terminal, so every behaviour is unit-tested; `draw` only reads it.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ambers::{SpssMetadata, Value};
use ambers_codebook::profile::{Profiler, ValueRow, VariableSummary};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use unicode_width::UnicodeWidthStr;

use super::detail::{self, Detail};
use super::format::{Cell, Column, Mode};
use super::loader::{End, Message};

/// Rows moved by one notch of the mouse wheel.
const WHEEL_ROWS: usize = 3;

/// The fields of the variable list: title, and whether it is right-aligned.
pub const VARIABLE_FIELDS: [(&str, bool); 8] = [
    ("#", true),
    ("Name", false),
    ("Type", false),
    ("Label", false),
    ("Values", false),
    ("N", true),
    ("Missing", true),
    ("Label check", false),
];

/// A mouse event the viewer acts on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mouse {
    ScrollUp,
    ScrollDown,
    ScrollLeft,
    ScrollRight,
    /// The left button, pressed.
    Click,
}

/// One field of the variable list on screen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VariableField {
    pub title: &'static str,
    /// Offset from the left edge of the list, frame included.
    pub x: usize,
    pub width: usize,
    pub right: bool,
}

/// A key press, already translated from the terminal's event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Key {
    Up,
    Down,
    Left,
    Right,
    PageUp,
    PageDown,
    HalfUp,
    HalfDown,
    /// First / last variable of the row.
    Home,
    End,
    /// First / last loaded row.
    Top,
    Bottom,
    Tab,
    Enter,
    Esc,
    Backspace,
    Char(char),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Load {
    Loading,
    Done,
    /// Loading stopped at the row limit; the file has more rows.
    Limit,
    /// Loading stopped at the memory budget; the file has more rows.
    Memory,
    /// Loading failed part-way; the rows read so far are kept.
    Failed(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Screen {
    Data,
    Variables,
}

/// A row number being typed: the grid jumps as digits arrive.
struct Goto {
    typed: String,
    /// Where the cursor was, to return to on Esc.
    origin: usize,
}

/// Text the viewer wants on the clipboard, and what to tell the user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Copy {
    pub text: String,
}

/// One column on screen: which variable, where, and how wide (the last
/// one may be cut by the edge).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Slot {
    pub column: usize,
    /// Offset from the left edge of the columns area.
    pub x: usize,
    pub width: usize,
}

pub struct State {
    pub file_name: String,
    pub columns: Vec<Column>,
    batches: Vec<RecordBatch>,
    batch_rows: usize,
    /// Rows loaded so far.
    pub rows: usize,
    /// Rows in the file according to its header.
    pub total_rows: Option<usize>,
    /// Rows to load at most, when the user set a limit.
    pub limit: Option<usize>,
    pub load: Load,

    pub mode: Mode,
    pub frozen: bool,
    pub screen: Screen,
    pub panel_open: bool,
    pub help: bool,

    pub cursor_row: usize,
    pub cursor_col: usize,
    pub top_row: usize,
    /// First scrollable column on screen.
    pub left_col: usize,
    /// First variable listed in the respondent panel.
    pub panel_top: usize,
    pub var_cursor: usize,
    pub var_top: usize,
    /// In Variable View: the highlighted field of the variable under the
    /// cursor, an index into `VARIABLE_FIELDS`.
    pub var_field: usize,
    /// In Variable View: the value table of the variable under the cursor,
    /// when it is open beside the list.
    pub detail: Option<Detail>,
    /// Value rows scrolled past in that table.
    pub detail_scroll: usize,
    /// A one-line message for the border, cleared by the next key.
    pub notice: Option<String>,

    goto: Option<Goto>,
    /// The loader thread publishes a copy of its profiler (types and
    /// counts) here after every batch.
    profiler: Arc<Mutex<Profiler>>,
    /// What drawing needs from the profiler, per variable, cached so that
    /// no frame computes it: the codebook summary and the label check.
    summaries: Vec<VariableSummary>,
    label_checks: Vec<String>,
    counted_rows: u64,
    refreshed: Option<Instant>,
    /// While loading, the cache is brought up to date this often.
    pub refresh_every: Duration,
    copy: Option<Copy>,
    width: usize,
    height: usize,
}

impl State {
    pub fn new(
        file_name: String,
        meta: &SpssMetadata,
        schema: &Schema,
        batch_rows: usize,
        limit: Option<usize>,
        mode: Mode,
    ) -> State {
        let columns = Column::from_metadata(meta, schema);
        let mut state = State {
            file_name,
            profiler: Arc::new(Mutex::new(Profiler::new(meta, schema))),
            summaries: Vec::new(),
            label_checks: Vec::new(),
            counted_rows: 0,
            refreshed: None,
            refresh_every: Duration::from_millis(250),
            copy: None,
            detail: None,
            detail_scroll: 0,
            var_field: 3,
            notice: None,
            columns,
            batches: Vec::new(),
            batch_rows,
            rows: 0,
            total_rows: meta.number_rows.and_then(|n| usize::try_from(n).ok()),
            limit,
            load: Load::Loading,
            mode,
            frozen: true,
            screen: Screen::Data,
            panel_open: false,
            help: false,
            cursor_row: 0,
            cursor_col: 0,
            top_row: 0,
            left_col: 0,
            panel_top: 0,
            var_cursor: 0,
            var_top: 0,
            goto: None,
            width: 80,
            height: 24,
        };
        state.refresh_codebook(true);
        state
    }

    /// The slot the loader thread publishes its profiler into.
    pub fn profiler_handle(&self) -> Arc<Mutex<Profiler>> {
        Arc::clone(&self.profiler)
    }

    /// A snapshot of the published profiler. The loader holds the lock
    /// only to swap a copy in, so this does not wait on its counting.
    fn profiler(&self) -> Profiler {
        // A panic in the loader does not invalidate the tallies so far.
        self.profiler
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Whether the label audit covers a variable: numeric, with labels.
    fn is_audited(&self, column: usize) -> bool {
        self.columns[column].numeric && !self.columns[column].labels.is_empty()
    }

    // --- loading ----------------------------------------------------------

    pub fn receive(&mut self, message: Message) {
        match message {
            Message::Batch(batch) => {
                self.rows += batch.num_rows();
                self.batches.push(batch);
                self.refresh_codebook(false);
            }
            Message::Finished(end) => {
                let more = self.total_rows.is_none_or(|total| total > self.rows);
                self.load = match end {
                    End::MemoryBudget if more => Load::Memory,
                    End::Data if more && self.limit.is_some_and(|limit| self.rows >= limit) => {
                        Load::Limit
                    }
                    _ => Load::Done,
                };
                self.refresh_codebook(true);
            }
            Message::Failed(error) => {
                self.load = Load::Failed(error);
                self.refresh_codebook(true);
            }
        }
    }

    // --- cells ------------------------------------------------------------

    pub fn cell(&self, row: usize, column: usize) -> Cell {
        self.cell_in(row, column, self.mode)
    }

    pub fn cell_in(&self, row: usize, column: usize, mode: Mode) -> Cell {
        let batch = &self.batches[row / self.batch_rows];
        self.columns[column].cell(batch.column(column), row % self.batch_rows, mode)
    }

    // --- codebook: types, counts, label audit -----------------------------

    /// Bring the cached types, counts and label checks up to date with
    /// what the loader has counted. While loading this happens a few times
    /// a second at most (`refresh_every`), not on every batch; `force`
    /// does it now. Drawing reads only the cache.
    fn refresh_codebook(&mut self, force: bool) {
        let due = self
            .refreshed
            .is_none_or(|at| at.elapsed() >= self.refresh_every);
        if !(force || due) {
            return;
        }
        let profiler = self.profiler();
        self.counted_rows = profiler.rows();
        self.summaries = profiler.summaries();
        self.label_checks = (0..self.columns.len())
            .map(|column| self.check_labels(&profiler, column))
            .collect();
        self.refreshed = Some(Instant::now());
        self.refresh_detail();
    }

    /// Rows the counts cover. The loader counts a batch just before it
    /// hands it over, and the cache is refreshed a few times a second, so
    /// this can differ slightly from `rows` while loading.
    pub fn counted_rows(&self) -> u64 {
        self.counted_rows
    }

    /// The codebook's summary of one variable: type and counts.
    pub fn summary(&self, column: usize) -> &VariableSummary {
        &self.summaries[column]
    }

    /// The codebook's value rows of one variable: `MISSING`, then a row
    /// per code for a categorical variable.
    pub fn value_rows(&self, column: usize) -> Vec<ValueRow> {
        self.profiler().values(column)
    }

    /// How many variables hold values that have no label.
    pub fn unlabeled_variables(&self) -> usize {
        (self.label_checks.iter())
            .filter(|check| check.contains("unlabelled"))
            .count()
    }

    /// The audit of one variable, for Variable View: `ok`, the unlabelled
    /// values, the duplicated labels, or empty when it is not audited.
    pub fn label_check(&self, column: usize) -> &str {
        &self.label_checks[column]
    }

    fn check_labels(&self, profiler: &Profiler, column: usize) -> String {
        if !self.is_audited(column) {
            return String::new();
        }
        let labels = &self.columns[column].labels;
        let many = profiler.overflowed(column);
        // The codes that occur in the data.
        let seen: Vec<Value> = (profiler.values(column).into_iter())
            .filter(|row| row.n > 0)
            .filter_map(|row| row.code)
            .collect();
        let mut findings = Vec::new();
        if let Some(found) = ambers_codebook::validate::unlabeled_values(&seen, labels) {
            let shown: Vec<String> = (found.values.iter().take(5))
                .map(|value| value.to_string())
                .collect();
            let more = if found.values.len() > 5 { ", …" } else { "" };
            let count = if many {
                "many".to_string()
            } else {
                found.values.len().to_string()
            };
            findings.push(format!("{count} unlabelled: {}{more}", shown.join(", ")));
        }
        for duplicate in ambers_codebook::validate::duplicate_labels(labels) {
            let codes: Vec<String> = duplicate.codes.iter().map(Value::to_string).collect();
            findings.push(format!(
                "duplicate: {} ({})",
                duplicate.label,
                codes.join(", ")
            ));
        }
        if findings.is_empty() {
            "ok".to_string()
        } else {
            findings.join("; ")
        }
    }

    // --- layout -----------------------------------------------------------

    pub fn resize(&mut self, width: u16, height: u16) {
        self.width = width as usize;
        self.height = height as usize;
        self.follow_cursor();
        self.follow_panel();
    }

    /// Data rows that fit: the screen minus the frame and the header.
    pub fn grid_rows(&self) -> usize {
        self.height.saturating_sub(3)
    }

    /// Width of the respondent panel, frame included; 0 when closed.
    pub fn panel_width(&self) -> usize {
        if !self.panel_open {
            return 0;
        }
        (self.width * 2 / 5)
            .clamp(36, 72)
            .min(self.width.saturating_sub(24))
    }

    /// Digits reserved for row numbers.
    pub fn gutter_width(&self) -> usize {
        let largest = self.rows.max(self.total_rows.unwrap_or(0)).max(1);
        largest.to_string().len().max(3)
    }

    /// Width left for variable columns, after the frame, the row-number
    /// gutter (` 123 │`) and the panel.
    pub fn columns_width(&self) -> usize {
        self.width
            .saturating_sub(2 + self.gutter_width() + 3 + self.panel_width())
    }

    fn frozen_active(&self) -> bool {
        self.frozen && self.columns.len() > 1
    }

    fn first_scrollable(&self) -> usize {
        usize::from(self.frozen_active())
    }

    /// The columns on screen, left to right, and where the divider after
    /// the frozen column goes.
    pub fn visible_columns(&self) -> (Vec<Slot>, Option<usize>) {
        let available = self.columns_width();
        let mut slots = Vec::new();
        let mut x = 0;
        let mut divider = None;
        let mut place = |column: usize, x: &mut usize| {
            // One space, the column, one space.
            if *x + 1 >= available {
                return false;
            }
            let width = self.columns[column].width(self.mode);
            slots.push(Slot {
                column,
                x: *x + 1,
                width: width.min(available - *x - 1),
            });
            *x += width + 2;
            true
        };
        if self.frozen_active() && place(0, &mut x) {
            divider = (x < available).then_some(x);
            x += 1;
        }
        for column in self.left_col.max(self.first_scrollable())..self.columns.len() {
            if !place(column, &mut x) {
                break;
            }
        }
        (slots, divider)
    }

    fn fully_visible(&self, column: usize) -> bool {
        let (slots, _) = self.visible_columns();
        slots.iter().any(|slot| {
            slot.column == column && slot.width == self.columns[column].width(self.mode)
        })
    }

    /// Scroll so the cursor cell is on screen.
    fn follow_cursor(&mut self) {
        let rows = self.grid_rows().max(1);
        if self.cursor_row < self.top_row {
            self.top_row = self.cursor_row;
        } else if self.cursor_row >= self.top_row + rows {
            self.top_row = self.cursor_row + 1 - rows;
        }

        let first = self.first_scrollable();
        self.left_col = self.left_col.max(first);
        if self.cursor_col >= first {
            if self.cursor_col < self.left_col {
                self.left_col = self.cursor_col;
            }
            while self.left_col < self.cursor_col && !self.fully_visible(self.cursor_col) {
                self.left_col += 1;
            }
        }

        let rows = self.grid_rows().max(1);
        if self.var_cursor < self.var_top {
            self.var_top = self.var_cursor;
        } else if self.var_cursor >= self.var_top + rows {
            self.var_top = self.var_cursor + 1 - rows;
        }
    }

    /// Variables listed in the respondent panel: the screen minus its frame.
    pub fn panel_rows(&self) -> usize {
        self.height.saturating_sub(2)
    }

    /// Scroll the panel so the cursor's variable is listed.
    fn follow_panel(&mut self) {
        let rows = self.panel_rows().max(1);
        if self.cursor_col < self.panel_top {
            self.panel_top = self.cursor_col;
        } else if self.cursor_col >= self.panel_top + rows {
            self.panel_top = self.cursor_col + 1 - rows;
        }
    }

    // --- status -----------------------------------------------------------

    /// The number being typed for go-to-row, if any.
    pub fn goto_text(&self) -> Option<&str> {
        self.goto.as_ref().map(|goto| goto.typed.as_str())
    }

    // --- keys -------------------------------------------------------------

    /// Text a key asked to put on the clipboard, handed over once.
    pub fn take_copy(&mut self) -> Option<Copy> {
        self.copy.take()
    }

    /// Apply a key. Returns `true` when the viewer should quit.
    pub fn on_key(&mut self, key: Key) -> bool {
        self.notice = None;
        if self.help {
            self.help = false;
            return false;
        }
        if self.goto.is_some() && self.goto_key(key) {
            return false;
        }
        let quit = match self.screen {
            Screen::Data => self.data_key(key),
            Screen::Variables => {
                self.variables_key(key);
                false
            }
        };
        self.follow_cursor();
        self.refresh_detail();
        quit
    }

    /// Keys while a row number is being typed. Returns `true` when the key
    /// was used up; any other key keeps the row and is handled normally.
    fn goto_key(&mut self, key: Key) -> bool {
        let goto = self.goto.as_mut().expect("goto is active");
        match key {
            Key::Char(digit) if digit.is_ascii_digit() => {
                if goto.typed.len() < 12 {
                    goto.typed.push(digit);
                }
            }
            Key::Backspace => {
                goto.typed.pop();
            }
            Key::Esc => {
                self.cursor_row = goto.origin;
                self.goto = None;
                self.follow_cursor();
                return true;
            }
            Key::Enter => {
                self.goto = None;
                return true;
            }
            _ => {
                self.goto = None;
                return false;
            }
        }
        match goto.typed.parse::<usize>() {
            Ok(row) => self.cursor_row = row.clamp(1, self.rows.max(1)) - 1,
            // Everything was deleted: back to where we started.
            Err(_) => {
                self.cursor_row = goto.origin;
                self.goto = None;
            }
        }
        self.follow_cursor();
        true
    }

    fn data_key(&mut self, key: Key) -> bool {
        let last_row = self.rows.saturating_sub(1);
        let last_col = self.columns.len().saturating_sub(1);
        let page = self.grid_rows().max(1);
        let column_before = self.cursor_col;
        match key {
            Key::Up | Key::Char('k') => self.cursor_row = self.cursor_row.saturating_sub(1),
            Key::Down | Key::Char('j') => self.cursor_row = (self.cursor_row + 1).min(last_row),
            Key::Left | Key::Char('h') => self.cursor_col = self.cursor_col.saturating_sub(1),
            Key::Right | Key::Char('l') => self.cursor_col = (self.cursor_col + 1).min(last_col),
            Key::PageUp => self.cursor_row = self.cursor_row.saturating_sub(page),
            Key::PageDown => self.cursor_row = (self.cursor_row + page).min(last_row),
            Key::HalfUp => self.cursor_row = self.cursor_row.saturating_sub(page / 2),
            Key::HalfDown => self.cursor_row = (self.cursor_row + page / 2).min(last_row),
            Key::Home => self.cursor_col = 0,
            Key::End => self.cursor_col = last_col,
            Key::Top | Key::Char('g') => self.cursor_row = 0,
            Key::Bottom | Key::Char('G') => self.cursor_row = last_row,
            Key::Tab => self.mode = self.mode.next(),
            Key::Enter => {
                self.panel_open = !self.panel_open;
                self.follow_panel();
            }
            Key::Char('i') => {
                self.var_cursor = self.cursor_col;
                self.screen = Screen::Variables;
            }
            Key::Char('f') => self.frozen = !self.frozen,
            Key::Char('y') => self.copy_data_cell(),
            Key::Char('?') => self.help = true,
            Key::Char('J') if self.panel_open => {
                self.panel_top = (self.panel_top + 1).min(last_col);
            }
            Key::Char('K') if self.panel_open => {
                self.panel_top = self.panel_top.saturating_sub(1);
            }
            Key::Char(digit @ '1'..='9') if self.rows > 0 => {
                self.goto = Some(Goto {
                    typed: String::new(),
                    origin: self.cursor_row,
                });
                self.goto_key(Key::Char(digit));
            }
            Key::Esc | Key::Char('q') => {
                if !self.panel_open {
                    return true;
                }
                self.panel_open = false;
            }
            _ => {}
        }
        if self.cursor_col != column_before {
            self.follow_panel();
        }
        false
    }

    fn variables_key(&mut self, key: Key) {
        let last = self.columns.len().saturating_sub(1);
        let last_field = VARIABLE_FIELDS.len() - 1;
        let page = self.grid_rows().max(1);
        match key {
            Key::Up | Key::Char('k') => self.var_cursor = self.var_cursor.saturating_sub(1),
            Key::Down | Key::Char('j') => self.var_cursor = (self.var_cursor + 1).min(last),
            Key::PageUp => self.var_cursor = self.var_cursor.saturating_sub(page),
            Key::PageDown => self.var_cursor = (self.var_cursor + page).min(last),
            Key::HalfUp => self.var_cursor = self.var_cursor.saturating_sub(page / 2),
            Key::HalfDown => self.var_cursor = (self.var_cursor + page / 2).min(last),
            Key::Top | Key::Char('g') => self.var_cursor = 0,
            Key::Bottom | Key::Char('G') => self.var_cursor = last,
            // The highlighted cell: the row number is not one.
            Key::Left | Key::Char('h') => self.var_field = (self.var_field - 1).max(1),
            Key::Right | Key::Char('l') => self.var_field = (self.var_field + 1).min(last_field),
            Key::Home => self.var_field = 1,
            Key::End => self.var_field = last_field,
            // The value table beside the list, like the respondent panel.
            Key::Enter if !self.columns.is_empty() => {
                self.detail = match self.detail {
                    Some(_) => None,
                    None => Some(self.build_detail()),
                };
                self.detail_scroll = 0;
            }
            Key::Char('J') if self.detail.is_some() => self.detail_scroll += 1,
            Key::Char('K') => self.detail_scroll = self.detail_scroll.saturating_sub(1),
            // Go to this variable in Data View.
            Key::Char('d') if !self.columns.is_empty() => {
                self.cursor_col = self.var_cursor;
                self.screen = Screen::Data;
                self.detail = None;
                self.follow_panel();
            }
            Key::Char('y') if self.detail.is_some() => self.copy_table(),
            Key::Char('y') | Key::Char('Y') => self.copy_variable_cell(),
            Key::Char('?') => self.help = true,
            Key::Esc | Key::Char('q') if self.detail.is_some() => self.detail = None,
            Key::Esc | Key::Char('i') | Key::Char('q') => {
                self.detail = None;
                self.screen = Screen::Data;
            }
            _ => {}
        }
    }

    // --- mouse --------------------------------------------------------------

    /// Apply a mouse event at screen cell (`x`, `y`). The wheel moves three
    /// rows at a time; a click moves the cursor to the cell under it.
    pub fn on_mouse(&mut self, mouse: Mouse, x: usize, y: usize) {
        if self.help {
            return;
        }
        self.notice = None;
        match self.screen {
            Screen::Data => self.data_mouse(mouse, x, y),
            Screen::Variables => self.variables_mouse(mouse, x, y),
        }
        self.follow_cursor();
        self.refresh_detail();
    }

    fn data_mouse(&mut self, mouse: Mouse, x: usize, y: usize) {
        let last_row = self.rows.saturating_sub(1);
        let last_col = self.columns.len().saturating_sub(1);
        let column_before = self.cursor_col;
        let panel_x = self.width.saturating_sub(self.panel_width());
        if self.panel_open && x >= panel_x {
            // Over the respondent panel: scroll it, or pick a variable.
            match mouse {
                Mouse::ScrollUp => self.panel_top = self.panel_top.saturating_sub(WHEEL_ROWS),
                Mouse::ScrollDown => self.panel_top = (self.panel_top + WHEEL_ROWS).min(last_col),
                Mouse::Click if (1..=self.panel_rows()).contains(&y) => {
                    self.cursor_col = (self.panel_top + y - 1).min(last_col);
                }
                _ => {}
            }
            return;
        }
        match mouse {
            Mouse::ScrollUp => self.cursor_row = self.cursor_row.saturating_sub(WHEEL_ROWS),
            Mouse::ScrollDown => self.cursor_row = (self.cursor_row + WHEEL_ROWS).min(last_row),
            Mouse::ScrollLeft => self.cursor_col = self.cursor_col.saturating_sub(1),
            Mouse::ScrollRight => self.cursor_col = (self.cursor_col + 1).min(last_col),
            Mouse::Click => {
                // The grid starts below the frame and the header.
                if y >= 2 && y - 2 < self.grid_rows() && self.top_row + y - 2 < self.rows {
                    self.cursor_row = self.top_row + y - 2;
                }
                // Columns start after the frame and the row-number gutter.
                let columns_x = 1 + self.gutter_width() + 3;
                if let Some(slot) = (self.visible_columns().0.into_iter())
                    .find(|slot| (columns_x + slot.x..columns_x + slot.x + slot.width).contains(&x))
                {
                    self.cursor_col = slot.column;
                }
            }
        }
        if self.cursor_col != column_before {
            self.follow_panel();
        }
    }

    fn variables_mouse(&mut self, mouse: Mouse, x: usize, y: usize) {
        let last = self.columns.len().saturating_sub(1);
        let last_field = VARIABLE_FIELDS.len() - 1;
        let detail_x = self.width.saturating_sub(self.detail_width());
        if self.detail.is_some() && x >= detail_x {
            // Over the value table: scroll it.
            match mouse {
                Mouse::ScrollUp => {
                    self.detail_scroll = self.detail_scroll.saturating_sub(WHEEL_ROWS)
                }
                Mouse::ScrollDown => self.detail_scroll += WHEEL_ROWS,
                _ => {}
            }
            return;
        }
        match mouse {
            Mouse::ScrollUp => self.var_cursor = self.var_cursor.saturating_sub(WHEEL_ROWS),
            Mouse::ScrollDown => self.var_cursor = (self.var_cursor + WHEEL_ROWS).min(last),
            Mouse::ScrollLeft => self.var_field = (self.var_field - 1).max(1),
            Mouse::ScrollRight => self.var_field = (self.var_field + 1).min(last_field),
            Mouse::Click => {
                if y >= 2 && y - 2 < self.grid_rows() && self.var_top + y - 2 <= last {
                    self.var_cursor = self.var_top + y - 2;
                }
                if let Some(field) = (self.variable_layout().iter())
                    .position(|field| (field.x..field.x + field.width).contains(&x))
                {
                    self.var_field = field.max(1);
                }
            }
        }
    }

    // --- Variable View ------------------------------------------------------

    /// Width of the value table beside Variable View, frame included; 0
    /// when it is closed.
    pub fn detail_width(&self) -> usize {
        if self.detail.is_none() {
            return 0;
        }
        (self.width / 2)
            .clamp(46, 96)
            .min(self.width.saturating_sub(24))
    }

    /// Where each field of the variable list is drawn: its offset from the
    /// left edge of the list and its width. Names are never cut.
    pub fn variable_layout(&self) -> Vec<VariableField> {
        let number = self.columns.len().to_string().len().max(1);
        let name = (self.columns.iter().map(|column| column.name.width()))
            .max()
            .unwrap_or(4)
            .max(4);
        let count = crate::info::group_digits(self.counted_rows()).len().max(7);
        let widths = [number, name, 11, 32, 30, count, count, 32];
        let mut x = 2;
        (VARIABLE_FIELDS.iter().zip(widths))
            .map(|((title, right), width)| {
                let field = VariableField {
                    title,
                    x,
                    width,
                    right: *right,
                };
                x += width + 2;
                field
            })
            .collect()
    }

    /// The full text of one cell of the variable list: what is copied, and
    /// what is drawn after shortening. Labelled values are one per line.
    pub fn variable_cell(&self, variable: usize, field: usize) -> String {
        let Some(column) = self.columns.get(variable) else {
            return String::new();
        };
        let summary = || self.summary(variable);
        match field {
            0 => (variable + 1).to_string(),
            1 => column.name.clone(),
            2 => summary().variable_type.name().to_string(),
            3 => column.label.clone(),
            4 => {
                let pairs: Vec<String> = (summary().values.iter())
                    .map(|(code, label)| format!("{code}={label}"))
                    .collect();
                pairs.join("\n")
            }
            5 => summary().n_valid.to_string(),
            6 => summary().n_missing.to_string(),
            _ => self.label_check(variable).to_string(),
        }
    }

    /// The value table of the variable under the cursor.
    fn build_detail(&self) -> Detail {
        let variable = self.var_cursor;
        let column = &self.columns[variable];
        let categorical = self.summary(variable).variable_type.is_categorical();
        let rows = if categorical {
            detail::categorical(column, self.value_rows(variable))
        } else {
            detail::distinct(column, variable, &self.batches)
        };
        Detail {
            variable,
            rows_loaded: self.rows,
            categorical,
            rows,
        }
    }

    /// Keep an open value table in step with the cursor and with loading,
    /// and its scroll position inside the table.
    fn refresh_detail(&mut self) {
        let Some(detail) = &self.detail else {
            return;
        };
        if detail.variable != self.var_cursor {
            self.detail_scroll = 0;
        }
        if detail.variable != self.var_cursor || detail.rows_loaded != self.rows {
            self.detail = Some(self.build_detail());
        }
        let rows = self.detail.as_ref().map_or(0, |detail| detail.rows.len());
        self.detail_scroll = self.detail_scroll.min(rows.saturating_sub(1));
    }

    // --- copy ---------------------------------------------------------------

    fn copy_text(&mut self, text: String, what: &str) {
        if text.is_empty() {
            self.notice = Some("nothing to copy".to_string());
            return;
        }
        self.notice = Some(format!("copied {what}"));
        self.copy = Some(Copy { text });
    }

    /// The highlighted cell of the variable list, in full.
    fn copy_variable_cell(&mut self) {
        let text = self.variable_cell(self.var_cursor, self.var_field);
        let what = VARIABLE_FIELDS[self.var_field].0.to_lowercase();
        self.copy_text(text, &what);
    }

    /// The open value table, as tab-separated columns.
    fn copy_table(&mut self) {
        let Some(detail) = &self.detail else {
            return;
        };
        let (text, rows) = (detail.tsv(), detail.rows.len());
        let noun = if rows == 1 { "row" } else { "rows" };
        self.copy_text(text, &format!("table ({rows} {noun})"));
    }

    /// The Data View cell under the cursor, in full, as it is shown.
    fn copy_data_cell(&mut self) {
        if self.rows == 0 || self.columns.is_empty() {
            return self.copy_text(String::new(), "");
        }
        let text = self.cell(self.cursor_row, self.cursor_col).text;
        self.copy_text(text, "cell");
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::fs::File;
    use std::io::BufReader;
    use std::path::Path;

    use ambers::scanner::SavScanner;

    /// A state with every row of a fixture loaded, on a `width`×`height` screen.
    pub(crate) fn loaded(name: &str, width: u16, height: u16) -> State {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/data")
            .join(name);
        let reader = BufReader::new(File::open(path).unwrap());
        let scanner = SavScanner::open(reader, 5).unwrap();
        let mut state = State::new(
            name.to_string(),
            scanner.metadata(),
            &scanner.schema(),
            5,
            None,
            Mode::Labels,
        );
        state.resize(width, height);
        // Tests look at the counts straight after a batch.
        state.refresh_every = Duration::ZERO;
        for message in super::super::loader::spawn(scanner, state.profiler_handle(), None) {
            state.receive(message);
        }
        state
    }

    fn press(state: &mut State, keys: &[Key]) {
        for &key in keys {
            assert!(!state.on_key(key), "{key:?} should not quit");
        }
    }

    #[test]
    fn loading_finishes() {
        let state = loaded("survey.sav", 100, 24);
        assert_eq!((state.rows, state.columns.len()), (20, 13));
        assert_eq!(state.load, Load::Done);
        assert_eq!(state.cell(1, 1).text, "Female");
    }

    #[test]
    fn stopping_at_the_limit_is_reported() {
        let mut state = loaded("survey.sav", 100, 24);
        // As if only 20 of a larger file were allowed.
        state.limit = Some(20);
        state.total_rows = Some(500);
        state.receive(Message::Finished(End::Data));
        assert_eq!(state.load, Load::Limit);
    }

    #[test]
    fn the_cursor_stops_at_every_edge() {
        let mut state = loaded("survey.sav", 100, 24);
        press(&mut state, &[Key::Up, Key::Left]);
        assert_eq!((state.cursor_row, state.cursor_col), (0, 0));
        press(&mut state, &[Key::Bottom, Key::End, Key::Down, Key::Right]);
        assert_eq!((state.cursor_row, state.cursor_col), (19, 12));
        press(&mut state, &[Key::Top, Key::Home]);
        assert_eq!((state.cursor_row, state.cursor_col), (0, 0));
        press(&mut state, &[Key::PageUp]);
        assert_eq!(state.cursor_row, 0);
    }

    #[test]
    fn vim_keys_move_like_the_arrows() {
        let mut state = loaded("survey.sav", 100, 24);
        press(
            &mut state,
            &[
                Key::Char('j'),
                Key::Char('j'),
                Key::Char('l'),
                Key::Char('k'),
            ],
        );
        assert_eq!((state.cursor_row, state.cursor_col), (1, 1));
        press(&mut state, &[Key::Char('h'), Key::Char('G')]);
        assert_eq!((state.cursor_row, state.cursor_col), (19, 0));
        press(&mut state, &[Key::Char('g')]);
        assert_eq!(state.cursor_row, 0);
    }

    #[test]
    fn rows_scroll_to_keep_the_cursor_on_screen() {
        // 13 lines: frame and header leave 10 data rows.
        let mut state = loaded("survey.sav", 100, 13);
        assert_eq!(state.grid_rows(), 10);
        press(&mut state, &[Key::PageDown]);
        assert_eq!((state.cursor_row, state.top_row), (10, 1));
        press(&mut state, &[Key::HalfDown]);
        assert_eq!((state.cursor_row, state.top_row), (15, 6));
        press(&mut state, &[Key::Bottom]);
        assert_eq!((state.cursor_row, state.top_row), (19, 10));
        press(&mut state, &[Key::Top]);
        assert_eq!((state.cursor_row, state.top_row), (0, 0));
    }

    #[test]
    fn columns_scroll_and_the_first_stays_frozen() {
        let mut state = loaded("survey.sav", 60, 24);
        let visible = |state: &State| -> Vec<usize> {
            state.visible_columns().0.iter().map(|s| s.column).collect()
        };
        assert_eq!(visible(&state)[..3], [0, 1, 2]);
        press(&mut state, &[Key::End]);
        // The last column is fully on screen and the ID column is still first.
        let columns = visible(&state);
        assert_eq!(columns[0], 0);
        assert!(columns.contains(&12));
        assert!(state.left_col > 1);
        assert!(state.visible_columns().1.is_some());
        press(&mut state, &[Key::Home]);
        // Back on the frozen column: nothing needs to scroll.
        assert_eq!(state.cursor_col, 0);
        press(&mut state, &[Key::Right]);
        assert_eq!(state.left_col, 1);
    }

    #[test]
    fn unfreezing_lets_the_first_column_scroll_away() {
        let mut state = loaded("survey.sav", 60, 24);
        press(&mut state, &[Key::Char('f'), Key::End]);
        let (slots, divider) = state.visible_columns();
        assert!(slots.iter().all(|slot| slot.column != 0));
        assert_eq!(divider, None);
        press(&mut state, &[Key::Char('f')]);
        assert_eq!(state.visible_columns().0[0].column, 0);
    }

    #[test]
    fn tab_cycles_the_modes() {
        let mut state = loaded("survey.sav", 100, 24);
        let seen: Vec<Mode> = (0..3)
            .map(|_| {
                press(&mut state, &[Key::Tab]);
                state.mode
            })
            .collect();
        assert_eq!(seen, [Mode::Codes, Mode::Both, Mode::Labels]);
    }

    #[test]
    fn typing_digits_jumps_to_the_row_and_esc_goes_back() {
        let mut state = loaded("survey.sav", 100, 24);
        press(&mut state, &[Key::Down, Key::Down]);
        press(&mut state, &[Key::Char('1')]);
        assert_eq!((state.cursor_row, state.goto_text()), (0, Some("1")));
        press(&mut state, &[Key::Char('5')]);
        assert_eq!((state.cursor_row, state.goto_text()), (14, Some("15")));
        press(&mut state, &[Key::Esc]);
        assert_eq!((state.cursor_row, state.goto_text()), (2, None));
    }

    #[test]
    fn goto_keeps_the_row_on_enter_and_clamps() {
        let mut state = loaded("survey.sav", 100, 24);
        press(&mut state, &[Key::Char('9'), Key::Char('9'), Key::Enter]);
        assert_eq!((state.cursor_row, state.goto_text()), (19, None));
        // Deleting every digit returns to the start; another key keeps the row.
        press(&mut state, &[Key::Char('3'), Key::Backspace]);
        assert_eq!((state.cursor_row, state.goto_text()), (19, None));
        press(&mut state, &[Key::Char('3'), Key::Down]);
        assert_eq!((state.cursor_row, state.goto_text()), (3, None));
    }

    #[test]
    fn the_panel_opens_closes_and_follows_the_cursor() {
        let mut state = loaded("survey.sav", 100, 8);
        press(&mut state, &[Key::Enter]);
        assert!(state.panel_open && state.panel_width() > 0);
        // 8 lines leave 6 panel rows; moving to the last variable scrolls it.
        press(&mut state, &[Key::End]);
        assert_eq!(state.panel_top, 13 - 6);
        press(&mut state, &[Key::Char('K')]);
        assert_eq!(state.panel_top, 6);
        // q closes the panel first, then quits.
        assert!(!state.on_key(Key::Char('q')));
        assert!(!state.panel_open);
        assert!(state.on_key(Key::Char('q')));
    }

    #[test]
    fn variable_view_moves_a_cell_cursor() {
        let mut state = loaded("survey.sav", 100, 24);
        press(&mut state, &[Key::Right, Key::Char('i')]);
        // Opens on the variable under the Data View cursor, on its label.
        assert_eq!(
            (state.screen, state.var_cursor, state.var_field),
            (Screen::Variables, 1, 3)
        );
        press(&mut state, &[Key::Down, Key::Right, Key::Right]);
        assert_eq!((state.var_cursor, state.var_field), (2, 5));
        // The row number is not a cell; the last field is the label check.
        press(&mut state, &[Key::Home, Key::Left]);
        assert_eq!(state.var_field, 1);
        press(&mut state, &[Key::End, Key::Right]);
        assert_eq!(state.var_field, 7);
        press(&mut state, &[Key::Bottom]);
        assert_eq!(state.var_cursor, 12);
        // Leaving with Esc does not move the Data View cursor.
        press(&mut state, &[Key::Esc]);
        assert_eq!((state.screen, state.cursor_col), (Screen::Data, 1));
    }

    #[test]
    fn d_goes_to_the_variable_in_data_view() {
        let mut state = loaded("survey.sav", 100, 24);
        press(
            &mut state,
            &[
                Key::Char('i'),
                Key::Down,
                Key::Down,
                Key::Down,
                Key::Char('d'),
            ],
        );
        assert_eq!((state.screen, state.cursor_col), (Screen::Data, 3));
        assert!(state.detail.is_none());
    }

    #[test]
    fn enter_opens_the_value_table_beside_the_list_and_it_follows_the_cursor() {
        let mut state = loaded("survey.sav", 120, 24);
        assert_eq!(state.detail_width(), 0);
        // satisfaction (variable 3): a missing row and six codes.
        press(
            &mut state,
            &[Key::Char('i'), Key::Down, Key::Down, Key::Enter],
        );
        let detail = state.detail.as_ref().unwrap();
        assert_eq!(
            (detail.variable, detail.categorical, detail.rows.len()),
            (2, true, 7)
        );
        assert_eq!(state.detail_width(), 60);

        // J / K scroll the table and stop at its last row.
        press(&mut state, &[Key::Char('J'), Key::Char('J')]);
        assert_eq!(state.detail_scroll, 2);
        for _ in 0..10 {
            press(&mut state, &[Key::Char('J')]);
        }
        assert_eq!(state.detail_scroll, 6);

        // Moving to another variable shows that variable, from the top.
        press(&mut state, &[Key::Down]);
        let detail = state.detail.as_ref().unwrap();
        assert_eq!(
            (detail.variable, detail.categorical, state.detail_scroll),
            (3, false, 0)
        );

        // Esc closes the table first, then Variable View; Enter toggles it.
        press(&mut state, &[Key::Esc]);
        assert_eq!(
            (state.screen, state.detail.is_some()),
            (Screen::Variables, false)
        );
        press(&mut state, &[Key::Enter, Key::Enter]);
        assert!(state.detail.is_none());
    }

    #[test]
    fn text_and_numeric_variables_list_their_distinct_values() {
        let mut state = loaded("variable_types.sav", 120, 24);
        let open = |state: &mut State, name: &str| {
            state.var_cursor = state.columns.iter().position(|c| c.name == name).unwrap();
            state.detail = None;
            state.on_key(Key::Enter);
            let rows = &state.detail.as_ref().unwrap().rows;
            rows.iter()
                .map(|row| (row.code.clone(), row.n))
                .collect::<Vec<_>>()
        };
        state.on_key(Key::Char('i'));
        let text = |code: &str, n: u64| (code.to_string(), n);
        assert_eq!(
            open(&mut state, "city"),
            [
                text("(missing)", 2),
                text("LA", 2),
                text("NYC", 3),
                text("Paris", 1)
            ]
        );
        assert_eq!(
            open(&mut state, "only1"),
            [text("(missing)", 1), text("1", 7)]
        );
        assert_eq!(open(&mut state, "when").len(), 8);
        // A categorical variable comes from the codebook: codes and labels.
        assert_eq!(open(&mut state, "likert").len(), 6);
    }

    #[test]
    fn an_open_value_table_keeps_up_with_loading() {
        let mut state = loaded("survey.sav", 120, 24);
        press(&mut state, &[Key::Char('i'), Key::Enter]);
        let before = state.detail.clone().unwrap();
        assert_eq!(before.rows_loaded, 20);
        // Five more rows arrive: the table is rebuilt for 25.
        let extra = state.batches[0].clone();
        state.receive(Message::Batch(extra));
        let after = state.detail.as_ref().unwrap();
        assert_eq!(after.rows_loaded, 25);
        assert_ne!(after.rows, before.rows);
    }

    #[test]
    fn y_copies_the_highlighted_cell_in_full() {
        let mut state = loaded("survey.sav", 100, 24);
        // gender's label.
        press(&mut state, &[Key::Char('i'), Key::Down, Key::Char('y')]);
        assert_eq!(state.take_copy().unwrap().text, "Gender");
        assert_eq!(state.notice.as_deref(), Some("copied label"));
        assert_eq!(state.take_copy(), None, "handed over once");

        // Its values, one per line; the notice goes with the next key.
        press(&mut state, &[Key::Right, Key::Char('y')]);
        assert_eq!(state.take_copy().unwrap().text, "1=Male\n2=Female");
        assert_eq!(state.notice.as_deref(), Some("copied values"));
        press(&mut state, &[Key::Home]);
        assert_eq!(state.notice, None);
        press(&mut state, &[Key::Char('y')]);
        assert_eq!(state.take_copy().unwrap().text, "gender");

        // Nothing to copy: say so, and leave the clipboard alone.
        let mut state = loaded("long_strings.sav", 100, 24);
        press(&mut state, &[Key::Char('i'), Key::Char('y')]);
        assert_eq!(state.take_copy(), None);
        assert_eq!(state.notice.as_deref(), Some("nothing to copy"));
    }

    #[test]
    fn y_copies_the_open_table_and_shift_y_the_cell() {
        let mut state = loaded("survey.sav", 120, 24);
        // gender: 10 Male, 10 Female, nothing missing.
        press(
            &mut state,
            &[Key::Char('i'), Key::Down, Key::Enter, Key::Char('y')],
        );
        assert_eq!(
            state.take_copy().unwrap().text,
            "Code\tLabel\tN\t% valid\t% total\n1.00\tMale\t10\t50.0%\t50.0%\n2.00\tFemale\t10\t50.0%\t50.0%"
        );
        assert_eq!(state.notice.as_deref(), Some("copied table (2 rows)"));
        press(&mut state, &[Key::Char('Y')]);
        assert_eq!(state.take_copy().unwrap().text, "Gender");
    }

    #[test]
    fn y_copies_the_data_cell_as_shown() {
        let mut state = loaded("survey.sav", 100, 24);
        press(&mut state, &[Key::Right, Key::Char('y')]);
        assert_eq!(state.take_copy().unwrap().text, "Male");
        assert_eq!(state.notice.as_deref(), Some("copied cell"));
        press(&mut state, &[Key::Tab, Key::Char('y')]);
        assert_eq!(state.take_copy().unwrap().text, "1.00");
    }

    #[test]
    fn the_wheel_scrolls_the_grid_and_a_click_selects_a_cell() {
        // 13 lines: 10 data rows. Gutter: ` 123 │`, so columns start at x = 7.
        let mut state = loaded("survey.sav", 100, 13);
        state.on_mouse(Mouse::ScrollDown, 30, 5);
        state.on_mouse(Mouse::ScrollDown, 30, 5);
        assert_eq!(state.cursor_row, 6);
        state.on_mouse(Mouse::ScrollUp, 30, 5);
        assert_eq!(state.cursor_row, 3);
        state.on_mouse(Mouse::ScrollRight, 30, 5);
        state.on_mouse(Mouse::ScrollRight, 30, 5);
        state.on_mouse(Mouse::ScrollLeft, 30, 5);
        assert_eq!(state.cursor_col, 1);
        for _ in 0..20 {
            state.on_mouse(Mouse::ScrollDown, 30, 5);
        }
        assert_eq!(state.cursor_row, 19, "stops at the last row");

        // Click the fourth data row (y = 2 is the first) on `gender`.
        let mut state = loaded("survey.sav", 100, 13);
        let gender = state.visible_columns().0[1];
        state.on_mouse(Mouse::Click, 7 + gender.x, 5);
        assert_eq!((state.cursor_row, state.cursor_col), (3, 1));
        // A click on the header or the frame moves no row.
        state.on_mouse(Mouse::Click, 7 + gender.x, 1);
        state.on_mouse(Mouse::Click, 7 + gender.x, 12);
        assert_eq!(state.cursor_row, 3);
    }

    #[test]
    fn the_mouse_works_in_the_panels() {
        // Over the respondent panel the wheel scrolls it; a click picks a variable.
        let mut state = loaded("survey.sav", 100, 8);
        press(&mut state, &[Key::Enter]);
        let panel_x = 100 - state.panel_width();
        state.on_mouse(Mouse::ScrollDown, panel_x + 5, 3);
        assert_eq!((state.panel_top, state.cursor_row), (3, 0));
        state.on_mouse(Mouse::Click, panel_x + 5, 2);
        assert_eq!(state.cursor_col, 4);

        // In Variable View: wheel over the list moves the cursor, over the
        // value table it scrolls the table; a click selects a cell.
        let mut state = loaded("survey.sav", 140, 24);
        press(
            &mut state,
            &[Key::Char('i'), Key::Down, Key::Down, Key::Enter],
        );
        let table_x = 140 - state.detail_width();
        state.on_mouse(Mouse::ScrollDown, table_x + 5, 10);
        assert_eq!((state.detail_scroll, state.var_cursor), (3, 2));
        state.on_mouse(Mouse::ScrollDown, 10, 10);
        assert_eq!(state.var_cursor, 5);
        let name = state.variable_layout()[1];
        state.on_mouse(Mouse::Click, name.x, 2);
        assert_eq!((state.var_cursor, state.var_field), (0, 1));
        assert_eq!(
            state.detail.as_ref().unwrap().variable,
            0,
            "the table follows"
        );
    }

    #[test]
    fn the_mouse_is_ignored_while_help_is_open() {
        let mut state = loaded("survey.sav", 100, 24);
        press(&mut state, &[Key::Char('?')]);
        state.on_mouse(Mouse::ScrollDown, 30, 5);
        assert!(state.help);
        assert_eq!(state.cursor_row, 0);
    }

    #[test]
    fn types_and_counts_come_from_the_codebook() {
        let state = loaded("variable_types.sav", 100, 24);
        let summary = |name: &str| {
            let column = state.columns.iter().position(|c| c.name == name).unwrap();
            state.summary(column)
        };
        assert_eq!(summary("flag").variable_type.name(), "categorical");
        assert_eq!(summary("only1").variable_type.name(), "numeric");
        assert_eq!(summary("city").variable_type.name(), "text");
        assert_eq!(summary("when").variable_type.name(), "date");
        let likert = summary("likert");
        assert_eq!(
            (likert.n_valid, likert.n_missing, likert.n_unique),
            (7, 1, 5)
        );
        assert_eq!(state.counted_rows(), 8);
        // Unlabelled 0/1 items are not a label problem: they have no labels to miss.
        assert_eq!(state.unlabeled_variables(), 0);
    }

    #[test]
    fn help_closes_on_any_key() {
        let mut state = loaded("survey.sav", 100, 24);
        press(&mut state, &[Key::Char('?')]);
        assert!(state.help);
        press(&mut state, &[Key::Down]);
        assert!(!state.help);
        assert_eq!(
            state.cursor_row, 0,
            "the key that closes help does nothing else"
        );
    }

    #[test]
    fn the_audit_matches_python_validate() {
        // Recorded from `am.validate()` on the same file:
        //   error   q1     2 unlabeled values: [7, 8] (4 of 6 unique values labeled)
        //   error   scale  10 unlabeled values: [1, 2, 3, 4, 5, ... and 5 more]
        //   warning brand  1 duplicate label: 'Other' (codes: 97, 98)
        let state = loaded("imperfect_labels.sav", 100, 24);
        let check = |name: &str| {
            let column = state.columns.iter().position(|c| c.name == name).unwrap();
            state.label_check(column)
        };
        assert_eq!(check("q1"), "2 unlabelled: 7, 8");
        assert_eq!(check("scale"), "10 unlabelled: 1, 2, 3, 4, 5, …");
        assert_eq!(check("brand"), "duplicate: Other (97, 98)");
        // Not audited: no labels, and a string variable.
        assert_eq!(check("age"), "");
        assert_eq!(check("city"), "");
        assert_eq!(state.unlabeled_variables(), 2);

        let survey = loaded("survey.sav", 100, 24);
        assert_eq!(survey.unlabeled_variables(), 0);
        assert_eq!(survey.label_check(1), "ok");
    }

    #[test]
    fn a_tiny_screen_does_not_panic() {
        for (width, height) in [(0, 0), (1, 1), (10, 3), (30, 4)] {
            let mut state = loaded("survey.sav", width, height);
            press(
                &mut state,
                &[Key::Enter, Key::End, Key::Bottom, Key::Char('i')],
            );
            press(&mut state, &[Key::Bottom, Key::Enter, Key::Top, Key::Home]);
            state.visible_columns();
        }
    }
}
