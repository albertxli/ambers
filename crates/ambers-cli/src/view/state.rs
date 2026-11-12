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
use super::filter::{self, Candidates, Matcher, Query};
use super::format::{Cell, Column, Mode};
use super::loader::{End, Message};

/// Rows or columns moved by one notch of the mouse wheel.
const WHEEL_ROWS: usize = 3;
/// Rows or columns moved by the fast keys (Shift+arrows, `{ } < >`).
const FAST_STEP: usize = 50;
/// Lines of the full-text bar under the variable list.
const BAR_LINES: usize = 3;
/// Fields of the variable list that never scroll sideways: `#` and Name.
const FIXED_FIELDS: usize = 2;
/// Said when the panel-copy key is pressed with no side panel open.
const NO_PANEL: &str = "open the panel with Enter first";
/// Longest text the filter bar accepts.
const PROMPT_LENGTH: usize = 60;

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
    /// Which field: an index into `VARIABLE_FIELDS`.
    pub index: usize,
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
    /// `FAST_STEP` rows or columns at once.
    FastUp,
    FastDown,
    FastLeft,
    FastRight,
    /// Far left / far right: the first / last variable of the row.
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

/// A row filter on one column: what was typed, made ready for matching.
struct RowFilter {
    column: usize,
    text: String,
    matcher: Matcher,
}

/// The filter bar while it is being typed into.
struct Prompt {
    text: String,
    target: Target,
    /// Why the text is not a usable filter yet, e.g. `>` with no number.
    problem: Option<String>,
}

enum Target {
    /// A row filter on `column`, in Data View.
    Rows {
        column: usize,
        /// The rows the other filters keep, which this one narrows; `None`
        /// when there is no other filter.
        base: Option<Vec<u32>>,
        /// The filter being edited and its place in the list.
        previous: Option<(usize, Box<RowFilter>)>,
        /// The file row the cursor was on.
        origin: usize,
        /// The "contains" text the rows now shown were found with: typing
        /// more of it only needs to test those rows again.
        narrowed: Option<String>,
    },
    /// The variable search, in Variable View.
    Variables,
}

/// The filter bar as text, for drawing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bar {
    pub left: String,
    pub right: String,
    /// The bar is being typed into.
    pub typing: bool,
    /// `right` says what is wrong with the text.
    pub problem: bool,
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

    /// The cursor's row and the first row on screen, as positions among
    /// the rows shown: file rows when nothing is filtered (`row_at`).
    pub cursor_row: usize,
    pub cursor_col: usize,
    pub top_row: usize,
    /// Row filters, in the order they were added; a row must pass all.
    filters: Vec<RowFilter>,
    /// The file rows shown, in order; `None` when every row is.
    visible: Option<Vec<u32>>,
    /// The filter bar, while it is typed into.
    prompt: Option<Prompt>,
    /// The variables listed in Variable View; `None` when every one is.
    var_visible: Option<Vec<usize>>,
    /// The variable search that was kept with Enter.
    var_search: String,
    /// First scrollable column on screen.
    pub left_col: usize,
    /// First variable listed in the respondent panel.
    pub panel_top: usize,
    /// The variable under the cursor (its place in the file), and the
    /// first one listed as a position in the list (`var_at`).
    pub var_cursor: usize,
    pub var_top: usize,
    /// In Variable View: the highlighted field of the variable under the
    /// cursor, an index into `VARIABLE_FIELDS`.
    pub var_field: usize,
    /// In Variable View: the first field shown after `#` and Name, which
    /// stay put while the rest scroll sideways.
    pub var_left: usize,
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
            var_left: FIXED_FIELDS,
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
            filters: Vec::new(),
            visible: None,
            prompt: None,
            var_visible: None,
            var_search: String::new(),
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
                let before = self.rows;
                self.rows += batch.num_rows();
                self.batches.push(batch);
                self.admit(before..self.rows);
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

    /// The cell at a position among the rows shown.
    pub fn cell_in(&self, position: usize, column: usize, mode: Mode) -> Cell {
        let row = self.row_at(position);
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

    /// Data rows that fit: the screen minus the frame, the header and the
    /// filter bar.
    pub fn grid_rows(&self) -> usize {
        self.height.saturating_sub(3 + self.data_bar())
    }

    /// Lines the filter bar takes in Data View: one while a filter is
    /// typed or active, none otherwise.
    pub fn data_bar(&self) -> usize {
        let typing = matches!(
            self.prompt,
            Some(Prompt {
                target: Target::Rows { .. },
                ..
            })
        );
        usize::from(typing || !self.filters.is_empty())
    }

    /// Lines the search bar takes in Variable View.
    pub fn variable_bar(&self) -> usize {
        let typing = matches!(
            self.prompt,
            Some(Prompt {
                target: Target::Variables,
                ..
            })
        );
        usize::from(typing || self.var_visible.is_some())
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

        let rows = self.variable_rows().max(1);
        let position = self.var_position();
        if position < self.var_top {
            self.var_top = position;
        } else if position >= self.var_top + rows {
            self.var_top = position + 1 - rows;
        }
        self.follow_field();
    }

    /// Scroll the variable list sideways so the highlighted field is on
    /// screen. `#` and Name never scroll.
    fn follow_field(&mut self) {
        let last = VARIABLE_FIELDS.len() - 1;
        self.var_left = self.var_left.clamp(FIXED_FIELDS, last);
        if self.var_field < FIXED_FIELDS {
            return;
        }
        if self.var_field < self.var_left {
            self.var_left = self.var_field;
        }
        while self.var_left < self.var_field && !self.field_fully_visible(self.var_field) {
            self.var_left += 1;
        }
        // Use spare room: scroll back as far as the field stays whole.
        while self.var_left > FIXED_FIELDS {
            self.var_left -= 1;
            if !self.field_fully_visible(self.var_field) {
                self.var_left += 1;
                break;
            }
        }
    }

    fn field_fully_visible(&self, field: usize) -> bool {
        let widths = self.variable_field_widths();
        (self.variable_layout().iter())
            .any(|shown| shown.index == field && shown.width == widths[field])
    }

    /// Lines of the full-text bar under the variable list; none on a
    /// screen too short to spare them.
    pub fn bar_lines(&self) -> usize {
        if self.height >= 14 { BAR_LINES } else { 0 }
    }

    /// Variables that fit in the list: the screen minus the frame, the
    /// header, the search bar, and the full-text bar with its rule.
    pub fn variable_rows(&self) -> usize {
        let bar = match self.bar_lines() {
            0 => 0,
            lines => lines + 1,
        };
        self.height.saturating_sub(3 + self.variable_bar() + bar)
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
        if self.prompt.is_some() {
            self.prompt_key(key);
            self.follow_cursor();
            self.refresh_detail();
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
            // The row with that number, or the next one a filter shows.
            Ok(row) => self.cursor_row = self.position_of(row.clamp(1, self.rows.max(1)) - 1),
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
        let last_row = self.shown_rows().saturating_sub(1);
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
            Key::FastUp | Key::Char('{') => {
                self.cursor_row = self.cursor_row.saturating_sub(FAST_STEP);
            }
            Key::FastDown | Key::Char('}') => {
                self.cursor_row = (self.cursor_row + FAST_STEP).min(last_row);
            }
            Key::FastLeft | Key::Char('<') => {
                self.cursor_col = self.cursor_col.saturating_sub(FAST_STEP);
            }
            Key::FastRight | Key::Char('>') => {
                self.cursor_col = (self.cursor_col + FAST_STEP).min(last_col);
            }
            // `0` reaches here only when no row number is being typed.
            Key::Home | Key::Char('0') => self.cursor_col = 0,
            Key::End | Key::Char('$') => self.cursor_col = last_col,
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
                self.snap_variable();
            }
            Key::Char('f') => self.frozen = !self.frozen,
            // Filter the rows by this column, or by this cell's value.
            Key::Char('/') => self.open_filter(),
            Key::Char('=') => self.filter_by_cell(),
            // `c` is the cell and `C` / `t` the open panel, in both views.
            Key::Char('c') => self.copy_data_cell(),
            Key::Char('C') | Key::Char('t') => self.copy_respondent(),
            Key::Char('?') => self.help = true,
            Key::Char('J') if self.panel_open => {
                self.panel_top = (self.panel_top + 1).min(last_col);
            }
            Key::Char('K') if self.panel_open => {
                self.panel_top = self.panel_top.saturating_sub(1);
            }
            Key::Char(digit @ '1'..='9') if self.shown_rows() > 0 => {
                self.goto = Some(Goto {
                    typed: String::new(),
                    origin: self.cursor_row,
                });
                self.goto_key(Key::Char(digit));
            }
            // One thing at a time: the panel, then the filters, then quit.
            Key::Esc | Key::Char('q') => {
                if self.panel_open {
                    self.panel_open = false;
                } else if !self.filters.is_empty() {
                    self.clear_filters();
                } else {
                    return true;
                }
            }
            _ => {}
        }
        if self.cursor_col != column_before {
            self.follow_panel();
        }
        false
    }

    fn variables_key(&mut self, key: Key) {
        // Up and down move through the variables listed, which a search
        // may have narrowed.
        let listed = self.shown_variables();
        let last = listed.saturating_sub(1);
        let last_field = VARIABLE_FIELDS.len() - 1;
        let page = self.variable_rows().max(1);
        let position = self.var_position();
        let moved = match key {
            Key::Up | Key::Char('k') => Some(position.saturating_sub(1)),
            Key::Down | Key::Char('j') => Some(position + 1),
            Key::PageUp => Some(position.saturating_sub(page)),
            Key::PageDown => Some(position + page),
            Key::HalfUp => Some(position.saturating_sub(page / 2)),
            Key::HalfDown => Some(position + page / 2),
            Key::FastUp | Key::Char('{') => Some(position.saturating_sub(FAST_STEP)),
            Key::FastDown | Key::Char('}') => Some(position + FAST_STEP),
            Key::Top | Key::Char('g') => Some(0),
            Key::Bottom | Key::Char('G') => Some(last),
            _ => None,
        };
        if let Some(position) = moved {
            if listed > 0 {
                self.var_cursor = self.var_at(position.min(last));
            }
            return;
        }
        match key {
            Key::Char('/') => self.open_search(),
            // A search that found nothing leaves no variable to act on.
            Key::Enter | Key::Char('d') | Key::Char('c') if listed == 0 => {}
            // The highlighted cell: the row number is not one.
            Key::Left | Key::Char('h') => self.var_field = (self.var_field - 1).max(1),
            Key::Right | Key::Char('l') => self.var_field = (self.var_field + 1).min(last_field),
            // There are only a few fields: a fast step reaches the edge.
            Key::Home | Key::FastLeft | Key::Char('<') | Key::Char('0') => self.var_field = 1,
            Key::End | Key::FastRight | Key::Char('>') | Key::Char('$') => {
                self.var_field = last_field;
            }
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
            Key::Char('c') => self.copy_variable_cell(),
            Key::Char('C') | Key::Char('t') => self.copy_table(),
            Key::Char('?') => self.help = true,
            Key::Esc | Key::Char('q') if self.detail.is_some() => self.detail = None,
            Key::Esc | Key::Char('q') if self.var_visible.is_some() => self.clear_search(),
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
        let last_row = self.shown_rows().saturating_sub(1);
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
            Mouse::ScrollLeft => self.cursor_col = self.cursor_col.saturating_sub(WHEEL_ROWS),
            Mouse::ScrollRight => self.cursor_col = (self.cursor_col + WHEEL_ROWS).min(last_col),
            Mouse::Click => {
                // The grid starts below the frame, the filter bar and the
                // header.
                let first = 2 + self.data_bar();
                if y >= first
                    && y - first < self.grid_rows()
                    && self.top_row + y - first < self.shown_rows()
                {
                    self.cursor_row = self.top_row + y - first;
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
        let listed = self.shown_variables();
        let last = listed.saturating_sub(1);
        let position = self.var_position();
        let go = |state: &mut State, position: usize| {
            if listed > 0 {
                state.var_cursor = state.var_at(position.min(last));
            }
        };
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
            Mouse::ScrollUp => go(self, position.saturating_sub(WHEEL_ROWS)),
            Mouse::ScrollDown => go(self, position + WHEEL_ROWS),
            Mouse::ScrollLeft => self.var_field = (self.var_field - 1).max(1),
            Mouse::ScrollRight => self.var_field = (self.var_field + 1).min(last_field),
            Mouse::Click => {
                // The list starts below the frame, the search bar and the
                // header.
                let first = 2 + self.variable_bar();
                if y >= first
                    && y - first < self.variable_rows()
                    && self.var_top + y - first < listed
                {
                    go(self, self.var_top + y - first);
                }
                if let Some(field) = (self.variable_layout().iter())
                    .find(|field| (field.x..field.x + field.width).contains(&x))
                {
                    self.var_field = field.index.max(1);
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
    ///
    /// `#` and Name are always shown; the other fields start at `var_left`
    /// and run until the list's right edge, where the last one may be cut.
    pub fn variable_layout(&self) -> Vec<VariableField> {
        let widths = self.variable_field_widths();
        // The list's inner right edge: the screen minus the value table
        // and the frame.
        let edge = self.width.saturating_sub(self.detail_width() + 1);
        let shown = (0..FIXED_FIELDS).chain(self.var_left.max(FIXED_FIELDS)..VARIABLE_FIELDS.len());
        let mut x = 2;
        let mut fields = Vec::new();
        for index in shown {
            if x >= edge {
                break;
            }
            let (title, right) = VARIABLE_FIELDS[index];
            fields.push(VariableField {
                index,
                title,
                x,
                width: widths[index].min(edge - x),
                right,
            });
            x += widths[index] + 2;
        }
        fields
    }

    /// The full width of each field of the variable list.
    fn variable_field_widths(&self) -> [usize; 8] {
        let number = self.columns.len().to_string().len().max(1);
        let name = (self.columns.iter().map(|column| column.name.width()))
            .max()
            .unwrap_or(4)
            .max(4);
        let count = crate::info::group_digits(self.counted_rows()).len().max(7);
        [number, name, 11, 32, 30, count, count, 32]
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

    // --- filters and search -------------------------------------------------
    //
    // A filter is the list of file rows it keeps (`visible`); the grid
    // looks rows up through it. Nothing is copied or reformatted.

    /// Rows shown: every loaded row, or those the filters keep.
    pub fn shown_rows(&self) -> usize {
        self.visible.as_ref().map_or(self.rows, Vec::len)
    }

    /// The file row at a position among the rows shown.
    pub fn row_at(&self, position: usize) -> usize {
        match &self.visible {
            Some(rows) => rows[position] as usize,
            None => position,
        }
    }

    /// The file row under the cursor; 0 when no row is shown.
    pub fn cursor_file_row(&self) -> usize {
        if self.shown_rows() == 0 {
            0
        } else {
            self.row_at(self.cursor_row)
        }
    }

    /// Where a file row is among the rows shown. When a filter hides it:
    /// the next row shown, or the last.
    fn position_of(&self, row: usize) -> usize {
        match &self.visible {
            Some(rows) => rows
                .partition_point(|&shown| (shown as usize) < row)
                .min(rows.len().saturating_sub(1)),
            None => row.min(self.rows.saturating_sub(1)),
        }
    }

    /// Variables listed in Variable View: all, or those the search found.
    pub fn shown_variables(&self) -> usize {
        (self.var_visible.as_ref()).map_or(self.columns.len(), Vec::len)
    }

    /// The variable at a position in the list.
    pub fn var_at(&self, position: usize) -> usize {
        match &self.var_visible {
            Some(found) => found[position],
            None => position,
        }
    }

    /// Where the variable under the cursor is in the list.
    fn var_position(&self) -> usize {
        match &self.var_visible {
            Some(found) => (found.iter())
                .position(|&variable| variable == self.var_cursor)
                .unwrap_or(0),
            None => self.var_cursor,
        }
    }

    /// Keep the variable cursor on a listed variable.
    fn snap_variable(&mut self) {
        if let Some(found) = &self.var_visible
            && !found.contains(&self.var_cursor)
            && let Some(&first) = found.first()
        {
            self.var_cursor = first;
        }
    }

    /// Show these rows, with the cursor on file row `row` or, when it is
    /// no longer shown, the next one that is.
    fn show(&mut self, visible: Option<Vec<u32>>, row: usize) {
        self.visible = visible;
        self.cursor_row = self.position_of(row);
        // A shorter list must not leave the screen half empty.
        let fits = (self.shown_rows()).saturating_sub(self.grid_rows().max(1));
        self.top_row = self.top_row.min(fits);
    }

    /// What was typed for `column`, ready for matching. `Ok(None)` is an
    /// empty filter; `Err` says what is missing.
    fn matcher_for(
        &self,
        column: usize,
        text: &str,
    ) -> Result<Option<(Vec<filter::Parsed>, Matcher)>, String> {
        match filter::parse(text)? {
            Some(parsed) => {
                let matcher = Matcher::new(&self.columns[column], &parsed)?;
                Ok(Some((parsed, matcher)))
            }
            None => Ok(None),
        }
    }

    /// The rows every kept filter lets through; `None` without filters.
    /// Each filter tests only the rows the ones before it kept.
    fn passing(&mut self) -> Option<Vec<u32>> {
        let mut kept: Option<Vec<u32>> = None;
        for filter in &mut self.filters {
            let candidates = match &kept {
                Some(rows) => Candidates::Rows(rows),
                None => Candidates::Range(0..self.rows),
            };
            let rows = filter.matcher.select(
                &self.columns[filter.column],
                filter.column,
                &self.batches,
                self.batch_rows,
                candidates,
            );
            kept = Some(rows);
        }
        kept
    }

    /// Rows that just arrived from the loader: test only those, so a
    /// filter is never re-run from the start while a file loads.
    fn admit(&mut self, range: std::ops::Range<usize>) {
        if self.visible.is_none() && self.filters.is_empty() {
            return;
        }
        let mut kept: Option<Vec<u32>> = None;
        for filter in &mut self.filters {
            let candidates = match &kept {
                Some(rows) => Candidates::Rows(rows),
                None => Candidates::Range(range.clone()),
            };
            let rows = filter.matcher.select(
                &self.columns[filter.column],
                filter.column,
                &self.batches,
                self.batch_rows,
                candidates,
            );
            kept = Some(rows);
        }
        let all = range.start as u32..range.end as u32;
        if let Some(Prompt {
            text,
            target: Target::Rows { column, base, .. },
            ..
        }) = &mut self.prompt
        {
            if let Some(base) = base {
                match &kept {
                    Some(rows) => base.extend(rows),
                    None => base.extend(all.clone()),
                }
            }
            let typed = filter::parse(text).ok().flatten();
            let matcher =
                typed.and_then(|parsed| Matcher::new(&self.columns[*column], &parsed).ok());
            if let Some(mut matcher) = matcher {
                let candidates = match &kept {
                    Some(rows) => Candidates::Rows(rows),
                    None => Candidates::Range(range.clone()),
                };
                let rows = matcher.select(
                    &self.columns[*column],
                    *column,
                    &self.batches,
                    self.batch_rows,
                    candidates,
                );
                kept = Some(rows);
            }
        }
        if let Some(visible) = &mut self.visible {
            match kept {
                Some(rows) => visible.extend(rows),
                None => visible.extend(all),
            }
        }
    }

    /// `/` in Data View: type a filter for the cursor's column, or edit
    /// the one it has.
    fn open_filter(&mut self) {
        if self.columns.is_empty() {
            return;
        }
        let column = self.cursor_col;
        let origin = self.cursor_file_row();
        let place = self.filters.iter().position(|f| f.column == column);
        let previous = place.map(|place| (place, Box::new(self.filters.remove(place))));
        let text = (previous.as_ref()).map_or(String::new(), |(_, filter)| filter.text.clone());
        let base = self.passing();
        self.prompt = Some(Prompt {
            text,
            target: Target::Rows {
                column,
                base,
                previous,
                origin,
                narrowed: None,
            },
            problem: None,
        });
    }

    /// `=` in Data View: keep the rows with this cell's value.
    fn filter_by_cell(&mut self) {
        if self.shown_rows() == 0 || self.columns.is_empty() {
            return;
        }
        let column = self.cursor_col;
        let origin = self.cursor_file_row();
        let value = self.cell_in(self.cursor_row, column, Mode::Codes).text;
        // An empty string counts as missing, like `.`.
        let text = if value.trim().is_empty() {
            "=.".to_string()
        } else {
            format!("={value}")
        };
        match self.matcher_for(column, &text) {
            Ok(Some((_, matcher))) => {
                self.filters.retain(|filter| filter.column != column);
                self.filters.push(RowFilter {
                    column,
                    text,
                    matcher,
                });
                let visible = self.passing();
                self.show(visible, origin);
            }
            Ok(None) => {}
            Err(problem) => self.notice = Some(problem),
        }
    }

    fn clear_filters(&mut self) {
        let origin = self.cursor_file_row();
        self.filters.clear();
        self.show(None, origin);
        self.notice = Some("filters cleared".to_string());
    }

    /// `/` in Variable View: search the variables by name or label.
    fn open_search(&mut self) {
        self.prompt = Some(Prompt {
            text: self.var_search.clone(),
            target: Target::Variables,
            problem: None,
        });
    }

    fn clear_search(&mut self) {
        self.var_search.clear();
        self.var_visible = None;
    }

    /// A key while the bar is typed into. Every key is used up.
    fn prompt_key(&mut self, key: Key) {
        let Some(prompt) = &mut self.prompt else {
            return;
        };
        match key {
            Key::Char(ch) if prompt.text.chars().count() < PROMPT_LENGTH => prompt.text.push(ch),
            Key::Backspace => {
                prompt.text.pop();
            }
            Key::Enter => return self.keep_prompt(),
            Key::Esc => return self.drop_prompt(),
            _ => return,
        }
        self.retype();
    }

    /// The text in the bar changed: show what it finds.
    fn retype(&mut self) {
        let Some(mut prompt) = self.prompt.take() else {
            return;
        };
        let ready = match &prompt.target {
            Target::Rows { column, .. } => self.matcher_for(*column, &prompt.text),
            Target::Variables => Ok(None),
        };
        match &mut prompt.target {
            Target::Variables => {
                let typed = !prompt.text.trim().is_empty();
                self.var_visible = typed.then(|| filter::variables(&self.columns, &prompt.text));
                self.var_top = 0;
                self.snap_variable();
            }
            Target::Rows {
                column,
                base,
                origin,
                narrowed,
                ..
            } => {
                prompt.problem = None;
                let visible = match ready {
                    // Not a filter yet, e.g. `>` before its number.
                    Err(problem) => {
                        prompt.problem = Some(problem);
                        *narrowed = None;
                        base.clone()
                    }
                    Ok(None) => {
                        *narrowed = None;
                        base.clone()
                    }
                    Ok(Some((parsed, mut matcher))) => {
                        // Only a single "contains": with alternatives
                        // (`a|b`) more text can keep more rows.
                        let needle = match parsed.as_slice() {
                            [
                                filter::Parsed {
                                    query: Query::Contains(needle),
                                    negate: false,
                                },
                            ] => Some(needle.clone()),
                            _ => None,
                        };
                        // More of the same text can only keep fewer rows:
                        // test the ones now shown, not the whole column.
                        let again = match (&needle, narrowed.as_ref(), &self.visible) {
                            (Some(new), Some(old), Some(shown)) if new.contains(old.as_str()) => {
                                Some(shown)
                            }
                            _ => None,
                        };
                        let candidates = match (again, base.as_ref()) {
                            (Some(shown), _) => Candidates::Rows(shown),
                            (None, Some(base)) => Candidates::Rows(base),
                            (None, None) => Candidates::Range(0..self.rows),
                        };
                        let rows = matcher.select(
                            &self.columns[*column],
                            *column,
                            &self.batches,
                            self.batch_rows,
                            candidates,
                        );
                        *narrowed = needle;
                        Some(rows)
                    }
                };
                let origin = *origin;
                self.show(visible, origin);
            }
        }
        self.prompt = Some(prompt);
    }

    /// Enter in the bar: keep the filter or the search. An empty filter
    /// removes the one that was being edited.
    fn keep_prompt(&mut self) {
        let Some(mut prompt) = self.prompt.take() else {
            return;
        };
        let text = prompt.text.trim().to_string();
        let column = match &prompt.target {
            Target::Rows { column, .. } => *column,
            Target::Variables => {
                if text.is_empty() {
                    self.var_visible = None;
                }
                self.var_search = text;
                return;
            }
        };
        match self.matcher_for(column, &text) {
            // Stay in the bar and say what is missing.
            Err(problem) => {
                self.notice = Some(problem.clone());
                prompt.problem = Some(problem);
                self.prompt = Some(prompt);
            }
            Ok(ready) => {
                if let Target::Rows { previous, .. } = prompt.target {
                    let end = self.filters.len();
                    let place = previous.map_or(end, |(place, _)| place.min(end));
                    if let Some((_, matcher)) = ready {
                        let filter = RowFilter {
                            column,
                            text,
                            matcher,
                        };
                        self.filters.insert(place, filter);
                    }
                }
            }
        }
    }

    /// Esc in the bar: back to how it was. For the variable search that
    /// means no search.
    fn drop_prompt(&mut self) {
        let Some(prompt) = self.prompt.take() else {
            return;
        };
        match prompt.target {
            Target::Variables => self.clear_search(),
            Target::Rows {
                previous, origin, ..
            } => {
                if let Some((place, filter)) = previous {
                    let place = place.min(self.filters.len());
                    self.filters.insert(place, *filter);
                }
                // Not a saved copy: rows may have arrived in the meantime.
                let visible = self.passing();
                self.show(visible, origin);
            }
        }
    }

    /// The filter bar of the current view, when it shows.
    pub fn bar(&self) -> Option<Bar> {
        let count = |n: usize| crate::info::group_digits(n as u64);
        match self.screen {
            Screen::Data => {
                let rows = format!("{} of {} rows", count(self.shown_rows()), count(self.rows));
                if let Some(Prompt {
                    text,
                    target: Target::Rows { column, .. },
                    problem,
                }) = &self.prompt
                {
                    return Some(Bar {
                        left: format!("Filter {}: {text}▏", self.columns[*column].name),
                        right: problem.clone().unwrap_or(rows),
                        typing: true,
                        problem: problem.is_some(),
                    });
                }
                if self.filters.is_empty() {
                    return None;
                }
                let filters: Vec<String> = (self.filters.iter())
                    .map(|filter| format!("{}: {}", self.columns[filter.column].name, filter.text))
                    .collect();
                Some(Bar {
                    left: format!("Filter {}", filters.join(" │ ")),
                    right: format!("{rows} ─ / edit ─ Esc clear"),
                    typing: false,
                    problem: false,
                })
            }
            Screen::Variables => {
                let found = format!(
                    "{} of {} variables",
                    count(self.shown_variables()),
                    count(self.columns.len())
                );
                if let Some(Prompt {
                    text,
                    target: Target::Variables,
                    ..
                }) = &self.prompt
                {
                    return Some(Bar {
                        left: format!("Search variables: {text}▏"),
                        right: found,
                        typing: true,
                        problem: false,
                    });
                }
                self.var_visible.as_ref()?;
                Some(Bar {
                    left: format!("Search variables: {}", self.var_search),
                    right: format!("{found} ─ / edit ─ Esc clear"),
                    typing: false,
                    problem: false,
                })
            }
        }
    }

    // --- copy ---------------------------------------------------------------
    //
    // `c` copies the highlighted cell and `C` / `t` the open side panel, the
    // same in both views.

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
            self.notice = Some(NO_PANEL.to_string());
            return;
        };
        let (text, rows) = (detail.tsv(), detail.rows.len());
        let noun = if rows == 1 { "row" } else { "rows" };
        self.copy_text(text, &format!("table ({rows} {noun})"));
    }

    /// The open respondent panel, as tab-separated columns: every variable
    /// of the current row, not only those the panel has room for, with the
    /// value as the current mode shows it.
    fn copy_respondent(&mut self) {
        if !self.panel_open {
            self.notice = Some(NO_PANEL.to_string());
            return;
        }
        if self.shown_rows() == 0 || self.columns.is_empty() {
            return self.copy_text(String::new(), "");
        }
        let mut text = String::from("Variable\tLabel\tValue");
        for (index, column) in self.columns.iter().enumerate() {
            let value = self.cell(self.cursor_row, index).text;
            text.push('\n');
            for (position, field) in [&column.name, &column.label, &value].iter().enumerate() {
                if position > 0 {
                    text.push('\t');
                }
                // A tab or line break inside a field would shift the columns.
                text.extend(field.chars().map(|ch| match ch {
                    '\t' | '\n' | '\r' => ' ',
                    other => other,
                }));
            }
        }
        let variables = self.columns.len();
        let noun = if variables == 1 {
            "variable"
        } else {
            "variables"
        };
        let row = self.row_at(self.cursor_row) + 1;
        let what = format!("row {row} ({variables} {noun})");
        self.copy_text(text, &what);
    }

    /// The Data View cell under the cursor, in full, as it is shown.
    fn copy_data_cell(&mut self) {
        if self.shown_rows() == 0 || self.columns.is_empty() {
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
    fn c_copies_the_highlighted_cell_in_full() {
        let mut state = loaded("survey.sav", 100, 24);
        // gender's label.
        press(&mut state, &[Key::Char('i'), Key::Down, Key::Char('c')]);
        assert_eq!(state.take_copy().unwrap().text, "Gender");
        assert_eq!(state.notice.as_deref(), Some("copied label"));
        assert_eq!(state.take_copy(), None, "handed over once");

        // Its values, one per line; the notice goes with the next key.
        press(&mut state, &[Key::Right, Key::Char('c')]);
        assert_eq!(state.take_copy().unwrap().text, "1=Male\n2=Female");
        assert_eq!(state.notice.as_deref(), Some("copied values"));
        press(&mut state, &[Key::Home]);
        assert_eq!(state.notice, None);
        press(&mut state, &[Key::Char('c')]);
        assert_eq!(state.take_copy().unwrap().text, "gender");

        // Nothing to copy: say so, and leave the clipboard alone.
        let mut state = loaded("long_strings.sav", 100, 24);
        press(&mut state, &[Key::Char('i'), Key::Char('c')]);
        assert_eq!(state.take_copy(), None);
        assert_eq!(state.notice.as_deref(), Some("nothing to copy"));
    }

    #[test]
    fn shift_c_or_t_copies_the_open_table_and_c_still_the_cell() {
        // gender: 10 Male, 10 Female, nothing missing.
        let table = "Code\tLabel\tN\t% valid\t% total\n1.00\tMale\t10\t50.0%\t50.0%\n2.00\tFemale\t10\t50.0%\t50.0%";
        for key in ['C', 't'] {
            let mut state = loaded("survey.sav", 120, 24);
            press(
                &mut state,
                &[Key::Char('i'), Key::Down, Key::Enter, Key::Char(key)],
            );
            assert_eq!(state.take_copy().unwrap().text, table);
            assert_eq!(state.notice.as_deref(), Some("copied table (2 rows)"));
            // The cell key means the cell, panel or no panel.
            press(&mut state, &[Key::Char('c')]);
            assert_eq!(state.take_copy().unwrap().text, "Gender");
        }
    }

    #[test]
    fn c_copies_the_data_cell_as_shown() {
        let mut state = loaded("survey.sav", 100, 24);
        press(&mut state, &[Key::Right, Key::Char('c')]);
        assert_eq!(state.take_copy().unwrap().text, "Male");
        assert_eq!(state.notice.as_deref(), Some("copied cell"));
        press(&mut state, &[Key::Tab, Key::Char('c')]);
        assert_eq!(state.take_copy().unwrap().text, "1.00");
    }

    #[test]
    fn shift_c_or_t_copies_the_open_respondent() {
        for key in ['C', 't'] {
            let mut state = loaded("survey.sav", 100, 24);
            // Second respondent, panel open.
            press(&mut state, &[Key::Down, Key::Enter, Key::Char(key)]);
            let text = state.take_copy().unwrap().text;
            let lines: Vec<&str> = text.lines().collect();
            assert_eq!(lines[0], "Variable\tLabel\tValue");
            // Every variable, not only those the panel has room for.
            assert_eq!(lines.len(), 1 + state.columns.len());
            assert_eq!(lines[2], "gender\tGender\tFemale");
            assert!(lines.iter().all(|line| line.matches('\t').count() == 2));
            assert_eq!(state.notice.as_deref(), Some("copied row 2 (13 variables)"));

            // The value follows the mode, as on screen.
            press(&mut state, &[Key::Tab, Key::Char(key)]);
            let text = state.take_copy().unwrap().text;
            assert_eq!(text.lines().nth(2), Some("gender\tGender\t2.00"));
        }
    }

    #[test]
    fn the_panel_keys_need_an_open_panel() {
        for key in ['C', 't'] {
            let mut state = loaded("survey.sav", 100, 24);
            press(&mut state, &[Key::Char(key)]);
            assert_eq!(state.take_copy(), None);
            assert_eq!(
                state.notice.as_deref(),
                Some("open the panel with Enter first")
            );
            press(&mut state, &[Key::Char('i'), Key::Char(key)]);
            assert_eq!(state.take_copy(), None);
            assert_eq!(
                state.notice.as_deref(),
                Some("open the panel with Enter first")
            );
        }
    }

    #[test]
    fn the_old_copy_keys_do_nothing() {
        let mut state = loaded("survey.sav", 120, 24);
        for key in ['y', 'Y', 'L'] {
            press(&mut state, &[Key::Char(key)]);
            assert_eq!(state.take_copy(), None, "{key} in Data View");
        }
        press(&mut state, &[Key::Char('i'), Key::Down, Key::Enter]);
        for key in ['y', 'Y', 'L'] {
            press(&mut state, &[Key::Char(key)]);
            assert_eq!(state.take_copy(), None, "{key} in Variable View");
        }
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
        assert_eq!(state.cursor_col, 3, "three columns per notch");
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

    #[test]
    fn fast_keys_move_fifty_and_stop_at_the_edges() {
        // survey.sav is 20 rows by 13 columns: a fast step reaches the edge.
        let mut state = loaded("survey.sav", 100, 24);
        press(&mut state, &[Key::FastDown]);
        assert_eq!(state.cursor_row, 19);
        press(&mut state, &[Key::FastRight]);
        assert_eq!(state.cursor_col, 12);
        press(&mut state, &[Key::FastUp, Key::FastLeft]);
        assert_eq!((state.cursor_row, state.cursor_col), (0, 0));
        // The letter forms do the same.
        press(&mut state, &[Key::Char('}'), Key::Char('>')]);
        assert_eq!((state.cursor_row, state.cursor_col), (19, 12));
        press(&mut state, &[Key::Char('{'), Key::Char('<')]);
        assert_eq!((state.cursor_row, state.cursor_col), (0, 0));
    }

    #[test]
    fn a_fast_step_is_fifty_rows_in_a_long_file() {
        // 26 variables in one row is the widest fixture; rows need a longer
        // file, so stack the same 20 rows until there are 200.
        let mut state = loaded("survey.sav", 100, 24);
        for _ in 0..36 {
            let again = state.batches[0].clone();
            state.receive(Message::Batch(again));
        }
        assert_eq!(state.rows, 200);
        press(&mut state, &[Key::FastDown]);
        assert_eq!(state.cursor_row, 50);
        press(&mut state, &[Key::FastDown, Key::FastDown, Key::FastUp]);
        assert_eq!(state.cursor_row, 100);
        press(&mut state, &[Key::FastDown, Key::FastDown, Key::FastDown]);
        assert_eq!(state.cursor_row, 199, "stops at the last row");
    }

    #[test]
    fn edge_keys_reach_every_corner() {
        let mut state = loaded("survey.sav", 100, 24);
        // 0 and $ are the far left and far right; g and G the top and bottom.
        press(&mut state, &[Key::Char('$'), Key::Char('G')]);
        assert_eq!((state.cursor_row, state.cursor_col), (19, 12));
        press(&mut state, &[Key::Char('0'), Key::Char('g')]);
        assert_eq!((state.cursor_row, state.cursor_col), (0, 0));
        press(&mut state, &[Key::End, Key::Bottom, Key::Home, Key::Top]);
        assert_eq!((state.cursor_row, state.cursor_col), (0, 0));
        // While a row number is being typed, 0 is a digit.
        press(
            &mut state,
            &[Key::Char('$'), Key::Char('1'), Key::Char('0'), Key::Enter],
        );
        assert_eq!((state.cursor_row, state.cursor_col), (9, 12));
    }

    #[test]
    fn the_same_keys_work_in_variable_view() {
        let mut state = loaded("survey.sav", 160, 30);
        press(&mut state, &[Key::Char('i'), Key::FastDown]);
        assert_eq!(state.var_cursor, 12);
        press(&mut state, &[Key::Char('{')]);
        assert_eq!(state.var_cursor, 0);
        press(&mut state, &[Key::Bottom, Key::Char('$')]);
        assert_eq!((state.var_cursor, state.var_field), (12, 7));
        press(&mut state, &[Key::Top, Key::Char('0')]);
        assert_eq!((state.var_cursor, state.var_field), (0, 1));
        press(&mut state, &[Key::FastRight]);
        assert_eq!(state.var_field, 7);
        press(&mut state, &[Key::FastLeft]);
        assert_eq!(state.var_field, 1);
    }

    #[test]
    fn the_variable_list_scrolls_sideways_and_keeps_the_name() {
        // 70 columns wide: Label fits after # and Name, Values does not.
        let mut state = loaded("survey.sav", 70, 30);
        press(&mut state, &[Key::Char('i')]);
        let shown = |state: &State| -> Vec<usize> {
            state
                .variable_layout()
                .iter()
                .map(|field| field.index)
                .collect()
        };
        assert_eq!(shown(&state)[..3], [0, 1, 2]);
        assert!(!shown(&state).contains(&7));

        // Moving to the last field brings it on screen, whole; # and Name stay.
        press(&mut state, &[Key::End]);
        let fields = state.variable_layout();
        let check = fields.iter().find(|field| field.index == 7).unwrap();
        assert_eq!(check.width, 32, "the label check is not cut");
        assert_eq!(shown(&state)[..2], [0, 1]);
        assert!(state.var_left > 2);

        // Back to the left: the first fields return.
        press(&mut state, &[Key::Home]);
        assert_eq!(state.var_field, 1);
        press(&mut state, &[Key::Right]);
        assert_eq!((state.var_left, shown(&state)[2]), (2, 2));
    }

    #[test]
    fn the_wheel_moves_three_columns_sideways() {
        let mut state = loaded("survey.sav", 100, 13);
        state.on_mouse(Mouse::ScrollRight, 30, 5);
        assert_eq!(state.cursor_col, 3);
        state.on_mouse(Mouse::ScrollRight, 30, 5);
        state.on_mouse(Mouse::ScrollLeft, 30, 5);
        assert_eq!(state.cursor_col, 3);
        for _ in 0..10 {
            state.on_mouse(Mouse::ScrollRight, 30, 5);
        }
        assert_eq!(state.cursor_col, 12, "stops at the last column");
    }

    // --- filters and search ---

    fn type_text(state: &mut State, text: &str) {
        for ch in text.chars() {
            assert!(!state.on_key(Key::Char(ch)));
        }
    }

    /// The 1-based file rows shown.
    fn shown(state: &State) -> Vec<usize> {
        (0..state.shown_rows())
            .map(|position| state.row_at(position) + 1)
            .collect()
    }

    /// A state on survey.sav with the cursor on `column`.
    fn on_column(name: &str) -> State {
        let mut state = loaded("survey.sav", 100, 24);
        state.cursor_col = state.columns.iter().position(|c| c.name == name).unwrap();
        state
    }

    #[test]
    fn a_filter_narrows_as_it_is_typed_and_enter_keeps_it() {
        let mut state = on_column("city");
        press(&mut state, &[Key::Char('/')]);
        // Letters are text here, not keys: `l` does not move, `i` does
        // not open Variable View.
        type_text(&mut state, "li");
        assert_eq!(state.screen, Screen::Data);
        assert_eq!(state.cursor_col, 5);
        assert_eq!(shown(&state).len(), 10, "Lisbon and Lima");
        type_text(&mut state, "s");
        assert_eq!(shown(&state), [1, 5, 9, 13, 17]);
        let bar = state.bar().unwrap();
        assert_eq!(
            (bar.left.as_str(), bar.right.as_str()),
            ("Filter city: lis▏", "5 of 20 rows")
        );
        assert!(bar.typing && !bar.problem);
        // One line fewer for the grid while the bar shows.
        assert_eq!(state.grid_rows(), 20);

        // Deleting widens it again.
        press(&mut state, &[Key::Backspace]);
        assert_eq!(shown(&state).len(), 10);
        type_text(&mut state, "s");

        press(&mut state, &[Key::Enter]);
        let bar = state.bar().unwrap();
        assert_eq!(bar.left, "Filter city: lis");
        assert_eq!(bar.right, "5 of 20 rows ─ / edit ─ Esc clear");
        assert!(!bar.typing);

        // The keys work on the rows shown.
        press(&mut state, &[Key::Bottom]);
        assert_eq!((state.cursor_row, state.cursor_file_row()), (4, 16));
        assert_eq!(state.cell(state.cursor_row, 5).text, "Lisbon");
        press(&mut state, &[Key::Down, Key::Top]);
        assert_eq!(state.cursor_file_row(), 0);
    }

    #[test]
    fn alternatives_widen_the_filter_as_they_are_typed() {
        let mut state = on_column("city");
        press(&mut state, &[Key::Char('/')]);
        type_text(&mut state, "osa");
        assert_eq!(shown(&state), [2, 6, 10, 14, 18]);
        // `osa|` is still just Osaka; the second city adds its rows.
        type_text(&mut state, "|");
        assert_eq!(shown(&state), [2, 6, 10, 14, 18]);
        type_text(&mut state, "lim");
        assert_eq!(shown(&state), [2, 3, 6, 7, 10, 11, 14, 15, 18, 19]);
        // Deleting it narrows again.
        press(&mut state, &[Key::Backspace; 4]);
        assert_eq!(shown(&state), [2, 6, 10, 14, 18]);
        type_text(&mut state, "|lima");
        press(&mut state, &[Key::Enter]);
        assert_eq!(state.shown_rows(), 10);
        assert_eq!(state.bar().unwrap().left, "Filter city: osa|lima");

        // Stacked with another column: (Osaka or Lima) and Female.
        state.cursor_col = 1;
        press(&mut state, &[Key::Char('/')]);
        type_text(&mut state, "=female");
        press(&mut state, &[Key::Enter]);
        assert_eq!(shown(&state), [2, 6, 10, 14, 18]);
    }

    #[test]
    fn esc_in_the_bar_goes_back_to_how_it_was() {
        let mut state = on_column("city");
        press(
            &mut state,
            &[Key::Down, Key::Down, Key::Down, Key::Char('/')],
        );
        type_text(&mut state, "zzz");
        assert_eq!(state.shown_rows(), 0);
        press(&mut state, &[Key::Esc]);
        assert_eq!(state.shown_rows(), 20);
        assert_eq!(state.cursor_row, 3, "back on the row it was on");
        assert_eq!(state.bar(), None);
        assert_eq!(state.grid_rows(), 21);
    }

    #[test]
    fn the_cursor_stays_on_its_row_or_the_next_one_shown() {
        let mut state = on_column("city");
        // Row 6 is Osaka; row 7 is Lima.
        press(&mut state, &[Key::Char('6'), Key::Enter, Key::Char('/')]);
        type_text(&mut state, "osa");
        assert_eq!(state.cursor_file_row() + 1, 6);
        press(&mut state, &[Key::Esc, Key::Down, Key::Char('/')]);
        type_text(&mut state, "osa");
        assert_eq!(
            state.cursor_file_row() + 1,
            10,
            "the next Osaka after row 7"
        );
    }

    #[test]
    fn filters_stack_and_can_be_edited_or_removed() {
        let mut state = on_column("gender");
        press(&mut state, &[Key::Char('/')]);
        type_text(&mut state, "=female");
        press(&mut state, &[Key::Enter]);
        assert_eq!(state.shown_rows(), 10);

        state.cursor_col = 5;
        press(&mut state, &[Key::Char('/')]);
        type_text(&mut state, "osa");
        press(&mut state, &[Key::Enter]);
        assert_eq!(shown(&state), [2, 6, 10, 14, 18]);
        assert_eq!(
            state.bar().unwrap().left,
            "Filter gender: =female │ city: osa"
        );

        // `/` on a filtered column edits its filter.
        press(&mut state, &[Key::Char('/')]);
        assert_eq!(state.bar().unwrap().left, "Filter city: osa▏");
        // Esc keeps it as it was.
        press(&mut state, &[Key::Backspace, Key::Esc]);
        assert_eq!(shown(&state), [2, 6, 10, 14, 18]);
        // Emptied and kept: that filter is removed, the other stays.
        press(
            &mut state,
            &[
                Key::Char('/'),
                Key::Backspace,
                Key::Backspace,
                Key::Backspace,
                Key::Enter,
            ],
        );
        assert_eq!(state.shown_rows(), 10);
        assert_eq!(state.bar().unwrap().left, "Filter gender: =female");
    }

    #[test]
    fn esc_clears_the_filters_before_it_quits() {
        let mut state = on_column("city");
        press(&mut state, &[Key::Char('/')]);
        type_text(&mut state, "osa");
        press(&mut state, &[Key::Enter, Key::Down, Key::Enter]);
        // The panel first, then the filters, then the viewer.
        assert!(!state.on_key(Key::Esc));
        assert!(!state.panel_open);
        assert_eq!(state.shown_rows(), 5);
        assert!(!state.on_key(Key::Esc));
        assert_eq!(state.shown_rows(), 20);
        assert_eq!(state.notice.as_deref(), Some("filters cleared"));
        assert_eq!(state.cursor_file_row() + 1, 6, "still on its row");
        assert!(state.on_key(Key::Esc));
    }

    #[test]
    fn equals_filters_by_the_cell_under_the_cursor() {
        let mut state = on_column("gender");
        press(&mut state, &[Key::Down, Key::Char('=')]);
        assert_eq!(state.shown_rows(), 10);
        assert_eq!(state.bar().unwrap().left, "Filter gender: =2.00");
        assert_eq!(state.cursor_file_row() + 1, 2);
        // Again on the same column: replaced, not added twice.
        press(&mut state, &[Key::Char('=')]);
        assert_eq!(state.bar().unwrap().left, "Filter gender: =2.00");

        // A system-missing cell: the rows where it is missing.
        let mut state = on_column("satisfaction");
        press(&mut state, &[Key::Char('7'), Key::Enter, Key::Char('=')]);
        assert_eq!(shown(&state), [7, 14]);
        assert_eq!(state.bar().unwrap().left, "Filter satisfaction: =.");

        // Text and dates.
        let mut state = on_column("city");
        press(&mut state, &[Key::Char('=')]);
        assert_eq!(shown(&state), [1, 5, 9, 13, 17]);
        let mut state = on_column("interview_date");
        press(&mut state, &[Key::Char('=')]);
        assert!(shown(&state).contains(&1));
    }

    #[test]
    fn a_half_typed_filter_shows_every_row_and_cannot_be_kept() {
        let mut state = on_column("id");
        press(&mut state, &[Key::Char('/'), Key::Char('>')]);
        let bar = state.bar().unwrap();
        assert_eq!(bar.right, "type a number after >");
        assert!(bar.problem);
        assert_eq!(state.shown_rows(), 20);
        // Enter stays in the bar.
        press(&mut state, &[Key::Enter]);
        assert!(state.bar().unwrap().typing);
        assert_eq!(state.notice.as_deref(), Some("type a number after >"));
        type_text(&mut state, "=18");
        press(&mut state, &[Key::Enter]);
        assert_eq!(shown(&state), [18, 19, 20]);

        // A comparison on text is refused the same way.
        let mut state = on_column("city");
        press(&mut state, &[Key::Char('/')]);
        type_text(&mut state, ">3");
        assert_eq!(
            state.bar().unwrap().right,
            "city is not numeric: > and < need numbers"
        );
    }

    #[test]
    fn go_to_row_lands_on_a_row_the_filter_shows() {
        let mut state = on_column("city");
        press(&mut state, &[Key::Char('/')]);
        type_text(&mut state, "osa");
        // Shown: 2, 6, 10, 14, 18.
        press(&mut state, &[Key::Enter, Key::Char('7'), Key::Enter]);
        assert_eq!(state.cursor_file_row() + 1, 10, "the next one shown");
        press(&mut state, &[Key::Char('1'), Key::Char('4'), Key::Enter]);
        assert_eq!(state.cursor_file_row() + 1, 14);
        press(&mut state, &[Key::Char('2'), Key::Char('0'), Key::Enter]);
        assert_eq!(state.cursor_file_row() + 1, 18, "past the end: the last");
    }

    #[test]
    fn the_panel_and_the_copy_keys_follow_the_filter() {
        let mut state = on_column("city");
        press(&mut state, &[Key::Char('/')]);
        type_text(&mut state, "osa");
        press(&mut state, &[Key::Enter, Key::Down, Key::Char('c')]);
        assert_eq!(state.take_copy().unwrap().text, "Osaka");
        press(&mut state, &[Key::Enter, Key::Char('C')]);
        let text = state.take_copy().unwrap().text;
        assert_eq!(text.lines().nth(1), Some("id\tRespondent ID\t6"));
        assert_eq!(state.notice.as_deref(), Some("copied row 6 (13 variables)"));
    }

    #[test]
    fn rows_that_arrive_are_tested_as_they_come() {
        let mut state = on_column("city");
        press(&mut state, &[Key::Char('/')]);
        type_text(&mut state, "osa");
        // Rows 1-5 again, as rows 21-25: one more Osaka, while typing.
        let extra = state.batches[0].clone();
        state.receive(Message::Batch(extra.clone()));
        assert_eq!(shown(&state), [2, 6, 10, 14, 18, 22]);
        // And with the filter kept, and a second one typed.
        press(&mut state, &[Key::Enter]);
        state.receive(Message::Batch(extra.clone()));
        assert_eq!(shown(&state), [2, 6, 10, 14, 18, 22, 27]);
        state.cursor_col = 1;
        press(&mut state, &[Key::Char('/')]);
        type_text(&mut state, "fem");
        state.receive(Message::Batch(extra));
        assert_eq!(shown(&state), [2, 6, 10, 14, 18, 22, 27, 32]);
        // Leaving the bar keeps the rows that came in meanwhile.
        press(&mut state, &[Key::Esc]);
        assert_eq!(shown(&state), [2, 6, 10, 14, 18, 22, 27, 32]);
        assert_eq!(
            state.bar().unwrap().right,
            "8 of 35 rows ─ / edit ─ Esc clear"
        );
    }

    #[test]
    fn a_filter_with_no_rows_is_safe_to_use() {
        let mut state = on_column("city");
        press(&mut state, &[Key::Char('/')]);
        type_text(&mut state, "zzz");
        press(&mut state, &[Key::Enter]);
        assert_eq!(state.shown_rows(), 0);
        press(
            &mut state,
            &[
                Key::Down,
                Key::Bottom,
                Key::PageDown,
                Key::Char('5'),
                Key::Char('='),
                Key::Enter,
                Key::Char('C'),
                Key::Enter,
                Key::Char('c'),
            ],
        );
        assert_eq!(state.take_copy(), None);
        state.on_mouse(Mouse::Click, 30, 5);
        state.on_mouse(Mouse::ScrollDown, 30, 5);
        assert_eq!(state.cursor_row, 0);
    }

    #[test]
    fn a_click_lands_on_the_row_under_it_below_the_bar() {
        let mut state = on_column("city");
        press(&mut state, &[Key::Char('/')]);
        type_text(&mut state, "osa");
        press(&mut state, &[Key::Enter]);
        // Frame, bar, header: the rows start on line 3.
        state.on_mouse(Mouse::Click, 30, 4);
        assert_eq!(state.cursor_file_row() + 1, 6);
        state.on_mouse(Mouse::Click, 30, 2);
        assert_eq!(state.cursor_file_row() + 1, 6, "the header is not a row");
    }

    #[test]
    fn the_variable_search_narrows_the_list() {
        let mut state = loaded("survey.sav", 100, 24);
        press(&mut state, &[Key::Char('i'), Key::Char('/')]);
        // `d` and `q` are text here.
        type_text(&mut state, "brand");
        assert_eq!(state.screen, Screen::Variables);
        assert_eq!(state.shown_variables(), 3);
        assert_eq!(state.var_cursor, 6, "the first one found");
        let bar = state.bar().unwrap();
        assert_eq!(
            (bar.left.as_str(), bar.right.as_str()),
            ("Search variables: brand▏", "3 of 13 variables")
        );
        press(&mut state, &[Key::Enter]);
        assert_eq!(
            state.bar().unwrap().right,
            "3 of 13 variables ─ / edit ─ Esc clear"
        );

        // Up and down move among the variables found.
        press(&mut state, &[Key::Down]);
        assert_eq!(state.var_cursor, 7);
        press(&mut state, &[Key::Bottom, Key::Down, Key::FastDown]);
        assert_eq!(state.var_cursor, 8);
        press(&mut state, &[Key::Top]);
        assert_eq!(state.var_cursor, 6);
        // A click on the second line of the list, under the bar.
        state.on_mouse(Mouse::Click, 10, 4);
        assert_eq!(state.var_cursor, 7);

        // `d` goes to that variable's column; the search is still there.
        press(&mut state, &[Key::Char('d')]);
        assert_eq!((state.screen, state.cursor_col), (Screen::Data, 7));
        press(&mut state, &[Key::Char('i')]);
        assert_eq!((state.shown_variables(), state.var_cursor), (3, 7));

        // Esc clears the search first, then leaves the view.
        press(&mut state, &[Key::Esc]);
        assert_eq!(
            (state.screen, state.shown_variables()),
            (Screen::Variables, 13)
        );
        assert_eq!(state.var_cursor, 7, "still on the variable");
        press(&mut state, &[Key::Esc]);
        assert_eq!(state.screen, Screen::Data);
    }

    #[test]
    fn the_variable_search_looks_at_labels_and_can_find_nothing() {
        let mut state = loaded("survey.sav", 100, 24);
        press(&mut state, &[Key::Char('i'), Key::Char('/')]);
        type_text(&mut state, "INCOME");
        assert_eq!((state.shown_variables(), state.var_cursor), (1, 4));
        // By label only: "Sampling weight".
        press(&mut state, &[Key::Esc, Key::Char('/')]);
        type_text(&mut state, "sampling");
        assert_eq!((state.shown_variables(), state.var_cursor), (1, 12));

        press(&mut state, &[Key::Esc, Key::Char('/')]);
        type_text(&mut state, "zzzz");
        press(&mut state, &[Key::Enter]);
        assert_eq!(state.shown_variables(), 0);
        // Nothing to open, copy or go to.
        press(
            &mut state,
            &[Key::Enter, Key::Char('c'), Key::Down, Key::Char('d')],
        );
        assert!(state.detail.is_none());
        assert_eq!(state.take_copy(), None);
        assert_eq!(state.screen, Screen::Variables);
        state.on_mouse(Mouse::Click, 10, 4);
        state.on_mouse(Mouse::ScrollDown, 10, 4);
    }

    #[test]
    fn the_full_text_bar_takes_rows_only_when_there_is_room() {
        let tall = loaded("survey.sav", 100, 30);
        assert_eq!((tall.bar_lines(), tall.variable_rows()), (3, 23));
        let short = loaded("survey.sav", 100, 12);
        assert_eq!((short.bar_lines(), short.variable_rows()), (0, 9));
    }
}
