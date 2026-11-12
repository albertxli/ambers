//! The viewer's state and what each key does to it. Nothing here touches
//! the terminal, so every behaviour is unit-tested; `draw` only reads it.

use std::collections::BTreeSet;

use ambers::{SpssMetadata, Value};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use super::format::{Cell, Column, Mode};
use super::loader::{DISTINCT_CAP, Message};

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

/// What the loaded rows say about one numeric variable with value labels.
#[derive(Default)]
struct Audit {
    seen: BTreeSet<Value>,
    /// More distinct values than are tracked.
    many: bool,
    unlabeled: bool,
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
    pub limit: usize,
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

    goto: Option<Goto>,
    audit: Vec<Audit>,
    width: usize,
    height: usize,
}

impl State {
    pub fn new(
        file_name: String,
        meta: &SpssMetadata,
        schema: &Schema,
        batch_rows: usize,
        limit: usize,
        mode: Mode,
    ) -> State {
        let columns = Column::from_metadata(meta, schema);
        State {
            file_name,
            audit: columns.iter().map(|_| Audit::default()).collect(),
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
        }
    }

    /// Numeric variables with value labels: the ones the label audit covers.
    pub fn audited_columns(&self) -> Vec<usize> {
        (0..self.columns.len())
            .filter(|&c| self.columns[c].numeric && !self.columns[c].labels.is_empty())
            .collect()
    }

    // --- loading ----------------------------------------------------------

    pub fn receive(&mut self, message: Message) {
        match message {
            Message::Batch { batch, new_values } => {
                self.rows += batch.num_rows();
                self.batches.push(batch);
                for (column, values) in new_values {
                    let audit = &mut self.audit[column];
                    for value in values {
                        let value = Value::Numeric(value);
                        audit.unlabeled |= !self.columns[column].labels.contains_key(&value);
                        audit.seen.insert(value);
                    }
                    audit.many = audit.seen.len() > DISTINCT_CAP;
                }
            }
            Message::Finished => {
                let more = self.total_rows.is_none_or(|total| total > self.rows);
                self.load = if self.rows >= self.limit && more {
                    Load::Limit
                } else {
                    Load::Done
                };
            }
            Message::Failed(error) => self.load = Load::Failed(error),
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

    // --- label audit ------------------------------------------------------

    /// How many variables hold values that have no label.
    pub fn unlabeled_variables(&self) -> usize {
        self.audit.iter().filter(|audit| audit.unlabeled).count()
    }

    /// The audit of one variable, for Variable View: `ok`, the unlabelled
    /// values, the duplicated labels, or empty when it is not audited.
    pub fn label_check(&self, column: usize) -> String {
        let labels = &self.columns[column].labels;
        if !self.columns[column].numeric || labels.is_empty() {
            return String::new();
        }
        let audit = &self.audit[column];
        let mut findings = Vec::new();
        if let Some(found) = ambers_codebook::validate::unlabeled_values(&audit.seen, labels) {
            let shown: Vec<String> = (found.values.iter().take(5))
                .map(|value| value.to_string())
                .collect();
            let more = if found.values.len() > 5 { ", …" } else { "" };
            let count = if audit.many {
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

    /// Apply a key. Returns `true` when the viewer should quit.
    pub fn on_key(&mut self, key: Key) -> bool {
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
        let page = self.grid_rows().max(1);
        match key {
            Key::Up | Key::Char('k') => self.var_cursor = self.var_cursor.saturating_sub(1),
            Key::Down | Key::Char('j') => self.var_cursor = (self.var_cursor + 1).min(last),
            Key::PageUp => self.var_cursor = self.var_cursor.saturating_sub(page),
            Key::PageDown => self.var_cursor = (self.var_cursor + page).min(last),
            Key::HalfUp => self.var_cursor = self.var_cursor.saturating_sub(page / 2),
            Key::HalfDown => self.var_cursor = (self.var_cursor + page / 2).min(last),
            Key::Top | Key::Home | Key::Char('g') => self.var_cursor = 0,
            Key::Bottom | Key::End | Key::Char('G') => self.var_cursor = last,
            Key::Char('?') => self.help = true,
            // Go to the chosen variable in Data View.
            Key::Enter => {
                self.cursor_col = self.var_cursor;
                self.screen = Screen::Data;
                self.follow_panel();
            }
            Key::Esc | Key::Char('i') | Key::Char('q') => self.screen = Screen::Data,
            _ => {}
        }
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
            1_000_000,
            Mode::Labels,
        );
        state.resize(width, height);
        for message in super::super::loader::spawn(scanner, state.audited_columns()) {
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
        state.limit = 20;
        state.total_rows = Some(500);
        state.receive(Message::Finished);
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
    fn variable_view_jumps_to_the_chosen_variable() {
        let mut state = loaded("survey.sav", 100, 24);
        press(&mut state, &[Key::Right, Key::Char('i')]);
        assert_eq!((state.screen, state.var_cursor), (Screen::Variables, 1));
        press(&mut state, &[Key::Down, Key::Down, Key::Enter]);
        assert_eq!((state.screen, state.cursor_col), (Screen::Data, 3));
        // Leaving with Esc does not move the cursor.
        press(&mut state, &[Key::Char('i'), Key::Bottom, Key::Esc]);
        assert_eq!((state.screen, state.cursor_col), (Screen::Data, 3));
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
