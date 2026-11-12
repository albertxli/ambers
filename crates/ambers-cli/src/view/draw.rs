//! Draws the viewer from its `State`. Nothing here changes the state.
//!
//! Everything is written cell by cell into the frame's buffer: the layout
//! (a fixed row-number gutter, a frozen first column, columns of fixed
//! width cut at the edge) is simpler to get exact that way than with
//! table widgets.

use ratatui::Frame;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Clear, Widget};

use super::format::{Cell, Mode, fit};
use super::state::{Load, Screen, State};
use super::theme::Theme;
use crate::info::group_digits;

/// The keys, as listed by `?`.
const HELP: &[(&str, &str)] = &[
    ("arrows, h j k l", "move one cell"),
    ("PgUp / PgDn", "one screen up / down"),
    ("Ctrl+u / Ctrl+d", "half a screen up / down"),
    ("Home / End", "first / last variable"),
    ("g / G", "first / last loaded row"),
    ("1-9 …", "go to row (Enter keeps, Esc goes back)"),
    ("Tab", "next mode: Labels, Codes, Both"),
    ("Enter", "respondent panel (J / K scroll it)"),
    ("i", "Variable View (Enter jumps to a variable)"),
    ("f", "freeze / unfreeze the first variable"),
    ("q, Esc", "close the panel or view; quit"),
];

pub fn draw(frame: &mut Frame, state: &State, theme: &Theme) {
    let area = frame.area();
    let buf = frame.buffer_mut();
    buf.set_style(area, theme.base);
    if area.width < 24 || area.height < 5 {
        put(buf, area, 0, 0, "window too small", theme.warn);
        return;
    }
    match state.screen {
        Screen::Data => {
            let panel = state.panel_width() as u16;
            let grid = Rect {
                width: area.width - panel,
                ..area
            };
            data_view(buf, grid, state, theme);
            if panel > 0 {
                let side = Rect {
                    x: grid.width,
                    width: panel,
                    ..area
                };
                respondent_panel(buf, side, state, theme);
            }
        }
        Screen::Variables => variable_view(buf, area, state, theme),
    }
    if state.help {
        help(buf, area, theme);
    }
}

// ---------------------------------------------------------------------------
// Data View
// ---------------------------------------------------------------------------

fn data_view(buf: &mut Buffer, area: Rect, state: &State, theme: &Theme) {
    // The right side (position, loading) always shows; the left side gets
    // whatever room remains between the corners.
    let right = data_status_right(state, theme);
    let room = (area.width as usize).saturating_sub(right.width() + 3);
    Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(theme.frame)
        .title(Line::styled(format!(" {} ", state.file_name), theme.title))
        .title_bottom(data_status_left(state, theme, room))
        .title_bottom(right.right_aligned())
        .render(area, buf);

    let gutter = state.gutter_width();
    let (slots, divider) = state.visible_columns();
    // Inside the frame: ` 123 │` then the columns.
    let columns_x = 1 + gutter + 3;
    let inner_width = area.width.saturating_sub(2);

    // Header.
    put(
        buf,
        area,
        1,
        1,
        &format!(" {:>gutter$} ", "#"),
        theme.gutter,
    );
    put(buf, area, 1 + gutter + 2, 1, "│", theme.frame);
    for slot in &slots {
        let column = &state.columns[slot.column];
        let name = fit(&column.name, slot.width, column.right_aligned(state.mode));
        let style = if slot.column == state.cursor_col {
            theme.header.patch(theme.cursor_row)
        } else {
            theme.header
        };
        put(buf, area, columns_x + slot.x, 1, &name, style);
    }
    if let Some(x) = divider {
        put(buf, area, columns_x + x, 1, "│", theme.frame);
    }

    if state.rows == 0 {
        let message = match &state.load {
            Load::Loading => "loading…",
            Load::Failed(_) => "the data could not be read",
            _ => "this file has no rows",
        };
        put(buf, area, columns_x + 1, 3, message, theme.dim);
        return;
    }

    for line in 0..state.grid_rows() {
        let row = state.top_row + line;
        if row >= state.rows {
            break;
        }
        let y = 2 + line;
        let row_style = if row == state.cursor_row {
            theme.cursor_row
        } else if row % 2 == 1 {
            theme.stripe
        } else {
            Style::new()
        };
        buf.set_style(
            Rect::new(area.x + 1, area.y + y as u16, inner_width, 1),
            row_style,
        );
        let number = group_digits((row + 1) as u64).replace(',', "");
        put(
            buf,
            area,
            1,
            y,
            &format!(" {number:>gutter$} "),
            theme.gutter,
        );
        put(buf, area, 1 + gutter + 2, y, "│", theme.frame);
        if let Some(x) = divider {
            put(buf, area, columns_x + x, y, "│", theme.frame);
        }

        for slot in &slots {
            let column = &state.columns[slot.column];
            let cell = state.cell(row, slot.column);
            let text = fit(&cell.text, slot.width, column.right_aligned(state.mode));
            let on_cursor = row == state.cursor_row && slot.column == state.cursor_col;
            if on_cursor {
                // The highlight covers the cell and the space either side.
                let x = area.x + (columns_x + slot.x - 1) as u16;
                let width = (slot.width + 2)
                    .min((area.width as usize).saturating_sub(columns_x + slot.x))
                    as u16;
                buf.set_style(Rect::new(x, area.y + y as u16, width, 1), theme.cursor_cell);
            }
            let style = cell_style(&cell, theme, on_cursor);
            put(buf, area, columns_x + slot.x, y, &text, style);
        }
    }
}

/// How a cell's flags show: a value without a label in the warning colour,
/// a user-missing value dimmed. The cursor cell keeps its own colours.
fn cell_style(cell: &Cell, theme: &Theme, on_cursor: bool) -> Style {
    if on_cursor {
        theme.cursor_cell
    } else if cell.unlabeled {
        theme.warn
    } else if cell.user_missing {
        theme.dim
    } else {
        Style::new()
    }
}

/// The left of the bottom border: the mode, then the current variable (or
/// the row number being typed), then the label warning. Each piece is
/// dropped whole when it does not fit in `room` columns.
fn data_status_left<'a>(state: &State, theme: &Theme, room: usize) -> Line<'a> {
    let mut pieces = vec![(format!(" {} ", state.mode.name()), theme.accent)];
    if let Some(typed) = state.goto_text() {
        pieces.push((format!(" Go to row: {typed}▏"), theme.accent));
    } else if let Some(column) = state.columns.get(state.cursor_col) {
        let label = if column.label.is_empty() {
            String::new()
        } else {
            let width = column.label.chars().count().min(40);
            format!(": {}", fit(&column.label, width, false).trim_end())
        };
        pieces.push((format!(" {}{label} ", column.name), Style::new()));
    }
    let unlabeled = state.unlabeled_variables();
    if unlabeled > 0 {
        let noun = if unlabeled == 1 { "var" } else { "vars" };
        pieces.push((
            format!(" ⚠ {unlabeled} {noun} with unlabelled values "),
            theme.warn,
        ));
    }

    let mut spans = Vec::new();
    let mut used = 0;
    for (text, style) in pieces {
        let span = Span::styled(text, style);
        // One more column for the `─` that separates the pieces.
        if used + span.width() + 1 > room {
            break;
        }
        if !spans.is_empty() {
            spans.push(Span::styled("─", theme.frame));
        }
        used += span.width() + 1;
        spans.push(span);
    }
    Line::from(spans)
}

fn data_status_right<'a>(state: &State, theme: &Theme) -> Line<'a> {
    let rows = group_digits(state.rows as u64);
    let total = state.total_rows.map(|total| group_digits(total as u64));
    let loading = match (&state.load, total) {
        (Load::Loading, Some(total)) => {
            Some((format!(" loading {rows} of {total}… "), theme.accent))
        }
        (Load::Loading, None) => Some((format!(" loading {rows}… "), theme.accent)),
        (Load::Limit, Some(total)) => Some((
            format!(" first {rows} of {total} rows (limit) "),
            theme.warn,
        )),
        (Load::Limit, None) => Some((format!(" first {rows} rows (limit) "), theme.warn)),
        (Load::Failed(error), _) => Some((format!(" stopped at row {rows}: {error} "), theme.warn)),
        (Load::Done, _) => None,
    };

    let mut spans = Vec::new();
    if let Some((text, style)) = loading {
        spans.push(Span::styled(text, style));
        spans.push(Span::styled("─", theme.frame));
    }
    let row = if state.rows == 0 {
        0
    } else {
        state.cursor_row + 1
    };
    spans.push(Span::raw(format!(
        " Row {}/{rows} ",
        group_digits(row as u64)
    )));
    spans.push(Span::styled("─", theme.frame));
    spans.push(Span::raw(format!(
        " Var {}/{} ",
        (state.cursor_col + 1).min(state.columns.len()),
        state.columns.len()
    )));
    spans.push(Span::styled("─", theme.frame));
    spans.push(Span::styled(" ? help ", theme.dim));
    Line::from(spans)
}

// ---------------------------------------------------------------------------
// Respondent panel
// ---------------------------------------------------------------------------

/// One respondent's answers, one variable per line, in file order: name,
/// the value with its label, and the variable's label.
fn respondent_panel(buf: &mut Buffer, area: Rect, state: &State, theme: &Theme) {
    let title = if state.rows == 0 {
        " Row ".to_string()
    } else {
        format!(" Row {} ", group_digits((state.cursor_row + 1) as u64))
    };
    Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(theme.frame)
        .title(Line::styled(title, theme.title))
        .title_bottom(Line::styled(" J/K scroll ─ Enter close ", theme.dim))
        .render(area, buf);
    if state.rows == 0 {
        return;
    }

    // Inside the frame, with a space of padding on each side:
    // name, two spaces, value, two spaces, variable label.
    let inner = (area.width as usize).saturating_sub(2);
    let text_width = inner.saturating_sub(2);
    let longest_name = (state.columns.iter().map(|c| c.name.chars().count()))
        .max()
        .unwrap_or(0);
    let name_width = longest_name.min(18).min(text_width / 3);
    let rest = text_width.saturating_sub(name_width + 4);
    let value_width = rest / 2;
    let label_width = rest - value_width;
    let (name_x, value_x) = (2, 2 + name_width + 2);
    let label_x = value_x + value_width + 2;

    for line in 0..state.panel_rows() {
        let index = state.panel_top + line;
        let Some(column) = state.columns.get(index) else {
            break;
        };
        let y = 1 + line;
        if index == state.cursor_col {
            buf.set_style(
                Rect::new(area.x + 1, area.y + y as u16, inner as u16, 1),
                theme.cursor_row,
            );
        }
        // Both mode: the code and the label, whatever the grid shows.
        let cell = state.cell_in(state.cursor_row, index, Mode::Both);
        let name = fit(&column.name, name_width, false);
        let value = fit(&cell.text, value_width, false);
        let label = fit(&column.label, label_width, false);
        put(buf, area, name_x, y, &name, theme.header);
        put(
            buf,
            area,
            value_x,
            y,
            &value,
            cell_style(&cell, theme, false),
        );
        put(buf, area, label_x, y, &label, theme.dim);
    }
}

// ---------------------------------------------------------------------------
// Variable View
// ---------------------------------------------------------------------------

fn variable_view(buf: &mut Buffer, area: Rect, state: &State, theme: &Theme) {
    let position = format!(
        " Var {}/{} ",
        (state.var_cursor + 1).min(state.columns.len()),
        state.columns.len()
    );
    Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(theme.frame)
        .title(Line::styled(
            format!(" {} ─ Variable View ", state.file_name),
            theme.title,
        ))
        .title_bottom(Line::styled(
            " ↑↓ move ─ Enter go to variable ─ i / Esc back ",
            theme.dim,
        ))
        .title_bottom(Line::raw(position).right_aligned())
        .render(area, buf);

    let number_width = state.columns.len().to_string().len().max(1);
    let name_width = (state.columns.iter().map(|c| c.name.chars().count()))
        .max()
        .unwrap_or(4)
        .max(4);
    // Title, width, right-aligned.
    let layout: [(&str, usize, bool); 11] = [
        ("#", number_width, true),
        ("Name", name_width, false),
        ("Type", 8, false),
        ("Width", 5, true),
        ("Dec", 3, true),
        ("Label", 28, false),
        ("Values", 26, false),
        // Not an SPSS column: what the loaded rows say about the labels.
        ("Label check", 32, false),
        ("Missing", 12, false),
        ("Measure", 8, false),
        ("Role", 9, false),
    ];
    let mut x = 2;
    let starts: Vec<usize> = (layout.iter())
        .map(|(_, width, _)| {
            let start = x;
            x += width + 2;
            start
        })
        .collect();

    for ((title, width, right), &start) in layout.iter().zip(&starts) {
        put(
            buf,
            area,
            start,
            1,
            &fit(title, *width, *right),
            theme.header,
        );
    }

    let inner_width = area.width.saturating_sub(2);
    for line in 0..state.grid_rows() {
        let index = state.var_top + line;
        let Some(column) = state.columns.get(index) else {
            break;
        };
        let y = 2 + line;
        let row_style = if index == state.var_cursor {
            theme.cursor_row
        } else if index % 2 == 1 {
            theme.stripe
        } else {
            Style::new()
        };
        buf.set_style(
            Rect::new(area.x + 1, area.y + y as u16, inner_width, 1),
            row_style,
        );

        let values = if column.labels.is_empty() {
            String::new()
        } else {
            let first: Vec<String> = (column.labels.iter().take(3))
                .map(|(code, label)| format!("{code}={label}"))
                .collect();
            format!("{}: {}", column.labels.len(), first.join(", "))
        };
        let check = state.label_check(index);
        let check_style = match check.as_str() {
            "" | "ok" => theme.dim,
            _ => theme.warn,
        };
        let cells: [(String, Style); 11] = [
            ((index + 1).to_string(), theme.gutter),
            (column.name.clone(), theme.header),
            (column.kind.to_string(), Style::new()),
            (column.format_width.to_string(), Style::new()),
            (column.decimals.to_string(), Style::new()),
            (column.label.clone(), Style::new()),
            (values, Style::new()),
            (check, check_style),
            (column.missing_text(), Style::new()),
            (column.measure.to_string(), Style::new()),
            (column.role.to_string(), Style::new()),
        ];
        for (((text, style), (_, width, right)), &start) in cells.iter().zip(&layout).zip(&starts) {
            put(buf, area, start, y, &fit(text, *width, *right), *style);
        }
    }
}

// ---------------------------------------------------------------------------
// Help
// ---------------------------------------------------------------------------

fn help(buf: &mut Buffer, area: Rect, theme: &Theme) {
    let key_width = HELP
        .iter()
        .map(|(key, _)| key.chars().count())
        .max()
        .unwrap_or(0);
    let text_width = HELP
        .iter()
        .map(|(_, text)| text.chars().count())
        .max()
        .unwrap_or(0);
    let width = ((key_width + text_width + 6) as u16).min(area.width);
    let height = (HELP.len() as u16 + 2).min(area.height);
    let popup = Rect::new(
        area.x + (area.width - width) / 2,
        area.y + (area.height - height) / 2,
        width,
        height,
    );
    Clear.render(popup, buf);
    buf.set_style(popup, theme.base);
    Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(theme.frame)
        .title(Line::styled(" Keys ", theme.title))
        .title_bottom(Line::styled(" any key closes ", theme.dim))
        .render(popup, buf);
    for (line, (key, text)) in HELP.iter().enumerate() {
        put(
            buf,
            popup,
            2,
            1 + line,
            &fit(key, key_width, false),
            theme.accent,
        );
        put(buf, popup, 2 + key_width + 2, 1 + line, text, Style::new());
    }
}

/// Write `text` at (`x`, `y`) inside `area`, clipped so it never crosses
/// the area's right border or its last line.
fn put(buf: &mut Buffer, area: Rect, x: usize, y: usize, text: &str, style: Style) {
    let (width, height) = (area.width as usize, area.height as usize);
    // The last column and line belong to the frame.
    if x + 1 >= width || y + 1 >= height {
        return;
    }
    buf.set_stringn(
        area.x + x as u16,
        area.y + y as u16,
        text,
        width - 1 - x,
        style,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::super::state::Key;
    use super::super::state::tests::loaded;
    use super::super::theme::ThemeName;

    /// The screen as text, after pressing `keys`.
    fn screen(name: &str, width: u16, height: u16, keys: &[Key]) -> String {
        let mut state = loaded(name, width, height);
        for &key in keys {
            state.on_key(key);
        }
        render(&state, width, height)
    }

    fn render(state: &State, width: u16, height: u16) -> String {
        let theme = Theme::pick(ThemeName::Dracula, false);
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        terminal.draw(|frame| draw(frame, state, &theme)).unwrap();
        let buffer = terminal.backend().buffer();
        let mut text = String::new();
        for y in 0..height {
            let line: String = (0..width).map(|x| buffer[(x, y)].symbol()).collect();
            text.push_str(line.trim_end());
            text.push('\n');
        }
        text
    }

    #[test]
    fn data_view_labels() {
        insta::assert_snapshot!(screen("survey.sav", 100, 14, &[Key::Down, Key::Right]));
    }

    #[test]
    fn data_view_codes() {
        insta::assert_snapshot!(screen("survey.sav", 100, 14, &[Key::Tab]));
    }

    #[test]
    fn data_view_both() {
        insta::assert_snapshot!(screen("survey.sav", 100, 14, &[Key::Tab, Key::Tab]));
    }

    #[test]
    fn data_view_scrolled_right_keeps_the_id_column() {
        insta::assert_snapshot!(screen("survey.sav", 70, 10, &[Key::End]));
    }

    #[test]
    fn data_view_scrolled_right_unfrozen() {
        insta::assert_snapshot!(screen("survey.sav", 70, 10, &[Key::Char('f'), Key::End]));
    }

    #[test]
    fn data_view_narrow() {
        insta::assert_snapshot!(screen("survey.sav", 40, 10, &[Key::Right, Key::Right]));
    }

    #[test]
    fn imperfect_labels_are_visible() {
        insta::assert_snapshot!(screen("imperfect_labels.sav", 100, 16, &[]));
    }

    #[test]
    fn go_to_row_prompt() {
        insta::assert_snapshot!(screen(
            "survey.sav",
            100,
            10,
            &[Key::Char('1'), Key::Char('8')]
        ));
    }

    #[test]
    fn loading_and_limit_are_reported() {
        let mut state = loaded("survey.sav", 100, 8);
        state.total_rows = Some(2_300_000);
        state.load = Load::Loading;
        let loading = render(&state, 100, 8);
        assert!(loading.contains(" loading 20 of 2,300,000… "), "{loading}");
        state.load = Load::Limit;
        let limit = render(&state, 100, 8);
        assert!(
            limit.contains(" first 20 of 2,300,000 rows (limit) "),
            "{limit}"
        );
        state.load = Load::Failed("unexpected end of file".to_string());
        let failed = render(&state, 100, 8);
        assert!(
            failed.contains(" stopped at row 20: unexpected end of file "),
            "{failed}"
        );
    }

    #[test]
    fn respondent_panel() {
        insta::assert_snapshot!(screen(
            "imperfect_labels.sav",
            110,
            12,
            &[Key::Down, Key::Down, Key::Down, Key::Enter]
        ));
    }

    #[test]
    fn variable_view() {
        insta::assert_snapshot!(screen(
            "imperfect_labels.sav",
            150,
            12,
            &[Key::Char('i'), Key::Down]
        ));
    }

    #[test]
    fn help_overlay() {
        insta::assert_snapshot!(screen("survey.sav", 80, 20, &[Key::Char('?')]));
    }

    #[test]
    fn a_tiny_window_says_so() {
        let text = screen("survey.sav", 20, 4, &[]);
        assert!(text.starts_with("window too small"), "{text}");
    }

    #[test]
    fn the_cursor_cell_and_flagged_cells_are_styled() {
        let state = loaded("imperfect_labels.sav", 100, 16);
        let theme = Theme::pick(ThemeName::Dracula, false);
        let mut terminal = Terminal::new(TestBackend::new(100, 16)).unwrap();
        terminal.draw(|frame| draw(frame, &state, &theme)).unwrap();
        let buffer = terminal.backend().buffer();
        // The style of the first cell of `needle` on screen line `y`.
        let style_at = |needle: &str, y: u16| {
            let line: String = (0..100).map(|x| buffer[(x, y)].symbol()).collect();
            let column = line
                .find(needle)
                .unwrap_or_else(|| panic!("no {needle:?} in {line:?}"));
            // Every character before it is one cell wide.
            let x = line[..column].chars().count() as u16;
            buffer[(x, y)].style()
        };
        // Row 1 holds the cursor, on the id column: `1 │ Agree`. The row
        // number left of it is only tinted with the cursor row's colour.
        assert_eq!(style_at("1 │ Agree", 2).bg, theme.cursor_cell.bg);
        assert_eq!(style_at("1 │", 2).bg, theme.cursor_row.bg);
        // Row 4: q1 is 7, which has no label.
        assert_eq!(style_at("7", 5).fg, theme.warn.fg);
        // Row 6: q1 is 99 "Refused", declared user-missing.
        assert_eq!(style_at("Refused", 7).fg, theme.dim.fg);
    }
}
