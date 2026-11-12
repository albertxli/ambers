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

use unicode_width::UnicodeWidthStr;

use super::detail::{RowKind, percent};
use super::format::{Cell, Mode, fit};
use super::state::{Load, Screen, State};
use super::theme::Theme;
use crate::info::group_digits;
use crate::render::wrap;

/// The keys, as listed by `?`.
const HELP: &[(&str, &str)] = &[
    ("arrows, h j k l", "move one cell"),
    ("mouse", "wheel scrolls (Shift: sideways), click selects"),
    ("PgUp / PgDn", "one screen up / down"),
    ("Ctrl+u / Ctrl+d", "half a screen up / down"),
    ("Home / End", "first / last variable (or field)"),
    ("g / G", "first / last row"),
    ("1-9 …", "go to row (Enter keeps, Esc goes back)"),
    ("Tab", "next mode: Labels, Codes, Both"),
    ("f", "freeze / unfreeze the first variable"),
    (
        "Enter",
        "side panel: this respondent, or this variable's values",
    ),
    ("J / K", "scroll the side panel"),
    ("i", "Variable View and back"),
    ("d", "in Variable View: go to the variable's data"),
    ("y", "copy the cell; with values open, the table"),
    ("Y", "in Variable View: copy the cell"),
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
        Screen::Variables => {
            let panel = state.detail_width() as u16;
            let list = Rect {
                width: area.width - panel,
                ..area
            };
            variable_view(buf, list, state, theme);
            if panel > 0 {
                let side = Rect {
                    x: list.width,
                    width: panel,
                    ..area
                };
                variable_detail(buf, side, state, theme);
            }
        }
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
    } else if let Some(notice) = &state.notice {
        pieces.push((format!(" {notice} "), theme.accent));
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

/// What the counts cover, for the border: they are computed over the rows
/// loaded so far.
fn counts_status(state: &State) -> String {
    let rows = group_digits(state.counted_rows());
    match state.load {
        Load::Loading => format!(" counts: {rows} rows, loading… "),
        Load::Limit => format!(" counts: first {rows} rows "),
        Load::Failed(_) => format!(" counts: {rows} rows read "),
        Load::Done => format!(" counts: {rows} rows "),
    }
}

/// One row per variable, as the codebook describes it: type, label,
/// labelled values, valid and missing cases, and the label check. One
/// cell is highlighted; `y` copies its full text.
fn variable_view(buf: &mut Buffer, area: Rect, state: &State, theme: &Theme) {
    let position = format!(
        " Var {}/{} ",
        (state.var_cursor + 1).min(state.columns.len()),
        state.columns.len()
    );
    let right = Line::from(vec![
        Span::raw(counts_status(state)),
        Span::styled("─", theme.frame),
        Span::raw(position),
    ]);
    // The longest hint that fits beside the counts. A notice replaces it,
    // unless the value table is open and shows the notice itself.
    let room = (area.width as usize).saturating_sub(right.width() + 3);
    let hints = [
        " ↑↓←→ cell ─ Enter values ─ y copy cell ─ d go to data ─ i / Esc back ",
        " Enter values ─ y copy ─ d data ─ i back ",
        " Enter values ─ y copy ",
        "",
    ];
    let left = match &state.notice {
        Some(notice) if state.detail.is_none() => Line::styled(format!(" {notice} "), theme.accent),
        _ => {
            let hint = hints
                .iter()
                .find(|hint| hint.width() <= room)
                .unwrap_or(&"");
            Line::styled(*hint, theme.dim)
        }
    };
    Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(theme.frame)
        .title(Line::styled(
            format!(" {} ─ Variable View ", state.file_name),
            theme.title,
        ))
        .title_bottom(left)
        .title_bottom(right.right_aligned())
        .render(area, buf);

    let layout = state.variable_layout();
    for field in &layout {
        let title = fit(field.title, field.width, field.right);
        put(buf, area, field.x, 1, &title, theme.header);
    }

    let inner_width = area.width.saturating_sub(2);
    for line in 0..state.grid_rows() {
        let index = state.var_top + line;
        if index >= state.columns.len() {
            break;
        }
        let y = 2 + line;
        let on_cursor_row = index == state.var_cursor;
        let row_style = if on_cursor_row {
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

        for (number, field) in layout.iter().enumerate() {
            // On one line: labelled values are one per line in full.
            let text = state.variable_cell(index, number).replace('\n', ", ");
            let on_cursor = on_cursor_row && number == state.var_field;
            let style = if on_cursor {
                // The highlight covers the cell and the space either side.
                let x = area.x + (field.x - 1) as u16;
                let room = (area.width as usize).saturating_sub(field.x);
                let width = (field.width + 2).min(room) as u16;
                buf.set_style(Rect::new(x, area.y + y as u16, width, 1), theme.cursor_cell);
                theme.cursor_cell
            } else {
                match (field.title, text.as_str()) {
                    ("#", _) => theme.gutter,
                    ("Name", _) => theme.header,
                    ("Label check", "" | "ok") => theme.dim,
                    ("Label check", _) => theme.warn,
                    _ => Style::new(),
                }
            };
            put(
                buf,
                area,
                field.x,
                y,
                &fit(&text, field.width, field.right),
                style,
            );
        }
    }
}

/// The value table of the variable under the cursor, beside the list: its
/// full label, its definition, and every value with its count and share.
fn variable_detail(buf: &mut Buffer, area: Rect, state: &State, theme: &Theme) {
    let (Some(detail), Some(column)) = (&state.detail, state.columns.get(state.var_cursor)) else {
        return;
    };
    let summary = state.summary(state.var_cursor);
    let notice = (state.notice.as_deref()).map_or(String::new(), |notice| format!(" {notice} "));
    // The longest hint that fits beside the notice.
    let room = (area.width as usize).saturating_sub(notice.width() + 3);
    let hints = [
        " J/K scroll ─ y copy table ─ Enter close ",
        " y copy table ",
        "",
    ];
    let hint = hints
        .iter()
        .find(|hint| hint.width() <= room)
        .unwrap_or(&"");
    Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(theme.frame)
        .title(Line::from(vec![
            Span::styled(format!(" {} ", column.name), theme.title),
            Span::styled("─", theme.frame),
            Span::styled(format!(" {} ", summary.variable_type.name()), theme.accent),
        ]))
        .title_bottom(Line::styled(*hint, theme.dim))
        .title_bottom(Line::styled(notice, theme.accent).right_aligned())
        .render(area, buf);

    // Inside the frame, with a space of padding on each side.
    let text_width = (area.width as usize).saturating_sub(4);
    let last_line = (area.height as usize).saturating_sub(1);

    // The text above the table.
    let (label, label_style) = if column.label.is_empty() {
        ("(no label)".to_string(), theme.dim)
    } else {
        (wrap(&column.label, text_width), Style::new())
    };
    let mut definition = vec![format!("Format {}", column.format)];
    if !column.measure.is_empty() {
        definition.push(format!("measure {}", column.measure));
    }
    if !column.role.is_empty() {
        definition.push(format!("role {}", column.role));
    }
    let missing = column.missing_text();
    if !missing.is_empty() {
        definition.push(format!("user-missing: {missing}"));
    }
    let mut counts = vec![
        format!("Valid {}", group_digits(summary.n_valid)),
        format!("Missing {}", group_digits(summary.n_missing)),
        format!("Total {}", group_digits(summary.n_total)),
    ];
    match (summary.n_labeled, summary.n_unlabeled) {
        (Some(labelled), Some(unlabelled)) => {
            counts.push(format!("Codes {}", group_digits(summary.n_unique)));
            counts.push(format!("labelled {}", group_digits(labelled)));
            counts.push(format!("unlabelled {}", group_digits(unlabelled)));
        }
        _ => {
            let distinct = detail
                .rows
                .iter()
                .filter(|row| row.kind != RowKind::Missing)
                .count();
            counts.push(format!("Distinct {}", group_digits(distinct as u64)));
        }
    }

    let mut y = 1;
    for line in label.lines() {
        put(buf, area, 2, y, line, label_style);
        y += 1;
    }
    for line in wrap(&definition.join(" · "), text_width).lines() {
        put(buf, area, 2, y, line, theme.dim);
        y += 1;
    }
    for line in wrap(&counts.join(" · "), text_width).lines() {
        put(buf, area, 2, y, line, theme.dim);
        y += 1;
    }
    y += 1;

    // The table: the code or value, its label when there is one, the
    // count, and the shares of valid and of all cases.
    let labels = detail.has_labels();
    let count_width = (detail.rows.iter().map(|row| group_digits(row.n).len()))
        .max()
        .unwrap_or(0)
        .max(5);
    let numbers_width = count_width + 2 + 8 + 2 + 8;
    let room = text_width.saturating_sub(numbers_width + 2);
    let longest_code = (detail.rows.iter().map(|row| row.code.width()))
        .max()
        .unwrap_or(0)
        .max(5);
    let (code_width, label_width) = if labels {
        // Codes are short; the label takes what is left.
        let code = longest_code.min(room / 3);
        (code, room.saturating_sub(code + 2))
    } else {
        (room, 0)
    };
    // Codes line up on the right beside a label; values alone read from the left.
    let code_right = labels && column.numeric;
    let label_x = 2 + code_width + 2;
    let count_x = 2 + room + 2;
    let (valid_x, total_x) = (count_x + count_width + 2, count_x + count_width + 2 + 8 + 2);

    let first = if detail.categorical { "Code" } else { "Value" };
    put(
        buf,
        area,
        2,
        y,
        &fit(first, code_width, code_right),
        theme.header,
    );
    if labels {
        put(
            buf,
            area,
            label_x,
            y,
            &fit("Label", label_width, false),
            theme.header,
        );
    }
    put(
        buf,
        area,
        count_x,
        y,
        &fit("N", count_width, true),
        theme.header,
    );
    put(
        buf,
        area,
        valid_x,
        y,
        &fit("% valid", 8, true),
        theme.header,
    );
    put(
        buf,
        area,
        total_x,
        y,
        &fit("% total", 8, true),
        theme.header,
    );
    y += 1;

    for row in detail.rows.iter().skip(state.detail_scroll) {
        if y >= last_line {
            break;
        }
        let style = match row.kind {
            RowKind::Missing | RowKind::UserMissing => theme.dim,
            RowKind::Unlabeled => theme.warn,
            RowKind::Plain => Style::new(),
        };
        put(
            buf,
            area,
            2,
            y,
            &fit(&row.code, code_width, code_right),
            style,
        );
        if labels {
            put(
                buf,
                area,
                label_x,
                y,
                &fit(&row.label, label_width, false),
                style,
            );
        }
        put(
            buf,
            area,
            count_x,
            y,
            &fit(&group_digits(row.n), count_width, true),
            style,
        );
        put(
            buf,
            area,
            valid_x,
            y,
            &fit(&percent(row.pct_valid), 8, true),
            style,
        );
        put(
            buf,
            area,
            total_x,
            y,
            &fit(&percent(row.pct_total), 8, true),
            style,
        );
        y += 1;
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

    #[test]
    fn variable_view_of_every_type() {
        insta::assert_snapshot!(screen("variable_types.sav", 150, 21, &[Key::Char('i')]));
    }

    #[test]
    fn variable_view_narrow_keeps_names_and_types() {
        insta::assert_snapshot!(screen("survey.sav", 70, 12, &[Key::Char('i')]));
    }

    #[test]
    fn variable_detail_categorical() {
        // q1: unlabelled codes 7 and 8, user-missing 99, one system-missing.
        insta::assert_snapshot!(screen(
            "imperfect_labels.sav",
            130,
            20,
            &[Key::Char('i'), Key::Down, Key::Enter]
        ));
    }

    #[test]
    fn variable_detail_scrolled_with_a_notice() {
        insta::assert_snapshot!(screen(
            "imperfect_labels.sav",
            130,
            20,
            &[
                Key::Char('i'),
                Key::Down,
                Key::Down,
                Key::Enter,
                Key::Char('J'),
                Key::Char('J'),
                Key::Char('y')
            ]
        ));
    }

    #[test]
    fn variable_detail_of_a_numeric_and_a_text_variable() {
        // age: every distinct value, in order, in its print format.
        let numeric = screen(
            "imperfect_labels.sav",
            130,
            20,
            &[Key::Char('i'), Key::Bottom, Key::Enter],
        );
        insta::assert_snapshot!(numeric);
        // city: values from A to Z after the missing row, with their labels.
        let text = screen(
            "imperfect_labels.sav",
            130,
            20,
            &[Key::Char('i'), Key::Bottom, Key::Up, Key::Enter],
        );
        insta::assert_snapshot!("variable_detail_text", text);
    }

    #[test]
    fn the_highlighted_cell_is_styled() {
        let mut state = loaded("survey.sav", 120, 12);
        for key in [Key::Char('i'), Key::Down] {
            state.on_key(key);
        }
        let theme = Theme::pick(ThemeName::Dracula, false);
        let mut terminal = Terminal::new(TestBackend::new(120, 12)).unwrap();
        terminal.draw(|frame| draw(frame, &state, &theme)).unwrap();
        let buffer = terminal.backend().buffer();
        // Row 2 (gender) is on screen line 3; its label field is highlighted.
        let label = state.variable_layout()[3];
        assert_eq!(buffer[(label.x as u16, 3)].style().bg, theme.cursor_cell.bg);
        let name = state.variable_layout()[1];
        assert_eq!(buffer[(name.x as u16, 3)].style().bg, theme.cursor_row.bg);
    }

    #[test]
    fn counts_say_what_they_cover() {
        let mut state = loaded("survey.sav", 120, 8);
        state.on_key(Key::Char('i'));
        assert!(render(&state, 120, 8).contains(" counts: 20 rows "));
        state.load = Load::Loading;
        assert!(render(&state, 120, 8).contains(" counts: 20 rows, loading… "));
        state.load = Load::Limit;
        assert!(render(&state, 120, 8).contains(" counts: first 20 rows "));
    }
}
