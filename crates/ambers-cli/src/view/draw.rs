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
use super::state::{Load, Screen, State, VARIABLE_FIELDS};
use super::theme::Theme;
use crate::info::group_digits;
use crate::render::wrap;

/// The keys, as listed by `?`.
const HELP: &[(&str, &str)] = &[
    ("arrows, h j k l", "move one cell"),
    (
        "mouse",
        "wheel scrolls (Shift or Alt: sideways), click selects",
    ),
    ("PgUp / PgDn", "one screen up / down"),
    ("Ctrl+u / Ctrl+d", "half a screen up / down"),
    ("Shift+arrows, { } < >", "50 rows or columns at once"),
    (
        "Ctrl+arrows, Home/End",
        "to the top, bottom, far left, far right",
    ),
    ("g G 0 $", "the same, where the terminal keeps Ctrl+arrows"),
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
    ("/", "filter rows by this column; search variables"),
    ("", "jap  =Japan  !=3  >30  <=5  =. (missing)"),
    ("=", "keep the rows with this cell's value"),
    ("c", "copy the highlighted cell, in full"),
    ("C or t", "copy the open side panel, for a spreadsheet"),
    ("q, Esc", "close the panel, clear filters, leave; quit"),
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
    let right = data_status_right(state, theme, area.width >= 70);
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

    // The filter bar, when there is one, then the header.
    filter_bar(buf, area, state, theme);
    let header_y = 1 + state.data_bar();
    put(
        buf,
        area,
        1,
        header_y,
        &format!(" {:>gutter$} ", "#"),
        theme.gutter,
    );
    put(buf, area, 1 + gutter + 2, header_y, "│", theme.frame);
    for slot in &slots {
        let column = &state.columns[slot.column];
        let name = fit(&column.name, slot.width, column.right_aligned(state.mode));
        let style = if slot.column == state.cursor_col {
            theme.header.patch(theme.cursor_row)
        } else {
            theme.header
        };
        put(buf, area, columns_x + slot.x, header_y, &name, style);
    }
    if let Some(x) = divider {
        put(buf, area, columns_x + x, header_y, "│", theme.frame);
    }

    if state.shown_rows() == 0 {
        let message = match &state.load {
            _ if state.rows > 0 => "no row matches the filter",
            Load::Loading => "loading…",
            Load::Failed(_) => "the data could not be read",
            _ => "this file has no rows",
        };
        put(buf, area, columns_x + 1, header_y + 2, message, theme.dim);
        return;
    }

    for line in 0..state.grid_rows() {
        // A position among the rows shown; a filter leaves gaps in the
        // file's row numbers.
        let row = state.top_row + line;
        if row >= state.shown_rows() {
            break;
        }
        let y = header_y + 1 + line;
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
        let number = (state.row_at(row) + 1).to_string();
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
    let mode = format!(" {} ", state.mode.name());
    let unlabeled = state.unlabeled_variables();
    let warning = (unlabeled > 0).then(|| {
        let noun = if unlabeled == 1 { "var" } else { "vars" };
        format!(" ⚠ {unlabeled} {noun} with unlabelled values ")
    });

    let mut pieces = vec![(mode.clone(), theme.accent)];
    if let Some(typed) = state.goto_text() {
        pieces.push((format!(" Go to row: {typed}▏"), theme.accent));
    } else if let Some(notice) = &state.notice {
        pieces.push((format!(" {notice} "), theme.accent));
    } else if let Some(column) = state.columns.get(state.cursor_col) {
        let label = if column.label.is_empty() {
            String::new()
        } else {
            // As much of the label as the border has room for, after the
            // mode, the name and the warning; `L` copies all of it.
            let taken = mode.width() + column.name.width() + 6;
            let taken = taken + warning.as_ref().map_or(0, |warning| warning.width() + 1);
            let width = column.label.width().min(room.saturating_sub(taken).max(12));
            format!(": {}", fit(&column.label, width, false).trim_end())
        };
        pieces.push((format!(" {}{label} ", column.name), Style::new()));
    }
    if let Some(warning) = warning {
        pieces.push((warning, theme.warn));
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

/// The right of the bottom border. `roomy` adds the filter hint, which a
/// narrow window has no room for.
fn data_status_right<'a>(state: &State, theme: &Theme, roomy: bool) -> Line<'a> {
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
        // The flag is named so the way to load more is on screen.
        (Load::Memory, Some(total)) => Some((
            format!(" first {rows} of {total} rows: memory budget reached (--memory GB for more) "),
            theme.warn,
        )),
        (Load::Memory, None) => Some((
            format!(" first {rows} rows: memory budget reached (--memory GB for more) "),
            theme.warn,
        )),
        (Load::Failed(error), _) => Some((format!(" stopped at row {rows}: {error} "), theme.warn)),
        (Load::Done, _) => None,
    };

    let mut spans = Vec::new();
    if let Some((text, style)) = loading {
        spans.push(Span::styled(text, style));
        spans.push(Span::styled("─", theme.frame));
    }
    // The file's row number, as in the gutter, whatever is filtered.
    let row = if state.shown_rows() == 0 {
        0
    } else {
        state.cursor_file_row() + 1
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
    let hint = if roomy {
        " / filter ─ ? help "
    } else {
        " ? help "
    };
    spans.push(Span::styled(hint, theme.dim));
    Line::from(spans)
}

/// The filter bar (Data View) or search bar (Variable View): one line
/// under the top border, shown only while typed into or active.
fn filter_bar(buf: &mut Buffer, area: Rect, state: &State, theme: &Theme) {
    let Some(bar) = state.bar() else {
        return;
    };
    // Inside the frame, with a space of padding on each side.
    let inner = (area.width as usize).saturating_sub(4);
    let right_width = bar.right.width().min(inner);
    let left_width = inner.saturating_sub(right_width + 2);
    let left_style = if bar.typing {
        theme.accent
    } else {
        theme.header
    };
    let right_style = if bar.problem { theme.warn } else { theme.dim };
    put(
        buf,
        area,
        2,
        1,
        &fit(&bar.left, left_width, false),
        left_style,
    );
    let right_x = (area.width as usize).saturating_sub(2 + right_width);
    put(
        buf,
        area,
        right_x,
        1,
        &fit(&bar.right, right_width, false),
        right_style,
    );
}

// ---------------------------------------------------------------------------
// Respondent panel
// ---------------------------------------------------------------------------

/// One respondent's answers, one variable per line, in file order: name,
/// the value with its label, and the variable's label.
fn respondent_panel(buf: &mut Buffer, area: Rect, state: &State, theme: &Theme) {
    let title = if state.shown_rows() == 0 {
        " Row ".to_string()
    } else {
        let row = state.cursor_file_row() + 1;
        format!(" Row {} ", group_digits(row as u64))
    };
    Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(theme.frame)
        .title(Line::styled(title, theme.title))
        .title_bottom(Line::styled(
            " J/K scroll ─ C copy row ─ Enter close ",
            theme.dim,
        ))
        .render(area, buf);
    if state.shown_rows() == 0 {
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
        Load::Limit | Load::Memory => format!(" counts: first {rows} rows "),
        Load::Failed(_) => format!(" counts: {rows} rows read "),
        Load::Done => format!(" counts: {rows} rows "),
    }
}

/// One row per variable, as the codebook describes it: type, label,
/// labelled values, valid and missing cases, and the label check. One
/// cell is highlighted; `c` copies its full text.
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
        " ↑↓←→ cell ─ Enter values ─ / search ─ c copy cell ─ d go to data ─ i / Esc back ",
        " Enter values ─ / search ─ c copy cell ─ d data ─ i back ",
        " Enter values ─ c copy cell ─ d data ─ i back ",
        " Enter values ─ c copy ─ i back ",
        " Enter values ─ c copy ",
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

    // The search bar, when there is one, then the header.
    filter_bar(buf, area, state, theme);
    let header_y = 1 + state.variable_bar();
    let layout = state.variable_layout();
    for field in &layout {
        let title = fit(field.title, field.width, field.right);
        put(buf, area, field.x, header_y, &title, theme.header);
    }
    if state.shown_variables() == 0 && !state.columns.is_empty() {
        let message = "no variable has that in its name or label";
        put(buf, area, 2, header_y + 2, message, theme.dim);
        return;
    }

    let inner_width = area.width.saturating_sub(2);
    for line in 0..state.variable_rows() {
        // A position in the list, which a search may have narrowed.
        let position = state.var_top + line;
        if position >= state.shown_variables() {
            break;
        }
        let index = state.var_at(position);
        let y = header_y + 1 + line;
        let on_cursor_row = index == state.var_cursor;
        let row_style = if on_cursor_row {
            theme.cursor_row
        } else if position % 2 == 1 {
            theme.stripe
        } else {
            Style::new()
        };
        buf.set_style(
            Rect::new(area.x + 1, area.y + y as u16, inner_width, 1),
            row_style,
        );

        for field in &layout {
            // On one line: labelled values are one per line in full.
            let text = state.variable_cell(index, field.index).replace('\n', ", ");
            let on_cursor = on_cursor_row && field.index == state.var_field;
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
    full_text_bar(buf, area, state, theme);
}

/// Under the variable list, the highlighted cell's text in full, wrapped:
/// the whole label, the whole value list. The list itself shows them cut.
fn full_text_bar(buf: &mut Buffer, area: Rect, state: &State, theme: &Theme) {
    let lines = state.bar_lines();
    if lines == 0 || state.columns.is_empty() {
        return;
    }
    // A rule, then the text, above the bottom border.
    let rule_y = (area.height as usize).saturating_sub(lines + 2);
    let inner_width = (area.width as usize).saturating_sub(2);
    put(buf, area, 1, rule_y, &"─".repeat(inner_width), theme.frame);

    let (title, _) = VARIABLE_FIELDS[state.var_field];
    let name = &state.columns[state.var_cursor].name;
    let heading = format!("{name} · {title}");
    put(buf, area, 2, rule_y, &format!(" {heading} "), theme.header);

    let text_width = inner_width.saturating_sub(2);
    let full = state.variable_cell(state.var_cursor, state.var_field);
    // Labelled values are one per line when copied; here they run on.
    let wrapped = wrap(&full.replace('\n', ", "), text_width);
    let mut shown: Vec<String> = wrapped.lines().take(lines).map(str::to_string).collect();
    if wrapped.lines().count() > lines {
        // More than fits: the last line says so. `C` still copies all of it.
        let last = shown.last_mut().expect("at least one line is shown");
        *last = fit(&format!("{last} "), text_width.saturating_sub(1), false)
            .trim_end()
            .to_string()
            + " …";
    }
    if full.is_empty() {
        put(buf, area, 2, rule_y + 1, "(empty)", theme.dim);
    }
    for (line, text) in shown.iter().enumerate() {
        put(buf, area, 2, rule_y + 1 + line, text, Style::new());
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
        " J/K scroll ─ C copy table ─ Enter close ",
        " C copy table ",
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
        insta::assert_snapshot!(screen("survey.sav", 80, 26, &[Key::Char('?')]));
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
                Key::Char('C')
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

    #[test]
    fn stopping_at_the_memory_budget_is_reported_with_the_way_out() {
        let mut state = loaded("survey.sav", 120, 8);
        state.total_rows = Some(395_330);
        state.load = Load::Memory;
        let data = render(&state, 120, 8);
        assert!(
            data.contains(
                " first 20 of 395,330 rows: memory budget reached (--memory GB for more) "
            ),
            "{data}"
        );
        state.on_key(Key::Char('i'));
        assert!(render(&state, 120, 8).contains(" counts: first 20 rows "));
    }

    #[test]
    fn the_full_text_bar_shows_the_highlighted_cell_in_full() {
        // gender's values, then its label; the list shows both cut or short.
        let values = screen(
            "survey.sav",
            80,
            18,
            &[Key::Char('i'), Key::Down, Key::Right],
        );
        insta::assert_snapshot!(values);
        assert!(values.contains(" gender · Values "), "{values}");
        assert!(values.contains("1=Male, 2=Female"), "{values}");
        let label = screen("survey.sav", 80, 18, &[Key::Char('i'), Key::Down]);
        assert!(label.contains(" gender · Label "), "{label}");
    }

    #[test]
    fn the_full_text_bar_wraps_a_long_value_list() {
        // satisfaction has six labelled values: more than one line at 80 wide.
        let text = screen(
            "survey.sav",
            80,
            18,
            &[Key::Char('i'), Key::Down, Key::Down, Key::Right],
        );
        assert!(text.contains(" satisfaction · Values "), "{text}");
        assert!(
            text.contains("99=Refused"),
            "the end of the list is on screen:\n{text}"
        );
    }

    #[test]
    fn the_variable_list_scrolled_to_its_last_field() {
        insta::assert_snapshot!(screen("survey.sav", 80, 18, &[Key::Char('i'), Key::End]));
    }

    /// `keys`, with each character of `typed` pressed after them.
    fn typing(keys: &[Key], typed: &str) -> Vec<Key> {
        keys.iter()
            .copied()
            .chain(typed.chars().map(Key::Char))
            .collect()
    }

    #[test]
    fn the_filter_bar_while_typing() {
        // On city: the grid narrows to Osaka as it is typed.
        let keys = typing(&[Key::Char('$'), Key::Char('/')], "osa");
        let text = screen("survey.sav", 100, 14, &keys[..]);
        // `$` went to the last column; the filter is on that one.
        assert!(text.contains("Filter weight: osa▏"), "{text}");

        let mut keys = vec![Key::Right; 5];
        keys.push(Key::Char('/'));
        let text = screen("survey.sav", 100, 14, &typing(&keys, "osa"));
        insta::assert_snapshot!(text);
        assert!(text.contains("Filter city: osa▏"), "{text}");
        assert!(text.contains("5 of 20 rows"), "{text}");
    }

    #[test]
    fn two_filters_are_listed_in_the_bar() {
        let mut keys = typing(&[Key::Right, Key::Char('/')], "=female");
        keys.push(Key::Enter);
        keys.extend([Key::Right; 4]);
        keys.push(Key::Char('/'));
        let mut keys = typing(&keys, "osa");
        keys.push(Key::Enter);
        let text = screen("survey.sav", 110, 14, &keys);
        insta::assert_snapshot!(text);
        assert!(
            text.contains("Filter gender: =female │ city: osa"),
            "{text}"
        );
        // The gutter keeps the file's row numbers.
        assert!(text.contains("│   2 │"), "{text}");
        assert!(text.contains("│  18 │"), "{text}");
        assert!(!text.contains("│   1 │"), "{text}");
    }

    #[test]
    fn a_filter_that_matches_nothing_says_so() {
        let keys = typing(&[Key::Char('/')], "zzz");
        let text = screen("survey.sav", 100, 14, &keys);
        insta::assert_snapshot!(text);
        assert!(text.contains("no row matches the filter"), "{text}");
        assert!(text.contains("0 of 20 rows"), "{text}");
    }

    #[test]
    fn a_half_typed_filter_says_what_is_missing() {
        let text = screen("survey.sav", 100, 14, &[Key::Char('/'), Key::Char('>')]);
        assert!(text.contains("Filter id: >▏"), "{text}");
        assert!(text.contains("type a number after >"), "{text}");
    }

    #[test]
    fn the_variable_search_bar() {
        let keys = typing(&[Key::Char('i'), Key::Char('/')], "brand");
        let text = screen("survey.sav", 100, 20, &keys);
        insta::assert_snapshot!(text);
        assert!(text.contains("Search variables: brand▏"), "{text}");
        assert!(text.contains("3 of 13 variables"), "{text}");
        // The numbers are the variables' places in the file.
        assert!(text.contains(" 7  brand_a"), "{text}");
        assert!(!text.contains("gender"), "{text}");

        let none = screen(
            "survey.sav",
            100,
            20,
            &typing(&[Key::Char('i'), Key::Char('/')], "zzzz"),
        );
        assert!(
            none.contains("no variable has that in its name or label"),
            "{none}"
        );
    }

    #[test]
    fn a_long_label_gets_the_room_the_border_has() {
        let mut state = loaded("survey.sav", 160, 10);
        state.columns[0].label =
            "Q12. Thinking about the last time you visited, how satisfied were you overall?"
                .to_string();
        // More than the old limit of 40 characters now shows.
        let wide = render(&state, 160, 10);
        assert!(wide.contains("how satisfied were you overall?"), "{wide}");
        // On a narrow screen it is cut, and the position still shows.
        let narrow = render(&state, 70, 10);
        assert!(narrow.contains(" Row 1/20 "), "{narrow}");
        assert!(!narrow.contains("how satisfied were you overall?"));
    }
}
