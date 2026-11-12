//! Table drawing for the CLI: grid tables, status colours, and the text
//! shortening that keeps them a readable width.
//!
//! Table width never depends on the terminal, so the output is the same in
//! a terminal, in a pipe and in the snapshot tests. Colour is written as
//! ANSI codes; the caller prints through `anstream`, which removes them
//! when the output is not a terminal.

use anstyle::{AnsiColor, Style};
use comfy_table::modifiers::UTF8_ROUND_CORNERS;
use comfy_table::presets::UTF8_FULL_CONDENSED;
use comfy_table::{Table, TableComponent};

/// Labels longer than this are cut in the middle.
const MAX_LABEL_CHARS: usize = 48;
/// What is kept of a cut label: the beginning and the end.
const LABEL_HEAD_CHARS: usize = 30;
const LABEL_TAIL_CHARS: usize = 13;
/// String data cells longer than this are cut at the end.
const MAX_CELL_CHARS: usize = 40;
/// Summary values (warnings, error messages) wrap at this width.
const SUMMARY_WRAP_CHARS: usize = 72;

/// The outcome of reading a file, shown as the summary table's border
/// colour and, because colour is lost in a pipe, as its `Status` row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Ok,
    Warnings(usize),
    Error,
}

impl Status {
    pub fn text(self) -> String {
        match self {
            Status::Ok => "ok".to_string(),
            Status::Warnings(1) => "1 warning".to_string(),
            Status::Warnings(n) => format!("{n} warnings"),
            Status::Error => "error".to_string(),
        }
    }

    fn colour(self) -> AnsiColor {
        match self {
            Status::Ok => AnsiColor::Green,
            Status::Warnings(_) => AnsiColor::Yellow,
            Status::Error => AnsiColor::Red,
        }
    }
}

/// The two-column file summary: rounded corners, border coloured by `status`.
pub fn summary_table(rows: &[(&str, String)], status: Status) -> String {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL_CONDENSED)
        .apply_modifier(UTF8_ROUND_CORNERS)
        // A solid divider between the two columns (the preset draws it dotted).
        .set_style(TableComponent::VerticalLines, '│');
    for (key, value) in rows {
        table.add_row([key.to_string(), wrap(value, SUMMARY_WRAP_CHARS)]);
    }
    paint_borders(&table.to_string(), status.colour())
}

/// A grid table with a header row, as used for variables and data rows.
pub fn grid_table(header: &[String], rows: Vec<Vec<String>>) -> String {
    let mut table = Table::new();
    table.load_preset(UTF8_FULL_CONDENSED).set_header(header);
    for row in rows {
        table.add_row(row);
    }
    table.to_string()
}

/// Wrap every run of box-drawing characters in `colour`, leaving cell text
/// in the terminal's default colour.
fn paint_borders(rendered: &str, colour: AnsiColor) -> String {
    let style = Style::new().fg_color(Some(colour.into()));
    let mut painted = String::with_capacity(rendered.len() * 2);
    let mut in_border = false;
    for ch in rendered.chars() {
        let is_border = ('\u{2500}'..='\u{257F}').contains(&ch);
        if is_border != in_border {
            let code = if is_border {
                style.render().to_string()
            } else {
                style.render_reset().to_string()
            };
            painted.push_str(&code);
            in_border = is_border;
        }
        painted.push(ch);
    }
    if in_border {
        painted.push_str(&style.render_reset().to_string());
    }
    painted
}

/// A variable label for the table: line breaks become spaces, and a long
/// label keeps its beginning and its end with ` ... ` between them.
pub fn shorten_middle(label: &str) -> String {
    let label = one_line(label);
    let chars: Vec<char> = label.chars().collect();
    if chars.len() <= MAX_LABEL_CHARS {
        return label;
    }
    let head: String = chars[..LABEL_HEAD_CHARS].iter().collect();
    let tail: String = chars[chars.len() - LABEL_TAIL_CHARS..].iter().collect();
    format!("{} ... {}", head.trim_end(), tail.trim_start())
}

/// A string data cell for the table: one line, cut at the end if long.
pub fn shorten_end(cell: &str) -> String {
    let cell = one_line(cell);
    match cell.char_indices().nth(MAX_CELL_CHARS) {
        Some((cut, _)) => format!("{}...", &cell[..cut]),
        None => cell,
    }
}

/// Replace line breaks and other control characters with spaces, so a
/// value cannot break a table row.
fn one_line(text: &str) -> String {
    text.chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect()
}

/// Break `text` into lines of at most `width` characters at spaces. A
/// single word longer than `width` (a path, say) stays whole.
pub(crate) fn wrap(text: &str, width: usize) -> String {
    let mut lines: Vec<String> = Vec::new();
    for paragraph in text.lines() {
        let mut line = String::new();
        for word in paragraph.split_whitespace() {
            let fits = line.chars().count() + 1 + word.chars().count() <= width;
            if !line.is_empty() && !fits {
                lines.push(std::mem::take(&mut line));
            }
            if !line.is_empty() {
                line.push(' ');
            }
            line.push_str(word);
        }
        lines.push(line);
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_labels_are_untouched() {
        assert_eq!(shorten_middle("Gender"), "Gender");
        let exactly_max = "x".repeat(MAX_LABEL_CHARS);
        assert_eq!(shorten_middle(&exactly_max), exactly_max);
    }

    #[test]
    fn long_labels_keep_both_ends() {
        let label =
            "Q12. Thinking about the last time you visited, how satisfied were you overall?";
        let short = shorten_middle(label);
        assert_eq!(short, "Q12. Thinking about the last t ... you overall?");
        assert!(short.chars().count() <= MAX_LABEL_CHARS);

        let one_over = format!("{}{}", "a".repeat(30), "b".repeat(19));
        assert_eq!(
            shorten_middle(&one_over),
            format!("{} ... {}", "a".repeat(30), "b".repeat(13))
        );
    }

    #[test]
    fn long_labels_are_cut_on_character_boundaries() {
        let label = "満足度".repeat(20);
        let short = shorten_middle(&label);
        assert_eq!(short.chars().count(), 30 + 5 + 13);
        assert!(short.starts_with("満足度"));
        assert!(short.ends_with("満足度"));
    }

    #[test]
    fn line_breaks_in_text_become_spaces() {
        assert_eq!(shorten_middle("first\nsecond"), "first second");
        assert_eq!(shorten_end("a\tb"), "a b");
    }

    #[test]
    fn long_cells_are_cut_at_the_end() {
        assert_eq!(shorten_end("short"), "short");
        assert_eq!(
            shorten_end(&"x".repeat(41)),
            format!("{}...", "x".repeat(40))
        );
    }

    #[test]
    fn wrapping_breaks_at_spaces_only() {
        assert_eq!(wrap("one two three", 7), "one two\nthree");
        assert_eq!(wrap("a/very/long/path", 5), "a/very/long/path");
        assert_eq!(wrap("", 5), "");
    }

    #[test]
    fn only_borders_are_painted() {
        let painted = paint_borders("│ ok │", AnsiColor::Green);
        assert_eq!(painted, "\u{1b}[32m│\u{1b}[0m ok \u{1b}[32m│\u{1b}[0m");
    }

    #[test]
    fn status_text() {
        assert_eq!(Status::Ok.text(), "ok");
        assert_eq!(Status::Warnings(1).text(), "1 warning");
        assert_eq!(Status::Warnings(3).text(), "3 warnings");
        assert_eq!(Status::Error.text(), "error");
    }
}
