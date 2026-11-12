//! Colours of the viewer, as a handful of roles so the drawing code never
//! names a colour.

use ratatui::style::{Color, Modifier, Style};

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum ThemeName {
    /// The Dracula palette, the same in every terminal
    Dracula,
    /// Your terminal's own colours
    Terminal,
}

pub struct Theme {
    /// Background and text of the whole screen.
    pub base: Style,
    pub frame: Style,
    pub title: Style,
    pub header: Style,
    /// Row numbers.
    pub gutter: Style,
    /// Laid over every other row.
    pub stripe: Style,
    pub cursor_row: Style,
    pub cursor_cell: Style,
    /// Secondary text, and user-missing values.
    pub dim: Style,
    /// Values without a label, and warnings.
    pub warn: Style,
    /// The mode tag and key hints.
    pub accent: Style,
}

impl Theme {
    /// The theme to draw with. `NO_COLOR` wins over the choice: then only
    /// bold, dim, underline and reverse video are used.
    pub fn pick(name: ThemeName, no_color: bool) -> Theme {
        match (no_color, name) {
            (true, _) => Theme::plain(),
            (false, ThemeName::Dracula) => Theme::dracula(),
            (false, ThemeName::Terminal) => Theme::terminal(),
        }
    }

    /// The open Dracula palette (draculatheme.com, MIT).
    fn dracula() -> Theme {
        const BACKGROUND: Color = Color::Rgb(0x28, 0x2a, 0x36);
        const CURRENT_LINE: Color = Color::Rgb(0x44, 0x47, 0x5a);
        const FOREGROUND: Color = Color::Rgb(0xf8, 0xf8, 0xf2);
        const COMMENT: Color = Color::Rgb(0x62, 0x72, 0xa4);
        const CYAN: Color = Color::Rgb(0x8b, 0xe9, 0xfd);
        const GREEN: Color = Color::Rgb(0x50, 0xfa, 0x7b);
        const ORANGE: Color = Color::Rgb(0xff, 0xb8, 0x6c);
        const PINK: Color = Color::Rgb(0xff, 0x79, 0xc6);
        const PURPLE: Color = Color::Rgb(0xbd, 0x93, 0xf9);
        // Between the background and the current-line colour.
        const STRIPE: Color = Color::Rgb(0x30, 0x32, 0x41);

        Theme {
            base: Style::new().fg(FOREGROUND).bg(BACKGROUND),
            frame: Style::new().fg(COMMENT),
            title: Style::new().fg(PURPLE).add_modifier(Modifier::BOLD),
            header: Style::new().fg(CYAN).add_modifier(Modifier::BOLD),
            gutter: Style::new().fg(COMMENT),
            stripe: Style::new().bg(STRIPE),
            cursor_row: Style::new().bg(CURRENT_LINE),
            cursor_cell: Style::new()
                .fg(BACKGROUND)
                .bg(PINK)
                .add_modifier(Modifier::BOLD),
            dim: Style::new().fg(COMMENT),
            warn: Style::new().fg(ORANGE),
            accent: Style::new().fg(GREEN).add_modifier(Modifier::BOLD),
        }
    }

    /// The terminal's own palette: its background and text colour, and its
    /// 16 named colours for the rest.
    fn terminal() -> Theme {
        Theme {
            base: Style::new(),
            frame: Style::new().fg(Color::DarkGray),
            title: Style::new().fg(Color::Magenta).add_modifier(Modifier::BOLD),
            header: Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD),
            gutter: Style::new().fg(Color::DarkGray),
            stripe: Style::new(),
            cursor_row: Style::new().add_modifier(Modifier::BOLD),
            cursor_cell: Style::new().add_modifier(Modifier::REVERSED | Modifier::BOLD),
            dim: Style::new().fg(Color::DarkGray),
            warn: Style::new().fg(Color::Yellow),
            accent: Style::new().fg(Color::Green).add_modifier(Modifier::BOLD),
        }
    }

    /// No colour at all.
    fn plain() -> Theme {
        Theme {
            base: Style::new(),
            frame: Style::new(),
            title: Style::new().add_modifier(Modifier::BOLD),
            header: Style::new().add_modifier(Modifier::BOLD),
            gutter: Style::new().add_modifier(Modifier::DIM),
            stripe: Style::new(),
            cursor_row: Style::new().add_modifier(Modifier::BOLD),
            cursor_cell: Style::new().add_modifier(Modifier::REVERSED | Modifier::BOLD),
            dim: Style::new().add_modifier(Modifier::DIM),
            warn: Style::new().add_modifier(Modifier::UNDERLINED),
            accent: Style::new().add_modifier(Modifier::BOLD),
        }
    }
}
