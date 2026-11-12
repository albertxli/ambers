//! `ambers view`: a full-screen viewer for the data of a .sav/.zsav file,
//! modelled on SPSS's Data View and Variable View.
//!
//! - `loader` reads the file on a background thread.
//! - `state` holds what is shown and what each key does; no terminal access.
//! - `format` decides a cell's text in each mode (codes, labels, both).
//! - `draw` paints the state; `theme` holds the colours.
//! - This module owns the terminal: setup, the event loop, teardown.

mod clipboard;
mod detail;
mod draw;
mod format;
mod loader;
mod state;
mod theme;

use std::fs::File;
use std::io::{self, BufReader, IsTerminal, Write};
use std::path::Path;
use std::sync::mpsc::{Receiver, TryRecvError};
use std::time::Duration;

use ambers::scanner::SavScanner;
use anyhow::Result;
use ratatui::DefaultTerminal;
use ratatui::crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyEventKind,
    KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use ratatui::crossterm::execute;

pub use self::format::Mode;
use self::loader::{BATCH_ROWS, Message};
use self::state::{Key, Load, Mouse, State};
use self::theme::Theme;
pub use self::theme::ThemeName;

pub struct Options {
    /// The mode the grid starts in.
    pub mode: Mode,
    /// Rows to load at most.
    pub limit: usize,
    pub theme: ThemeName,
}

/// The viewer was started with its input or output redirected.
#[derive(Debug)]
pub struct NotATerminal;

impl std::fmt::Display for NotATerminal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("view needs an interactive terminal")
    }
}

impl std::error::Error for NotATerminal {}

pub fn run(file: &Path, options: &Options) -> Result<()> {
    // Open the file first: an unreadable file is reported the same way
    // whether or not there is a terminal, and before the screen is taken.
    let reader = BufReader::with_capacity(8 * 1024 * 1024, File::open(file)?);
    let mut scanner = SavScanner::open(reader, BATCH_ROWS)?;
    scanner.limit(options.limit);

    if !(io::stdin().is_terminal() && io::stdout().is_terminal()) {
        return Err(NotATerminal.into());
    }

    let name = file.file_name().map_or_else(
        || file.display().to_string(),
        |name| name.to_string_lossy().into_owned(),
    );
    let mut state = State::new(
        name,
        scanner.metadata(),
        &scanner.schema(),
        BATCH_ROWS,
        options.limit,
        options.mode,
    );
    let receiver = loader::spawn(scanner, state.profiler_handle());
    let no_color = std::env::var_os("NO_COLOR").is_some_and(|value| !value.is_empty());
    let theme = Theme::pick(options.theme, no_color);

    // A panic must not leave the terminal reporting mouse movements as
    // text. `ratatui::init` chains to this hook after restoring the screen.
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = execute!(io::stdout(), DisableMouseCapture);
        previous(info);
    }));
    // Raw mode and the alternate screen, with a panic hook that undoes both.
    let mut terminal = ratatui::init();
    // The wheel scrolls and a click selects a cell. While this is on, the
    // terminal's own text selection needs Shift held down.
    let result = execute!(io::stdout(), EnableMouseCapture)
        .map_err(Into::into)
        .and_then(|()| event_loop(&mut terminal, &mut state, &receiver, &theme));
    let _ = execute!(io::stdout(), DisableMouseCapture);
    ratatui::restore();
    result
}

fn event_loop(
    terminal: &mut DefaultTerminal,
    state: &mut State,
    receiver: &Receiver<Message>,
    theme: &Theme,
) -> Result<()> {
    let mut loading = true;
    let mut redraw = true;
    loop {
        // Take what the loader has ready, a few batches at a time so keys
        // stay responsive while a large file streams in.
        for _ in 0..8 {
            if !loading {
                break;
            }
            match receiver.try_recv() {
                Ok(message) => {
                    loading = matches!(message, Message::Batch { .. });
                    state.receive(message);
                    redraw = true;
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    loading = false;
                    state.load = Load::Failed("the loader stopped unexpectedly".to_string());
                    redraw = true;
                }
            }
        }

        if redraw {
            let size = terminal.size()?;
            state.resize(size.width, size.height);
            terminal.draw(|frame| draw::draw(frame, state, theme))?;
            redraw = false;
            if let Some(copy) = state.take_copy() {
                // The system's clipboard command, or failing that the
                // terminal's escape sequence: written after the frame, so
                // the two do not interleave on the way out.
                if !clipboard::copy(&copy.text) {
                    let mut out = io::stdout().lock();
                    out.write_all(clipboard::escape_sequence(&copy.text).as_bytes())?;
                    out.flush()?;
                }
            }
        }

        // While loading, wake up often to pick up new rows; afterwards only
        // a key or a resize needs attention.
        let wait = Duration::from_millis(if loading { 30 } else { 1_000 });
        if !event::poll(wait)? {
            continue;
        }
        match event::read()? {
            // Windows also reports key releases; acting on them would move twice.
            Event::Key(key) if key.kind != KeyEventKind::Release => {
                if is_interrupt(&key) {
                    return Ok(());
                }
                if let Some(key) = translate(&key) {
                    if state.on_key(key) {
                        return Ok(());
                    }
                    redraw = true;
                }
            }
            Event::Mouse(event) => {
                if let Some(mouse) = translate_mouse(&event) {
                    state.on_mouse(mouse, event.column as usize, event.row as usize);
                    redraw = true;
                }
            }
            Event::Resize(..) => redraw = true,
            _ => {}
        }
    }
}

/// A terminal mouse event as a viewer one. Shift turns the vertical wheel
/// into a horizontal one, for mice without a tilt wheel.
fn translate_mouse(event: &MouseEvent) -> Option<Mouse> {
    let shift = event.modifiers.contains(KeyModifiers::SHIFT);
    Some(match event.kind {
        MouseEventKind::ScrollUp if shift => Mouse::ScrollLeft,
        MouseEventKind::ScrollDown if shift => Mouse::ScrollRight,
        MouseEventKind::ScrollUp => Mouse::ScrollUp,
        MouseEventKind::ScrollDown => Mouse::ScrollDown,
        MouseEventKind::ScrollLeft => Mouse::ScrollLeft,
        MouseEventKind::ScrollRight => Mouse::ScrollRight,
        MouseEventKind::Down(MouseButton::Left) => Mouse::Click,
        _ => return None,
    })
}

/// Ctrl+C. In raw mode it arrives as a key, not as a signal.
fn is_interrupt(key: &KeyEvent) -> bool {
    key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c')
}

/// A terminal key event as a viewer key. Shift is not consulted for
/// characters: `G` and `?` already arrive as those characters, and some
/// terminals report Shift with them and some do not.
fn translate(key: &KeyEvent) -> Option<Key> {
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    let alt = key.modifiers.contains(KeyModifiers::ALT);
    Some(match key.code {
        KeyCode::Up => Key::Up,
        KeyCode::Down => Key::Down,
        KeyCode::Left => Key::Left,
        KeyCode::Right => Key::Right,
        KeyCode::PageUp => Key::PageUp,
        KeyCode::PageDown => Key::PageDown,
        KeyCode::Home if ctrl => Key::Top,
        KeyCode::End if ctrl => Key::Bottom,
        KeyCode::Home => Key::Home,
        KeyCode::End => Key::End,
        KeyCode::Tab => Key::Tab,
        KeyCode::Enter => Key::Enter,
        KeyCode::Esc => Key::Esc,
        KeyCode::Backspace => Key::Backspace,
        KeyCode::Char('u') if ctrl => Key::HalfUp,
        KeyCode::Char('d') if ctrl => Key::HalfDown,
        KeyCode::Char('b') if ctrl => Key::PageUp,
        KeyCode::Char('f') if ctrl => Key::PageDown,
        KeyCode::Char(ch) if !ctrl && !alt => Key::Char(ch),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(code: KeyCode, modifiers: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, modifiers)
    }

    #[test]
    fn characters_do_not_depend_on_shift() {
        for modifiers in [KeyModifiers::NONE, KeyModifiers::SHIFT] {
            assert_eq!(
                translate(&key(KeyCode::Char('G'), modifiers)),
                Some(Key::Char('G'))
            );
            assert_eq!(
                translate(&key(KeyCode::Char('?'), modifiers)),
                Some(Key::Char('?'))
            );
        }
    }

    #[test]
    fn control_changes_the_meaning() {
        let ctrl = KeyModifiers::CONTROL;
        assert_eq!(
            translate(&key(KeyCode::Char('d'), ctrl)),
            Some(Key::HalfDown)
        );
        assert_eq!(translate(&key(KeyCode::Char('u'), ctrl)), Some(Key::HalfUp));
        assert_eq!(translate(&key(KeyCode::Home, ctrl)), Some(Key::Top));
        assert_eq!(
            translate(&key(KeyCode::Home, KeyModifiers::NONE)),
            Some(Key::Home)
        );
        // An unbound Ctrl combination is not typed as a character.
        assert_eq!(translate(&key(KeyCode::Char('x'), ctrl)), None);
        assert!(is_interrupt(&key(KeyCode::Char('c'), ctrl)));
        assert!(!is_interrupt(&key(KeyCode::Char('c'), KeyModifiers::NONE)));
    }
}

#[cfg(test)]
mod mouse_tests {
    use super::*;

    fn event(kind: MouseEventKind, modifiers: KeyModifiers) -> MouseEvent {
        MouseEvent {
            kind,
            column: 0,
            row: 0,
            modifiers,
        }
    }

    #[test]
    fn the_wheel_scrolls_and_shift_turns_it_sideways() {
        let none = KeyModifiers::NONE;
        let shift = KeyModifiers::SHIFT;
        assert_eq!(
            translate_mouse(&event(MouseEventKind::ScrollDown, none)),
            Some(Mouse::ScrollDown)
        );
        assert_eq!(
            translate_mouse(&event(MouseEventKind::ScrollUp, shift)),
            Some(Mouse::ScrollLeft)
        );
        assert_eq!(
            translate_mouse(&event(MouseEventKind::ScrollDown, shift)),
            Some(Mouse::ScrollRight)
        );
        assert_eq!(
            translate_mouse(&event(MouseEventKind::ScrollRight, none)),
            Some(Mouse::ScrollRight)
        );
    }

    #[test]
    fn only_a_left_press_is_a_click() {
        let none = KeyModifiers::NONE;
        let left = MouseEventKind::Down(MouseButton::Left);
        assert_eq!(translate_mouse(&event(left, none)), Some(Mouse::Click));
        for kind in [
            MouseEventKind::Up(MouseButton::Left),
            MouseEventKind::Down(MouseButton::Right),
            MouseEventKind::Moved,
            MouseEventKind::Drag(MouseButton::Left),
        ] {
            assert_eq!(translate_mouse(&event(kind, none)), None);
        }
    }
}
