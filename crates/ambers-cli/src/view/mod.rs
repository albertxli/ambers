//! `ambers view`: a full-screen viewer for the data of a .sav/.zsav file,
//! modelled on SPSS's Data View and Variable View.
//!
//! - `loader` reads the file on a background thread.
//! - `state` holds what is shown and what each key does; no terminal access.
//! - `format` decides a cell's text in each mode (codes, labels, both).
//! - `draw` paints the state; `theme` holds the colours.
//! - This module owns the terminal: setup, the event loop, teardown.

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
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

pub use self::format::Mode;
use self::loader::{BATCH_ROWS, Message};
use self::state::{Key, Load, State};
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

    // Raw mode and the alternate screen, with a panic hook that undoes both.
    let mut terminal = ratatui::init();
    let result = event_loop(&mut terminal, &mut state, &receiver, &theme);
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
            // After the frame, so the two do not interleave on the way out.
            if let Some(copy) = state.take_copy() {
                let mut out = io::stdout().lock();
                out.write_all(clipboard_sequence(&copy.text).as_bytes())?;
                out.flush()?;
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
            Event::Resize(..) => redraw = true,
            _ => {}
        }
    }
}

/// The escape sequence that asks the terminal to put `text` on the system
/// clipboard (OSC 52). It needs no clipboard library and works over SSH;
/// Windows Terminal and most current terminals honour it, the legacy
/// Windows console ignores it.
fn clipboard_sequence(text: &str) -> String {
    format!("\x1b]52;c;{}\x07", base64(text.as_bytes()))
}

/// Standard base64 with padding.
fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let group = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        for position in 0..4 {
            if position <= chunk.len() {
                let index = (group >> (18 - 6 * position)) & 0x3f;
                encoded.push(ALPHABET[index as usize] as char);
            } else {
                encoded.push('=');
            }
        }
    }
    encoded
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
mod clipboard_tests {
    use super::*;

    #[test]
    fn base64_matches_the_standard_vectors() {
        // RFC 4648, section 10.
        for (plain, encoded) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("fooba", "Zm9vYmE="),
            ("foobar", "Zm9vYmFy"),
        ] {
            assert_eq!(base64(plain.as_bytes()), encoded, "{plain:?}");
        }
    }

    #[test]
    fn the_clipboard_sequence_carries_utf8() {
        assert_eq!(clipboard_sequence("Gender"), "\x1b]52;c;R2VuZGVy\x07");
        // "é" is two bytes in UTF-8.
        assert_eq!(clipboard_sequence("é"), "\x1b]52;c;w6k=\x07");
    }
}
