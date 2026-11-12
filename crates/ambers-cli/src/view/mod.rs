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
mod filter;
mod format;
mod loader;
mod state;
#[cfg(test)]
mod stress;
mod theme;

use std::io::{self, IsTerminal, Write};
use std::path::Path;
use std::sync::mpsc::{Receiver, TryRecvError};
use std::time::Duration;

use anyhow::Result;
use ratatui::DefaultTerminal;
use ratatui::crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyEventKind,
    KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use ratatui::crossterm::execute;

pub use self::format::Mode;
use self::loader::Message;
use self::state::{Key, Load, Mouse, State};
use self::theme::Theme;
pub use self::theme::ThemeName;

pub struct Options {
    /// The mode the grid starts in.
    pub mode: Mode,
    /// Rows to load at most, if the user set a limit.
    pub limit: Option<usize>,
    /// Memory to load at most, in GB, if the user set a budget.
    pub memory_gb: Option<f64>,
    pub theme: ThemeName,
}

/// The memory the loaded rows may take, in bytes: what the user asked for,
/// or half of what the machine has available right now, so that opening a
/// file larger than memory stops loading instead of exhausting it.
/// `None` (no cap) only when the system does not report its memory.
fn memory_budget(explicit_gb: Option<f64>) -> Option<u64> {
    const GB: f64 = 1024.0 * 1024.0 * 1024.0;
    match explicit_gb {
        Some(gb) => Some((gb.max(0.0) * GB) as u64),
        None => {
            let mut system = sysinfo::System::new().ok()?;
            system.refresh_memory();
            Some(system.available_memory() / 2).filter(|&bytes| bytes > 0)
        }
    }
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
    let (scanner, batch_rows) = loader::open(file, options.limit)?;

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
        batch_rows,
        options.limit,
        options.mode,
    );
    let budget = memory_budget(options.memory_gb);
    let receiver = loader::spawn(scanner, state.profiler_handle(), budget);
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
        // Take every event that is already waiting before drawing once.
        // A held key or a spinning wheel sends events faster than frames
        // can be drawn; one frame per event would build a backlog that
        // keeps the viewer busy long after the key is released.
        for _ in 0..MAX_EVENTS_PER_FRAME {
            match handle(state, event::read()?) {
                Handled::Quit => return Ok(()),
                Handled::Changed => redraw = true,
                Handled::Ignored => {}
            }
            if !event::poll(Duration::ZERO)? {
                break;
            }
        }
    }
}

/// Events applied before the screen is drawn again, at most.
const MAX_EVENTS_PER_FRAME: usize = 500;

enum Handled {
    Quit,
    /// The state changed: draw.
    Changed,
    Ignored,
}

fn handle(state: &mut State, event: Event) -> Handled {
    match event {
        // Windows also reports key releases; acting on them would move twice.
        Event::Key(key) if key.kind != KeyEventKind::Release => {
            if is_interrupt(&key) {
                return Handled::Quit;
            }
            match translate(&key) {
                Some(key) if state.on_key(key) => Handled::Quit,
                Some(_) => Handled::Changed,
                None => Handled::Ignored,
            }
        }
        Event::Mouse(event) => match translate_mouse(&event) {
            Some(mouse) => {
                state.on_mouse(mouse, event.column as usize, event.row as usize);
                Handled::Changed
            }
            None => Handled::Ignored,
        },
        Event::Resize(..) => Handled::Changed,
        _ => Handled::Ignored,
    }
}

/// A terminal mouse event as a viewer one. Shift or Alt turns the vertical
/// wheel into a horizontal one, for mice without a tilt wheel; both are
/// accepted because some terminals keep Shift+wheel for themselves.
fn translate_mouse(event: &MouseEvent) -> Option<Mouse> {
    let sideways = event
        .modifiers
        .intersects(KeyModifiers::SHIFT | KeyModifiers::ALT);
    Some(match event.kind {
        MouseEventKind::ScrollUp if sideways => Mouse::ScrollLeft,
        MouseEventKind::ScrollDown if sideways => Mouse::ScrollRight,
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
    let shift = key.modifiers.contains(KeyModifiers::SHIFT);
    Some(match key.code {
        // Ctrl+arrow: to the edge, as in a spreadsheet.
        KeyCode::Up if ctrl => Key::Top,
        KeyCode::Down if ctrl => Key::Bottom,
        KeyCode::Left if ctrl => Key::Home,
        KeyCode::Right if ctrl => Key::End,
        // Shift+arrow: a fast step.
        KeyCode::Up if shift => Key::FastUp,
        KeyCode::Down if shift => Key::FastDown,
        KeyCode::Left if shift => Key::FastLeft,
        KeyCode::Right if shift => Key::FastRight,
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

#[cfg(test)]
mod handle_tests {
    use super::*;
    use std::path::Path;

    fn state() -> State {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/data/survey.sav");
        let (scanner, batch_rows) = loader::open(&path, None).unwrap();
        let mut state = State::new(
            "survey.sav".to_string(),
            scanner.metadata(),
            &scanner.schema(),
            batch_rows,
            None,
            Mode::Labels,
        );
        state.resize(100, 24);
        for message in loader::spawn(scanner, state.profiler_handle(), None) {
            state.receive(message);
        }
        state
    }

    fn press(code: KeyCode) -> Event {
        Event::Key(KeyEvent::new(code, KeyModifiers::NONE))
    }

    #[test]
    fn events_change_the_state_quit_or_are_ignored() {
        let mut state = state();
        assert!(matches!(
            handle(&mut state, press(KeyCode::Down)),
            Handled::Changed
        ));
        assert_eq!(state.cursor_row, 1);
        // A key the viewer does not use, a key release, a mouse move.
        assert!(matches!(
            handle(&mut state, press(KeyCode::F(5))),
            Handled::Ignored
        ));
        let mut release = KeyEvent::new(KeyCode::Down, KeyModifiers::NONE);
        release.kind = KeyEventKind::Release;
        assert!(matches!(
            handle(&mut state, Event::Key(release)),
            Handled::Ignored
        ));
        let moved = MouseEvent {
            kind: MouseEventKind::Moved,
            column: 5,
            row: 5,
            modifiers: KeyModifiers::NONE,
        };
        assert!(matches!(
            handle(&mut state, Event::Mouse(moved)),
            Handled::Ignored
        ));
        assert_eq!(state.cursor_row, 1);
        assert!(matches!(
            handle(&mut state, Event::Resize(80, 20)),
            Handled::Changed
        ));
        assert!(matches!(
            handle(&mut state, press(KeyCode::Char('q'))),
            Handled::Quit
        ));
    }

    #[test]
    fn the_budget_is_what_was_asked_or_half_of_what_is_free() {
        assert_eq!(memory_budget(Some(2.0)), Some(2 * 1024 * 1024 * 1024));
        assert_eq!(memory_budget(Some(0.5)), Some(512 * 1024 * 1024));
        // Detected: this machine reports its memory, and the budget is a
        // positive share of it.
        let detected = memory_budget(None).expect("the system reports its memory");
        assert!(detected > 64 * 1024 * 1024, "{detected}");
    }

    #[test]
    fn loader_open_sizes_batches_from_the_file() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/data/survey.sav");
        let (scanner, batch_rows) = loader::open(&path, Some(7)).unwrap();
        // 13 columns: the cap on rows per batch.
        assert_eq!(batch_rows, 20_000);
        assert_eq!(scanner.schema().fields().len(), 13);
        assert!(loader::open(Path::new("no/such/file.sav"), None).is_err());
    }
}

#[cfg(test)]
mod navigation_tests {
    use super::*;

    fn key(code: KeyCode, modifiers: KeyModifiers) -> Option<Key> {
        translate(&KeyEvent::new(code, modifiers))
    }

    #[test]
    fn shift_arrows_are_fast_steps_and_control_arrows_reach_the_edges() {
        let (none, shift, ctrl) = (
            KeyModifiers::NONE,
            KeyModifiers::SHIFT,
            KeyModifiers::CONTROL,
        );
        assert_eq!(key(KeyCode::Down, none), Some(Key::Down));
        assert_eq!(key(KeyCode::Up, shift), Some(Key::FastUp));
        assert_eq!(key(KeyCode::Down, shift), Some(Key::FastDown));
        assert_eq!(key(KeyCode::Left, shift), Some(Key::FastLeft));
        assert_eq!(key(KeyCode::Right, shift), Some(Key::FastRight));
        assert_eq!(key(KeyCode::Up, ctrl), Some(Key::Top));
        assert_eq!(key(KeyCode::Down, ctrl), Some(Key::Bottom));
        assert_eq!(key(KeyCode::Left, ctrl), Some(Key::Home));
        assert_eq!(key(KeyCode::Right, ctrl), Some(Key::End));
        // The letter forms arrive as characters, with or without Shift.
        for symbol in ['{', '}', '<', '>', '0', '$', 'C'] {
            assert_eq!(key(KeyCode::Char(symbol), shift), Some(Key::Char(symbol)));
        }
    }

    #[test]
    fn shift_or_alt_turns_the_wheel_sideways() {
        let wheel = |kind, modifiers| {
            translate_mouse(&MouseEvent {
                kind,
                column: 0,
                row: 0,
                modifiers,
            })
        };
        for modifiers in [KeyModifiers::SHIFT, KeyModifiers::ALT] {
            assert_eq!(
                wheel(MouseEventKind::ScrollUp, modifiers),
                Some(Mouse::ScrollLeft)
            );
            assert_eq!(
                wheel(MouseEventKind::ScrollDown, modifiers),
                Some(Mouse::ScrollRight)
            );
        }
        assert_eq!(
            wheel(MouseEventKind::ScrollDown, KeyModifiers::NONE),
            Some(Mouse::ScrollDown)
        );
    }
}
