//! A measurement, not a unit test: how long the screen side of the viewer
//! is busy while a large file loads. Run it by hand, optimised:
//!
//! ```text
//! cargo test --release -p ambers-cli stress -- --ignored --nocapture
//! ```
//!
//! It needs `test_data/test_6_large.sav` (5.4 GB, 916 columns) and is
//! skipped without it.

use std::path::Path;
use std::time::{Duration, Instant};

use ratatui::Terminal;
use ratatui::backend::TestBackend;

use super::draw::draw;
use super::format::Mode;
use super::loader::{self, Message};
use super::state::{Key, State};
use super::theme::{Theme, ThemeName};

#[test]
#[ignore = "measurement on a 5.4 GB private file; run by hand with --release"]
fn the_screen_stays_responsive_while_a_large_file_loads() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../test_data/test_6_large.sav");
    if !path.exists() {
        eprintln!("Skipping: {} not present", path.display());
        return;
    }
    let (scanner, batch_rows) = loader::open(&path, None).unwrap();
    let mut state = State::new(
        "test_6_large.sav".to_string(),
        scanner.metadata(),
        &scanner.schema(),
        batch_rows,
        None,
        Mode::Labels,
    );
    state.resize(200, 60);
    // Variable View with the values panel open: the heaviest screen.
    for key in [
        Key::Char('i'),
        Key::Down,
        Key::Down,
        Key::Down,
        Key::Down,
        Key::Enter,
    ] {
        state.on_key(key);
    }
    let theme = Theme::pick(ThemeName::Dracula, false);
    let mut terminal = Terminal::new(TestBackend::new(200, 60)).unwrap();

    // Redraw continuously, as a held key or a spinning wheel would, while
    // the loader runs: any wait on the loader shows up as a slow frame.
    let started = Instant::now();
    let receiver = loader::spawn(scanner, state.profiler_handle(), None);
    let (mut frames, mut longest, mut over_50ms, mut first_screen) =
        (0u32, Duration::ZERO, 0u32, None);
    let mut loading = true;
    while loading {
        let frame = Instant::now();
        while let Ok(message) = receiver.try_recv() {
            loading = matches!(message, Message::Batch { .. });
            state.receive(message);
        }
        state.on_key(if frames % 2 == 0 { Key::Down } else { Key::Up });
        terminal.draw(|frame| draw(frame, &state, &theme)).unwrap();
        let took = frame.elapsed();
        if state.rows > 0 {
            first_screen.get_or_insert_with(|| started.elapsed());
        }
        frames += 1;
        longest = longest.max(took);
        over_50ms += u32::from(took > Duration::from_millis(50));
        std::thread::sleep(Duration::from_millis(2));
    }
    eprintln!(
        "rows loaded: {}; first rows on screen after {:.0?}; loaded in {:.1?}; \
         {frames} frames, {over_50ms} slower than 50 ms, slowest {:.0?}",
        state.rows,
        first_screen.unwrap(),
        started.elapsed(),
        longest,
    );

    // Stepping through variables with the values panel open: each step
    // shows that variable's distinct values.
    let step = Instant::now();
    state.on_key(Key::Top);
    eprintln!(
        "values panel on {} ({} distinct values): {:.0?}",
        state.columns[0].name,
        state.detail.as_ref().map_or(0, |detail| detail.rows.len()),
        step.elapsed()
    );
    let (mut slowest, mut slowest_name, mut total) =
        (Duration::ZERO, String::new(), Duration::ZERO);
    for index in 0..state.columns.len() {
        let step = Instant::now();
        state.on_key(Key::Down);
        terminal.draw(|frame| draw(frame, &state, &theme)).unwrap();
        let took = step.elapsed();
        total += took;
        if took > slowest {
            slowest = took;
            slowest_name = state.columns[index.min(state.columns.len() - 1)]
                .name
                .clone();
        }
    }
    eprintln!(
        "values panel, one step per variable: {} steps in {:.1?}, slowest {:.0?} (near {slowest_name})",
        state.columns.len(),
        total,
        slowest,
    );
}
