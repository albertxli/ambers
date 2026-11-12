//! The `ambers` command-line interface.
//!
//! All of the CLI lives in this library so that the standalone binary
//! (`src/main.rs`) and the Python wheel's `ambers` entry point run exactly
//! the same code. This crate never depends on Python.
//!
//! Conventions: data goes to stdout, errors go to stderr. The exit code is
//! 0 on success, 1 when a command fails, and 2 for a usage error. Colour is
//! used only when the stream is a terminal (and `NO_COLOR` is unset);
//! everything colour says is also in the text.

mod info;
mod render;
mod view;

use std::ffi::OsString;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use clap::{CommandFactory, FromArgMatches, Parser, Subcommand};

#[derive(Parser)]
#[command(name = "ambers", version, about = "Inspect SPSS .sav and .zsav files")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Print a file's summary and its variables
    ///
    /// The summary's border is green when the file read cleanly, yellow when
    /// the reader recorded warnings, and red when the file could not be read.
    /// By default the variable list shows the first 10 and the last 5, with
    /// each variable's type as the codebook defines it (categorical, numeric,
    /// text, date).
    Info {
        /// Path to a .sav or .zsav file
        file: PathBuf,
        /// List only the first N variables
        #[arg(long, value_name = "N")]
        head: Option<usize>,
        /// List only the last N variables (with --head: the first and the last)
        #[arg(long, value_name = "N")]
        tail: Option<usize>,
        /// Read the data to add N (valid cases) and Missing per variable
        ///
        /// Without it the type comes from the metadata alone, which is exact
        /// except that an unlabelled variable holding only 0 and 1 shows as
        /// numeric; with it, such a variable is categorical, as in the codebook.
        #[arg(long)]
        counts: bool,
        /// Also print the first N data rows of the first few columns
        #[arg(long, value_name = "N")]
        rows: Option<usize>,
    },
    /// Browse a file's data in a full-screen grid
    ///
    /// Like SPSS's Data View: move with the arrow keys, press Tab to switch
    /// between value labels, stored codes and both, Enter for one
    /// respondent's answers, i for the variable list, ? for every key.
    /// Needs an interactive terminal.
    View {
        /// Path to a .sav or .zsav file
        file: PathBuf,
        /// What cells show at the start; Tab cycles through all three
        #[arg(long, value_enum, default_value = "labels")]
        mode: view::Mode,
        /// Load at most N rows (the rest of a larger file is not read)
        #[arg(long, value_name = "N")]
        limit: Option<usize>,
        /// Load at most this many GB of data into memory
        ///
        /// By default the viewer stops loading at half of the memory the
        /// machine has free when it starts, and says so. The rows already
        /// loaded stay usable.
        #[arg(long, value_name = "GB")]
        memory: Option<f64>,
        /// Colours
        #[arg(long, value_enum, default_value = "dracula")]
        theme: view::ThemeName,
    },
}

/// Run the CLI with the given arguments (the first is the program name) and
/// return the process exit code.
pub fn run<I, T>(args: I) -> u8
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let args: Vec<OsString> = args.into_iter().map(Into::into).collect();
    let parsed = Cli::command()
        .bin_name(program_name(args.first()))
        .try_get_matches_from(args)
        .and_then(|matches| Cli::from_arg_matches(&matches));
    let cli = match parsed {
        Ok(cli) => cli,
        Err(err) => {
            // Help and version go to stdout with code 0; usage errors to stderr with 2.
            let _ = err.print();
            return err.exit_code() as u8;
        }
    };

    let (file, result) = match cli.command {
        Command::Info {
            file,
            head,
            tail,
            counts,
            rows,
        } => {
            // `anstream` strips the colour codes unless the stream is a terminal.
            let mut out = anstream::stdout().lock();
            let options = info::Options {
                head,
                tail,
                counts,
                rows,
            };
            let result =
                info::run(&mut out, &file, &options).and_then(|()| out.flush().map_err(Into::into));
            (file, result)
        }
        Command::View {
            file,
            mode,
            limit,
            memory,
            theme,
        } => {
            let options = view::Options {
                mode,
                limit,
                memory_gb: memory,
                theme,
            };
            let result = view::run(&file, &options);
            (file, result)
        }
    };

    match result {
        Ok(()) => 0,
        // The reader of our output went away (`ambers info f.sav | head`): not an error.
        Err(err) if is_broken_pipe(&err) => 0,
        // Nothing is wrong with the file, so no file report: just say why.
        Err(err) if err.is::<view::NotATerminal>() => {
            let _ = writeln!(anstream::stderr().lock(), "error: {err}");
            1
        }
        Err(err) => {
            let _ = writeln!(
                anstream::stderr().lock(),
                "{}",
                info::error_summary(&file, &err)
            );
            1
        }
    }
}

/// The name shown in usage text: `am` when started through the short alias,
/// `ambers` otherwise (`ambers.exe`, the wheel's entry point script, a
/// renamed binary), so help reads the same on every platform.
fn program_name(argv0: Option<&OsString>) -> &'static str {
    let stem = argv0
        .map(Path::new)
        .and_then(Path::file_stem)
        .and_then(|stem| stem.to_str());
    match stem {
        Some(stem) if stem.eq_ignore_ascii_case("am") => "am",
        _ => "ambers",
    }
}

fn is_broken_pipe(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<io::Error>()
            .is_some_and(|io_err| io_err.kind() == io::ErrorKind::BrokenPipe)
    })
}
