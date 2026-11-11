//! The `ambers` command-line interface.
//!
//! All of the CLI lives in this library so that the standalone binary
//! (`src/main.rs`) and the Python wheel's `ambers` entry point run exactly
//! the same code. This crate never depends on Python.
//!
//! Conventions: data goes to stdout, errors go to stderr. The exit code is
//! 0 on success, 1 when a command fails, and 2 for a usage error.

mod info;

use std::ffi::OsString;
use std::io::{self, Write};
use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
// `bin_name` is fixed so usage text reads `ambers` however the program was
// started (`ambers.exe`, the wheel's entry point script, a renamed binary).
#[command(
    name = "ambers",
    bin_name = "ambers",
    version,
    about = "Inspect SPSS .sav and .zsav files"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Print a summary of a file: size, compression, encoding, rows, columns, variables
    Info {
        /// Path to a .sav or .zsav file
        file: PathBuf,
        /// Also print the first N rows of the first few columns
        #[arg(long, value_name = "N")]
        head: Option<usize>,
    },
}

/// Run the CLI with the given arguments (the first is the program name) and
/// return the process exit code.
pub fn run<I, T>(args: I) -> u8
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let cli = match Cli::try_parse_from(args) {
        Ok(cli) => cli,
        Err(err) => {
            // Help and version go to stdout with code 0; usage errors to stderr with 2.
            let _ = err.print();
            return err.exit_code() as u8;
        }
    };

    let mut out = io::stdout().lock();
    let result = match cli.command {
        Command::Info { file, head } => info::run(&mut out, &file, head),
    };
    let result = result.and_then(|()| out.flush().map_err(Into::into));

    match result {
        Ok(()) => 0,
        // The reader of our output went away (`ambers info f.sav | head`): not an error.
        Err(err) if is_broken_pipe(&err) => 0,
        Err(err) => {
            eprintln!("error: {err:#}");
            1
        }
    }
}

fn is_broken_pipe(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<io::Error>()
            .is_some_and(|io_err| io_err.kind() == io::ErrorKind::BrokenPipe)
    })
}
