//! The `am` binary: a short alias of `ambers`, running the same
//! [`ambers_cli::run`].

use std::process::ExitCode;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> ExitCode {
    ExitCode::from(ambers_cli::run(std::env::args_os()))
}
