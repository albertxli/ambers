//! PyO3 Python bindings for ambers: the `ambers._ambers` extension module.

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod conversions;
mod diff;
mod io;
mod metadata;

use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;

pub use self::diff::PyMetaDiff;
pub use self::io::{PyArrowData, PySavBatchReader};
pub use self::metadata::PySpssMetadata;

use self::io::{_read_sav, _read_sav_meta, _write_sav};

/// Convert SpssError to PyErr.
fn spss_err(e: ambers::error::SpssError) -> PyErr {
    PyIOError::new_err(format!("{e}"))
}

/// Run the `ambers` command line (the same code as the standalone binary)
/// and return its exit code. `argv[0]` is the program name.
#[pyfunction]
fn _run_cli(py: Python<'_>, argv: Vec<String>) -> u8 {
    py.detach(|| ambers_cli::run(argv))
}

#[pymodule]
fn _ambers(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(_run_cli, m)?)?;
    m.add_function(wrap_pyfunction!(_read_sav, m)?)?;
    m.add_function(wrap_pyfunction!(_read_sav_meta, m)?)?;
    m.add_function(wrap_pyfunction!(_write_sav, m)?)?;
    m.add_class::<PySpssMetadata>()?;
    m.add_class::<PyMetaDiff>()?;
    m.add_class::<PyArrowData>()?;
    m.add_class::<PySavBatchReader>()?;
    Ok(())
}
