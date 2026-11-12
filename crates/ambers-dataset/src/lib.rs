//! A loaded SPSS file ready to look at.
//!
//! This crate is the one place that decides how a value shows (print
//! format, value label, missing), which rows a filter keeps, and what a
//! variable's value table holds. The terminal viewer and the browser
//! viewer both sit on it, so they cannot disagree. It has no user
//! interface code: no terminal, no HTTP.
//!
//! - [`loader`] reads a file on a background thread, in batches sized by
//!   cells, up to a memory budget.
//! - [`Dataset`] holds what has arrived: columns, rows, the codebook's
//!   types and counts, label checks, and row filters.
//! - [`format`] turns a stored value into the text of a cell.
//! - [`filter`] parses what a user types and matches it on Arrow columns.
//! - [`detail`] builds a variable's value table.

pub mod dataset;
pub mod detail;
pub mod filter;
pub mod format;
pub mod loader;

pub use dataset::{Arrived, Dataset, Load, RowFilter};
pub use format::{Cell, Column, Mode};
