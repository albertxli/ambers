//! Validating a `.sav` file against a codebook.
//!
//! Will compare a file's metadata, and optionally its data, with a
//! codebook and return every difference in one pass as a list of problems
//! with stable codes, serialisable as a versioned report.
