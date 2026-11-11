//! Codebooks for SPSS files, built on the `ambers` reader.
//!
//! A codebook describes the variables of a `.sav` file: names, labels,
//! types, value labels, missing values and multiple response sets. This
//! crate will export one from a file's metadata, import one from disk, and
//! validate a file against one.
//!
//! This is a skeleton: the modules below document their future
//! responsibility and contain no logic yet.
//!
//! Anything that needs a second input besides the `.sav` file (a codebook,
//! a mapping) belongs here, not in the `ambers` engine crate.

pub mod export;
pub mod html;
pub mod import;
pub mod model;
pub mod validate;
