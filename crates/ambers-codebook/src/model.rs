//! The codebook model: the format-independent types every other module
//! reads or produces.
//!
//! Will hold the `Codebook` type and its parts (variables, value labels,
//! missing values, multiple response sets), built from
//! `ambers::SpssMetadata`. The serialised form is a public, versioned
//! format with a JSON Schema under `schema/`; changing it needs a version
//! bump.
