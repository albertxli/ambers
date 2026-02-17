use thiserror::Error;

#[derive(Error, Debug)]
pub enum SpssError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid magic number: expected \"$FL2\" or \"$FL3\", found {found:?}")]
    InvalidMagic { found: [u8; 4] },

    #[error("unsupported compression type: {0}")]
    UnsupportedCompression(i32),

    #[error("unexpected record type {record_type} at offset {offset}")]
    UnexpectedRecordType { record_type: i32, offset: u64 },

    #[error("invalid variable record: {0}")]
    InvalidVariable(String),

    #[error("encoding error: {0}")]
    Encoding(String),

    #[error("zlib decompression failed: {0}")]
    Zlib(String),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("truncated file: expected {expected} bytes, got {actual}")]
    TruncatedFile { expected: usize, actual: usize },

    #[error("invalid format specification: type={format_type}, width={width}, decimals={decimals}")]
    InvalidFormat {
        format_type: u8,
        width: u8,
        decimals: u8,
    },

    #[error("invalid value label record: {0}")]
    InvalidValueLabel(String),

    #[error("unsupported feature: {0}")]
    Unsupported(String),
}

pub type Result<T> = std::result::Result<T, SpssError>;
