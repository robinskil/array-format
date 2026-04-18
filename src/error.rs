//! Error types for the array-format crate.

use crate::dtype::DType;

/// The error type for array-format operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// I/O error from the underlying storage backend.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Footer serialization or deserialization failed.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// The file footer is invalid or corrupt.
    #[error("invalid footer: {0}")]
    InvalidFooter(String),

    /// The requested array was not found.
    #[error("array not found: {name}")]
    ArrayNotFound {
        /// Name of the array that was not found.
        name: String,
    },

    /// A referenced block id is out of range.
    #[error("block out of range: {block_id}")]
    BlockOutOfRange {
        /// The invalid block id.
        block_id: u32,
    },

    /// Compression or decompression failed.
    #[error("codec error: {0}")]
    Codec(String),

    /// Storage backend error.
    #[error("storage error: {0}")]
    Storage(String),

    /// The array already exists.
    #[error("array already exists: {name}")]
    ArrayAlreadyExists {
        /// Name of the duplicate array.
        name: String,
    },

    /// The dtype of the data does not match the dtype of the array.
    #[error("dtype mismatch: expected {expected:?}, got {actual:?}")]
    DTypeMismatch {
        /// The dtype declared in the array metadata.
        expected: DType,
        /// The dtype of the data being read or written.
        actual: DType,
    },
}

/// A specialized [`Result`] type for array-format operations.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display() {
        let e = Error::ArrayNotFound {
            name: "temp".into(),
        };
        assert_eq!(e.to_string(), "array not found: temp");
    }

    #[test]
    fn io_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "gone");
        let e: Error = io_err.into();
        assert!(matches!(e, Error::Io(_)));
    }
}
