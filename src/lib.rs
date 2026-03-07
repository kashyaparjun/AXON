pub mod archive;
pub mod format;
pub mod manifest;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, AxonError>;

#[derive(Debug, Error)]
pub enum AxonError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid AXON archive: {0}")]
    InvalidArchive(&'static str),
    #[error("file already exists: {0}")]
    AlreadyExists(String),
    #[error("entry already exists: {0}")]
    EntryExists(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("unsupported operation: {0}")]
    Unsupported(&'static str),
}
