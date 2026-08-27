//! Errors returned by bbolt operations. Messages match upstream etcd-io/bbolt.

use std::io;
use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("database not open")]
    DatabaseNotOpen,
    #[error("invalid database")]
    Invalid,
    #[error("database isn't correctly mapped")]
    InvalidMapping,
    #[error("version mismatch")]
    VersionMismatch,
    #[error("checksum error")]
    Checksum,
    #[error("timeout")]
    Timeout,
    #[error("tx not writable")]
    TxNotWritable,
    #[error("tx closed")]
    TxClosed,
    #[error("database is in read-only mode")]
    DatabaseReadOnly,
    #[error("free pages are not pre-loaded")]
    FreePagesNotLoaded,
    #[error("bucket not found")]
    BucketNotFound,
    #[error("bucket already exists")]
    BucketExists,
    #[error("bucket name required")]
    BucketNameRequired,
    #[error("key required")]
    KeyRequired,
    #[error("key too large")]
    KeyTooLarge,
    #[error("value too large")]
    ValueTooLarge,
    #[error("database reached maximum size")]
    MaxSizeReached,
    #[error("incompatible value")]
    IncompatibleValue,
    #[error("the source and target are the same bucket")]
    SameBuckets,
    #[error("the source and target buckets are in different database files")]
    DifferentDb,
    #[error("managed transaction commit/rollback is not allowed")]
    ManagedTx,
    #[error("database file {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("{0}")]
    Corrupt(String),
}

impl Error {
    pub(crate) fn io(path: impl Into<PathBuf>, source: io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
