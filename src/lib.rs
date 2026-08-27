//! A Rust port of [etcd-io/bbolt](https://github.com/etcd-io/bbolt): an embedded
//! mmap-backed B+tree key/value store.
//!
//! # Example
//!
//! ```no_run
//! use bbolt::{Db, Options};
//!
//! let db = Db::open("my.db", 0o600, Some(Options { page_size: 4096, ..Options::default() }))?;
//! db.update(|tx| {
//!     let b = tx.create_bucket(b"users")?;
//!     b.put(b"alice", b"data")?;
//!     Ok(())
//! })?;
//! db.view(|tx| {
//!     let b = tx.bucket(b"users").unwrap();
//!     assert_eq!(b.get(b"alice").as_deref(), Some(&b"data"[..]));
//!     Ok(())
//! })?;
//! # Ok::<(), bbolt::Error>(())
//! ```

mod batch;
mod bucket;
mod cursor;
mod db;
mod error;
mod freelist;
mod inner;
mod page;
mod tx;

pub use bucket::Bucket;
pub use cursor::{Cursor, KeyValue};
pub use db::{Db, Options};
pub use error::{Error, Result};
pub use page::{DEFAULT_FILL_PERCENT, MAGIC, MAX_KEY_SIZE, MAX_VALUE_SIZE, VERSION};
pub use tx::Tx;

pub use page::{Pgid, Txid};
