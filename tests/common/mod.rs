//! Shared helpers for bbolt integration tests (upstream `btesting` subset).
#![allow(dead_code)]

use std::path::{Path, PathBuf};

use bbolt::{Db, Error, Options, Result};

pub const PAGE_SIZE: usize = 4096;

pub fn default_opts() -> Options {
    Options {
        page_size: PAGE_SIZE,
        ..Options::default()
    }
}

pub fn opts_with(mut opts: Options) -> Options {
    if opts.page_size == 0 {
        opts.page_size = PAGE_SIZE;
    }
    opts
}

/// Create a temp directory and open a new database (upstream `MustCreateDB`).
pub fn open_tmp() -> (tempfile::TempDir, Db) {
    let dir = tempfile::tempdir().unwrap();
    let db = must_create_db_in(dir.path());
    (dir, db)
}

pub fn db_path(dir: &tempfile::TempDir) -> PathBuf {
    dir.path().join("test.db")
}

pub fn must_create_db_in(dir: &Path) -> Db {
    let path = dir.join("test.db");
    Db::open(&path, 0o600, Some(default_opts())).unwrap()
}

pub fn reopen(dir: &tempfile::TempDir, opts: Option<Options>) -> Db {
    Db::open(db_path(dir), 0o600, Some(opts.unwrap_or_else(default_opts))).unwrap()
}

pub fn reopen_path(path: &Path, opts: Option<Options>) -> Db {
    Db::open(path, 0o600, Some(opts.unwrap_or_else(default_opts))).unwrap()
}

pub fn file_size(path: &Path) -> u64 {
    std::fs::metadata(path).unwrap().len()
}

pub fn must_check(db: &Db) {
    db.view(|tx| {
        let errs = tx.check();
        assert!(errs.is_empty(), "check failed: {errs:?}");
        Ok(())
    })
    .unwrap();
}

pub fn fill_bucket(
    db: &Db,
    bucket: &[u8],
    n: i32,
    key_fn: impl Fn(i32) -> Vec<u8>,
    val_fn: impl Fn(i32) -> Vec<u8>,
) -> Result<()> {
    db.update(|tx| {
        let b = tx.create_bucket_if_not_exists(bucket)?;
        for i in 0..n {
            b.put(&key_fn(i), &val_fn(i))?;
        }
        Ok(())
    })
}

pub fn assert_err<T: std::fmt::Debug>(result: Result<T>, expected: Error) {
    match result {
        Err(e) => assert_eq!(e.to_string(), expected.to_string(), "unexpected error"),
        Ok(v) => panic!("expected error {expected}, got Ok({v:?})"),
    }
}
