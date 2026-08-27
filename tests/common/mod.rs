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

/// Create a temp directory and open a new database with custom options.
pub fn open_tmp_with(opts: Options) -> (tempfile::TempDir, Db) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    let db = Db::open(&path, 0o600, Some(opts_with(opts))).unwrap();
    (dir, db)
}

/// Create a temp directory and open a new database (upstream `MustCreateDB`).
pub fn open_tmp() -> (tempfile::TempDir, Db) {
    open_tmp_with(default_opts())
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

/// Corrupt both meta pages' version field without fixing checksums.
pub fn corrupt_meta_version(path: &Path, page_size: usize) {
    use bbolt::{meta_from_page, PAGE_HEADER_SIZE};
    let mut buf = std::fs::read(path).unwrap();
    for i in 0..2u64 {
        let off = i as usize * page_size + PAGE_HEADER_SIZE;
        let mut meta = meta_from_page(&buf[off..]);
        meta.version += 1;
        let ver_off = off + 4;
        buf[ver_off..ver_off + 4].copy_from_slice(&meta.version.to_le_bytes());
    }
    std::fs::write(path, buf).unwrap();
}

/// Corrupt both meta checksums by bumping pgid without recalculating (upstream `TestOpen_ErrChecksum`).
pub fn corrupt_meta_checksum(path: &Path, page_size: usize) {
    use bbolt::{meta_from_page, PAGE_HEADER_SIZE};
    let mut buf = std::fs::read(path).unwrap();
    for i in 0..2u64 {
        let off = i as usize * page_size + PAGE_HEADER_SIZE;
        let mut meta = meta_from_page(&buf[off..]);
        meta.pgid += 1;
        let meta_off = off + 40;
        buf[meta_off..meta_off + 8].copy_from_slice(&meta.pgid.to_le_bytes());
    }
    std::fs::write(path, buf).unwrap();
}

/// Create a filled DB for max-size open tests.
pub fn create_filled_db(
    dir: &Path,
    alloc_size: usize,
    num_keys: i32,
) -> (tempfile::TempDir, Db) {
    let td = tempfile::tempdir_in(dir).unwrap();
    let db = must_create_db_in(td.path());
    db.set_alloc_size(alloc_size);
    fill_bucket(
        &db,
        b"data",
        num_keys,
        |k| format!("{k:04}").into_bytes(),
        |_| vec![0u8; 1000],
    )
    .unwrap();
    (td, db)
}
