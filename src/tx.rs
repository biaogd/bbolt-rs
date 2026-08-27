//! Transactions: [`Tx`].

use std::cell::RefCell;
use std::io::Write;
use std::path::Path;
use std::rc::Rc;

use crate::bucket::Bucket;
use crate::check::{check_tx, CheckOptions};
use crate::cursor::Cursor;
use crate::error::{Error, Result};
use crate::inner::{BucketId, TxInner};
use crate::page::{write_meta_page, Meta, Pgid};
use crate::platform;
use crate::stats::{BucketStructure, TxStats};

/// A read-only or read-write transaction.
///
/// Individual transactions are not thread-safe. [`crate::Db`] is.
pub struct Tx {
    pub(crate) inner: Rc<RefCell<TxInner>>,
}

impl Tx {
    pub fn id(&self) -> i64 {
        let inner = self.inner.borrow();
        if inner.closed {
            -1
        } else {
            inner.meta.txid as i64
        }
    }

    pub fn writable(&self) -> bool {
        self.inner.borrow().writable
    }

    pub fn size(&self) -> u64 {
        let inner = self.inner.borrow();
        inner.meta.pgid * inner.db.page_size as u64
    }

    pub fn page_size(&self) -> usize {
        self.inner.borrow().db.page_size
    }

    /// Root-bucket cursor. Every key is a top-level bucket (value is `None`).
    pub fn cursor(&self) -> Cursor {
        Cursor::new(Rc::clone(&self.inner), 0)
    }

    pub fn bucket(&self, name: &[u8]) -> Option<Bucket> {
        let mut inner = self.inner.borrow_mut();
        inner
            .bucket_by_name(0, name)
            .ok()
            .flatten()
            .map(|id| Bucket::new(Rc::clone(&self.inner), id))
    }

    pub fn create_bucket(&self, name: &[u8]) -> Result<Bucket> {
        let mut inner = self.inner.borrow_mut();
        let id = inner.create_bucket(0, name)?;
        Ok(Bucket::new(Rc::clone(&self.inner), id))
    }

    pub fn create_bucket_if_not_exists(&self, name: &[u8]) -> Result<Bucket> {
        let mut inner = self.inner.borrow_mut();
        let id = inner.create_bucket_if_not_exists(0, name)?;
        Ok(Bucket::new(Rc::clone(&self.inner), id))
    }

    pub fn delete_bucket(&self, name: &[u8]) -> Result<()> {
        self.inner.borrow_mut().delete_bucket(0, name)
    }

    /// Move a child bucket. `src`/`dst` of `None` means the root bucket.
    pub fn move_bucket(
        &self,
        child: &[u8],
        src: Option<&Bucket>,
        dst: Option<&Bucket>,
    ) -> Result<()> {
        let src_id: BucketId = src.map(|b| b.id).unwrap_or(0);
        let dst_id: BucketId = dst.map(|b| b.id).unwrap_or(0);
        self.inner.borrow_mut().move_bucket(src_id, dst_id, child)
    }

    pub fn for_each<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &Bucket) -> Result<()>,
    {
        let mut c = self.cursor();
        let mut kv = c.first()?;
        while let Some(k) = kv.0.clone() {
            let b = self.bucket(&k).ok_or(Error::BucketNotFound)?;
            f(&k, &b)?;
            kv = c.next()?;
        }
        Ok(())
    }

    pub fn commit(&self) -> Result<()> {
        self.inner.borrow_mut().commit()
    }

    pub fn rollback(&self) -> Result<()> {
        self.inner.borrow_mut().rollback()
    }

    pub fn high_water_mark(&self) -> Pgid {
        self.inner.borrow().meta.pgid
    }

    /// Consistency check (upstream `Tx.Check`). Returns all problems found.
    pub fn check(&self) -> Vec<Error> {
        self.check_with(CheckOptions::default())
    }

    pub fn check_with(&self, opts: CheckOptions) -> Vec<Error> {
        let mut inner = self.inner.borrow_mut();
        check_tx(&mut inner, opts)
    }

    /// Nested bucket structure (upstream `Tx.Inspect`).
    pub fn inspect(&self) -> BucketStructure {
        inspect_bucket(self, b"root")
    }

    /// Lightweight tx stats snapshot (counters are not fully instrumented yet).
    pub fn stats(&self) -> TxStats {
        TxStats::default()
    }

    /// Write a consistent snapshot of the database to `w` (upstream `Tx.WriteTo`).
    pub fn write_to<W: Write>(&self, mut w: W) -> Result<u64> {
        let inner = self.inner.borrow();
        let page_size = inner.db.page_size;
        let mut n = 0u64;

        let mut buf = vec![0u8; page_size];
        let mut meta = inner.meta.clone();
        write_meta_page(&mut buf, &meta);
        // Ensure page id 0
        crate::page::set_page_id(&mut buf, 0);
        w.write_all(&buf).map_err(|e| Error::io("<WriteTo meta0>", e))?;
        n += buf.len() as u64;

        meta.txid = meta.txid.saturating_sub(1);
        write_meta_page(&mut buf, &meta);
        crate::page::set_page_id(&mut buf, 1);
        w.write_all(&buf).map_err(|e| Error::io("<WriteTo meta1>", e))?;
        n += buf.len() as u64;

        let data_offset = (page_size * 2) as u64;
        let data_size = inner.meta.pgid * page_size as u64 - data_offset;
        let mut remaining = data_size;
        let mut off = data_offset;
        let mut chunk = vec![0u8; page_size.min(remaining as usize).max(1)];
        while remaining > 0 {
            let want = remaining.min(chunk.len() as u64) as usize;
            platform::read_exact_at(&inner.db.file, &mut chunk[..want], off)
                .map_err(|e| Error::io(&inner.db.path, e))?;
            w.write_all(&chunk[..want])
                .map_err(|e| Error::io("<WriteTo data>", e))?;
            n += want as u64;
            off += want as u64;
            remaining -= want as u64;
        }
        Ok(n)
    }

    /// Deprecated alias for [`Self::write_to`] (upstream `Tx.Copy`).
    pub fn copy<W: Write>(&self, w: W) -> Result<()> {
        self.write_to(w).map(|_| ())
    }

    /// Copy the database snapshot to a new file (upstream `Tx.CopyFile`).
    pub fn copy_file<P: AsRef<Path>>(&self, path: P, mode: u32) -> Result<()> {
        let path = path.as_ref();
        let mut f = platform::open_db_file(path, false, mode)?;
        // Truncate
        f.set_len(0).map_err(|e| Error::io(path, e))?;
        self.write_to(&mut f)?;
        f.sync_all().map_err(|e| Error::io(path, e))?;
        Ok(())
    }

    /// Meta page used by this transaction (for CLI / debugging).
    pub fn meta(&self) -> Meta {
        self.inner.borrow().meta.clone()
    }

    /// Read a page for inspection (CLI / debugging).
    pub fn page_info(&self, id: Pgid) -> Result<crate::stats::PageInfo> {
        let inner = self.inner.borrow();
        if id >= inner.meta.pgid {
            return Err(Error::Corrupt(format!("page {id} out of range")));
        }
        let page = inner.db.read_page(id)?;
        let hdr = crate::page::PageHeader::read(&page);
        Ok(crate::stats::PageInfo {
            id: hdr.id,
            page_type: hdr.typ(),
            count: hdr.count,
            overflow: hdr.overflow,
        })
    }
}

fn inspect_bucket(tx: &Tx, _name: &[u8]) -> BucketStructure {
    let mut bs = BucketStructure {
        name: "root".into(),
        key_n: 0,
        children: Vec::new(),
    };
    let mut c = tx.cursor();
    let mut kv = match c.first() {
        Ok(v) => v,
        Err(_) => return bs,
    };
    while let Some(k) = kv.0.clone() {
        if kv.1.is_none() {
            if let Some(child) = tx.bucket(&k) {
                bs.children.push(inspect_named(&child, &k));
            }
        } else {
            bs.key_n += 1;
        }
        kv = match c.next() {
            Ok(v) => v,
            Err(_) => break,
        };
    }
    bs
}

fn inspect_named(b: &Bucket, name: &[u8]) -> BucketStructure {
    let mut bs = BucketStructure {
        name: String::from_utf8_lossy(name).into_owned(),
        key_n: 0,
        children: Vec::new(),
    };
    let mut c = b.cursor();
    let mut kv = match c.first() {
        Ok(v) => v,
        Err(_) => return bs,
    };
    while let Some(k) = kv.0.clone() {
        if kv.1.is_none() {
            if let Some(child) = b.bucket(&k) {
                bs.children.push(inspect_named(&child, &k));
            }
        } else {
            bs.key_n += 1;
        }
        kv = match c.next() {
            Ok(v) => v,
            Err(_) => break,
        };
    }
    bs
}
