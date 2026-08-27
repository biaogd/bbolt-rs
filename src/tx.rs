//! Transactions: [`Tx`].

use std::cell::RefCell;
use std::rc::Rc;

use crate::bucket::Bucket;
use crate::cursor::Cursor;
use crate::error::{Error, Result};
use crate::inner::{BucketId, TxInner};
use crate::page::Pgid;

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
}

impl Drop for Tx {
    fn drop(&mut self) {
        if let Ok(mut inner) = self.inner.try_borrow_mut() {
            if !inner.closed {
                inner.rollback_internal();
            }
        }
    }
}
