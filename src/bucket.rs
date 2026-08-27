//! Buckets: collections of keys, including nested buckets.

use std::cell::RefCell;
use std::rc::Rc;

use crate::cursor::Cursor;
use crate::error::Result;
use crate::inner::{BucketId, TxInner};
use crate::page::Pgid;

/// A collection of key/value pairs. Valid only for the lifetime of the transaction.
pub struct Bucket {
    tx: Rc<RefCell<TxInner>>,
    pub(crate) id: BucketId,
}

impl Bucket {
    pub(crate) fn new(tx: Rc<RefCell<TxInner>>, id: BucketId) -> Self {
        Self { tx, id }
    }

    pub fn writable(&self) -> bool {
        self.tx.borrow().writable
    }

    pub fn root(&self) -> Pgid {
        self.tx.borrow().buckets[&self.id].header.root
    }

    pub fn fill_percent(&self) -> f64 {
        self.tx.borrow().buckets[&self.id].fill_percent
    }

    pub fn set_fill_percent(&self, v: f64) {
        self.tx
            .borrow_mut()
            .buckets
            .get_mut(&self.id)
            .unwrap()
            .fill_percent = v;
    }

    pub fn cursor(&self) -> Cursor {
        Cursor::new(Rc::clone(&self.tx), self.id)
    }

    pub fn bucket(&self, name: &[u8]) -> Option<Bucket> {
        let mut inner = self.tx.borrow_mut();
        inner
            .bucket_by_name(self.id, name)
            .ok()
            .flatten()
            .map(|id| Bucket::new(Rc::clone(&self.tx), id))
    }

    pub fn create_bucket(&self, name: &[u8]) -> Result<Bucket> {
        let mut inner = self.tx.borrow_mut();
        let id = inner.create_bucket(self.id, name)?;
        Ok(Bucket::new(Rc::clone(&self.tx), id))
    }

    pub fn create_bucket_if_not_exists(&self, name: &[u8]) -> Result<Bucket> {
        let mut inner = self.tx.borrow_mut();
        let id = inner.create_bucket_if_not_exists(self.id, name)?;
        Ok(Bucket::new(Rc::clone(&self.tx), id))
    }

    pub fn delete_bucket(&self, name: &[u8]) -> Result<()> {
        self.tx.borrow_mut().delete_bucket(self.id, name)
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.tx.borrow_mut().get(self.id, key).ok().flatten()
    }

    /// Zero-copy existence check equivalent to Go `Bucket.Get(key) != nil` for values
    /// (no heap allocation of the value bytes).
    pub fn has_value(&self, key: &[u8]) -> bool {
        self.tx
            .borrow()
            .has_value(self.id, key)
            .unwrap_or(false)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.tx.borrow_mut().put(self.id, key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.tx.borrow_mut().delete(self.id, key)
    }

    pub fn sequence(&self) -> u64 {
        self.tx.borrow().sequence(self.id)
    }

    pub fn set_sequence(&self, v: u64) -> Result<()> {
        self.tx.borrow_mut().set_sequence(self.id, v)
    }

    pub fn next_sequence(&self) -> Result<u64> {
        self.tx.borrow_mut().next_sequence(self.id)
    }

    /// Iterate keys in lexicographic order. Nested buckets yield `value == None`.
    pub fn for_each<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], Option<&[u8]>) -> Result<()>,
    {
        let mut c = self.cursor();
        let mut kv = c.first()?;
        while let Some(k) = kv.0.clone() {
            let v = kv.1.as_deref();
            f(&k, v)?;
            kv = c.next()?;
        }
        Ok(())
    }

    pub fn for_each_bucket<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        let mut stack = Vec::new();
        let mut inner = self.tx.borrow_mut();
        let mut kv = inner.cursor_first(self.id, &mut stack)?;
        while let Some(k) = kv.0.clone() {
            if kv.2 & crate::page::BUCKET_LEAF_FLAG != 0 {
                f(&k)?;
            }
            kv = inner.cursor_next(self.id, &mut stack)?;
        }
        Ok(())
    }

    /// Page and key statistics for this bucket (upstream `Bucket.Stats`).
    pub fn stats(&self) -> crate::stats::BucketStats {
        let inner = self.tx.borrow();
        crate::bucket_stats::bucket_stats(&inner, self.id)
    }

    pub fn move_bucket(&self, key: &[u8], dst: &Bucket) -> Result<()> {
        if !self.same_db(dst) {
            return Err(crate::error::Error::DifferentDb);
        }
        self.tx.borrow_mut().move_bucket(self.id, dst.id, key)
    }

    pub(crate) fn same_db(&self, other: &Bucket) -> bool {
        std::sync::Arc::ptr_eq(&self.tx.borrow().db, &other.tx.borrow().db)
    }
}
