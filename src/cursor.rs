//! Cursors: lexicographic iteration over a bucket.

use std::cell::RefCell;
use std::rc::Rc;

use crate::bucket::Bucket;
use crate::error::Result;
use crate::inner::{BucketId, ElemRef, TxInner};
use crate::page::BUCKET_LEAF_FLAG;

/// Key and optional value. Nested buckets yield `None` for the value.
pub type KeyValue = (Option<Vec<u8>>, Option<Vec<u8>>);

/// Iterator over a bucket. Valid only while the transaction is open.
pub struct Cursor {
    tx: Rc<RefCell<TxInner>>,
    bucket: BucketId,
    pub(crate) stack: Vec<ElemRef>,
}

impl Cursor {
    pub(crate) fn new(tx: Rc<RefCell<TxInner>>, bucket: BucketId) -> Self {
        Self {
            tx,
            bucket,
            stack: Vec::new(),
        }
    }

    pub fn bucket(&self) -> Bucket {
        Bucket::new(Rc::clone(&self.tx), self.bucket)
    }

    pub fn first(&mut self) -> Result<KeyValue> {
        let mut inner = self.tx.borrow_mut();
        inner.check_open()?;
        let (k, v, flags) = inner.cursor_first(self.bucket, &mut self.stack)?;
        Ok(Self::hide_bucket(k, v, flags))
    }

    pub fn last(&mut self) -> Result<KeyValue> {
        let mut inner = self.tx.borrow_mut();
        inner.check_open()?;
        let (k, v, flags) = inner.cursor_last(self.bucket, &mut self.stack)?;
        Ok(Self::hide_bucket(k, v, flags))
    }

    /// Advance to the next key. Named `next` to match Go bbolt's `Cursor.Next`.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<KeyValue> {
        let mut inner = self.tx.borrow_mut();
        inner.check_open()?;
        let (k, v, flags) = inner.cursor_next(self.bucket, &mut self.stack)?;
        Ok(Self::hide_bucket(k, v, flags))
    }

    pub fn prev(&mut self) -> Result<KeyValue> {
        let mut inner = self.tx.borrow_mut();
        inner.check_open()?;
        let (k, v, flags) = inner.cursor_prev(self.bucket, &mut self.stack)?;
        Ok(Self::hide_bucket(k, v, flags))
    }

    pub fn seek(&mut self, key: &[u8]) -> Result<KeyValue> {
        let mut inner = self.tx.borrow_mut();
        inner.check_open()?;
        let (k, v, flags) = inner.cursor_seek_may_next(self.bucket, &mut self.stack, key)?;
        Ok(Self::hide_bucket(k, v, flags))
    }

    pub fn delete(&mut self) -> Result<()> {
        let mut inner = self.tx.borrow_mut();
        inner.check_writable()?;
        let (k, _, flags) = inner.key_value(self.bucket, &self.stack)?;
        if flags & BUCKET_LEAF_FLAG != 0 {
            return Err(crate::error::Error::IncompatibleValue);
        }
        let Some(key) = k else {
            return Ok(());
        };
        let nid = inner.cursor_node(self.bucket, &mut self.stack)?;
        inner.node_del(self.bucket, nid, &key);
        Ok(())
    }

    fn hide_bucket(
        k: Option<Vec<u8>>,
        v: Option<Vec<u8>>,
        flags: u32,
    ) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
        if flags & BUCKET_LEAF_FLAG != 0 {
            (k, None)
        } else {
            (k, v)
        }
    }
}
