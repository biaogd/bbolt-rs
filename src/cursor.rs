//! Cursors: lexicographic iteration over a bucket.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use memmap2::Mmap;

use crate::bucket::Bucket;
use crate::db::DbInner;
use crate::error::{Error, Result};
use crate::inner::{BucketId, ElemRef, TxInner};
use crate::page::{leaf_at, PageHeader, BUCKET_LEAF_FLAG, Pgid};

/// Key and optional value. Nested buckets yield `None` for the value.
pub type KeyValue = (Option<Vec<u8>>, Option<Vec<u8>>);

/// Iterator over a bucket. Valid only while the transaction is open.
///
/// View APIs (`first_view` / `next_view` / `key` / `value`) copy the current
/// key/value into reusable buffers so callers get stable `&[u8]` slices without
/// caching raw mmap pointers on the cursor.
pub struct Cursor {
    tx: Rc<RefCell<TxInner>>,
    bucket: BucketId,
    pub(crate) stack: Vec<ElemRef>,
    /// Pinned mmap for zero-copy page reads via [`DbInner::page_bytes`].
    mmap: Arc<Mmap>,
    page_size: usize,
    /// Disk leaf currently being scanned (`0` = not on a mmap leaf).
    leaf_pgid: Pgid,
    leaf_count: usize,
    leaf_index: usize,
    /// Reused buffers for the current Go-style key/value view.
    key_buf: Vec<u8>,
    val_buf: Vec<u8>,
    val_is_bucket: bool,
    view_valid: bool,
}

impl Cursor {
    pub(crate) fn new(tx: Rc<RefCell<TxInner>>, bucket: BucketId) -> Self {
        let (mmap, page_size) = {
            let inner = tx.borrow();
            (Arc::clone(&inner.mmap_pin), inner.db.page_size)
        };
        Self {
            tx,
            bucket,
            stack: Vec::new(),
            mmap,
            page_size,
            leaf_pgid: 0,
            leaf_count: 0,
            leaf_index: 0,
            key_buf: Vec::with_capacity(64),
            val_buf: Vec::with_capacity(64),
            val_is_bucket: false,
            view_valid: false,
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

    /// Like Go `Cursor.First`: positions the cursor and exposes key/value views.
    pub fn first_view(&mut self) -> Result<bool> {
        let ok = {
            let inner = self.tx.borrow();
            self.mmap = Arc::clone(&inner.mmap_pin);
            self.page_size = inner.db.page_size;
            inner.check_open()?;
            inner.cursor_first_move(self.bucket, &mut self.stack)?
        };
        if !ok {
            self.clear_view();
            return Ok(false);
        }
        self.cache_leaf_from_stack();
        self.bind_current_view()
    }

    /// Like Go `Cursor.Next` with reusable key/value view buffers.
    pub fn next_view(&mut self) -> Result<bool> {
        // Hot path: advance within the current mmap leaf via safe page_bytes.
        if self.leaf_pgid != 0 && self.leaf_index + 1 < self.leaf_count {
            self.leaf_index += 1;
            if let Some(r) = self.stack.last_mut() {
                r.index = self.leaf_index;
            }
            let pgid = self.leaf_pgid;
            let index = self.leaf_index;
            let (flags, key, val) = {
                let page = DbInner::page_bytes(&self.mmap, self.page_size, pgid)?;
                let (flags, key, val) = leaf_at(page, index);
                (flags, key.to_vec(), val.to_vec())
            };
            self.key_buf = key;
            self.val_is_bucket = flags & BUCKET_LEAF_FLAG != 0;
            if self.val_is_bucket {
                self.val_buf.clear();
            } else {
                self.val_buf = val;
            }
            self.view_valid = true;
            return Ok(true);
        }

        if self.leaf_pgid != 0 {
            if let Some(r) = self.stack.last_mut() {
                r.index = self.leaf_index;
            }
        }

        let ok = {
            let inner = self.tx.borrow();
            inner.check_open()?;
            inner.cursor_next_move(self.bucket, &mut self.stack)?
        };
        if !ok {
            self.clear_view();
            return Ok(false);
        }
        self.cache_leaf_from_stack();
        self.bind_current_view()
    }

    /// Like Go `Cursor.Seek`: positions to the given key (or next) with views.
    pub fn seek_view(&mut self, key: &[u8]) -> Result<bool> {
        let ok = {
            let mut inner = self.tx.borrow_mut();
            inner.check_open()?;
            self.mmap = Arc::clone(&inner.mmap_pin);
            self.page_size = inner.db.page_size;
            let root = inner.buckets[&self.bucket].header.root;
            self.stack.clear();
            inner.search(self.bucket, &mut self.stack, key, root)?;
            if let Some(last) = self.stack.last() {
                let count = inner.ref_count(self.bucket, last)?;
                if last.index >= count {
                    inner.cursor_next_move(self.bucket, &mut self.stack)?
                } else {
                    true
                }
            } else {
                false
            }
        };
        if !ok {
            self.clear_view();
            return Ok(false);
        }
        self.cache_leaf_from_stack();
        self.bind_current_view()
    }

    pub fn key(&self) -> Option<&[u8]> {
        if self.view_valid {
            Some(self.key_buf.as_slice())
        } else {
            None
        }
    }

    pub fn value(&self) -> Option<&[u8]> {
        if !self.view_valid || self.val_is_bucket {
            None
        } else {
            Some(self.val_buf.as_slice())
        }
    }

    pub fn delete(&mut self) -> Result<()> {
        let mut inner = self.tx.borrow_mut();
        inner.check_writable()?;
        let (k, _, flags) = inner.key_value(self.bucket, &self.stack)?;
        if flags & BUCKET_LEAF_FLAG != 0 {
            return Err(Error::IncompatibleValue);
        }
        let Some(key) = k else {
            return Ok(());
        };
        let nid = inner.cursor_node(self.bucket, &mut self.stack)?;
        inner.node_del(self.bucket, nid, &key);
        Ok(())
    }

    fn clear_view(&mut self) {
        self.view_valid = false;
        self.key_buf.clear();
        self.val_buf.clear();
        self.val_is_bucket = false;
        self.leaf_pgid = 0;
        self.leaf_count = 0;
        self.leaf_index = 0;
    }

    fn apply_owned(&mut self, flags: u32, key: Vec<u8>, val: Vec<u8>) {
        self.key_buf = key;
        self.val_is_bucket = flags & BUCKET_LEAF_FLAG != 0;
        if self.val_is_bucket {
            self.val_buf.clear();
        } else {
            self.val_buf = val;
        }
        self.view_valid = true;
    }

    fn cache_leaf_from_stack(&mut self) {
        self.leaf_pgid = 0;
        self.leaf_count = 0;
        self.leaf_index = 0;
        let Some(r) = self.stack.last().copied() else {
            return;
        };
        if r.node_id.is_some() || r.pgid == 0 {
            return;
        }
        let Ok(page) = DbInner::page_bytes(&self.mmap, self.page_size, r.pgid) else {
            return;
        };
        let hdr = PageHeader::read(page);
        if !hdr.is_leaf() {
            return;
        }
        self.leaf_pgid = r.pgid;
        self.leaf_count = hdr.count as usize;
        self.leaf_index = r.index;
    }

    fn bind_current_view(&mut self) -> Result<bool> {
        let Some(r) = self.stack.last().copied() else {
            self.clear_view();
            return Ok(false);
        };
        let owned = {
            let inner = self.tx.borrow();
            let count = inner.ref_count(self.bucket, &r)?;
            if count == 0 || r.index >= count {
                None
            } else if let Some(id) = r.node_id {
                let inode = &inner.buckets[&self.bucket].nodes[&id].inodes[r.index];
                Some((inode.flags, inode.key.clone(), inode.value.clone()))
            } else if inner.buckets[&self.bucket].header.root == 0 {
                let page = inner.buckets[&self.bucket]
                    .inline_page
                    .as_ref()
                    .ok_or_else(|| Error::Corrupt("inline missing".into()))?;
                let (flags, key, val) = leaf_at(page, r.index);
                Some((flags, key.to_vec(), val.to_vec()))
            } else {
                let pgid = if self.leaf_pgid != 0 {
                    self.leaf_pgid
                } else {
                    r.pgid
                };
                let page = DbInner::page_bytes(&inner.mmap_pin, inner.db.page_size, pgid)?;
                let (flags, key, val) = leaf_at(page, r.index);
                Some((flags, key.to_vec(), val.to_vec()))
            }
        };
        match owned {
            None => {
                self.clear_view();
                Ok(false)
            }
            Some((flags, key, val)) => {
                self.apply_owned(flags, key, val);
                Ok(true)
            }
        }
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
