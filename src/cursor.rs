//! Cursors: lexicographic iteration over a bucket.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use memmap2::Mmap;

use crate::bucket::Bucket;
use crate::db::DbInner;
use crate::error::Result;
use crate::inner::{BucketId, ElemRef, TxInner};
use crate::page::{leaf_at, PageHeader, BUCKET_LEAF_FLAG};

/// Key and optional value. Nested buckets yield `None` for the value.
pub type KeyValue = (Option<Vec<u8>>, Option<Vec<u8>>);

/// Iterator over a bucket. Valid only while the transaction is open.
pub struct Cursor {
    tx: Rc<RefCell<TxInner>>,
    bucket: BucketId,
    pub(crate) stack: Vec<ElemRef>,
    /// Pinned mmap for zero-copy leaf scans (avoids RefCell on the hot path).
    mmap: Arc<Mmap>,
    page_size: usize,
    /// Current disk leaf page base (null if on a materialized node / inline).
    leaf_base: *const u8,
    leaf_bytes: usize,
    leaf_count: usize,
    leaf_index: usize,
    /// Go-style view into page/node memory (valid until the next cursor move / tx end).
    key_ptr: *const u8,
    key_len: usize,
    val_ptr: *const u8,
    val_len: usize,
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
            leaf_base: std::ptr::null(),
            leaf_bytes: 0,
            leaf_count: 0,
            leaf_index: 0,
            key_ptr: std::ptr::null(),
            key_len: 0,
            val_ptr: std::ptr::null(),
            val_len: 0,
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

    /// Like Go `Cursor.First`: positions the cursor and exposes key/value as mmap/node views.
    pub fn first_view(&mut self) -> Result<bool> {
        let ptrs = {
            let inner = self.tx.borrow();
            // Refresh pin in case a writer remapped before this view began.
            self.mmap = Arc::clone(&inner.mmap_pin);
            self.page_size = inner.db.page_size;
            inner.check_open()?;
            if !inner.cursor_first_move(self.bucket, &mut self.stack)? {
                None
            } else {
                inner.cursor_kv_ptrs(self.bucket, &self.stack)?
            }
        };
        self.cache_leaf_from_stack();
        self.apply_view(ptrs);
        Ok(self.view_valid)
    }

    /// Like Go `Cursor.Next` with zero-copy key/value views (no per-step heap alloc).
    pub fn next_view(&mut self) -> Result<bool> {
        // Hot path: advance within the current mmap leaf without touching RefCell.
        if !self.leaf_base.is_null() && self.leaf_index + 1 < self.leaf_count {
            self.leaf_index += 1;
            // SAFETY: leaf_base points into self.mmap for this page's lifetime.
            let page =
                unsafe { std::slice::from_raw_parts(self.leaf_base, self.leaf_bytes) };
            let (flags, key, val) = leaf_at(page, self.leaf_index);
            self.key_ptr = key.as_ptr();
            self.key_len = key.len();
            self.val_is_bucket = flags & BUCKET_LEAF_FLAG != 0;
            if self.val_is_bucket {
                self.val_ptr = std::ptr::null();
                self.val_len = 0;
            } else {
                self.val_ptr = val.as_ptr();
                self.val_len = val.len();
            }
            self.view_valid = true;
            return Ok(true);
        }

        // Sync leaf index back onto the stack before the slow path.
        if !self.leaf_base.is_null() {
            if let Some(r) = self.stack.last_mut() {
                r.index = self.leaf_index;
            }
        }

        let ptrs = {
            let inner = self.tx.borrow();
            inner.check_open()?;
            if !inner.cursor_next_move(self.bucket, &mut self.stack)? {
                None
            } else {
                inner.cursor_kv_ptrs(self.bucket, &self.stack)?
            }
        };
        self.cache_leaf_from_stack();
        self.apply_view(ptrs);
        Ok(self.view_valid)
    }

    /// Like Go `Cursor.Seek`: positions to the given key (or next) with zero-copy views.
    pub fn seek_view(&mut self, key: &[u8]) -> Result<bool> {
        let ptrs = {
            let mut inner = self.tx.borrow_mut();
            inner.check_open()?;
            self.mmap = Arc::clone(&inner.mmap_pin);
            self.page_size = inner.db.page_size;
            let root = inner.buckets[&self.bucket].header.root;
            self.stack.clear();
            inner.search(self.bucket, &mut self.stack, key, root)?;
            // Go Seek moves to next if index past end of leaf.
            if let Some(last) = self.stack.last() {
                let count = inner.ref_count(self.bucket, last)?;
                if last.index >= count {
                    if !inner.cursor_next_move(self.bucket, &mut self.stack)? {
                        None
                    } else {
                        inner.cursor_kv_ptrs(self.bucket, &self.stack)?
                    }
                } else {
                    inner.cursor_kv_ptrs(self.bucket, &self.stack)?
                }
            } else {
                None
            }
        };
        self.cache_leaf_from_stack();
        self.apply_view(ptrs);
        Ok(self.view_valid)
    }

    pub fn key(&self) -> Option<&[u8]> {
        if !self.view_valid || self.key_ptr.is_null() {
            None
        } else {
            // SAFETY: pointers were taken from the pinned mmap or node inodes of the
            // open transaction; invalidated by the next cursor move (same contract as Go).
            Some(unsafe { std::slice::from_raw_parts(self.key_ptr, self.key_len) })
        }
    }

    pub fn value(&self) -> Option<&[u8]> {
        if !self.view_valid || self.val_is_bucket || self.val_ptr.is_null() {
            None
        } else {
            Some(unsafe { std::slice::from_raw_parts(self.val_ptr, self.val_len) })
        }
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

    fn cache_leaf_from_stack(&mut self) {
        self.leaf_base = std::ptr::null();
        self.leaf_bytes = 0;
        self.leaf_count = 0;
        self.leaf_index = 0;
        let Some(r) = self.stack.last() else {
            return;
        };
        if r.node_id.is_some() {
            return;
        }
        // Inline buckets are not in the mmap pin.
        if r.pgid == 0 {
            return;
        }
        let Ok(page) = DbInner::page_bytes(&self.mmap, self.page_size, r.pgid) else {
            return;
        };
        let hdr = PageHeader::read(page);
        if !hdr.is_leaf() {
            return;
        }
        self.leaf_base = page.as_ptr();
        self.leaf_bytes = page.len();
        self.leaf_count = hdr.count as usize;
        self.leaf_index = r.index;
    }

    fn apply_view(&mut self, ptrs: Option<(*const u8, usize, *const u8, usize, u32)>) {
        match ptrs {
            None => {
                self.view_valid = false;
                self.key_ptr = std::ptr::null();
                self.key_len = 0;
                self.val_ptr = std::ptr::null();
                self.val_len = 0;
                self.val_is_bucket = false;
            }
            Some((kp, kl, vp, vl, flags)) => {
                self.key_ptr = kp;
                self.key_len = kl;
                self.val_is_bucket = flags & BUCKET_LEAF_FLAG != 0;
                if self.val_is_bucket {
                    self.val_ptr = std::ptr::null();
                    self.val_len = 0;
                } else {
                    self.val_ptr = vp;
                    self.val_len = vl;
                }
                self.view_valid = true;
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