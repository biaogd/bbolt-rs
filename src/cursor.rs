//! Cursors: lexicographic iteration over a bucket.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use memmap2::Mmap;

use crate::bucket::Bucket;
use crate::db::DbInner;
use crate::error::{Error, Result};
use crate::inner::{BucketId, ElemRef, TxInner};
use crate::page::{
    leaf_at, read_u32, PageHeader, BUCKET_LEAF_FLAG, LEAF_PAGE_ELEMENT_SIZE, PAGE_HEADER_SIZE,
    Pgid,
};

/// Key and optional value. Nested buckets yield `None` for the value.
pub type KeyValue = (Option<Vec<u8>>, Option<Vec<u8>>);

/// Iterator over a bucket. Valid only while the transaction is open.
///
/// View APIs expose the current key/value as `&[u8]` slices. For on-disk leaves
/// those slices borrow the pinned [`Mmap`] via **byte offsets** (no raw pointers
/// stored on the cursor). Materialized / inline nodes copy into reusable buffers.
pub struct Cursor {
    tx: Rc<RefCell<TxInner>>,
    bucket: BucketId,
    pub(crate) stack: Vec<ElemRef>,
    mmap: Arc<Mmap>,
    page_size: usize,
    /// Disk leaf being scanned (`0` = not on a mmap leaf).
    leaf_pgid: Pgid,
    /// Absolute byte offset of this leaf page within [`mmap`].
    leaf_page_start: usize,
    leaf_count: usize,
    leaf_index: usize,
    /// When true, key/value are mmap offsets; otherwise [`key_buf`]//`val_buf`].
    mmap_view: bool,
    key_off: usize,
    key_len: usize,
    val_off: usize,
    val_len: usize,
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
            leaf_page_start: 0,
            leaf_count: 0,
            leaf_index: 0,
            mmap_view: false,
            key_off: 0,
            key_len: 0,
            val_off: 0,
            val_len: 0,
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

    /// Like Go `Cursor.Next` with key/value views (mmap offsets or owned buffers).
    pub fn next_view(&mut self) -> Result<bool> {
        if self.leaf_pgid != 0 && self.leaf_index + 1 < self.leaf_count {
            self.leaf_index += 1;
            if let Some(r) = self.stack.last_mut() {
                r.index = self.leaf_index;
            }
            self.bind_mmap_leaf_cached(self.leaf_index)?;
            return Ok(true);
        }

        // Try the next sibling leaf under a disk parent branch (no Tx borrow).
        if self.try_next_sibling_leaf()? {
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

    /// Advance to the next leaf child of the parent branch using safe page_bytes.
    fn try_next_sibling_leaf(&mut self) -> Result<bool> {
        if self.stack.len() < 2 {
            return Ok(false);
        }
        let parent = self.stack[self.stack.len() - 2];
        if parent.node_id.is_some() || parent.pgid == 0 {
            return Ok(false);
        }
        let next_index = parent.index + 1;
        let parent_page = match DbInner::page_bytes(&self.mmap, self.page_size, parent.pgid) {
            Ok(p) => p,
            Err(_) => return Ok(false),
        };
        let ph = PageHeader::read(parent_page);
        if !ph.is_branch() || next_index >= ph.count as usize {
            return Ok(false);
        }
        let (child_pgid, _) = crate::page::branch_at(parent_page, next_index);
        let child_page = match DbInner::page_bytes(&self.mmap, self.page_size, child_pgid) {
            Ok(p) => p,
            Err(_) => return Ok(false),
        };
        let ch = PageHeader::read(child_page);
        if !ch.is_leaf() || ch.count == 0 {
            return Ok(false);
        }
        let last = self.stack.len();
        self.stack[last - 2].index = next_index;
        self.stack[last - 1] = ElemRef {
            node_id: None,
            pgid: child_pgid,
            index: 0,
            count: ch.count as i32,
        };
        self.leaf_pgid = child_pgid;
        self.leaf_page_start = child_pgid as usize * self.page_size;
        self.leaf_count = ch.count as usize;
        self.leaf_index = 0;
        self.bind_mmap_leaf_cached(0)?;
        Ok(true)
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
        if !self.view_valid {
            return None;
        }
        if self.mmap_view {
            Some(&self.mmap[self.key_off..self.key_off + self.key_len])
        } else {
            Some(self.key_buf.as_slice())
        }
    }

    pub fn value(&self) -> Option<&[u8]> {
        if !self.view_valid || self.val_is_bucket {
            return None;
        }
        if self.mmap_view {
            Some(&self.mmap[self.val_off..self.val_off + self.val_len])
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
        self.mmap_view = false;
        self.key_len = 0;
        self.val_len = 0;
        self.key_buf.clear();
        self.val_buf.clear();
        self.val_is_bucket = false;
        self.leaf_pgid = 0;
        self.leaf_page_start = 0;
        self.leaf_count = 0;
        self.leaf_index = 0;
    }

    fn cache_leaf_from_stack(&mut self) {
        self.leaf_pgid = 0;
        self.leaf_page_start = 0;
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
        self.leaf_page_start = r.pgid as usize * self.page_size;
        self.leaf_count = hdr.count as usize;
        self.leaf_index = r.index;
    }

    /// Bind using the cached leaf page start (no page-header reparse).
    fn bind_mmap_leaf_cached(&mut self, index: usize) -> Result<()> {
        let elem_off = self.leaf_page_start + PAGE_HEADER_SIZE + index * LEAF_PAGE_ELEMENT_SIZE;
        let end = elem_off + LEAF_PAGE_ELEMENT_SIZE;
        if end > self.mmap.len() {
            return Err(Error::Corrupt("leaf element past mmap".into()));
        }
        let mmap = &self.mmap[..];
        let flags = read_u32(mmap, elem_off);
        let pos = read_u32(mmap, elem_off + 4) as usize;
        let ksize = read_u32(mmap, elem_off + 8) as usize;
        let vsize = read_u32(mmap, elem_off + 12) as usize;
        let key_off = elem_off + pos;
        if key_off + ksize + vsize > self.mmap.len() {
            return Err(Error::Corrupt("leaf kv past mmap".into()));
        }
        self.mmap_view = true;
        self.key_off = key_off;
        self.key_len = ksize;
        self.val_is_bucket = flags & BUCKET_LEAF_FLAG != 0;
        if self.val_is_bucket {
            self.val_off = 0;
            self.val_len = 0;
        } else {
            self.val_off = key_off + ksize;
            self.val_len = vsize;
        }
        self.view_valid = true;
        Ok(())
    }

    /// Bind view to a leaf element using integer offsets into the pinned mmap.
    fn bind_mmap_leaf(&mut self, pgid: Pgid, index: usize) -> Result<()> {
        self.leaf_page_start = pgid as usize * self.page_size;
        // Validate page exists / is a leaf once.
        let page = DbInner::page_bytes(&self.mmap, self.page_size, pgid)?;
        let hdr = PageHeader::read(page);
        if !hdr.is_leaf() {
            return Err(Error::Corrupt("expected leaf page".into()));
        }
        self.bind_mmap_leaf_cached(index)
    }

    fn bind_owned(&mut self, flags: u32, key: &[u8], val: &[u8]) {
        self.mmap_view = false;
        self.key_buf.clear();
        self.key_buf.extend_from_slice(key);
        self.val_is_bucket = flags & BUCKET_LEAF_FLAG != 0;
        self.val_buf.clear();
        if !self.val_is_bucket {
            self.val_buf.extend_from_slice(val);
        }
        self.view_valid = true;
    }

    fn bind_current_view(&mut self) -> Result<bool> {
        let Some(r) = self.stack.last().copied() else {
            self.clear_view();
            return Ok(false);
        };

        // On-disk leaf: bind via mmap byte offsets.
        if r.node_id.is_none() && r.pgid != 0 {
            let count = {
                let inner = self.tx.borrow();
                inner.ref_count(self.bucket, &r)?
            };
            if count == 0 || r.index >= count {
                self.clear_view();
                return Ok(false);
            }
            let pgid = if self.leaf_pgid != 0 {
                self.leaf_pgid
            } else {
                r.pgid
            };
            self.bind_mmap_leaf(pgid, r.index)?;
            return Ok(true);
        }

        let owned = {
            let inner = self.tx.borrow();
            let count = inner.ref_count(self.bucket, &r)?;
            if count == 0 || r.index >= count {
                None
            } else if let Some(id) = r.node_id {
                let inode = &inner.buckets[&self.bucket].nodes[&id].inodes[r.index];
                Some((inode.flags, inode.key.clone(), inode.value.clone()))
            } else {
                // Inline bucket page (not in mmap).
                let page = inner.buckets[&self.bucket]
                    .inline_page
                    .as_ref()
                    .ok_or_else(|| Error::Corrupt("inline missing".into()))?;
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
                self.bind_owned(flags, &key, &val);
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
