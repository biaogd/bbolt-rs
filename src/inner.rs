//! In-memory transaction state: nodes, buckets, cursors, commit.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use memmap2::Mmap;

use crate::db::DbInner;
use crate::error::{Error, Result};
use crate::stats::TxStats;
use crate::page::{
    branch_at, branch_pgid, inodes_size, inodes_size_less_than, leaf_at, read_inodes, write_inodes,
    write_meta_page,
    InBucket, Inode, Meta, PageHeader, Pgid, BUCKET_HEADER_SIZE, BUCKET_LEAF_FLAG,
    DEFAULT_FILL_PERCENT, LEAF_PAGE_FLAG, MAX_FILL_PERCENT, MAX_KEY_SIZE, MAX_VALUE_SIZE,
    MIN_FILL_PERCENT, MIN_KEYS_PER_PAGE, PAGE_HEADER_SIZE, PGID_NO_FREELIST,
};

pub type BucketId = u64;
pub type NodeId = u64;
pub(crate) type Kv = (Option<Vec<u8>>, Option<Vec<u8>>, u32);

#[derive(Clone, Debug)]
pub struct Node {
    pub is_leaf: bool,
    pub unbalanced: bool,
    pub spilled: bool,
    pub key: Vec<u8>,
    pub pgid: Pgid,
    pub overflow: u32,
    pub parent: Option<NodeId>,
    pub children: Vec<NodeId>,
    pub inodes: VecDeque<Inode>,
}

pub struct BucketInner {
    pub header: InBucket,
    pub nodes: NodeMap,
    pub nodes_by_pgid: HashMap<Pgid, NodeId>,
    pub root_node: Option<NodeId>,
    pub inline_page: Option<Vec<u8>>,
    pub subbuckets: HashMap<Vec<u8>, BucketId>,
    pub fill_percent: f64,
    pub next_node_id: NodeId,
}

/// Dense `NodeId` → `Node` store (IDs are allocated monotonically from 1).
#[derive(Default)]
pub struct NodeMap {
    slots: Vec<Option<Node>>,
}

impl NodeMap {
    fn new() -> Self {
        Self { slots: Vec::new() }
    }

    #[inline]
    fn get(&self, id: &NodeId) -> Option<&Node> {
        self.slots.get(*id as usize).and_then(|s| s.as_ref())
    }

    #[inline]
    fn get_mut(&mut self, id: &NodeId) -> Option<&mut Node> {
        self.slots.get_mut(*id as usize).and_then(|s| s.as_mut())
    }

    #[inline]
    fn insert(&mut self, id: NodeId, node: Node) {
        let i = id as usize;
        if self.slots.len() <= i {
            self.slots.resize_with(i + 1, || None);
        }
        self.slots[i] = Some(node);
    }

    #[inline]
    fn remove(&mut self, id: &NodeId) -> Option<Node> {
        self.slots.get_mut(*id as usize).and_then(|s| s.take())
    }

    fn iter_ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(i, n)| n.as_ref().map(|_| i as NodeId))
    }

    #[allow(dead_code)]
    fn values_mut(&mut self) -> impl Iterator<Item = &mut Node> {
        self.slots.iter_mut().filter_map(|s| s.as_mut())
    }
}

impl std::ops::Index<&NodeId> for NodeMap {
    type Output = Node;
    #[inline]
    fn index(&self, id: &NodeId) -> &Node {
        self.slots[*id as usize].as_ref().unwrap()
    }
}

impl std::ops::IndexMut<&NodeId> for NodeMap {
    #[inline]
    fn index_mut(&mut self, id: &NodeId) -> &mut Node {
        self.slots[*id as usize].as_mut().unwrap()
    }
}

impl BucketInner {
    fn new(header: InBucket) -> Self {
        Self {
            header,
            nodes: NodeMap::new(),
            nodes_by_pgid: HashMap::new(),
            root_node: None,
            inline_page: None,
            subbuckets: HashMap::new(),
            fill_percent: DEFAULT_FILL_PERCENT,
            next_node_id: 1,
        }
    }

    fn alloc_node(&mut self) -> NodeId {
        let id = self.next_node_id;
        self.next_node_id += 1;
        id
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ElemRef {
    pub node_id: Option<NodeId>,
    pub pgid: Pgid,
    pub index: usize,
    /// Cached element count for this page/node (-1 = unknown).
    pub count: i32,
}

impl ElemRef {
    fn disk(pgid: Pgid, index: usize) -> Self {
        Self {
            node_id: None,
            pgid,
            index,
            count: -1,
        }
    }

    fn node(node_id: NodeId, pgid: Pgid, index: usize, count: usize) -> Self {
        Self {
            node_id: Some(node_id),
            pgid,
            index,
            count: count as i32,
        }
    }
}

pub struct TxInner {
    pub db: Arc<DbInner>,
    pub writable: bool,
    pub managed: bool,
    pub closed: bool,
    pub meta: Meta,
    pub dirty: HashMap<Pgid, Vec<u8>>,
    pub buckets: HashMap<BucketId, BucketInner>,
    pub next_bucket_id: BucketId,
    pub root: BucketId,
    pub hold_writer: bool,
    pub commit_handlers: Vec<Box<dyn FnOnce()>>,
    pub stats: TxStats,
    /// Stable mmap view for this transaction (zero-copy page reads).
    pub(crate) mmap_pin: Arc<Mmap>,
    /// Reused search stack for Get/has_value (avoids per-op Vec alloc).
    get_stack: Vec<ElemRef>,
    /// Hint for sequential deletes: last leaf node we deleted from.
    delete_hint: Option<(BucketId, NodeId)>,
    /// Hint for sequential puts: last leaf node we appended to.
    put_hint: Option<(BucketId, NodeId)>,
}

enum PageNode {
    Node(NodeId),
    /// On-disk page; read via [`TxInner::with_page`] (no owned copy).
    Disk(Pgid),
    /// Inline bucket page stored in memory.
    Inline,
}

impl TxInner {
    pub fn new(db: Arc<DbInner>, meta: Meta, writable: bool) -> Self {
        let mmap_pin = db
            .pin_mmap()
            .expect("mmap must be mapped when starting a transaction");
        let mut buckets = HashMap::new();
        buckets.insert(0, BucketInner::new(meta.root));
        Self {
            db,
            writable,
            managed: false,
            closed: false,
            meta,
            dirty: HashMap::new(),
            buckets,
            next_bucket_id: 1,
            root: 0,
            hold_writer: writable,
            commit_handlers: Vec::new(),
            stats: TxStats::default(),
            mmap_pin,
            get_stack: Vec::with_capacity(16),
            delete_hint: None,
            put_hint: None,
        }
    }

    pub fn check_open(&self) -> Result<()> {
        if self.closed || !self.db.opened.load(std::sync::atomic::Ordering::SeqCst) {
            Err(Error::TxClosed)
        } else {
            Ok(())
        }
    }

    pub fn check_writable(&self) -> Result<()> {
        self.check_open()?;
        if !self.writable {
            Err(Error::TxNotWritable)
        } else {
            Ok(())
        }
    }

    fn read_page(&self, pgid: Pgid) -> Result<Vec<u8>> {
        if let Some(p) = self.dirty.get(&pgid) {
            return Ok(p.clone());
        }
        Ok(DbInner::page_bytes(&self.mmap_pin, self.db.page_size, pgid)?.to_vec())
    }

    /// Borrow a page for the duration of `f` (dirty copy or zero-copy mmap slice).
    fn with_page<R>(&self, pgid: Pgid, f: impl FnOnce(&[u8]) -> R) -> Result<R> {
        if !self.dirty.is_empty() {
            if let Some(p) = self.dirty.get(&pgid) {
                return Ok(f(p));
            }
        }
        Ok(f(DbInner::page_bytes(
            &self.mmap_pin,
            self.db.page_size,
            pgid,
        )?))
    }

    /// Refresh the mmap pin after the file mapping may have grown.
    pub(crate) fn refresh_mmap_pin(&mut self) -> Result<()> {
        self.mmap_pin = self.db.pin_mmap()?;
        Ok(())
    }

    fn page_node(&self, bid: BucketId, pgid: Pgid) -> Result<PageNode> {
        let b = self.buckets.get(&bid).ok_or(Error::TxClosed)?;
        if b.header.root == 0 {
            if pgid != 0 {
                panic!("inline bucket non-zero page access: {pgid}");
            }
            if let Some(id) = b.root_node {
                return Ok(PageNode::Node(id));
            }
            return Ok(PageNode::Inline);
        }
        if !b.nodes_by_pgid.is_empty() {
            if let Some(&id) = b.nodes_by_pgid.get(&pgid) {
                return Ok(PageNode::Node(id));
            }
        }
        Ok(PageNode::Disk(pgid))
    }

    fn materialize_node(
        &mut self,
        bid: BucketId,
        pgid: Pgid,
        parent: Option<NodeId>,
    ) -> Result<NodeId> {
        {
            let b = self.buckets.get(&bid).ok_or(Error::TxClosed)?;
            if pgid == 0 {
                if let Some(id) = b.root_node {
                    return Ok(id);
                }
            } else if let Some(&id) = b.nodes_by_pgid.get(&pgid) {
                return Ok(id);
            }
        }
        let page = if pgid == 0 {
            self.buckets[&bid]
                .inline_page
                .clone()
                .ok_or_else(|| Error::Corrupt("inline page missing".into()))?
        } else {
            self.read_page(pgid)?
        };
        let hdr = PageHeader::read(&page);
        let inodes: VecDeque<Inode> = read_inodes(&page).into();
        let b = self.buckets.get_mut(&bid).unwrap();
        let id = b.alloc_node();
        let key = inodes.front().map(|i| i.key.clone()).unwrap_or_default();
        b.nodes.insert(
            id,
            Node {
                is_leaf: hdr.is_leaf(),
                unbalanced: false,
                spilled: false,
                key,
                pgid,
                overflow: hdr.overflow,
                parent,
                children: Vec::new(),
                inodes,
            },
        );
        if pgid != 0 {
            b.nodes_by_pgid.insert(pgid, id);
        }
        if parent.is_none() {
            b.root_node = Some(id);
        } else if let Some(p) = parent {
            b.nodes.get_mut(&p).unwrap().children.push(id);
        }
        Ok(id)
    }

    fn child_at(&mut self, bid: BucketId, parent: NodeId, index: usize) -> Result<NodeId> {
        let pgid = self.buckets[&bid].nodes[&parent].inodes[index].pgid;
        self.materialize_node(bid, pgid, Some(parent))
    }

    #[allow(clippy::too_many_arguments)]
    fn node_put(
        &mut self,
        bid: BucketId,
        nid: NodeId,
        old_key: &[u8],
        new_key: &[u8],
        value: &[u8],
        pgid: Pgid,
        flags: u32,
    ) {
        assert!(!old_key.is_empty(), "put: zero-length old key");
        assert!(!new_key.is_empty(), "put: zero-length new key");
        if pgid >= self.meta.pgid && pgid != 0 {
            panic!("pgId ({pgid}) above high water mark ({})", self.meta.pgid);
        }
        let n = self
            .buckets
            .get_mut(&bid)
            .unwrap()
            .nodes
            .get_mut(&nid)
            .unwrap();
        // Fast path: sequential append (common for bulk loads).
        if let Some(last) = n.inodes.back() {
            if last.key.as_slice() < old_key {
                if n.inodes.len() == n.inodes.capacity() {
                    // Grow in page-sized chunks to cut realloc traffic on bulk loads.
                    n.inodes.reserve((self.db.page_size / 32).max(64));
                }
                n.inodes.push_back(Inode {
                    flags,
                    pgid,
                    key: new_key.to_vec(),
                    value: value.to_vec(),
                });
                return;
            }
            if last.key.as_slice() == old_key {
                let inode = n.inodes.back_mut().unwrap();
                inode.flags = flags;
                inode.key = new_key.to_vec();
                inode.value = value.to_vec();
                inode.pgid = pgid;
                return;
            }
        } else {
            n.inodes.push_back(Inode {
                flags,
                pgid,
                key: new_key.to_vec(),
                value: value.to_vec(),
            });
            return;
        }
        let index = n.inodes.partition_point(|ino| ino.key.as_slice() < old_key);
        let exact = index < n.inodes.len() && n.inodes[index].key == old_key;
        if !exact {
            n.inodes.insert(index, Inode::default());
        }
        let inode = &mut n.inodes[index];
        inode.flags = flags;
        inode.key = new_key.to_vec();
        inode.value = value.to_vec();
        inode.pgid = pgid;
    }

    pub(crate) fn node_del(&mut self, bid: BucketId, nid: NodeId, key: &[u8]) {
        let n = self
            .buckets
            .get_mut(&bid)
            .unwrap()
            .nodes
            .get_mut(&nid)
            .unwrap();
        let index = n.inodes.partition_point(|ino| ino.key.as_slice() < key);
        if index >= n.inodes.len() || n.inodes[index].key != key {
            return;
        }
        Self::inode_remove(&mut n.inodes, index);
        n.unbalanced = true;
    }

    pub(crate) fn node_del_at(&mut self, bid: BucketId, nid: NodeId, index: usize) {
        let n = self
            .buckets
            .get_mut(&bid)
            .unwrap()
            .nodes
            .get_mut(&nid)
            .unwrap();
        if index >= n.inodes.len() {
            return;
        }
        Self::inode_remove(&mut n.inodes, index);
        n.unbalanced = true;
    }

    #[inline]
    fn inode_remove(inodes: &mut VecDeque<Inode>, index: usize) {
        if index == 0 {
            inodes.pop_front();
        } else if index + 1 == inodes.len() {
            inodes.pop_back();
        } else {
            inodes.remove(index);
        }
    }

    pub fn cursor_seek(
        &mut self,
        bid: BucketId,
        stack: &mut Vec<ElemRef>,
        key: &[u8],
    ) -> Result<Kv> {
        self.check_open()?;
        stack.clear();
        let root = self.buckets[&bid].header.root;
        self.search(bid, stack, key, root)?;
        self.key_value(bid, stack)
    }

    pub fn cursor_seek_may_next(
        &mut self,
        bid: BucketId,
        stack: &mut Vec<ElemRef>,
        key: &[u8],
    ) -> Result<Kv> {
        let (mut k, mut v, mut flags) = self.cursor_seek(bid, stack, key)?;
        if let Some(last) = stack.last() {
            if last.index >= self.ref_count(bid, last)? {
                let n = self.cursor_next(bid, stack)?;
                k = n.0;
                v = n.1;
                flags = n.2;
            }
        }
        Ok((k, v, flags))
    }

    pub(crate) fn search(
        &mut self,
        bid: BucketId,
        stack: &mut Vec<ElemRef>,
        key: &[u8],
        pgid: Pgid,
    ) -> Result<()> {
        match self.page_node(bid, pgid)? {
            PageNode::Node(id) => {
                let count = self.buckets[&bid].nodes[&id].inodes.len();
                stack.push(ElemRef::node(id, pgid, 0, count));
                let is_leaf = self.buckets[&bid].nodes[&id].is_leaf;
                if is_leaf {
                    self.nsearch_node(bid, stack, key, id);
                    return Ok(());
                }
                self.search_node(bid, stack, key, id)
            }
            PageNode::Disk(pgid) => {
                stack.push(ElemRef::disk(pgid, 0));
                let (is_leaf, child) = {
                    let page = if let Some(p) = self.dirty.get(&pgid) {
                        p.as_slice()
                    } else {
                        DbInner::page_bytes(&self.mmap_pin, self.db.page_size, pgid)?
                    };
                    let hdr = PageHeader::read(page);
                    if !hdr.is_branch() && !hdr.is_leaf() {
                        panic!("invalid page type: {}: {:x}", hdr.id, hdr.flags);
                    }
                    stack.last_mut().unwrap().count = hdr.count as i32;
                    if hdr.is_leaf() {
                        nsearch_page(stack, page, key);
                        (true, 0)
                    } else {
                        let child = search_page_index(stack, page, key);
                        (false, child)
                    }
                };
                if is_leaf {
                    return Ok(());
                }
                self.search(bid, stack, key, child)
            }
            PageNode::Inline => {
                stack.push(ElemRef::disk(0, 0));
                let child = {
                    let page = self.buckets[&bid]
                        .inline_page
                        .as_ref()
                        .ok_or_else(|| Error::Corrupt("inline bucket missing page".into()))?;
                    let hdr = PageHeader::read(page);
                    stack.last_mut().unwrap().count = hdr.count as i32;
                    if hdr.is_leaf() {
                        nsearch_page(stack, page, key);
                        None
                    } else {
                        Some(search_page_index(stack, page, key))
                    }
                };
                if let Some(child) = child {
                    self.search(bid, stack, key, child)?;
                }
                Ok(())
            }
        }
    }

    fn search_node(
        &mut self,
        bid: BucketId,
        stack: &mut Vec<ElemRef>,
        key: &[u8],
        nid: NodeId,
    ) -> Result<()> {
        let inodes = &self.buckets[&bid].nodes[&nid].inodes;
        let mut exact = false;
        let mut index = inodes.partition_point(|ino| ino.key.as_slice() < key);
        if index < inodes.len() && inodes[index].key == key {
            exact = true;
        }
        if !exact && index > 0 {
            index -= 1;
        }
        if index >= inodes.len() {
            index = inodes.len().saturating_sub(1);
        }
        let child = inodes[index].pgid;
        stack.last_mut().unwrap().index = index;
        self.search(bid, stack, key, child)
    }

    fn nsearch_node(&self, bid: BucketId, stack: &mut [ElemRef], key: &[u8], nid: NodeId) {
        let inodes = &self.buckets[&bid].nodes[&nid].inodes;
        let index = inodes.partition_point(|ino| ino.key.as_slice() < key);
        stack.last_mut().unwrap().index = index;
    }

    pub(crate) fn ref_count(&self, bid: BucketId, r: &ElemRef) -> Result<usize> {
        // Always re-read materialized nodes — inode count changes on put/delete.
        if let Some(id) = r.node_id {
            return Ok(self.buckets[&bid].nodes[&id].inodes.len());
        }
        if r.count >= 0 {
            return Ok(r.count as usize);
        }
        if self.buckets[&bid].header.root == 0 {
            let page = self.buckets[&bid]
                .inline_page
                .as_ref()
                .ok_or_else(|| Error::Corrupt("inline missing".into()))?;
            Ok(PageHeader::read(page).count as usize)
        } else {
            self.with_page(r.pgid, |page| PageHeader::read(page).count as usize)
        }
    }

    fn fill_ref_count(&self, bid: BucketId, r: &mut ElemRef) -> Result<usize> {
        if r.count >= 0 {
            return Ok(r.count as usize);
        }
        let c = self.ref_count(bid, r)?;
        r.count = c as i32;
        Ok(c)
    }

    fn ref_is_leaf(&self, bid: BucketId, r: &ElemRef) -> Result<bool> {
        if let Some(id) = r.node_id {
            Ok(self.buckets[&bid].nodes[&id].is_leaf)
        } else if self.buckets[&bid].header.root == 0 {
            let page = self.buckets[&bid].inline_page.as_ref().unwrap();
            Ok(PageHeader::read(page).is_leaf())
        } else {
            self.with_page(r.pgid, |page| PageHeader::read(page).is_leaf())
        }
    }

    pub fn key_value(&self, bid: BucketId, stack: &[ElemRef]) -> Result<Kv> {
        let mut kbuf = Vec::new();
        let mut vbuf = Vec::new();
        let (ok, flags, has_val) = self.key_value_into(bid, stack, &mut kbuf, &mut vbuf)?;
        if !ok {
            return Ok((None, None, 0));
        }
        let v = if has_val { Some(vbuf) } else { None };
        Ok((Some(kbuf), v, flags))
    }

    /// Copy current cursor KV into caller buffers (avoids intermediate allocations).
    /// Returns `(present, flags, has_value)`.
    pub fn key_value_into(
        &self,
        bid: BucketId,
        stack: &[ElemRef],
        key_buf: &mut Vec<u8>,
        val_buf: &mut Vec<u8>,
    ) -> Result<(bool, u32, bool)> {
        key_buf.clear();
        val_buf.clear();
        let Some(r) = stack.last() else {
            return Ok((false, 0, false));
        };
        let count = self.ref_count(bid, r)?;
        if count == 0 || r.index >= count {
            return Ok((false, 0, false));
        }
        if let Some(id) = r.node_id {
            let inode = &self.buckets[&bid].nodes[&id].inodes[r.index];
            let flags = inode.flags;
            key_buf.extend_from_slice(&inode.key);
            let has_val = flags & BUCKET_LEAF_FLAG == 0;
            if has_val {
                val_buf.extend_from_slice(&inode.value);
            }
            return Ok((true, flags, has_val));
        }
        if self.buckets[&bid].header.root == 0 {
            let page_ref = self.buckets[&bid].inline_page.as_ref().unwrap();
            let (flags, key, val) = leaf_at(page_ref, r.index);
            key_buf.extend_from_slice(key);
            let has_val = flags & BUCKET_LEAF_FLAG == 0;
            if has_val {
                val_buf.extend_from_slice(val);
            }
            return Ok((true, flags, has_val));
        }
        self.with_page(r.pgid, |page_ref| {
            let (flags, key, val) = leaf_at(page_ref, r.index);
            key_buf.extend_from_slice(key);
            let has_val = flags & BUCKET_LEAF_FLAG == 0;
            if has_val {
                val_buf.extend_from_slice(val);
            }
            (true, flags, has_val)
        })
    }

    /// Raw key/value including bucket payload (for open_bucket).
    fn key_value_raw(&self, bid: BucketId, stack: &[ElemRef]) -> Result<Kv> {
        let Some(r) = stack.last() else {
            return Ok((None, None, 0));
        };
        let count = self.ref_count(bid, r)?;
        if count == 0 || r.index >= count {
            return Ok((None, None, 0));
        }
        if let Some(id) = r.node_id {
            let inode = &self.buckets[&bid].nodes[&id].inodes[r.index];
            return Ok((
                Some(inode.key.clone()),
                Some(inode.value.clone()),
                inode.flags,
            ));
        }
        if self.buckets[&bid].header.root == 0 {
            let page_ref = self.buckets[&bid].inline_page.as_ref().unwrap();
            let (flags, key, val) = leaf_at(page_ref, r.index);
            return Ok((Some(key.to_vec()), Some(val.to_vec()), flags));
        }
        self.with_page(r.pgid, |page_ref| {
            let (flags, key, val) = leaf_at(page_ref, r.index);
            (Some(key.to_vec()), Some(val.to_vec()), flags)
        })
    }

    fn go_to_first(&self, bid: BucketId, stack: &mut Vec<ElemRef>) -> Result<()> {
        loop {
            let last = stack.last().cloned().unwrap();
            if self.ref_is_leaf(bid, &last)? {
                break;
            }
            let pgid = self.child_pgid(bid, &last)?;
            match self.page_node(bid, pgid)? {
                PageNode::Node(id) => {
                    let count = self.buckets[&bid].nodes[&id].inodes.len();
                    stack.push(ElemRef::node(id, pgid, 0, count));
                }
                PageNode::Disk(pgid) => {
                    let count = self.with_page(pgid, |page| PageHeader::read(page).count as i32)?;
                    let mut er = ElemRef::disk(pgid, 0);
                    er.count = count;
                    stack.push(er);
                }
                PageNode::Inline => {
                    let count = self.buckets[&bid]
                        .inline_page
                        .as_ref()
                        .map(|p| PageHeader::read(p).count as i32)
                        .unwrap_or(0);
                    let mut er = ElemRef::disk(0, 0);
                    er.count = count;
                    stack.push(er);
                }
            }
        }
        Ok(())
    }

    fn child_pgid(&self, bid: BucketId, r: &ElemRef) -> Result<Pgid> {
        if let Some(id) = r.node_id {
            Ok(self.buckets[&bid].nodes[&id].inodes[r.index].pgid)
        } else if self.buckets[&bid].header.root == 0 {
            let page = self.buckets[&bid].inline_page.as_ref().unwrap();
            Ok(branch_pgid(page, r.index))
        } else {
            self.with_page(r.pgid, |page| branch_pgid(page, r.index))
        }
    }

    pub fn cursor_first(&mut self, bid: BucketId, stack: &mut Vec<ElemRef>) -> Result<Kv> {
        if !self.cursor_first_move(bid, stack)? {
            return Ok((None, None, 0));
        }
        self.key_value(bid, stack)
    }

    pub fn cursor_first_move(&self, bid: BucketId, stack: &mut Vec<ElemRef>) -> Result<bool> {
        self.check_open()?;
        stack.clear();
        let root = self.buckets[&bid].header.root;
        match self.page_node(bid, root)? {
            PageNode::Node(id) => {
                let count = self.buckets[&bid].nodes[&id].inodes.len();
                stack.push(ElemRef::node(id, root, 0, count));
            }
            PageNode::Disk(pgid) => {
                let count = self.with_page(pgid, |page| PageHeader::read(page).count as i32)?;
                let mut er = ElemRef::disk(pgid, 0);
                er.count = count;
                stack.push(er);
            }
            PageNode::Inline => {
                let count = self.buckets[&bid]
                    .inline_page
                    .as_ref()
                    .map(|p| PageHeader::read(p).count as i32)
                    .unwrap_or(0);
                let mut er = ElemRef::disk(0, 0);
                er.count = count;
                stack.push(er);
            }
        }
        self.go_to_first(bid, stack)?;
        if let Some(last) = stack.last_mut() {
            let count = self.fill_ref_count(bid, last)?;
            if count == 0 {
                return self.cursor_next_move(bid, stack);
            }
        }
        Ok(!stack.is_empty())
    }

    pub fn cursor_last(&mut self, bid: BucketId, stack: &mut Vec<ElemRef>) -> Result<Kv> {
        self.check_open()?;
        stack.clear();
        let root = self.buckets[&bid].header.root;
        match self.page_node(bid, root)? {
            PageNode::Node(id) => {
                let count = self.buckets[&bid].nodes[&id].inodes.len();
                stack.push(ElemRef::node(
                    id,
                    root,
                    count.saturating_sub(1),
                    count,
                ));
            }
            PageNode::Disk(pgid) => {
                let count = self.with_page(pgid, |page| PageHeader::read(page).count as usize)?;
                let mut er = ElemRef::disk(pgid, count.saturating_sub(1));
                er.count = count as i32;
                stack.push(er);
            }
            PageNode::Inline => {
                let count = self.buckets[&bid]
                    .inline_page
                    .as_ref()
                    .map(|p| PageHeader::read(p).count as usize)
                    .unwrap_or(0);
                let mut er = ElemRef::disk(0, count.saturating_sub(1));
                er.count = count as i32;
                stack.push(er);
            }
        }
        self.go_to_last(bid, stack)?;
        while stack.len() > 1 {
            let last = stack.last().cloned().unwrap();
            if self.ref_count(bid, &last)? == 0 {
                self.cursor_prev(bid, stack)?;
            } else {
                break;
            }
        }
        if stack.is_empty() {
            return Ok((None, None, 0));
        }
        self.key_value(bid, stack)
    }

    fn go_to_last(&mut self, bid: BucketId, stack: &mut Vec<ElemRef>) -> Result<()> {
        loop {
            let last = stack.last().cloned().unwrap();
            if self.ref_is_leaf(bid, &last)? {
                break;
            }
            let pgid = self.child_pgid(bid, &last)?;
            match self.page_node(bid, pgid)? {
                PageNode::Node(id) => {
                    let count = self.buckets[&bid].nodes[&id].inodes.len();
                    stack.push(ElemRef::node(
                        id,
                        pgid,
                        count.saturating_sub(1),
                        count,
                    ));
                }
                PageNode::Disk(pgid) => {
                    let count = self.with_page(pgid, |page| PageHeader::read(page).count as usize)?;
                    let mut er = ElemRef::disk(pgid, count.saturating_sub(1));
                    er.count = count as i32;
                    stack.push(er);
                }
                PageNode::Inline => {
                    let count = self.buckets[&bid]
                        .inline_page
                        .as_ref()
                        .map(|p| PageHeader::read(p).count as usize)
                        .unwrap_or(0);
                    let mut er = ElemRef::disk(0, count.saturating_sub(1));
                    er.count = count as i32;
                    stack.push(er);
                }
            }
        }
        Ok(())
    }

    pub fn cursor_next(&mut self, bid: BucketId, stack: &mut Vec<ElemRef>) -> Result<Kv> {
        if !self.cursor_next_move(bid, stack)? {
            return Ok((None, None, 0));
        }
        self.key_value(bid, stack)
    }

    pub fn cursor_next_move(&self, bid: BucketId, stack: &mut Vec<ElemRef>) -> Result<bool> {
        loop {
            let mut i = stack.len() as isize - 1;
            while i >= 0 {
                let idx = i as usize;
                let count = self.fill_ref_count(bid, &mut stack[idx])?;
                if stack[idx].index + 1 < count {
                    stack[idx].index += 1;
                    // Stayed on the same page — no need to descend again.
                    if idx == stack.len() - 1 {
                        return Ok(true);
                    }
                    break;
                }
                i -= 1;
            }
            if i == -1 {
                stack.clear();
                return Ok(false);
            }
            stack.truncate(i as usize + 1);
            self.go_to_first(bid, stack)?;
            let count = {
                let last = stack.last_mut().unwrap();
                self.fill_ref_count(bid, last)?
            };
            if count == 0 {
                continue;
            }
            return Ok(true);
        }
    }

    pub fn cursor_prev(&mut self, bid: BucketId, stack: &mut Vec<ElemRef>) -> Result<Kv> {
        let mut i = stack.len() as isize - 1;
        while i >= 0 {
            let idx = i as usize;
            if stack[idx].index > 0 {
                stack[idx].index -= 1;
                break;
            }
            if stack.len() == 1 {
                let _ = self.cursor_first(bid, stack)?;
                return Ok((None, None, 0));
            }
            stack.truncate(idx);
            i -= 1;
        }
        if stack.is_empty() {
            return Ok((None, None, 0));
        }
        self.go_to_last(bid, stack)?;
        self.key_value(bid, stack)
    }

    pub fn cursor_node(&mut self, bid: BucketId, stack: &mut [ElemRef]) -> Result<NodeId> {
        assert!(
            !stack.is_empty(),
            "accessing a node with a zero-length cursor stack"
        );
        if let Some(id) = stack.last().unwrap().node_id {
            if self.buckets[&bid].nodes[&id].is_leaf {
                return Ok(id);
            }
        }
        let first_pgid = stack[0].pgid;
        let mut n = if let Some(id) = stack[0].node_id {
            id
        } else {
            let id = self.materialize_node(bid, first_pgid, None)?;
            stack[0].node_id = Some(id);
            id
        };
        for i in 0..stack.len() - 1 {
            let idx = stack[i].index;
            n = self.child_at(bid, n, idx)?;
            stack[i + 1].node_id = Some(n);
        }
        Ok(n)
    }

    fn open_bucket(&mut self, value: &[u8]) -> BucketId {
        let header = InBucket::read(value);
        let id = self.next_bucket_id;
        self.next_bucket_id += 1;
        let mut b = BucketInner::new(header);
        if header.root == 0 && value.len() >= BUCKET_HEADER_SIZE {
            b.inline_page = Some(value[BUCKET_HEADER_SIZE..].to_vec());
        }
        self.buckets.insert(id, b);
        id
    }

    pub fn bucket_by_name(&mut self, parent: BucketId, name: &[u8]) -> Result<Option<BucketId>> {
        self.check_open()?;
        if let Some(&id) = self.buckets[&parent].subbuckets.get(name) {
            return Ok(Some(id));
        }
        let mut stack = Vec::new();
        let (k, v, flags) = {
            self.cursor_seek(parent, &mut stack, name)?;
            self.key_value_raw(parent, &stack)?
        };
        if k.as_deref() != Some(name) || flags & BUCKET_LEAF_FLAG == 0 {
            return Ok(None);
        }
        let value = v.unwrap();
        let child = self.open_bucket(&value);
        self.buckets
            .get_mut(&parent)
            .unwrap()
            .subbuckets
            .insert(name.to_vec(), child);
        Ok(Some(child))
    }

    fn empty_inline_bucket_value() -> Vec<u8> {
        let header = InBucket::new(0, 0);
        let mut value = vec![0u8; BUCKET_HEADER_SIZE + PAGE_HEADER_SIZE];
        header.write(&mut value[..BUCKET_HEADER_SIZE]);
        crate::page::set_page_flags(&mut value[BUCKET_HEADER_SIZE..], LEAF_PAGE_FLAG);
        crate::page::set_page_count(&mut value[BUCKET_HEADER_SIZE..], 0);
        value
    }

    pub fn create_bucket(&mut self, parent: BucketId, name: &[u8]) -> Result<BucketId> {
        self.check_writable()?;
        if name.is_empty() {
            return Err(Error::BucketNameRequired);
        }
        if name.len() > MAX_KEY_SIZE {
            return Err(Error::KeyTooLarge);
        }
        let new_key = name.to_vec();
        let mut stack = Vec::new();
        let (k, _, flags) = self.cursor_seek(parent, &mut stack, &new_key)?;
        if k.as_deref() == Some(new_key.as_slice()) {
            if flags & BUCKET_LEAF_FLAG != 0 {
                return Err(Error::BucketExists);
            }
            return Err(Error::IncompatibleValue);
        }
        let value = Self::empty_inline_bucket_value();
        let nid = self.cursor_node(parent, &mut stack)?;
        self.node_put(parent, nid, &new_key, &new_key, &value, 0, BUCKET_LEAF_FLAG);
        self.buckets.get_mut(&parent).unwrap().inline_page = None;
        self.bucket_by_name(parent, &new_key)?
            .ok_or_else(|| Error::Corrupt("created bucket missing".into()))
    }

    pub fn create_bucket_if_not_exists(
        &mut self,
        parent: BucketId,
        name: &[u8],
    ) -> Result<BucketId> {
        self.check_writable()?;
        if name.is_empty() {
            return Err(Error::BucketNameRequired);
        }
        if let Some(&id) = self.buckets[&parent].subbuckets.get(name) {
            return Ok(id);
        }
        match self.create_bucket(parent, name) {
            Ok(id) => Ok(id),
            Err(Error::BucketExists) => self
                .bucket_by_name(parent, name)?
                .ok_or(Error::BucketExists),
            Err(e) => Err(e),
        }
    }

    pub fn delete_bucket(&mut self, parent: BucketId, name: &[u8]) -> Result<()> {
        self.check_writable()?;
        let new_key = name.to_vec();
        let mut stack = Vec::new();
        let (k, _, flags) = self.cursor_seek(parent, &mut stack, &new_key)?;
        if k.as_deref() != Some(new_key.as_slice()) {
            return Err(Error::BucketNotFound);
        }
        if flags & BUCKET_LEAF_FLAG == 0 {
            return Err(Error::IncompatibleValue);
        }
        let child = self
            .bucket_by_name(parent, &new_key)?
            .ok_or(Error::BucketNotFound)?;
        let nested: Vec<Vec<u8>> = {
            let mut names = Vec::new();
            let mut st = Vec::new();
            let mut kv = self.cursor_first(child, &mut st)?;
            while let Some(k) = kv.0.clone() {
                if kv.2 & BUCKET_LEAF_FLAG != 0 {
                    names.push(k);
                }
                kv = self.cursor_next(child, &mut st)?;
            }
            names
        };
        for n in nested {
            self.delete_bucket(child, &n)?;
        }
        self.buckets
            .get_mut(&parent)
            .unwrap()
            .subbuckets
            .remove(&new_key);
        self.free_bucket(child)?;
        let nid = self.cursor_node(parent, &mut stack)?;
        self.node_del(parent, nid, &new_key);
        Ok(())
    }

    fn free_bucket(&mut self, bid: BucketId) -> Result<()> {
        if self.buckets[&bid].header.root == 0 {
            return Ok(());
        }
        let mut pages: Vec<(Pgid, u32)> = Vec::new();
        self.collect_pages(bid, self.buckets[&bid].header.root, &mut pages)?;
        let txid = self.meta.txid;
        {
            let mut fl = self.db.freelist.lock();
            for (pgid, overflow) in pages {
                if pgid > 1 {
                    fl.free(txid, pgid, overflow);
                }
            }
        }
        for nid in self.buckets[&bid].nodes.iter_ids().collect::<Vec<_>>() {
            let n = self
                .buckets
                .get_mut(&bid)
                .unwrap()
                .nodes
                .get_mut(&nid)
                .unwrap();
            n.pgid = 0;
        }
        self.buckets.get_mut(&bid).unwrap().header.root = 0;
        Ok(())
    }

    fn collect_pages(&self, bid: BucketId, pgid: Pgid, out: &mut Vec<(Pgid, u32)>) -> Result<()> {
        match self.page_node(bid, pgid)? {
            PageNode::Node(id) => {
                let n = &self.buckets[&bid].nodes[&id];
                if n.pgid != 0 {
                    out.push((n.pgid, n.overflow));
                }
                if !n.is_leaf {
                    let children: Vec<Pgid> = n.inodes.iter().map(|i| i.pgid).collect();
                    for c in children {
                        self.collect_pages(bid, c, out)?;
                    }
                }
            }
            PageNode::Disk(pgid) => {
                let (id, overflow, is_branch, children) = self.with_page(pgid, |page| {
                    let hdr = PageHeader::read(page);
                    let children = if hdr.is_branch() {
                        (0..hdr.count as usize)
                            .map(|i| branch_pgid(page, i))
                            .collect::<Vec<_>>()
                    } else {
                        Vec::new()
                    };
                    (hdr.id, hdr.overflow, hdr.is_branch(), children)
                })?;
                if id > 1 {
                    out.push((id, overflow));
                }
                if is_branch {
                    for c in children {
                        self.collect_pages(bid, c, out)?;
                    }
                }
            }
            PageNode::Inline => {}
        }
        Ok(())
    }

    pub fn put(&mut self, bid: BucketId, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_writable()?;
        if key.is_empty() {
            return Err(Error::KeyRequired);
        }
        if key.len() > MAX_KEY_SIZE {
            return Err(Error::KeyTooLarge);
        }
        if value.len() > MAX_VALUE_SIZE {
            return Err(Error::ValueTooLarge);
        }
        // Fast path: sequential appends stay on the same leaf (bulk loads).
        if let Some((hbid, nid)) = self.put_hint {
            if hbid == bid {
                let can_append = self.buckets.get(&bid).and_then(|b| b.nodes.get(&nid)).is_some_and(
                    |n| {
                        n.is_leaf
                            && n.inodes
                                .back()
                                .is_none_or(|last| last.key.as_slice() < key)
                    },
                );
                if can_append {
                    // Ensure we are not colliding with a nested bucket at `key`
                    // (impossible on a pure append past last key, but keep the check cheap).
                    self.node_put(bid, nid, key, key, value, 0, 0);
                    return Ok(());
                }
            }
        }
        let mut stack = std::mem::take(&mut self.get_stack);
        stack.clear();
        let result = (|| {
            let (k, _, flags) = self.cursor_seek(bid, &mut stack, key)?;
            if k.as_deref() == Some(key) && flags & BUCKET_LEAF_FLAG != 0 {
                return Err(Error::IncompatibleValue);
            }
            let nid = self.cursor_node(bid, &mut stack)?;
            self.node_put(bid, nid, key, key, value, 0, 0);
            self.put_hint = Some((bid, nid));
            Ok(())
        })();
        self.get_stack = stack;
        result
    }

    pub fn get(&mut self, bid: BucketId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.check_open()?;
        let mut stack = std::mem::take(&mut self.get_stack);
        stack.clear();
        let result = (|| {
            let root = self.buckets[&bid].header.root;
            self.search(bid, &mut stack, key, root)?;
            let Some(r) = stack.last() else {
                return Ok(None);
            };
            let count = self.ref_count(bid, r)?;
            if count == 0 || r.index >= count {
                return Ok(None);
            }
            if let Some(id) = r.node_id {
                let inode = &self.buckets[&bid].nodes[&id].inodes[r.index];
                if inode.flags & BUCKET_LEAF_FLAG != 0 || inode.key.as_slice() != key {
                    return Ok(None);
                }
                return Ok(Some(inode.value.clone()));
            }
            if self.buckets[&bid].header.root == 0 {
                let page = self.buckets[&bid].inline_page.as_ref().unwrap();
                let (flags, k, v) = leaf_at(page, r.index);
                if flags & BUCKET_LEAF_FLAG != 0 || k != key {
                    return Ok(None);
                }
                return Ok(Some(v.to_vec()));
            }
            self.with_page(r.pgid, |page| {
                let (flags, k, v) = leaf_at(page, r.index);
                if flags & BUCKET_LEAF_FLAG != 0 || k != key {
                    None
                } else {
                    Some(v.to_vec())
                }
            })
        })();
        self.get_stack = stack;
        result
    }

    /// Like Go `Bucket.Get(key) != nil` without allocating value bytes.
    pub fn has_value(&self, bid: BucketId, key: &[u8]) -> Result<bool> {
        if self.closed {
            return Err(Error::TxClosed);
        }
        let b = self.buckets.get(&bid).ok_or(Error::TxClosed)?;
        let mut pgid = b.header.root;
        if pgid == 0 {
            let page = b
                .inline_page
                .as_ref()
                .ok_or_else(|| Error::Corrupt("inline missing".into()))?;
            return Ok(leaf_has_value(page, key));
        }
        loop {
            if !b.nodes_by_pgid.is_empty() {
                if let Some(id) = b.nodes_by_pgid.get(&pgid).copied() {
                    let n = &b.nodes[&id];
                    if n.is_leaf {
                        let index = n.inodes.partition_point(|ino| ino.key.as_slice() < key);
                        return Ok(index < n.inodes.len()
                            && n.inodes[index].key.as_slice() == key
                            && n.inodes[index].flags & BUCKET_LEAF_FLAG == 0);
                    }
                    let mut exact = false;
                    let mut index = n.inodes.partition_point(|ino| ino.key.as_slice() < key);
                    if index < n.inodes.len() && n.inodes[index].key == key {
                        exact = true;
                    }
                    if !exact && index > 0 {
                        index -= 1;
                    }
                    if index >= n.inodes.len() {
                        index = n.inodes.len().saturating_sub(1);
                    }
                    pgid = n.inodes[index].pgid;
                    continue;
                }
            }
            if !self.dirty.is_empty() {
                if let Some(p) = self.dirty.get(&pgid) {
                    let hdr = PageHeader::read(p);
                    if hdr.is_leaf() {
                        return Ok(leaf_has_value(p, key));
                    }
                    pgid = search_page_index_pgid(p, key);
                    continue;
                }
            }
            let page = DbInner::page_bytes(&self.mmap_pin, self.db.page_size, pgid)?;
            let hdr = PageHeader::read(page);
            if hdr.is_leaf() {
                return Ok(leaf_has_value(page, key));
            }
            pgid = search_page_index_pgid(page, key);
        }
    }

    pub fn delete(&mut self, bid: BucketId, key: &[u8]) -> Result<()> {
        self.check_writable()?;
        self.put_hint = None;
        // Fast path: sequential deletes often stay on the same leaf.
        if let Some((hbid, nid)) = self.delete_hint {
            if hbid == bid {
                let hit = self.buckets.get(&bid).and_then(|b| b.nodes.get(&nid)).and_then(|n| {
                    if !n.is_leaf || n.inodes.is_empty() {
                        return None;
                    }
                    if n.inodes.front().map(|i| i.key.as_slice()) == Some(key) {
                        return Some((0usize, n.inodes[0].flags));
                    }
                    let first = n.inodes.front()?.key.as_slice();
                    let last = n.inodes.back()?.key.as_slice();
                    if key < first || key > last {
                        return None;
                    }
                    let index = n.inodes.partition_point(|ino| ino.key.as_slice() < key);
                    if index < n.inodes.len() && n.inodes[index].key.as_slice() == key {
                        Some((index, n.inodes[index].flags))
                    } else {
                        None
                    }
                });
                if let Some((index, flags)) = hit {
                    if flags & BUCKET_LEAF_FLAG != 0 {
                        return Err(Error::IncompatibleValue);
                    }
                    self.node_del_at(bid, nid, index);
                    if self.buckets[&bid]
                        .nodes
                        .get(&nid)
                        .map(|n| n.inodes.is_empty())
                        .unwrap_or(true)
                    {
                        self.delete_hint = None;
                    }
                    return Ok(());
                }
            }
        }

        let mut stack = std::mem::take(&mut self.get_stack);
        stack.clear();
        let result = (|| {
            let root = self.buckets[&bid].header.root;
            self.search(bid, &mut stack, key, root)?;
            let Some(r) = stack.last().cloned() else {
                return Ok(());
            };
            let count = self.ref_count(bid, &r)?;
            if count == 0 || r.index >= count {
                return Ok(());
            }
            let index = r.index;
            let (matches, is_bucket) = if let Some(id) = r.node_id {
                let inode = &self.buckets[&bid].nodes[&id].inodes[index];
                (
                    inode.key.as_slice() == key,
                    inode.flags & BUCKET_LEAF_FLAG != 0,
                )
            } else if self.buckets[&bid].header.root == 0 {
                let page = self.buckets[&bid].inline_page.as_ref().unwrap();
                let (flags, k, _) = leaf_at(page, index);
                (k == key, flags & BUCKET_LEAF_FLAG != 0)
            } else {
                self.with_page(r.pgid, |page| {
                    let (flags, k, _) = leaf_at(page, index);
                    (k == key, flags & BUCKET_LEAF_FLAG != 0)
                })?
            };
            if !matches {
                return Ok(());
            }
            if is_bucket {
                return Err(Error::IncompatibleValue);
            }
            let nid = self.cursor_node(bid, &mut stack)?;
            let leaf_index = stack.last().map(|e| e.index).unwrap_or(index);
            self.node_del_at(bid, nid, leaf_index);
            self.delete_hint = if self.buckets[&bid].nodes[&nid].inodes.is_empty() {
                None
            } else {
                Some((bid, nid))
            };
            Ok(())
        })();
        self.get_stack = stack;
        result
    }

    pub fn next_sequence(&mut self, bid: BucketId) -> Result<u64> {
        self.check_writable()?;
        self.ensure_root_node(bid)?;
        let b = self.buckets.get_mut(&bid).unwrap();
        b.header.sequence += 1;
        Ok(b.header.sequence)
    }

    pub fn set_sequence(&mut self, bid: BucketId, v: u64) -> Result<()> {
        self.check_writable()?;
        self.ensure_root_node(bid)?;
        self.buckets.get_mut(&bid).unwrap().header.sequence = v;
        Ok(())
    }

    pub fn sequence(&self, bid: BucketId) -> u64 {
        self.buckets[&bid].header.sequence
    }

    fn ensure_root_node(&mut self, bid: BucketId) -> Result<()> {
        if self.buckets[&bid].root_node.is_some() {
            return Ok(());
        }
        let root = self.buckets[&bid].header.root;
        self.materialize_node(bid, root, None)?;
        Ok(())
    }

    fn allocate(&mut self, count: usize) -> Result<Pgid> {
        let (id, buf) = self
            .db
            .allocate(self.meta.txid, count, &mut self.meta.pgid)?;
        // Writers may have grown the mapping; keep this tx's pin current.
        let _ = self.refresh_mmap_pin();
        self.dirty.insert(id, buf);
        Ok(id)
    }

    fn free_node_page(&mut self, bid: BucketId, nid: NodeId) {
        let n = self
            .buckets
            .get_mut(&bid)
            .unwrap()
            .nodes
            .get_mut(&nid)
            .unwrap();
        if n.pgid == 0 {
            return;
        }
        let pgid = n.pgid;
        let overflow = n.overflow;
        n.pgid = 0;
        self.buckets
            .get_mut(&bid)
            .unwrap()
            .nodes_by_pgid
            .remove(&pgid);
        self.db.freelist.lock().free(self.meta.txid, pgid, overflow);
    }

    fn split_index(_is_leaf: bool, inodes: &[Inode], threshold: usize) -> usize {
        let elsz = 16;
        let mut sz = PAGE_HEADER_SIZE;
        let mut index = 0;
        let limit = inodes.len().saturating_sub(MIN_KEYS_PER_PAGE);
        for (i, inode) in inodes.iter().enumerate().take(limit) {
            index = i;
            let elsize = elsz + inode.key.len() + inode.value.len();
            if index >= MIN_KEYS_PER_PAGE && sz + elsize > threshold {
                break;
            }
            sz += elsize;
        }
        index
    }

    /// Split `nid` into page-sized sibling nodes. O(n) in inode count.
    ///
    /// Unlike Go's reslice-based peel loop, Rust `Vec::split_off` from the front is
    /// O(n) per peel and became O(n²) for a huge dirty leaf. We take the inode list
    /// once and consume prefixes instead.
    fn split_node(&mut self, bid: BucketId, nid: NodeId, page_size: usize) -> Vec<NodeId> {
        let (is_leaf, fill, needs_parent, inode_len) = {
            let n = &self.buckets[&bid].nodes[&nid];
            (
                n.is_leaf,
                self.buckets[&bid].fill_percent,
                n.parent.is_none(),
                n.inodes.len(),
            )
        };
        if inode_len <= MIN_KEYS_PER_PAGE * 2 {
            return vec![nid];
        }
        {
            let n = &self.buckets[&bid].nodes[&nid];
            if inodes_size_less_than(n.is_leaf, &n.inodes, page_size) {
                return vec![nid];
            }
        }

        if needs_parent {
            let pid = self.buckets.get_mut(&bid).unwrap().alloc_node();
            self.buckets.get_mut(&bid).unwrap().nodes.insert(
                pid,
                Node {
                    is_leaf: false,
                    unbalanced: false,
                    spilled: false,
                    key: Vec::new(),
                    pgid: 0,
                    overflow: 0,
                    parent: None,
                    children: vec![nid],
                    inodes: VecDeque::new(),
                },
            );
            self.buckets
                .get_mut(&bid)
                .unwrap()
                .nodes
                .get_mut(&nid)
                .unwrap()
                .parent = Some(pid);
        }
        let parent_id = self.buckets[&bid].nodes[&nid].parent.unwrap();
        let fill = fill.clamp(MIN_FILL_PERCENT, MAX_FILL_PERCENT);
        let threshold = (page_size as f64 * fill) as usize;

        let all: Vec<Inode> = std::mem::take(
            &mut self
                .buckets
                .get_mut(&bid)
                .unwrap()
                .nodes
                .get_mut(&nid)
                .unwrap()
                .inodes,
        )
        .into();

        // Cut points into `all` (exclusive ends).
        let mut cuts = Vec::with_capacity(all.len() / MIN_KEYS_PER_PAGE + 2);
        cuts.push(0usize);
        let mut start = 0usize;
        while start < all.len() {
            let slice = &all[start..];
            if slice.len() <= MIN_KEYS_PER_PAGE * 2
                || inodes_size_less_than(is_leaf, slice, page_size)
            {
                cuts.push(all.len());
                break;
            }
            let local = Self::split_index(is_leaf, slice, threshold);
            if local == 0 || local >= slice.len() {
                cuts.push(all.len());
                break;
            }
            let next = start + local;
            if next <= start || next >= all.len() {
                cuts.push(all.len());
                break;
            }
            cuts.push(next);
            start = next;
        }

        let mut nodes = Vec::with_capacity(cuts.len().saturating_sub(1));
        let mut iter = all.into_iter();
        for (i, w) in cuts.windows(2).enumerate() {
            let len = w[1] - w[0];
            let chunk: Vec<Inode> = iter.by_ref().take(len).collect();
            if i == 0 {
                self.buckets
                    .get_mut(&bid)
                    .unwrap()
                    .nodes
                    .get_mut(&nid)
                    .unwrap()
                    .inodes = VecDeque::from(chunk);
                nodes.push(nid);
            } else {
                let id = self.buckets.get_mut(&bid).unwrap().alloc_node();
                self.buckets.get_mut(&bid).unwrap().nodes.insert(
                    id,
                    Node {
                        is_leaf,
                        unbalanced: false,
                        spilled: false,
                        key: Vec::new(),
                        pgid: 0,
                        overflow: 0,
                        parent: Some(parent_id),
                        children: Vec::new(),
                        inodes: VecDeque::from(chunk),
                    },
                );
                self.buckets
                    .get_mut(&bid)
                    .unwrap()
                    .nodes
                    .get_mut(&parent_id)
                    .unwrap()
                    .children
                    .push(id);
                nodes.push(id);
            }
        }
        nodes
    }

    fn spill_node(&mut self, bid: BucketId, nid: NodeId) -> Result<()> {
        if self.buckets[&bid].nodes[&nid].spilled {
            return Ok(());
        }
        let mut children = self.buckets[&bid].nodes[&nid].children.clone();
        children.sort_by(|a, b| {
            let ka = self.buckets[&bid].nodes[a]
                .inodes
                .front()
                .map(|i| i.key.as_slice())
                .unwrap_or(b"");
            let kb = self.buckets[&bid].nodes[b]
                .inodes
                .front()
                .map(|i| i.key.as_slice())
                .unwrap_or(b"");
            ka.cmp(kb)
        });
        for c in children {
            self.spill_node(bid, c)?;
        }
        self.buckets
            .get_mut(&bid)
            .unwrap()
            .nodes
            .get_mut(&nid)
            .unwrap()
            .children
            .clear();

        let page_size = self.db.page_size;
        let parts = self.split_node(bid, nid, page_size);
        for pid in parts {
            self.free_node_page(bid, pid);
            let (is_leaf, parent, old_key, first_key, inodes) = {
                let n = &self.buckets[&bid].nodes[&pid];
                let first_key = n.inodes.front().map(|i| i.key.clone()).unwrap_or_default();
                (
                    n.is_leaf,
                    n.parent,
                    n.key.clone(),
                    first_key,
                    // page-sized after split (~tens of keys), not the full pre-split node
                    n.inodes.iter().cloned().collect::<Vec<_>>(),
                )
            };
            let size = inodes_size(is_leaf, &inodes);
            let npages = size.div_ceil(page_size).max(1);
            let pgid = self.allocate(npages)?;
            {
                let page = self.dirty.get_mut(&pgid).unwrap();
                write_inodes(page, is_leaf, &inodes);
            }
            {
                let n = self
                    .buckets
                    .get_mut(&bid)
                    .unwrap()
                    .nodes
                    .get_mut(&pid)
                    .unwrap();
                n.pgid = pgid;
                n.overflow = (npages - 1) as u32;
                n.spilled = true;
                if !first_key.is_empty() {
                    n.key = first_key.clone();
                }
                self.buckets
                    .get_mut(&bid)
                    .unwrap()
                    .nodes_by_pgid
                    .insert(pgid, pid);
            }
            if let Some(parent) = parent {
                let key = if old_key.is_empty() {
                    first_key.clone()
                } else {
                    old_key
                };
                if !key.is_empty() && !first_key.is_empty() {
                    self.node_put(bid, parent, &key, &first_key, &[], pgid, 0);
                }
            }
        }

        let parent = self.buckets[&bid].nodes[&nid].parent;
        if let Some(p) = parent {
            if self.buckets[&bid].nodes[&p].pgid == 0 {
                self.buckets
                    .get_mut(&bid)
                    .unwrap()
                    .nodes
                    .get_mut(&nid)
                    .unwrap()
                    .children
                    .clear();
                return self.spill_node(bid, p);
            }
        }
        Ok(())
    }

    fn inlineable(&self, bid: BucketId) -> bool {
        let b = &self.buckets[&bid];
        let Some(nid) = b.root_node else {
            return false;
        };
        let n = &b.nodes[&nid];
        if !n.is_leaf {
            return false;
        }
        let mut size = PAGE_HEADER_SIZE;
        for inode in &n.inodes {
            size += 16 + inode.key.len() + inode.value.len();
            if inode.flags & BUCKET_LEAF_FLAG != 0 {
                return false;
            }
            if size > self.db.page_size / 4 {
                return false;
            }
        }
        true
    }

    fn write_inline(&self, bid: BucketId) -> Vec<u8> {
        let b = &self.buckets[&bid];
        let n = &b.nodes[&b.root_node.unwrap()];
        let sz = BUCKET_HEADER_SIZE + inodes_size(true, &n.inodes);
        let mut value = vec![0u8; sz];
        b.header.write(&mut value[..BUCKET_HEADER_SIZE]);
        let flat: Vec<Inode> = n.inodes.iter().cloned().collect();
        write_inodes(&mut value[BUCKET_HEADER_SIZE..], true, &flat);
        value
    }

    fn spill_bucket(&mut self, bid: BucketId) -> Result<()> {
        let children: Vec<(Vec<u8>, BucketId)> = self.buckets[&bid]
            .subbuckets
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        for (name, child) in children {
            let value = if self.inlineable(child) {
                self.free_bucket(child)?;
                self.write_inline(child)
            } else {
                self.spill_bucket(child)?;
                let mut value = vec![0u8; BUCKET_HEADER_SIZE];
                self.buckets[&child].header.write(&mut value);
                value
            };
            if self.buckets[&child].root_node.is_none() {
                continue;
            }
            let mut stack = Vec::new();
            let (k, _, flags) = self.cursor_seek(bid, &mut stack, &name)?;
            if k.as_deref() != Some(name.as_slice()) {
                panic!("misplaced bucket header");
            }
            if flags & BUCKET_LEAF_FLAG == 0 {
                panic!("unexpected bucket header flag: {flags:x}");
            }
            let nid = self.cursor_node(bid, &mut stack)?;
            self.node_put(bid, nid, &name, &name, &value, 0, BUCKET_LEAF_FLAG);
        }
        if self.buckets[&bid].root_node.is_none() {
            return Ok(());
        }
        let root = self.buckets[&bid].root_node.unwrap();
        self.spill_node(bid, root)?;
        let mut top = self.buckets[&bid].root_node.unwrap();
        while let Some(p) = self.buckets[&bid].nodes[&top].parent {
            top = p;
        }
        self.buckets.get_mut(&bid).unwrap().root_node = Some(top);
        let pgid = self.buckets[&bid].nodes[&top].pgid;
        if pgid >= self.meta.pgid {
            panic!("pgid ({pgid}) above high water mark ({})", self.meta.pgid);
        }
        self.buckets.get_mut(&bid).unwrap().header.root = pgid;
        Ok(())
    }

    fn rebalance_node(&mut self, bid: BucketId, nid: NodeId) -> Result<()> {
        {
            let Some(n) = self
                .buckets
                .get_mut(&bid)
                .and_then(|b| b.nodes.get_mut(&nid))
            else {
                return Ok(());
            };
            if !n.unbalanced {
                return Ok(());
            }
            n.unbalanced = false;
        }
        let page_size = self.db.page_size;
        let fill = self.buckets[&bid].fill_percent;
        let threshold = ((page_size as f64 * fill) / 2.0) as usize;
        let (size, nkeys, is_leaf, parent, min_keys) = {
            let n = &self.buckets[&bid].nodes[&nid];
            (
                inodes_size(n.is_leaf, &n.inodes),
                n.inodes.len(),
                n.is_leaf,
                n.parent,
                if n.is_leaf { 1 } else { 2 },
            )
        };
        if size > threshold && nkeys > min_keys {
            return Ok(());
        }
        if parent.is_none() {
            if !is_leaf && nkeys == 1 {
                let child_pgid = self.buckets[&bid].nodes[&nid].inodes[0].pgid;
                let child = self.materialize_node(bid, child_pgid, Some(nid))?;
                let (is_leaf, inodes, children) = {
                    let c = self
                        .buckets
                        .get_mut(&bid)
                        .unwrap()
                        .nodes
                        .get_mut(&child)
                        .unwrap();
                    (
                        c.is_leaf,
                        std::mem::take(&mut c.inodes),
                        std::mem::take(&mut c.children),
                    )
                };
                {
                    let n = self
                        .buckets
                        .get_mut(&bid)
                        .unwrap()
                        .nodes
                        .get_mut(&nid)
                        .unwrap();
                    n.is_leaf = is_leaf;
                    n.inodes = inodes;
                    n.children = children;
                }
                for inode_pgid in self.buckets[&bid].nodes[&nid]
                    .inodes
                    .iter()
                    .map(|i| i.pgid)
                    .collect::<Vec<_>>()
                {
                    if let Some(&cid) = self.buckets[&bid].nodes_by_pgid.get(&inode_pgid) {
                        self.buckets
                            .get_mut(&bid)
                            .unwrap()
                            .nodes
                            .get_mut(&cid)
                            .unwrap()
                            .parent = Some(nid);
                    }
                }
                self.buckets
                    .get_mut(&bid)
                    .unwrap()
                    .nodes
                    .get_mut(&child)
                    .unwrap()
                    .parent = None;
                let cpgid = self.buckets[&bid].nodes[&child].pgid;
                self.free_node_page(bid, child);
                self.buckets.get_mut(&bid).unwrap().nodes.remove(&child);
                self.buckets
                    .get_mut(&bid)
                    .unwrap()
                    .nodes_by_pgid
                    .remove(&cpgid);
            }
            return Ok(());
        }
        let parent = parent.unwrap();
        if nkeys == 0 {
            let key = self.buckets[&bid].nodes[&nid].key.clone();
            self.node_del(bid, parent, &key);
            self.remove_child(bid, parent, nid);
            let pgid = self.buckets[&bid].nodes[&nid].pgid;
            self.free_node_page(bid, nid);
            self.buckets.get_mut(&bid).unwrap().nodes.remove(&nid);
            self.buckets
                .get_mut(&bid)
                .unwrap()
                .nodes_by_pgid
                .remove(&pgid);
            return self.rebalance_node(bid, parent);
        }
        let pindex = self.child_index(bid, parent, nid);
        let use_next = pindex == 0;
        let (left, right) = if use_next {
            let right = self.child_at(bid, parent, pindex + 1)?;
            (nid, right)
        } else {
            let left = self.child_at(bid, parent, pindex - 1)?;
            (left, nid)
        };
        let right_inodes = std::mem::take(
            &mut self
                .buckets
                .get_mut(&bid)
                .unwrap()
                .nodes
                .get_mut(&right)
                .unwrap()
                .inodes,
        );
        for inode in &right_inodes {
            if let Some(&cid) = self.buckets[&bid].nodes_by_pgid.get(&inode.pgid) {
                let old_parent = self.buckets[&bid].nodes[&cid].parent;
                if let Some(op) = old_parent {
                    self.remove_child(bid, op, cid);
                }
                self.buckets
                    .get_mut(&bid)
                    .unwrap()
                    .nodes
                    .get_mut(&cid)
                    .unwrap()
                    .parent = Some(left);
                self.buckets
                    .get_mut(&bid)
                    .unwrap()
                    .nodes
                    .get_mut(&left)
                    .unwrap()
                    .children
                    .push(cid);
            }
        }
        self.buckets
            .get_mut(&bid)
            .unwrap()
            .nodes
            .get_mut(&left)
            .unwrap()
            .inodes
            .extend(right_inodes);
        let rkey = self.buckets[&bid].nodes[&right].key.clone();
        self.node_del(bid, parent, &rkey);
        self.remove_child(bid, parent, right);
        let rpgid = self.buckets[&bid].nodes[&right].pgid;
        self.free_node_page(bid, right);
        self.buckets.get_mut(&bid).unwrap().nodes.remove(&right);
        self.buckets
            .get_mut(&bid)
            .unwrap()
            .nodes_by_pgid
            .remove(&rpgid);
        self.rebalance_node(bid, parent)
    }

    fn child_index(&self, bid: BucketId, parent: NodeId, child: NodeId) -> usize {
        let key = &self.buckets[&bid].nodes[&child].key;
        let pinodes = &self.buckets[&bid].nodes[&parent].inodes;
        pinodes.partition_point(|ino| ino.key.as_slice() < key.as_slice())
    }

    fn remove_child(&mut self, bid: BucketId, parent: NodeId, child: NodeId) {
        let ch = &mut self
            .buckets
            .get_mut(&bid)
            .unwrap()
            .nodes
            .get_mut(&parent)
            .unwrap()
            .children;
        if let Some(i) = ch.iter().position(|c| *c == child) {
            ch.remove(i);
        }
    }

    fn rebalance_bucket(&mut self, bid: BucketId) -> Result<()> {
        let ids: Vec<NodeId> = self.buckets[&bid].nodes.iter_ids().collect();
        for id in ids {
            if self.buckets[&bid].nodes.get(&id).is_some() {
                self.rebalance_node(bid, id)?;
            }
        }
        let children: Vec<BucketId> = self.buckets[&bid].subbuckets.values().copied().collect();
        for c in children {
            self.rebalance_bucket(c)?;
        }
        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        if self.managed {
            return Err(Error::ManagedTx);
        }
        self.check_writable()?;
        self.rebalance_bucket(self.root)?;
        let opgid = self.meta.pgid;
        self.spill_bucket(self.root)?;
        self.meta.root = self.buckets[&self.root].header;

        if self.meta.freelist != PGID_NO_FREELIST {
            if let Ok(page) = self.db.read_page(self.meta.freelist) {
                let hdr = PageHeader::read(&page);
                self.db
                    .freelist
                    .lock()
                    .free(self.meta.txid, self.meta.freelist, hdr.overflow);
            }
        }
        if !self.db.no_freelist_sync {
            self.commit_freelist()?;
        } else {
            self.meta.freelist = PGID_NO_FREELIST;
        }

        if self.meta.pgid > opgid {
            let sz = (self.meta.pgid as usize + 1) * self.db.page_size;
            self.db.grow(sz)?;
        }

        self.write_pages()?;
        self.write_meta()?;
        let file_len = self.db.file_size()? as usize;
        let _ = self.db.ensure_mapped(file_len);
        *self.db.committed_meta.lock() = self.meta.clone();
        let handlers = std::mem::take(&mut self.commit_handlers);
        self.close_internal();
        for h in handlers {
            h();
        }
        Ok(())
    }

    fn commit_freelist(&mut self) -> Result<()> {
        let n = self.db.freelist.lock().pages_for_write(self.db.page_size);
        let pgid = self.allocate(n)?;
        {
            let page = self.dirty.get_mut(&pgid).unwrap();
            self.db.freelist.lock().write_page(page);
        }
        self.meta.freelist = pgid;
        Ok(())
    }

    fn write_pages(&mut self) -> Result<()> {
        let mut pages: Vec<(Pgid, Vec<u8>)> = self.dirty.drain().collect();
        pages.sort_by_key(|(id, _)| *id);
        for (id, buf) in &pages {
            let off = *id * self.db.page_size as u64;
            self.db.write_at(buf, off)?;
        }
        if !self.db.no_sync {
            self.db.fdatasync()?;
        }
        Ok(())
    }

    fn write_meta(&mut self) -> Result<()> {
        let mut buf = vec![0u8; self.db.page_size];
        write_meta_page(&mut buf, &self.meta);
        let id = self.meta.txid % 2;
        let _g = self.db.metalock.lock();
        self.db.write_at(&buf, id * self.db.page_size as u64)?;
        drop(_g);
        if !self.db.no_sync {
            self.db.fdatasync()?;
        }
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<()> {
        if self.managed {
            return Err(Error::ManagedTx);
        }
        if self.closed {
            return Err(Error::TxClosed);
        }
        self.rollback_internal();
        Ok(())
    }

    pub fn rollback_internal(&mut self) {
        if self.closed {
            return;
        }
        if self.writable {
            self.db.freelist.lock().rollback(self.meta.txid);
        }
        self.close_internal();
    }

    /// Error-path rollback that also reloads freelist from disk / scan (Go `tx.rollback`).
    #[allow(dead_code)]
    pub fn rollback_and_reload(&mut self) {
        if self.closed {
            return;
        }
        if self.writable {
            self.db.freelist.lock().rollback(self.meta.txid);
            let _ = self.db.reload_freelist_after_rollback();
        }
        self.close_internal();
    }

    fn close_internal(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        if self.hold_writer {
            if !self.db.no_statistics {
                let fl = self.db.freelist.lock();
                let mut st = self.db.stats.lock();
                st.free_page_n = fl.free_count();
                st.pending_page_n = fl.pending_count();
                st.free_alloc = (st.free_page_n + st.pending_page_n) * self.db.page_size;
                st.freelist_inuse = fl.estimated_write_page_size();
            }
            self.db.writer.unlock();
            self.hold_writer = false;
        } else {
            self.db
                .open_ro_tx
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            if self.db.freelist_loaded.load(std::sync::atomic::Ordering::SeqCst) {
                self.db.freelist.lock().remove_readonly_txid(self.meta.txid);
            }
        }
        self.dirty.clear();
    }

    pub fn move_bucket(&mut self, src: BucketId, dst: BucketId, key: &[u8]) -> Result<()> {
        self.check_writable()?;
        if src == dst {
            return Err(Error::SameBuckets);
        }
        let new_key = key.to_vec();
        let mut stack = Vec::new();
        let (k, v, flags) = {
            self.cursor_seek(src, &mut stack, &new_key)?;
            self.key_value_raw(src, &stack)?
        };
        if k.as_deref() != Some(new_key.as_slice()) {
            return Err(Error::BucketNotFound);
        }
        if flags & BUCKET_LEAF_FLAG == 0 {
            return Err(Error::IncompatibleValue);
        }
        let mut dst_stack = Vec::new();
        let (dk, _, dflags) = self.cursor_seek(dst, &mut dst_stack, &new_key)?;
        if dk.as_deref() == Some(new_key.as_slice()) {
            if dflags & BUCKET_LEAF_FLAG != 0 {
                return Err(Error::BucketExists);
            }
            return Err(Error::IncompatibleValue);
        }
        self.buckets
            .get_mut(&src)
            .unwrap()
            .subbuckets
            .remove(&new_key);
        let nid = self.cursor_node(src, &mut stack)?;
        self.node_del(src, nid, &new_key);
        let dnid = self.cursor_node(dst, &mut dst_stack)?;
        self.node_put(
            dst,
            dnid,
            &new_key,
            &new_key,
            &v.unwrap(),
            0,
            BUCKET_LEAF_FLAG,
        );
        Ok(())
    }
}

/// Binary-search a branch page; sets stack index; returns child pgid.
fn search_page_index(stack: &mut [ElemRef], page: &[u8], key: &[u8]) -> Pgid {
    let (index, pgid) = branch_search(page, key);
    stack.last_mut().unwrap().index = index;
    pgid
}

fn search_page_index_pgid(page: &[u8], key: &[u8]) -> Pgid {
    branch_search(page, key).1
}

#[inline(always)]
fn branch_search(page: &[u8], key: &[u8]) -> (usize, Pgid) {
    let hdr = PageHeader::read(page);
    let count = hdr.count as usize;
    if count == 0 {
        return (0, 0);
    }
    let mut exact = false;
    let mut lo = 0;
    let mut hi = count;
    while lo < hi {
        let mid = (lo + hi) / 2;
        let (_, k) = branch_at(page, mid);
        match k.cmp(key) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
            std::cmp::Ordering::Equal => {
                exact = true;
                lo = mid;
                break;
            }
        }
    }
    let mut index = lo;
    if !exact && index > 0 {
        index -= 1;
    }
    if index >= count {
        index = count - 1;
    }
    (index, branch_pgid(page, index))
}

fn nsearch_page(stack: &mut [ElemRef], page: &[u8], key: &[u8]) {
    let count = PageHeader::read(page).count as usize;
    let mut lo = 0;
    let mut hi = count;
    while lo < hi {
        let mid = (lo + hi) / 2;
        let (_, k, _) = leaf_at(page, mid);
        if k < key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    stack.last_mut().unwrap().index = lo;
}

#[inline(always)]
fn leaf_has_value(page: &[u8], key: &[u8]) -> bool {
    let count = PageHeader::read(page).count as usize;
    let mut lo = 0;
    let mut hi = count;
    while lo < hi {
        let mid = (lo + hi) / 2;
        let (_, k, _) = leaf_at(page, mid);
        if k < key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if lo >= count {
        return false;
    }
    let (flags, k, _) = leaf_at(page, lo);
    flags & BUCKET_LEAF_FLAG == 0 && k == key
}

impl Drop for TxInner {
    fn drop(&mut self) {
        if !self.closed {
            self.rollback_internal();
        }
    }
}
