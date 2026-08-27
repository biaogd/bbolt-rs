//! In-memory transaction state: nodes, buckets, cursors, commit.

use std::collections::HashMap;
use std::sync::Arc;

use crate::db::DbInner;
use crate::error::{Error, Result};
use crate::stats::TxStats;
use crate::page::{
    branch_at, branch_pgid, inodes_size, leaf_at, read_inodes, write_inodes, write_meta_page,
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
    pub inodes: Vec<Inode>,
}

pub struct BucketInner {
    pub header: InBucket,
    pub nodes: HashMap<NodeId, Node>,
    pub nodes_by_pgid: HashMap<Pgid, NodeId>,
    pub root_node: Option<NodeId>,
    pub inline_page: Option<Vec<u8>>,
    pub subbuckets: HashMap<Vec<u8>, BucketId>,
    pub fill_percent: f64,
    pub next_node_id: NodeId,
}

impl BucketInner {
    fn new(header: InBucket) -> Self {
        Self {
            header,
            nodes: HashMap::new(),
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

#[derive(Clone, Debug)]
pub struct ElemRef {
    pub node_id: Option<NodeId>,
    pub pgid: Pgid,
    pub index: usize,
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
}

enum PageNode {
    Node(NodeId),
    Bytes(Vec<u8>),
}

impl TxInner {
    pub fn new(db: Arc<DbInner>, meta: Meta, writable: bool) -> Self {
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
        self.db.read_page(pgid)
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
            let page = b
                .inline_page
                .clone()
                .ok_or_else(|| Error::Corrupt("inline bucket missing page".into()))?;
            return Ok(PageNode::Bytes(page));
        }
        if let Some(&id) = b.nodes_by_pgid.get(&pgid) {
            return Ok(PageNode::Node(id));
        }
        Ok(PageNode::Bytes(self.read_page(pgid)?))
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
        let inodes = read_inodes(&page);
        let b = self.buckets.get_mut(&bid).unwrap();
        let id = b.alloc_node();
        let key = inodes.first().map(|i| i.key.clone()).unwrap_or_default();
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
        n.inodes.remove(index);
        n.unbalanced = true;
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

    fn search(
        &mut self,
        bid: BucketId,
        stack: &mut Vec<ElemRef>,
        key: &[u8],
        pgid: Pgid,
    ) -> Result<()> {
        match self.page_node(bid, pgid)? {
            PageNode::Node(id) => {
                stack.push(ElemRef {
                    node_id: Some(id),
                    pgid,
                    index: 0,
                });
                let is_leaf = self.buckets[&bid].nodes[&id].is_leaf;
                if is_leaf {
                    self.nsearch_node(bid, stack, key, id);
                    return Ok(());
                }
                self.search_node(bid, stack, key, id)
            }
            PageNode::Bytes(page) => {
                stack.push(ElemRef {
                    node_id: None,
                    pgid,
                    index: 0,
                });
                let hdr = PageHeader::read(&page);
                if !hdr.is_branch() && !hdr.is_leaf() {
                    panic!("invalid page type: {}: {:x}", hdr.id, hdr.flags);
                }
                if hdr.is_leaf() {
                    self.nsearch_page(stack, &page, key);
                    return Ok(());
                }
                self.search_page(bid, stack, key, &page)
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

    fn search_page(
        &mut self,
        bid: BucketId,
        stack: &mut Vec<ElemRef>,
        key: &[u8],
        page: &[u8],
    ) -> Result<()> {
        let hdr = PageHeader::read(page);
        let count = hdr.count as usize;
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
        if count == 0 {
            return Ok(());
        }
        if index >= count {
            index = count - 1;
        }
        stack.last_mut().unwrap().index = index;
        let child = branch_pgid(page, index);
        self.search(bid, stack, key, child)
    }

    fn nsearch_node(&self, bid: BucketId, stack: &mut [ElemRef], key: &[u8], nid: NodeId) {
        let inodes = &self.buckets[&bid].nodes[&nid].inodes;
        let index = inodes.partition_point(|ino| ino.key.as_slice() < key);
        stack.last_mut().unwrap().index = index;
    }

    fn nsearch_page(&self, stack: &mut [ElemRef], page: &[u8], key: &[u8]) {
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

    fn ref_count(&self, bid: BucketId, r: &ElemRef) -> Result<usize> {
        if let Some(id) = r.node_id {
            Ok(self.buckets[&bid].nodes[&id].inodes.len())
        } else if self.buckets[&bid].header.root == 0 {
            let page = self.buckets[&bid]
                .inline_page
                .as_ref()
                .ok_or_else(|| Error::Corrupt("inline missing".into()))?;
            Ok(PageHeader::read(page).count as usize)
        } else {
            let page = self.read_page(r.pgid)?;
            Ok(PageHeader::read(&page).count as usize)
        }
    }

    fn ref_is_leaf(&self, bid: BucketId, r: &ElemRef) -> Result<bool> {
        if let Some(id) = r.node_id {
            Ok(self.buckets[&bid].nodes[&id].is_leaf)
        } else if self.buckets[&bid].header.root == 0 {
            let page = self.buckets[&bid].inline_page.as_ref().unwrap();
            Ok(PageHeader::read(page).is_leaf())
        } else {
            let page = self.read_page(r.pgid)?;
            Ok(PageHeader::read(&page).is_leaf())
        }
    }

    pub fn key_value(&self, bid: BucketId, stack: &[ElemRef]) -> Result<Kv> {
        let Some(r) = stack.last() else {
            return Ok((None, None, 0));
        };
        let count = self.ref_count(bid, r)?;
        if count == 0 || r.index >= count {
            return Ok((None, None, 0));
        }
        if let Some(id) = r.node_id {
            let inode = &self.buckets[&bid].nodes[&id].inodes[r.index];
            let flags = inode.flags;
            let k = Some(inode.key.clone());
            let v = if flags & BUCKET_LEAF_FLAG != 0 {
                None
            } else {
                Some(inode.value.clone())
            };
            return Ok((k, v, flags));
        }
        let page;
        let page_ref: &[u8] = if self.buckets[&bid].header.root == 0 {
            self.buckets[&bid].inline_page.as_ref().unwrap()
        } else {
            page = self.read_page(r.pgid)?;
            &page
        };
        let (flags, key, val) = leaf_at(page_ref, r.index);
        let k = Some(key.to_vec());
        let v = if flags & BUCKET_LEAF_FLAG != 0 {
            None
        } else {
            Some(val.to_vec())
        };
        Ok((k, v, flags))
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
        let page;
        let page_ref: &[u8] = if self.buckets[&bid].header.root == 0 {
            self.buckets[&bid].inline_page.as_ref().unwrap()
        } else {
            page = self.read_page(r.pgid)?;
            &page
        };
        let (flags, key, val) = leaf_at(page_ref, r.index);
        Ok((Some(key.to_vec()), Some(val.to_vec()), flags))
    }

    fn go_to_first(&mut self, bid: BucketId, stack: &mut Vec<ElemRef>) -> Result<()> {
        loop {
            let last = stack.last().cloned().unwrap();
            if self.ref_is_leaf(bid, &last)? {
                break;
            }
            let pgid = self.child_pgid(bid, &last)?;
            match self.page_node(bid, pgid)? {
                PageNode::Node(id) => stack.push(ElemRef {
                    node_id: Some(id),
                    pgid,
                    index: 0,
                }),
                PageNode::Bytes(_) => stack.push(ElemRef {
                    node_id: None,
                    pgid,
                    index: 0,
                }),
            }
        }
        Ok(())
    }

    fn child_pgid(&self, bid: BucketId, r: &ElemRef) -> Result<Pgid> {
        if let Some(id) = r.node_id {
            Ok(self.buckets[&bid].nodes[&id].inodes[r.index].pgid)
        } else {
            let page = if self.buckets[&bid].header.root == 0 {
                self.buckets[&bid].inline_page.clone().unwrap()
            } else {
                self.read_page(r.pgid)?
            };
            Ok(branch_pgid(&page, r.index))
        }
    }

    pub fn cursor_first(&mut self, bid: BucketId, stack: &mut Vec<ElemRef>) -> Result<Kv> {
        self.check_open()?;
        stack.clear();
        let root = self.buckets[&bid].header.root;
        match self.page_node(bid, root)? {
            PageNode::Node(id) => stack.push(ElemRef {
                node_id: Some(id),
                pgid: root,
                index: 0,
            }),
            PageNode::Bytes(_) => stack.push(ElemRef {
                node_id: None,
                pgid: root,
                index: 0,
            }),
        }
        self.go_to_first(bid, stack)?;
        if let Some(last) = stack.last() {
            if self.ref_count(bid, last)? == 0 {
                return self.cursor_next(bid, stack);
            }
        }
        self.key_value(bid, stack)
    }

    pub fn cursor_last(&mut self, bid: BucketId, stack: &mut Vec<ElemRef>) -> Result<Kv> {
        self.check_open()?;
        stack.clear();
        let root = self.buckets[&bid].header.root;
        match self.page_node(bid, root)? {
            PageNode::Node(id) => {
                let count = self.buckets[&bid].nodes[&id].inodes.len();
                stack.push(ElemRef {
                    node_id: Some(id),
                    pgid: root,
                    index: count.saturating_sub(1),
                });
            }
            PageNode::Bytes(page) => {
                let count = PageHeader::read(&page).count as usize;
                stack.push(ElemRef {
                    node_id: None,
                    pgid: root,
                    index: count.saturating_sub(1),
                });
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
                    stack.push(ElemRef {
                        node_id: Some(id),
                        pgid,
                        index: count.saturating_sub(1),
                    });
                }
                PageNode::Bytes(page) => {
                    let count = PageHeader::read(&page).count as usize;
                    stack.push(ElemRef {
                        node_id: None,
                        pgid,
                        index: count.saturating_sub(1),
                    });
                }
            }
        }
        Ok(())
    }

    pub fn cursor_next(&mut self, bid: BucketId, stack: &mut Vec<ElemRef>) -> Result<Kv> {
        loop {
            let mut i = stack.len() as isize - 1;
            while i >= 0 {
                let idx = i as usize;
                let count = self.ref_count(bid, &stack[idx])?;
                if stack[idx].index + 1 < count {
                    stack[idx].index += 1;
                    break;
                }
                i -= 1;
            }
            if i == -1 {
                return Ok((None, None, 0));
            }
            stack.truncate(i as usize + 1);
            self.go_to_first(bid, stack)?;
            let last = stack.last().cloned().unwrap();
            if self.ref_count(bid, &last)? == 0 {
                continue;
            }
            return self.key_value(bid, stack);
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
            self.materialize_node(bid, first_pgid, None)?
        };
        let indices: Vec<usize> = stack[..stack.len() - 1].iter().map(|r| r.index).collect();
        for idx in indices {
            n = self.child_at(bid, n, idx)?;
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
        for nid in self.buckets[&bid].nodes.keys().copied().collect::<Vec<_>>() {
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
            PageNode::Bytes(page) => {
                let hdr = PageHeader::read(&page);
                if hdr.id > 1 {
                    out.push((hdr.id, hdr.overflow));
                }
                if hdr.is_branch() {
                    for i in 0..hdr.count as usize {
                        let child = branch_pgid(&page, i);
                        self.collect_pages(bid, child, out)?;
                    }
                }
            }
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
        let new_key = key.to_vec();
        let mut stack = Vec::new();
        let (k, _, flags) = self.cursor_seek(bid, &mut stack, &new_key)?;
        if k.as_deref() == Some(new_key.as_slice()) && flags & BUCKET_LEAF_FLAG != 0 {
            return Err(Error::IncompatibleValue);
        }
        let nid = self.cursor_node(bid, &mut stack)?;
        self.node_put(bid, nid, &new_key, &new_key, value, 0, 0);
        Ok(())
    }

    pub fn get(&mut self, bid: BucketId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.check_open()?;
        let mut stack = Vec::new();
        let (k, v, flags) = self.cursor_seek(bid, &mut stack, key)?;
        if flags & BUCKET_LEAF_FLAG != 0 {
            return Ok(None);
        }
        if k.as_deref() != Some(key) {
            return Ok(None);
        }
        Ok(v)
    }

    pub fn delete(&mut self, bid: BucketId, key: &[u8]) -> Result<()> {
        self.check_writable()?;
        let mut stack = Vec::new();
        let (k, _, flags) = self.cursor_seek(bid, &mut stack, key)?;
        if k.as_deref() != Some(key) {
            return Ok(());
        }
        if flags & BUCKET_LEAF_FLAG != 0 {
            return Err(Error::IncompatibleValue);
        }
        let nid = self.cursor_node(bid, &mut stack)?;
        self.node_del(bid, nid, key);
        Ok(())
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

    fn split_two(
        &mut self,
        bid: BucketId,
        nid: NodeId,
        page_size: usize,
    ) -> (NodeId, Option<NodeId>) {
        let (too_small, fill) = {
            let b = &self.buckets[&bid];
            let n = &b.nodes[&nid];
            let small = n.inodes.len() <= MIN_KEYS_PER_PAGE * 2
                || inodes_size(n.is_leaf, &n.inodes) < page_size;
            (small, b.fill_percent)
        };
        if too_small {
            return (nid, None);
        }
        let fill = fill.clamp(MIN_FILL_PERCENT, MAX_FILL_PERCENT);
        let threshold = (page_size as f64 * fill) as usize;
        let split_index = {
            let n = &self.buckets[&bid].nodes[&nid];
            Self::split_index(n.is_leaf, &n.inodes, threshold)
        };
        if split_index == 0 || split_index >= self.buckets[&bid].nodes[&nid].inodes.len() {
            return (nid, None);
        }
        if self.buckets[&bid].nodes[&nid].parent.is_none() {
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
                    inodes: Vec::new(),
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
        let next_id = self.buckets.get_mut(&bid).unwrap().alloc_node();
        let is_leaf = self.buckets[&bid].nodes[&nid].is_leaf;
        let rest = self
            .buckets
            .get_mut(&bid)
            .unwrap()
            .nodes
            .get_mut(&nid)
            .unwrap()
            .inodes
            .split_off(split_index);
        self.buckets.get_mut(&bid).unwrap().nodes.insert(
            next_id,
            Node {
                is_leaf,
                unbalanced: false,
                spilled: false,
                key: Vec::new(),
                pgid: 0,
                overflow: 0,
                parent: Some(parent_id),
                children: Vec::new(),
                inodes: rest,
            },
        );
        self.buckets
            .get_mut(&bid)
            .unwrap()
            .nodes
            .get_mut(&parent_id)
            .unwrap()
            .children
            .push(next_id);
        (nid, Some(next_id))
    }

    fn split_node(&mut self, bid: BucketId, nid: NodeId, page_size: usize) -> Vec<NodeId> {
        let mut nodes = Vec::new();
        let mut cur = nid;
        loop {
            let (a, b) = self.split_two(bid, cur, page_size);
            nodes.push(a);
            match b {
                Some(next) => cur = next,
                None => break,
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
                .first()
                .map(|i| i.key.as_slice())
                .unwrap_or(b"");
            let kb = self.buckets[&bid].nodes[b]
                .inodes
                .first()
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
            let (is_leaf, inodes, parent, old_key) = {
                let n = &self.buckets[&bid].nodes[&pid];
                (n.is_leaf, n.inodes.clone(), n.parent, n.key.clone())
            };
            let size = inodes_size(is_leaf, &inodes);
            let npages = size.div_ceil(page_size).max(1);
            let pgid = self.allocate(npages)?;
            {
                let page = self.dirty.get_mut(&pgid).unwrap();
                write_inodes(page, is_leaf, &inodes);
            }
            let first_key = inodes.first().map(|i| i.key.clone()).unwrap_or_default();
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
        write_inodes(&mut value[BUCKET_HEADER_SIZE..], true, &n.inodes);
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
                    let c = &self.buckets[&bid].nodes[&child];
                    (c.is_leaf, c.inodes.clone(), c.children.clone())
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
        let right_inodes = self.buckets[&bid].nodes[&right].inodes.clone();
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
        let ids: Vec<NodeId> = self.buckets[&bid].nodes.keys().copied().collect();
        for id in ids {
            if self.buckets[&bid].nodes.contains_key(&id) {
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

impl Drop for TxInner {
    fn drop(&mut self) {
        if !self.closed {
            self.rollback_internal();
        }
    }
}
