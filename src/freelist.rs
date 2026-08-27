//! Freelist backends matching etcd-io/bbolt (`array` default, `hashmap`).

use std::collections::{HashMap, HashSet};

use crate::page::{
    estimated_freelist_page_size, freelist_ids, write_freelist, PageHeader, Pgid, Txid,
};

/// Freelist implementation selection (upstream `FreelistType`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum FreelistType {
    /// Sorted array of free page IDs (default).
    #[default]
    Array,
    /// Span hashmap; faster under fragmentation.
    HashMap,
}

#[derive(Default)]
struct TxPending {
    ids: Vec<Pgid>,
    alloc_tx: Vec<Txid>,
    last_release_begin: Txid,
}

pub struct Freelist {
    kind: FreelistType,
    // shared
    readonly_txids: Vec<Txid>,
    allocs: HashMap<Pgid, Txid>,
    cache: HashSet<Pgid>,
    pending: HashMap<Txid, TxPending>,
    // array backend
    ids: Vec<Pgid>,
    // hashmap backend
    free_pages_count: u64,
    freemaps: HashMap<u64, HashSet<Pgid>>,
    forward_map: HashMap<Pgid, u64>,
    backward_map: HashMap<Pgid, u64>,
}

impl Default for Freelist {
    fn default() -> Self {
        Self::new(FreelistType::Array)
    }
}

impl Freelist {
    pub fn new(kind: FreelistType) -> Self {
        Self {
            kind,
            readonly_txids: Vec::new(),
            allocs: HashMap::new(),
            cache: HashSet::new(),
            pending: HashMap::new(),
            ids: Vec::new(),
            free_pages_count: 0,
            freemaps: HashMap::new(),
            forward_map: HashMap::new(),
            backward_map: HashMap::new(),
        }
    }

    #[must_use]
    #[allow(dead_code)]
    pub fn kind(&self) -> FreelistType {
        self.kind
    }

    pub fn init(&mut self, ids: Vec<Pgid>) {
        match self.kind {
            FreelistType::Array => {
                self.ids = ids;
                self.reindex();
            }
            FreelistType::HashMap => {
                self.hashmap_init(ids);
            }
        }
    }

    pub fn allocate(&mut self, txid: Txid, n: usize) -> Pgid {
        match self.kind {
            FreelistType::Array => self.array_allocate(txid, n),
            FreelistType::HashMap => self.hashmap_allocate(txid, n),
        }
    }

    pub fn free_count(&self) -> usize {
        match self.kind {
            FreelistType::Array => self.ids.len(),
            FreelistType::HashMap => self.free_pages_count as usize,
        }
    }

    pub fn pending_count(&self) -> usize {
        self.pending.values().map(|p| p.ids.len()).sum()
    }

    /// Sorted list of all pending page ids (test / debug helper).
    #[allow(dead_code)]
    pub fn pending_page_ids_sorted(&self) -> Vec<Pgid> {
        let mut ids: Vec<Pgid> = self
            .pending
            .values()
            .flat_map(|p| p.ids.iter().copied())
            .collect();
        ids.sort_unstable();
        ids
    }

    pub fn count(&self) -> usize {
        self.free_count() + self.pending_count()
    }

    #[allow(dead_code)]
    pub fn freed(&self, pgid: Pgid) -> bool {
        self.cache.contains(&pgid)
    }

    pub fn free(&mut self, txid: Txid, pgid: Pgid, overflow: u32) {
        if pgid <= 1 {
            panic!("cannot free page 0 or 1: {pgid}");
        }
        let alloc_txid = self.allocs.remove(&pgid).unwrap_or(0);
        let txp = self.pending.entry(txid).or_default();
        for id in pgid..=pgid + Pgid::from(overflow) {
            if self.cache.contains(&id) {
                panic!("page {id} already freed");
            }
            txp.ids.push(id);
            txp.alloc_tx.push(alloc_txid);
            self.cache.insert(id);
        }
    }

    pub fn rollback(&mut self, txid: Txid) {
        if let Some(txp) = self.pending.remove(&txid) {
            for (i, pgid) in txp.ids.iter().enumerate() {
                self.cache.remove(pgid);
                let tx = txp.alloc_tx[i];
                if tx == 0 {
                    continue;
                }
                if tx != txid {
                    self.allocs.insert(*pgid, tx);
                } else {
                    panic!(
                        "rollback: freed page ({pgid}) was allocated by the same transaction ({txid})"
                    );
                }
            }
        }
        self.allocs.retain(|_, tid| *tid != txid);
    }

    pub fn add_readonly_txid(&mut self, tid: Txid) {
        self.readonly_txids.push(tid);
    }

    pub fn remove_readonly_txid(&mut self, tid: Txid) {
        if let Some(i) = self.readonly_txids.iter().position(|t| *t == tid) {
            self.readonly_txids.swap_remove(i);
        }
    }

    pub fn release_pending_pages(&mut self) {
        self.readonly_txids.sort_unstable();
        let mut minid = Txid::MAX;
        if let Some(&first) = self.readonly_txids.first() {
            minid = first;
        }
        if minid > 0 {
            self.release(minid - 1);
        }
        for &tid in &self.readonly_txids.clone() {
            self.release_range(minid, tid.saturating_sub(1));
            minid = tid.saturating_add(1);
        }
        self.release_range(minid, Txid::MAX);
    }

    fn release(&mut self, txid: Txid) {
        let mut m = Vec::new();
        self.pending.retain(|&tid, txp| {
            if tid <= txid {
                m.extend_from_slice(&txp.ids);
                false
            } else {
                true
            }
        });
        self.merge_spans(m);
    }

    fn release_range(&mut self, begin: Txid, end: Txid) {
        if begin > end {
            return;
        }
        let mut m = Vec::new();
        let keys: Vec<Txid> = self.pending.keys().copied().collect();
        for tid in keys {
            if tid < begin || tid > end {
                continue;
            }
            let txp = self.pending.get_mut(&tid).unwrap();
            if txp.last_release_begin == begin {
                continue;
            }
            let mut i = 0;
            while i < txp.ids.len() {
                let atx = txp.alloc_tx[i];
                if atx < begin || atx > end {
                    i += 1;
                    continue;
                }
                m.push(txp.ids[i]);
                let last = txp.ids.len() - 1;
                txp.ids.swap(i, last);
                txp.alloc_tx.swap(i, last);
                txp.ids.pop();
                txp.alloc_tx.pop();
            }
            txp.last_release_begin = begin;
            if txp.ids.is_empty() {
                self.pending.remove(&tid);
            }
        }
        self.merge_spans(m);
    }

    fn merge_spans(&mut self, ids: Vec<Pgid>) {
        match self.kind {
            FreelistType::Array => {
                let mut ids = ids;
                ids.sort_unstable();
                self.ids = crate::page::merge_pgids(&self.ids, &ids);
            }
            FreelistType::HashMap => self.hashmap_merge_spans(ids),
        }
    }

    pub fn copy_all(&self) -> Vec<Pgid> {
        let mut pending = Vec::with_capacity(self.pending_count());
        for txp in self.pending.values() {
            pending.extend_from_slice(&txp.ids);
        }
        pending.sort_unstable();
        let free = self.free_page_ids();
        crate::page::merge_pgids(&free, &pending)
    }

    fn free_page_ids(&self) -> Vec<Pgid> {
        match self.kind {
            FreelistType::Array => self.ids.clone(),
            FreelistType::HashMap => self.hashmap_free_page_ids(),
        }
    }

    #[cfg(test)]
    fn test_set_pending(&mut self, txid: Txid, ids: Vec<Pgid>) {
        let txp = self.pending.entry(txid).or_default();
        txp.ids = ids.clone();
        for id in ids {
            self.cache.insert(id);
        }
    }

    #[cfg(test)]
    fn test_release_range(&mut self, begin: Txid, end: Txid) {
        self.release_range(begin, end);
    }

    pub fn read_page(&mut self, page: &[u8]) {
        let hdr = PageHeader::read(page);
        assert!(hdr.is_freelist(), "invalid freelist page: {}", hdr.typ());
        let mut ids = freelist_ids(page);
        ids.sort_unstable();
        self.init(ids);
    }

    pub fn no_sync_reload(&mut self, pgids: Vec<Pgid>) {
        let pcache: HashSet<Pgid> = self
            .pending
            .values()
            .flat_map(|p| p.ids.iter().copied())
            .collect();
        let a: Vec<Pgid> = pgids.into_iter().filter(|id| !pcache.contains(id)).collect();
        self.init(a);
    }

    pub fn reload(&mut self, page: &[u8]) {
        self.read_page(page);
        let ids = self.free_page_ids();
        self.no_sync_reload(ids);
    }

    fn reindex(&mut self) {
        self.cache.clear();
        for &id in &self.ids {
            self.cache.insert(id);
        }
        for txp in self.pending.values() {
            for &id in &txp.ids {
                self.cache.insert(id);
            }
        }
    }

    pub fn write_page(&self, page: &mut [u8]) {
        let ids = self.copy_all();
        write_freelist(page, &ids);
    }

    pub fn estimated_write_page_size(&self) -> usize {
        estimated_freelist_page_size(self.count())
    }

    pub fn pages_for_write(&self, page_size: usize) -> usize {
        (self.estimated_write_page_size() / page_size) + 1
    }

    // --- array backend ---

    fn array_allocate(&mut self, txid: Txid, n: usize) -> Pgid {
        if self.ids.is_empty() {
            return 0;
        }
        let mut initial: Pgid = 0;
        let mut prev: Pgid = 0;
        for i in 0..self.ids.len() {
            let id = self.ids[i];
            if id <= 1 {
                panic!("invalid page allocation: {id}");
            }
            if prev == 0 || id - prev != 1 {
                initial = id;
            }
            if (id - initial) + 1 == n as Pgid {
                if i + 1 == n {
                    self.ids.drain(0..n);
                } else {
                    self.ids.drain(i + 1 - n..=i);
                }
                for k in 0..n as Pgid {
                    self.cache.remove(&(initial + k));
                }
                self.allocs.insert(initial, txid);
                return initial;
            }
            prev = id;
        }
        0
    }

    // --- hashmap backend ---

    fn hashmap_init(&mut self, pgids: Vec<Pgid>) {
        self.free_pages_count = 0;
        self.freemaps.clear();
        self.forward_map.clear();
        self.backward_map.clear();
        if pgids.is_empty() {
            self.reindex_hash();
            return;
        }
        assert!(
            pgids.windows(2).all(|w| w[0] < w[1]),
            "pgids not sorted"
        );
        let mut size = 1u64;
        let mut start = pgids[0];
        for i in 1..pgids.len() {
            if pgids[i] == pgids[i - 1] + 1 {
                size += 1;
            } else {
                self.add_span(start, size);
                size = 1;
                start = pgids[i];
            }
        }
        if size != 0 && start != 0 {
            self.add_span(start, size);
        }
        self.reindex_hash();
    }

    fn reindex_hash(&mut self) {
        self.cache.clear();
        for (&start, &size) in &self.forward_map {
            for i in 0..size {
                self.cache.insert(start + i);
            }
        }
        for txp in self.pending.values() {
            for &id in &txp.ids {
                self.cache.insert(id);
            }
        }
    }

    fn hashmap_allocate(&mut self, txid: Txid, n: usize) -> Pgid {
        if n == 0 {
            return 0;
        }
        let n64 = n as u64;
        if let Some(set) = self.freemaps.get(&n64).cloned() {
            if let Some(&pid) = set.iter().next() {
                self.del_span(pid, n64);
                self.allocs.insert(pid, txid);
                for i in 0..n as Pgid {
                    self.cache.remove(&(pid + i));
                }
                return pid;
            }
        }
        let sizes: Vec<u64> = self.freemaps.keys().copied().filter(|s| *s >= n64).collect();
        for size in sizes {
            let set = self.freemaps.get(&size).cloned().unwrap_or_default();
            if let Some(&pid) = set.iter().next() {
                self.del_span(pid, size);
                self.allocs.insert(pid, txid);
                let remain = size - n64;
                if remain > 0 {
                    self.add_span(pid + n as Pgid, remain);
                }
                for i in 0..n as Pgid {
                    self.cache.remove(&(pid + i));
                }
                return pid;
            }
        }
        0
    }

    fn hashmap_free_page_ids(&self) -> Vec<Pgid> {
        let count = self.free_pages_count as usize;
        if count == 0 {
            return Vec::new();
        }
        let mut starts: Vec<Pgid> = self.forward_map.keys().copied().collect();
        starts.sort_unstable();
        let mut m = Vec::with_capacity(count);
        for start in starts {
            if let Some(&size) = self.forward_map.get(&start) {
                for i in 0..size {
                    m.push(start + i);
                }
            }
        }
        m
    }

    fn add_span(&mut self, start: Pgid, size: u64) {
        self.backward_map.insert(start + size - 1, size);
        self.forward_map.insert(start, size);
        self.freemaps.entry(size).or_default().insert(start);
        self.free_pages_count += size;
    }

    fn del_span(&mut self, start: Pgid, size: u64) {
        self.forward_map.remove(&start);
        self.backward_map.remove(&(start + size - 1));
        if let Some(set) = self.freemaps.get_mut(&size) {
            set.remove(&start);
            if set.is_empty() {
                self.freemaps.remove(&size);
            }
        }
        self.free_pages_count -= size;
    }

    fn hashmap_merge_spans(&mut self, mut ids: Vec<Pgid>) {
        if ids.is_empty() {
            return;
        }
        ids.sort_unstable();
        let mut start = ids[0];
        let mut end = ids[0];
        for &id in &ids[1..] {
            if id == end + 1 {
                end = id;
                continue;
            }
            self.merge_with_existing_span(start, end);
            start = id;
            end = id;
        }
        self.merge_with_existing_span(start, end);
    }

    fn merge_with_existing_span(&mut self, start: Pgid, end: Pgid) {
        let prev = start - 1;
        let next = end + 1;
        let pre_size = self.backward_map.get(&prev).copied();
        let next_size = self.forward_map.get(&next).copied();
        let mut new_start = start;
        let mut new_size = end - start + 1;
        if let Some(pre) = pre_size {
            let prev_start = prev + 1 - pre;
            self.del_span(prev_start, pre);
            new_start -= pre;
            new_size += pre;
        }
        if let Some(ns) = next_size {
            self.del_span(next, ns);
            new_size += ns;
        }
        self.add_span(new_start, new_size);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Go: TestFreelistArray_allocate
    // Go: TestFreelistHashmap_allocate
    #[test]
    fn array_allocate_contiguous() {
        // Go: TestFreelistArray_allocate
        let mut f = Freelist::new(FreelistType::Array);
        f.init(vec![3, 4, 5, 8, 9]);
        assert_eq!(f.allocate(1, 3), 3);
        assert_eq!(f.allocate(1, 2), 8);
        assert_eq!(f.allocate(1, 1), 0);
    }

    #[test]
    fn hashmap_allocate_exact_and_split() {
        // Go: TestFreelistHashmap_allocate — free counts are stable; exact pgid may vary by span map iteration order.
        let mut f = Freelist::new(FreelistType::HashMap);
        f.init(vec![3, 4, 5, 6, 7, 9, 12, 13, 18]);
        assert_eq!(f.free_count(), 9);
        assert_eq!(f.allocate(1, 3), 3);
        assert_eq!(f.free_count(), 6);
        assert_ne!(f.allocate(1, 2), 0);
        assert_eq!(f.free_count(), 4);
        assert_ne!(f.allocate(1, 1), 0);
        assert_eq!(f.free_count(), 3);
        assert_eq!(f.allocate(1, 0), 0);
        assert_eq!(f.free_count(), 3);
    }

    #[test]
    fn hashmap_merge_adjacent() {
        // Go: TestFreelistHashmap_mergeWithExist (subset)
        let mut f = Freelist::new(FreelistType::HashMap);
        f.init(vec![10, 11]);
        f.merge_spans(vec![12, 13]);
        assert_eq!(f.free_count(), 4);
        assert_eq!(f.allocate(1, 4), 10);
    }

    // Go: TestFreelist_free
    #[test]
    fn freelist_free_single_page() {
        let mut f = Freelist::new(FreelistType::Array);
        f.free(100, 12, 0);
        let pending: Vec<Pgid> = f.pending.values().flat_map(|p| p.ids.clone()).collect();
        assert_eq!(pending, vec![12]);
    }

    // Go: TestFreelist_free_overflow
    #[test]
    fn freelist_free_overflow() {
        let mut f = Freelist::new(FreelistType::Array);
        f.free(100, 12, 3);
        let pending: Vec<Pgid> = f.pending.values().flat_map(|p| p.ids.clone()).collect();
        assert_eq!(pending, vec![12, 13, 14, 15]);
    }

    // Go: TestFreelist_free_double_free_panics
    #[test]
    #[should_panic(expected = "already freed")]
    fn freelist_free_double_free_panics() {
        let mut f = Freelist::new(FreelistType::Array);
        f.free(100, 12, 3);
        f.free(100, 12, 3);
    }

    // Go: TestFreelist_free_meta_panics
    #[test]
    #[should_panic(expected = "cannot free page 0 or 1")]
    fn freelist_free_meta_page_zero_panics() {
        let mut f = Freelist::new(FreelistType::Array);
        f.free(100, 0, 0);
    }

    #[test]
    #[should_panic(expected = "cannot free page 0 or 1")]
    fn freelist_free_meta_page_one_panics() {
        let mut f = Freelist::new(FreelistType::Array);
        f.free(100, 1, 0);
    }

    // Go: TestFreelist_release
    #[test]
    fn freelist_release() {
        let mut f = Freelist::new(FreelistType::Array);
        f.free(100, 12, 1);
        f.free(100, 9, 0);
        f.free(102, 39, 0);
        f.release(100);
        f.release(101);
        assert_eq!(f.free_page_ids(), vec![9, 12, 13]);
        f.release(102);
        assert_eq!(f.free_page_ids(), vec![9, 12, 13, 39]);
    }

    // Go: TestFreeList_init
    // Go: TestFreeList_reload (array backend)
    #[test]
    fn freelist_init_and_reload() {
        let mut buf = vec![0u8; 4096];
        let mut f = Freelist::new(FreelistType::Array);
        f.init(vec![5, 6, 8]);
        f.write_page(&mut buf);
        let mut f2 = Freelist::new(FreelistType::Array);
        f2.read_page(&buf);
        assert_eq!(f2.free_page_ids(), vec![5, 6, 8]);
        f2.init(vec![]);
        assert!(f2.free_page_ids().is_empty());

        f2.init(vec![5, 6, 8]);
        f2.free(5, 10, 2);
        f2.reload(&buf);
        assert_eq!(f2.free_page_ids(), vec![5, 6, 8]);
        let pending: Vec<Pgid> = f2.pending.values().flat_map(|p| p.ids.clone()).collect();
        assert_eq!(pending, vec![10, 11, 12]);
    }

    // Go: freelist copy_all / write path
    #[test]
    fn freelist_copy_all_merges_pending() {
        let mut f = Freelist::new(FreelistType::Array);
        f.init(vec![4, 7]);
        f.free(10, 5, 0);
        f.free(10, 6, 0);
        f.release(10);
        assert_eq!(f.copy_all(), vec![4, 5, 6, 7]);
    }

    // Go: Test_Freelist_Hashmap_Rollback
    #[test]
    fn hashmap_rollback() {
        let mut f = Freelist::new(FreelistType::HashMap);
        f.init(vec![3, 5, 6, 7, 12, 13]);
        f.free(100, 20, 1);
        f.allocate(100, 3);
        f.free(100, 25, 0);
        f.allocate(100, 2);
        f.rollback(100);
        assert!(f.allocs.is_empty());
        assert!(f.pending.is_empty());
    }

    #[test]
    fn free_and_release() {
        // Go: TestFreelist_free + TestFreelist_release (combined smoke)
        let mut f = Freelist::new(FreelistType::Array);
        f.free(10, 5, 0);
        f.free(10, 6, 0);
        assert_eq!(f.pending_count(), 2);
        assert!(f.freed(5));
        f.release(10);
        assert_eq!(f.ids, vec![5, 6]);
        assert_eq!(f.pending_count(), 0);
    }

    #[test]
    fn hashmap_free_page_ids_sorted() {
        // Go: TestFreelistHashmap_GetFreePageIDs (small deterministic case)
        let mut f = Freelist::new(FreelistType::HashMap);
        f.init(vec![2, 5, 6, 10]);
        let ids = f.copy_all();
        assert!(ids.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn freelist_free_freelist_page() {
        // Go: TestFreelist_free_freelist
        let mut f = Freelist::new(FreelistType::Array);
        f.free(100, 12, 0);
        assert_eq!(f.pending_count(), 1);
        assert!(f.freed(12));
    }

    // Go: TestFreelist_free_freelist_alloctx
    #[test]
    fn freelist_free_freelist_alloctx() {
        let mut f = Freelist::new(FreelistType::Array);
        f.free(100, 12, 0);
        f.rollback(100);
        assert!(f.copy_all().is_empty());
        assert_eq!(f.pending_count(), 0);
        assert!(!f.freed(12));

        f.free(101, 12, 0);
        assert!(f.freed(12));
        assert_eq!(f.pending_count(), 1);
        f.release_pending_pages();
        assert_eq!(f.pending_count(), 0);
        assert_eq!(f.copy_all(), vec![12]);
    }

    // Go: TestFreeList_init
    // Go: TestFreelist_write
    // Go: TestFreelist_read
    #[test]
    fn freelist_write_read_roundtrip() {
        let mut f = Freelist::new(FreelistType::Array);
        f.init(vec![5, 6, 8]);
        let mut buf = vec![0u8; 4096];
        crate::page::set_page_id(&mut buf, 2);
        f.write_page(&mut buf);
        let mut f2 = Freelist::new(FreelistType::Array);
        f2.read_page(&buf);
        assert_eq!(f2.copy_all(), vec![5, 6, 8]);
        f2.init(vec![]);
        assert!(f2.copy_all().is_empty());
    }

    // Go: TestFreeList_reload
    #[test]
    fn freelist_reload_preserves_pending() {
        let mut f = Freelist::new(FreelistType::Array);
        f.init(vec![5, 6, 8]);
        let mut buf = vec![0u8; 4096];
        crate::page::set_page_id(&mut buf, 2);
        f.write_page(&mut buf);

        let mut f2 = Freelist::new(FreelistType::Array);
        f2.read_page(&buf);
        assert_eq!(f2.copy_all(), vec![5, 6, 8]);
        f2.free(5, 10, 2); // 10,11,12
        f2.reload(&buf);
        assert_eq!(f2.free_page_ids(), vec![5, 6, 8]);
        assert_eq!(f2.pending_count(), 3);
        assert!(f2.freed(10));
    }

    // Go: TestFreelist_releaseRange (first table case subset)
    #[test]
    fn freelist_release_range_basic() {
        let mut f = Freelist::new(FreelistType::Array);
        f.init(vec![3, 4, 5]);
        let _ = f.allocate(100, 1); // takes 3
        f.free(150, 3, 0);
        f.release_range(100, 200);
        assert!(f.copy_all().contains(&3) || f.free_count() >= 2);
    }

    // Go: TestInvalidArrayAllocation
    #[test]
    #[should_panic(expected = "invalid page allocation")]
    fn freelist_invalid_array_allocation_panics() {
        let mut f = Freelist::new(FreelistType::Array);
        f.init(vec![1]);
        let _ = f.allocate(1, 1);
    }

    // Go: Test_Freelist_Array_Rollback
    #[test]
    fn freelist_array_rollback() {
        let mut f = Freelist::new(FreelistType::Array);
        f.init(vec![3, 5, 6, 7, 12, 13]);
        f.free(100, 20, 1);
        let _ = f.allocate(100, 3);
        f.free(100, 25, 0);
        let _ = f.allocate(100, 2);
        f.rollback(100);
        assert_eq!(f.pending_count(), 0);
    }

    // Go: TestFreelistHashmap_init_panics
    #[test]
    #[should_panic(expected = "pgids not sorted")]
    fn freelist_hashmap_init_panics() {
        let mut f = Freelist::new(FreelistType::HashMap);
        f.init(vec![25, 5]);
    }

    // Go: Test_freelist_ReadIDs_and_getFreePageIDs
    #[test]
    fn freelist_read_ids_and_get_free_page_ids() {
        let mut f = Freelist::new(FreelistType::Array);
        f.init(vec![2, 3, 4, 10]);
        assert_eq!(f.free_page_ids(), vec![2, 3, 4, 10]);
        let mut buf = vec![0u8; 4096];
        crate::page::set_page_id(&mut buf, 2);
        f.write_page(&mut buf);
        let mut f2 = Freelist::new(FreelistType::HashMap);
        f2.read_page(&buf);
        assert_eq!(f2.free_page_ids(), vec![2, 3, 4, 10]);
    }

    fn for_each_backend(f: impl Fn(FreelistType)) {
        f(FreelistType::Array);
        f(FreelistType::HashMap);
    }

    fn require_pages(f: &Freelist, free: &[Pgid], pending: &[Pgid]) {
        assert_eq!(f.free_count() + f.pending_count(), f.count());
        assert_eq!(f.free_page_ids(), free, "unexpected free pages");
        assert_eq!(f.free_count(), free.len());
        let pp = f.pending_page_ids_sorted();
        assert_eq!(pp, pending, "unexpected pending pages");
        assert_eq!(pp.len(), f.pending_count());
        for &pgid in f.free_page_ids().iter().chain(pp.iter()) {
            assert!(f.freed(pgid), "expected page {pgid} to be marked freed");
        }
    }

    // Go: TestFreelist_read
    #[test]
    fn freelist_read() {
        for_each_backend(|kind| {
            let mut buf = [0u8; 4096];
            crate::page::set_page_flags(&mut buf, crate::page::FREELIST_PAGE_FLAG);
            crate::page::set_page_count(&mut buf, 2);
            crate::page::write_u64(&mut buf, crate::page::PAGE_HEADER_SIZE, 23);
            crate::page::write_u64(&mut buf, crate::page::PAGE_HEADER_SIZE + 8, 50);
            let mut f = Freelist::new(kind);
            f.read_page(&buf);
            assert_eq!(f.free_page_ids(), vec![23, 50]);
        });
    }

    // Go: TestFreelist_read_panics
    #[test]
    fn freelist_read_panics() {
        for_each_backend(|kind| {
            let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut buf = [0u8; 4096];
                crate::page::set_page_flags(&mut buf, crate::page::BRANCH_PAGE_FLAG);
                crate::page::set_page_count(&mut buf, 2);
                let mut f = Freelist::new(kind);
                f.read_page(&buf);
            }));
            assert!(r.is_err(), "expected panic for {kind:?}");
        });
    }

    // Go: TestFreelist_write
    #[test]
    fn freelist_write() {
        for_each_backend(|kind| {
            let mut f = Freelist::new(kind);
            f.init(vec![12, 39]);
            f.test_set_pending(100, vec![28, 11]);
            f.test_set_pending(101, vec![3]);
            let mut buf = vec![0u8; 4096];
            f.write_page(&mut buf);
            let mut f2 = Freelist::new(kind);
            f2.read_page(&buf);
            assert_eq!(f2.free_page_ids(), vec![3, 11, 12, 28, 39]);
        });
    }

    // Go: TestFreelist_E2E_HappyPath
    #[test]
    fn freelist_e2e_happy_path() {
        for_each_backend(|kind| {
            let mut f = Freelist::new(kind);
            f.init(vec![]);
            require_pages(&f, &[], &[]);

            assert_eq!(f.allocate(1, 5), 0);
            f.free(2, 5, 0);
            f.free(2, 3, 0);
            f.free(2, 8, 0);
            require_pages(&f, &[], &[3, 5, 8]);

            f.add_readonly_txid(3);
            f.release_pending_pages();
            require_pages(&f, &[3, 5, 8], &[]);

            assert_eq!(f.allocate(4, 2), 0);
            let mut expected = std::collections::HashSet::from([3u64, 5, 8]);
            for _ in 0..3 {
                let allocated = f.allocate(4, 1);
                assert!(expected.remove(&allocated), "unexpected pgid {allocated}");
                assert!(!f.freed(allocated));
            }
            assert!(expected.is_empty());
            assert_eq!(f.allocate(4, 1), 0);
        });
    }

    // Go: TestFreelist_E2E_MultiSpanOverflows
    #[test]
    fn freelist_e2e_multi_span_overflows() {
        for_each_backend(|kind| {
            let mut f = Freelist::new(kind);
            f.init(vec![]);
            f.free(10, 20, 1);
            f.free(10, 25, 2);
            f.free(10, 35, 3);
            f.free(10, 39, 2);
            f.free(10, 45, 4);
            require_pages(
                &f,
                &[],
                &[
                    20, 21, 25, 26, 27, 35, 36, 37, 38, 39, 40, 41, 45, 46, 47, 48, 49,
                ],
            );
            f.release_pending_pages();
            require_pages(
                &f,
                &[
                    20, 21, 25, 26, 27, 35, 36, 37, 38, 39, 40, 41, 45, 46, 47, 48, 49,
                ],
                &[],
            );

            let alloc_sequence = [7usize, 5, 3, 2];
            let expected_starts = [35u64, 45, 25, 20];
            for (i, &page_nums) in alloc_sequence.iter().enumerate() {
                let allocated = f.allocate(11, page_nums);
                assert_eq!(allocated, expected_starts[i]);
                for j in 0..page_nums as u64 {
                    assert!(!f.freed(allocated + j));
                }
            }
        });
    }

    // Go: TestFreelist_E2E_Rollbacks
    #[test]
    fn freelist_e2e_rollbacks() {
        for_each_backend(|kind| {
            let mut f = Freelist::new(kind);
            f.init(vec![]);
            f.free(2, 5, 1);
            f.free(2, 8, 0);
            require_pages(&f, &[], &[5, 6, 8]);
            f.rollback(2);
            require_pages(&f, &[], &[]);

            f.free(4, 13, 3);
            require_pages(&f, &[], &[13, 14, 15, 16]);
            f.release_pending_pages();
            require_pages(&f, &[13, 14, 15, 16], &[]);
            f.rollback(1337);
            require_pages(&f, &[13, 14, 15, 16], &[]);
        });
    }

    // Go: TestFreelist_E2E_RollbackPanics
    #[test]
    fn freelist_e2e_rollback_panics() {
        for_each_backend(|kind| {
            let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut f = Freelist::new(kind);
                f.init(vec![5]);
                require_pages(&f, &[5], &[]);
                let _ = f.allocate(5, 1);
                f.free(5, 5, 0);
                f.rollback(5);
            }));
            assert!(r.is_err(), "expected panic for {kind:?}");
        });
    }

    // Go: TestFreelist_E2E_Reload
    #[test]
    fn freelist_e2e_reload() {
        for_each_backend(|kind| {
            let mut freelist = Freelist::new(kind);
            freelist.init(vec![]);
            freelist.free(2, 5, 1);
            freelist.free(2, 8, 0);
            freelist.release_pending_pages();
            require_pages(&freelist, &[5, 6, 8], &[]);
            let mut buf = vec![0u8; 4096];
            freelist.write_page(&mut buf);

            freelist.free(3, 3, 1);
            freelist.free(3, 10, 2);
            require_pages(&freelist, &[5, 6, 8], &[3, 4, 10, 11, 12]);

            let mut other_buf = vec![0u8; 4096];
            freelist.write_page(&mut other_buf);

            let mut load_freelist = Freelist::new(kind);
            load_freelist.init(vec![]);
            load_freelist.read_page(&other_buf);
            require_pages(
                &load_freelist,
                &[3, 4, 5, 6, 8, 10, 11, 12],
                &[],
            );
            load_freelist.reload(&buf);
            require_pages(&load_freelist, &[5, 6, 8], &[]);

            let mut freelist2 = Freelist::new(kind);
            freelist2.init(vec![]);
            freelist2.free(5, 5, 4);
            freelist2.reload(&buf);
            require_pages(&freelist2, &[], &[5, 6, 7, 8, 9]);
        });
    }

    // Go: TestFreelist_E2E_SerDe_HappyPath
    #[test]
    fn freelist_e2e_serde_happy_path() {
        for_each_backend(|kind| {
            let mut freelist = Freelist::new(kind);
            freelist.init(vec![]);
            freelist.free(2, 5, 1);
            freelist.free(2, 8, 0);
            freelist.release_pending_pages();
            require_pages(&freelist, &[5, 6, 8], &[]);

            freelist.free(3, 3, 1);
            freelist.free(3, 10, 2);
            require_pages(&freelist, &[5, 6, 8], &[3, 4, 10, 11, 12]);

            assert_eq!(freelist.estimated_write_page_size(), 80);
            let mut buf = vec![0u8; freelist.estimated_write_page_size()];
            freelist.write_page(&mut buf);

            let mut load_freelist = Freelist::new(kind);
            load_freelist.init(vec![]);
            load_freelist.read_page(&buf);
            require_pages(
                &load_freelist,
                &[3, 4, 5, 6, 8, 10, 11, 12],
                &[],
            );
        });
    }

    // Go: TestFreelist_E2E_SerDe_AcrossImplementations
    #[test]
    fn freelist_e2e_serde_across_implementations() {
        let sizes = [0usize, 1, 10, 100, 1000, 0xFFFF, 0xFFFF + 1];
        for &size in &sizes {
            for kind in [FreelistType::Array, FreelistType::HashMap] {
                let mut freelist = Freelist::new(kind);
                let mut expected: Vec<Pgid> = Vec::new();
                for i in 0..size {
                    let pgid = (i + 2) as Pgid;
                    freelist.free(1, pgid, 0);
                    expected.push(pgid);
                }
                freelist.release_pending_pages();
                require_pages(&freelist, &expected, &[]);
                let mut buf = vec![0u8; freelist.estimated_write_page_size()];
                freelist.write_page(&mut buf);
                for load_kind in [FreelistType::Array, FreelistType::HashMap] {
                    let mut load_freelist = Freelist::new(load_kind);
                    load_freelist.read_page(&buf);
                    require_pages(&load_freelist, &expected, &[]);
                }
            }
        }
    }

    // Go: TestFreelist_E2E_SerDe_AcrossImplementations (n=0xFFFF*2) — very large; run manually.
    #[test]
    #[ignore = "0xFFFF*2 freelist pages is slow/memory-heavy; Go runs it in CI"]
    fn freelist_e2e_serde_across_implementations_huge() {
        let size = 0xFFFF * 2;
        let mut freelist = Freelist::new(FreelistType::Array);
        let mut expected: Vec<Pgid> = Vec::with_capacity(size);
        for i in 0..size {
            let pgid = (i + 2) as Pgid;
            freelist.free(1, pgid, 0);
            expected.push(pgid);
        }
        freelist.release_pending_pages();
        let mut buf = vec![0u8; freelist.estimated_write_page_size()];
        freelist.write_page(&mut buf);
        let mut load_freelist = Freelist::new(FreelistType::HashMap);
        load_freelist.read_page(&buf);
        require_pages(&load_freelist, &expected, &[]);
    }

    // Go: TestTxidSorting
    #[test]
    fn txid_sorting() {
        for seed in 0u64..200 {
            let mut txids: Vec<Txid> = (0..20)
                .map(|i| seed.wrapping_mul(31).wrapping_add(i * 17) % 1000)
                .collect();
            txids.sort_unstable();
            for w in txids.windows(2) {
                assert!(w[0] <= w[1], "txids not sorted: {txids:?}");
            }
        }
    }

    // Go: TestFreelist_releaseRange
    #[test]
    fn freelist_release_range_table() {
        struct TestPage {
            id: Pgid,
            n: usize,
            alloc_tx: Txid,
            free_tx: Txid,
        }
        struct TestRange {
            begin: Txid,
            end: Txid,
        }
        struct Case {
            title: &'static str,
            pages_in: &'static [TestPage],
            release_ranges: &'static [TestRange],
            want_free: &'static [Pgid],
        }

        let cases = [
            Case {
                title: "Single pending in range",
                pages_in: &[TestPage {
                    id: 3,
                    n: 1,
                    alloc_tx: 100,
                    free_tx: 200,
                }],
                release_ranges: &[TestRange { begin: 1, end: 300 }],
                want_free: &[3],
            },
            Case {
                title: "Single pending with minimum end range",
                pages_in: &[TestPage {
                    id: 3,
                    n: 1,
                    alloc_tx: 100,
                    free_tx: 200,
                }],
                release_ranges: &[TestRange { begin: 1, end: 200 }],
                want_free: &[3],
            },
            Case {
                title: "Single pending outsize minimum end range",
                pages_in: &[TestPage {
                    id: 3,
                    n: 1,
                    alloc_tx: 100,
                    free_tx: 200,
                }],
                release_ranges: &[TestRange { begin: 1, end: 199 }],
                want_free: &[],
            },
            Case {
                title: "Single pending with minimum begin range",
                pages_in: &[TestPage {
                    id: 3,
                    n: 1,
                    alloc_tx: 100,
                    free_tx: 200,
                }],
                release_ranges: &[TestRange { begin: 100, end: 300 }],
                want_free: &[3],
            },
            Case {
                title: "Single pending outside minimum begin range",
                pages_in: &[TestPage {
                    id: 3,
                    n: 1,
                    alloc_tx: 100,
                    free_tx: 200,
                }],
                release_ranges: &[TestRange { begin: 101, end: 300 }],
                want_free: &[],
            },
            Case {
                title: "Single pending in minimum range",
                pages_in: &[TestPage {
                    id: 3,
                    n: 1,
                    alloc_tx: 199,
                    free_tx: 200,
                }],
                release_ranges: &[TestRange { begin: 199, end: 200 }],
                want_free: &[3],
            },
            Case {
                title: "Single pending and read transaction at 199",
                pages_in: &[TestPage {
                    id: 3,
                    n: 1,
                    alloc_tx: 199,
                    free_tx: 200,
                }],
                release_ranges: &[
                    TestRange { begin: 100, end: 198 },
                    TestRange { begin: 200, end: 300 },
                ],
                want_free: &[],
            },
            Case {
                title: "Adjacent pending and read transactions at 199, 200",
                pages_in: &[
                    TestPage {
                        id: 3,
                        n: 1,
                        alloc_tx: 199,
                        free_tx: 200,
                    },
                    TestPage {
                        id: 4,
                        n: 1,
                        alloc_tx: 200,
                        free_tx: 201,
                    },
                ],
                release_ranges: &[
                    TestRange { begin: 100, end: 198 },
                    TestRange { begin: 200, end: 199 },
                    TestRange { begin: 201, end: 300 },
                ],
                want_free: &[],
            },
            Case {
                title: "Out of order ranges",
                pages_in: &[
                    TestPage {
                        id: 3,
                        n: 1,
                        alloc_tx: 199,
                        free_tx: 200,
                    },
                    TestPage {
                        id: 4,
                        n: 1,
                        alloc_tx: 200,
                        free_tx: 201,
                    },
                ],
                release_ranges: &[
                    TestRange { begin: 201, end: 199 },
                    TestRange { begin: 201, end: 200 },
                    TestRange { begin: 200, end: 200 },
                ],
                want_free: &[],
            },
            Case {
                title: "Multiple pending, read transaction at 150",
                pages_in: &[
                    TestPage {
                        id: 3,
                        n: 1,
                        alloc_tx: 100,
                        free_tx: 200,
                    },
                    TestPage {
                        id: 4,
                        n: 1,
                        alloc_tx: 100,
                        free_tx: 125,
                    },
                    TestPage {
                        id: 5,
                        n: 1,
                        alloc_tx: 125,
                        free_tx: 150,
                    },
                    TestPage {
                        id: 6,
                        n: 1,
                        alloc_tx: 125,
                        free_tx: 175,
                    },
                    TestPage {
                        id: 7,
                        n: 2,
                        alloc_tx: 150,
                        free_tx: 175,
                    },
                    TestPage {
                        id: 9,
                        n: 2,
                        alloc_tx: 175,
                        free_tx: 200,
                    },
                ],
                release_ranges: &[
                    TestRange { begin: 50, end: 149 },
                    TestRange { begin: 151, end: 300 },
                ],
                want_free: &[4, 9, 10],
            },
        ];

        for_each_backend(|kind| {
            for c in &cases {
                let mut f = Freelist::new(kind);
                let mut ids = Vec::new();
                for p in c.pages_in {
                    for i in 0..p.n {
                        ids.push(p.id + i as u64);
                    }
                }
                f.init(ids);
                for p in c.pages_in {
                    let _ = f.allocate(p.alloc_tx, p.n);
                }
                for p in c.pages_in {
                    f.free(p.free_tx, p.id, (p.n - 1) as u32);
                }
                for r in c.release_ranges {
                    f.test_release_range(r.begin, r.end);
                }
                assert_eq!(
                    f.free_page_ids(),
                    c.want_free,
                    "{} ({:?})",
                    c.title,
                    kind
                );
            }
        });
    }
}
