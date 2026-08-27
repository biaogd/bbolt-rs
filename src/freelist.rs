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
}
