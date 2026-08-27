//! Array-backed freelist matching etcd-io/bbolt's default `FreelistArrayType`.

use std::collections::{HashMap, HashSet};

use crate::page::{
    estimated_freelist_page_size, freelist_ids, write_freelist, PageHeader, Pgid, Txid,
    PAGE_HEADER_SIZE,
};

#[derive(Default)]
struct TxPending {
    ids: Vec<Pgid>,
    alloc_tx: Vec<Txid>,
    last_release_begin: Txid,
}

pub struct Freelist {
    ids: Vec<Pgid>,
    readonly_txids: Vec<Txid>,
    allocs: HashMap<Pgid, Txid>,
    cache: HashSet<Pgid>,
    pending: HashMap<Txid, TxPending>,
}

impl Default for Freelist {
    fn default() -> Self {
        Self::new()
    }
}

impl Freelist {
    pub fn new() -> Self {
        Self {
            ids: Vec::new(),
            readonly_txids: Vec::new(),
            allocs: HashMap::new(),
            cache: HashSet::new(),
            pending: HashMap::new(),
        }
    }

    pub fn init(&mut self, ids: Vec<Pgid>) {
        self.ids = ids;
        self.reindex();
    }

    pub fn allocate(&mut self, txid: Txid, n: usize) -> Pgid {
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

    pub fn free_count(&self) -> usize {
        self.ids.len()
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

    fn merge_spans(&mut self, mut ids: Vec<Pgid>) {
        ids.sort_unstable();
        self.ids = crate::page::merge_pgids(&self.ids, &ids);
    }

    pub fn copy_all(&self) -> Vec<Pgid> {
        let mut pending = Vec::with_capacity(self.pending_count());
        for txp in self.pending.values() {
            pending.extend_from_slice(&txp.ids);
        }
        pending.sort_unstable();
        crate::page::merge_pgids(&self.ids, &pending)
    }

    pub fn read_page(&mut self, page: &[u8]) {
        let hdr = PageHeader::read(page);
        assert!(hdr.is_freelist(), "invalid freelist page: {}", hdr.typ());
        let mut ids = freelist_ids(page);
        ids.sort_unstable();
        self.init(ids);
    }

    #[allow(dead_code)]
    pub fn no_sync_reload(&mut self, pgids: Vec<Pgid>) {
        let pcache: HashSet<Pgid> = self
            .pending
            .values()
            .flat_map(|p| p.ids.iter().copied())
            .collect();
        let a: Vec<Pgid> = pgids
            .into_iter()
            .filter(|id| !pcache.contains(id))
            .collect();
        self.init(a);
    }

    #[allow(dead_code)]
    pub fn reload(&mut self, page: &[u8]) {
        self.read_page(page);
        let ids = self.ids.clone();
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
}

impl Freelist {
    #[allow(dead_code)]
    pub fn header_overhead() -> usize {
        PAGE_HEADER_SIZE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocate_contiguous() {
        let mut f = Freelist::new();
        f.init(vec![3, 4, 5, 8, 9]);
        assert_eq!(f.allocate(1, 3), 3);
        assert_eq!(f.ids, vec![8, 9]);
        assert_eq!(f.allocate(1, 2), 8);
        assert_eq!(f.ids, Vec::<Pgid>::new());
        assert_eq!(f.allocate(1, 1), 0);
    }

    #[test]
    fn free_and_release() {
        let mut f = Freelist::new();
        f.free(10, 5, 0);
        f.free(10, 6, 0);
        assert_eq!(f.pending_count(), 2);
        assert!(f.freed(5));
        f.release(10);
        assert_eq!(f.ids, vec![5, 6]);
        assert_eq!(f.pending_count(), 0);
    }
}
