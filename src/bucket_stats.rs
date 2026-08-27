//! Per-bucket page statistics (upstream `Bucket.Stats`).

use crate::inner::{BucketId, TxInner};
use crate::page::{
    branch_at, leaf_at, read_u32, InBucket, PageHeader, Pgid, BUCKET_LEAF_FLAG,
    BRANCH_PAGE_ELEMENT_SIZE, LEAF_PAGE_ELEMENT_SIZE, PAGE_HEADER_SIZE,
};
use crate::stats::BucketStats;

pub fn bucket_stats(tx: &TxInner, bid: BucketId) -> BucketStats {
    let b = &tx.buckets[&bid];
    let page_size = tx.db.page_size;
    let root = b.header.root;
    let mut s = BucketStats {
        bucket_n: 1,
        ..Default::default()
    };
    if root == 0 {
        s.inline_bucket_n = 1;
    }
    let mut sub = BucketStats::default();
    if root == 0 {
        if let Some(page) = &b.inline_page {
            walk_page(tx, page, 0, true, &mut s, &mut sub);
        }
    } else {
        walk_tree(tx, root, 0, &mut s, &mut sub);
    }
    s.depth += sub.depth;
    s.add(&sub);
    s.branch_alloc = (s.branch_page_n + s.branch_overflow_n) * page_size;
    s.leaf_alloc = (s.leaf_page_n + s.leaf_overflow_n) * page_size;
    s
}

fn walk_tree(tx: &TxInner, pgid: Pgid, depth: usize, s: &mut BucketStats, sub: &mut BucketStats) {
    let page = tx.db.read_page(pgid).unwrap_or_else(|_| panic!("read page {pgid}"));
    walk_page(tx, &page, depth, false, s, sub);
}

fn walk_page(
    tx: &TxInner,
    page: &[u8],
    depth: usize,
    inline_root: bool,
    s: &mut BucketStats,
    sub: &mut BucketStats,
) {
    let hdr = PageHeader::read(page);
    if depth + 1 > s.depth {
        s.depth = depth + 1;
    }
    if hdr.is_leaf() {
        s.key_n += hdr.count as usize;
        let used = leaf_inuse(page);
        if inline_root {
            s.inline_bucket_inuse += used;
        } else {
            s.leaf_page_n += 1;
            s.leaf_inuse += used;
            s.leaf_overflow_n += hdr.overflow as usize;
        }
        for i in 0..hdr.count as usize {
            let (flags, _k, v) = leaf_at(page, i);
            if flags & BUCKET_LEAF_FLAG != 0 {
                sub.add(&stats_from_bucket_value(tx, v));
            }
        }
    } else if hdr.is_branch() {
        s.branch_page_n += 1;
        s.branch_inuse += branch_inuse(page);
        s.branch_overflow_n += hdr.overflow as usize;
        for i in 0..hdr.count as usize {
            let (child, _) = branch_at(page, i);
            walk_tree(tx, child, depth + 1, s, sub);
        }
    }
}

fn stats_from_bucket_value(tx: &TxInner, value: &[u8]) -> BucketStats {
    if value.len() < crate::page::BUCKET_HEADER_SIZE {
        return BucketStats::default();
    }
    let ib = InBucket::read(value);
    let mut s = BucketStats {
        bucket_n: 1,
        ..Default::default()
    };
    let mut sub = BucketStats::default();
    if ib.root == 0 {
        s.inline_bucket_n = 1;
        if value.len() > crate::page::BUCKET_HEADER_SIZE {
            let page = &value[crate::page::BUCKET_HEADER_SIZE..];
            walk_page(tx, page, 0, true, &mut s, &mut sub);
        }
    } else {
        walk_tree(tx, ib.root, 0, &mut s, &mut sub);
    }
    s.depth += sub.depth;
    s.add(&sub);
    let page_size = tx.db.page_size;
    s.branch_alloc = (s.branch_page_n + s.branch_overflow_n) * page_size;
    s.leaf_alloc = (s.leaf_page_n + s.leaf_overflow_n) * page_size;
    s
}

fn leaf_inuse(page: &[u8]) -> usize {
    let hdr = PageHeader::read(page);
    let count = hdr.count as usize;
    let mut used = PAGE_HEADER_SIZE;
    if count != 0 {
        used += LEAF_PAGE_ELEMENT_SIZE * (count - 1);
        let off = PAGE_HEADER_SIZE + (count - 1) * LEAF_PAGE_ELEMENT_SIZE;
        let pos = read_u32(page, off + 4) as usize;
        let ksize = read_u32(page, off + 8) as usize;
        let vsize = read_u32(page, off + 12) as usize;
        used += pos + ksize + vsize;
    }
    used
}

fn branch_inuse(page: &[u8]) -> usize {
    let hdr = PageHeader::read(page);
    let count = hdr.count as usize;
    let mut used = PAGE_HEADER_SIZE;
    if count != 0 {
        used += BRANCH_PAGE_ELEMENT_SIZE * (count - 1);
        let off = PAGE_HEADER_SIZE + (count - 1) * BRANCH_PAGE_ELEMENT_SIZE;
        let pos = read_u32(page, off) as usize;
        let ksize = read_u32(page, off + 4) as usize;
        used += pos + ksize;
    }
    used
}
