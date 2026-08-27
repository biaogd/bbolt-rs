//! Consistency checker (`Tx::Check` in upstream).

use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::inner::TxInner;
use crate::page::{
    branch_at, leaf_at, InBucket, PageHeader, Pgid, BUCKET_LEAF_FLAG, BRANCH_PAGE_FLAG,
    LEAF_PAGE_FLAG,
};

/// Options for [`crate::Tx::check`].
#[derive(Clone, Debug, Default)]
pub struct CheckOptions {
    /// If non-zero, check starting from this page instead of the whole DB.
    pub page_id: u64,
}

/// Run consistency checks. Returns all problems found (empty = OK).
pub fn check_tx(tx: &mut TxInner, opts: CheckOptions) -> Vec<Error> {
    let mut errs = Vec::new();
    if let Err(e) = check_tx_inner(tx, opts, &mut errs) {
        errs.push(e);
    }
    errs
}

fn check_tx_inner(tx: &mut TxInner, opts: CheckOptions, errs: &mut Vec<Error>) -> Result<()> {
    // Force-load freelist for read-only opens that skipped preload.
    let _ = tx.db.ensure_freelist_loaded();

    let mut freed: HashMap<Pgid, bool> = HashMap::new();
    {
        let fl = tx.db.freelist.lock();
        for id in fl.copy_all() {
            if freed.insert(id, true).is_some() {
                errs.push(Error::Corrupt(format!("page {id}: already freed")));
            }
        }
    }

    let mut reachable: HashMap<Pgid, bool> = HashMap::new();
    reachable.insert(0, true);
    reachable.insert(1, true);

    if tx.meta.freelist != crate::page::PGID_NO_FREELIST {
        if let Ok(page) = tx.db.read_page(tx.meta.freelist) {
            let hdr = PageHeader::read(&page);
            for i in 0..=hdr.overflow {
                reachable.insert(tx.meta.freelist + Pgid::from(i), true);
            }
        }
    }

    if opts.page_id == 0 {
        let root = tx.meta.root.root;
        if root != 0 {
            recursively_check_page(tx, root, &mut reachable, &freed, errs)?;
        }
        for i in 0..tx.meta.pgid {
            let is_reachable = reachable.contains_key(&i);
            let is_freed = freed.get(&i).copied().unwrap_or(false);
            if !is_reachable && !is_freed {
                errs.push(Error::Corrupt(format!(
                    "page {i}: unreachable unfreed"
                )));
            }
        }
    } else {
        if opts.page_id < 2 || opts.page_id >= tx.meta.pgid {
            errs.push(Error::Corrupt(format!(
                "page ID ({}) out of range [2, {})",
                opts.page_id, tx.meta.pgid
            )));
            return Ok(());
        }
        recursively_check_page(tx, opts.page_id, &mut reachable, &freed, errs)?;
    }
    Ok(())
}

fn recursively_check_page(
    tx: &TxInner,
    page_id: Pgid,
    reachable: &mut HashMap<Pgid, bool>,
    freed: &HashMap<Pgid, bool>,
    errs: &mut Vec<Error>,
) -> Result<()> {
    check_invariant(tx, page_id, reachable, freed, errs)?;
    recursively_check_bucket_in_page(tx, page_id, reachable, freed, errs)
}

fn recursively_check_bucket_in_page(
    tx: &TxInner,
    page_id: Pgid,
    reachable: &mut HashMap<Pgid, bool>,
    freed: &HashMap<Pgid, bool>,
    errs: &mut Vec<Error>,
) -> Result<()> {
    let page = tx.db.read_page(page_id)?;
    let hdr = PageHeader::read(&page);
    if hdr.is_branch() {
        for i in 0..hdr.count as usize {
            let (child, _) = branch_at(&page, i);
            recursively_check_bucket_in_page(tx, child, reachable, freed, errs)?;
        }
    } else if hdr.is_leaf() {
        for i in 0..hdr.count as usize {
            let (flags, _k, v) = leaf_at(&page, i);
            if flags & BUCKET_LEAF_FLAG != 0 && v.len() >= 16 {
                let ib = InBucket::read(v);
                if ib.root != 0 {
                    recursively_check_page(tx, ib.root, reachable, freed, errs)?;
                }
            }
        }
    } else {
        errs.push(Error::Corrupt(format!(
            "unexpected page type (flags: {:x}) for pgId:{page_id}",
            hdr.flags
        )));
    }
    Ok(())
}

fn check_invariant(
    tx: &TxInner,
    page_id: Pgid,
    reachable: &mut HashMap<Pgid, bool>,
    freed: &HashMap<Pgid, bool>,
    errs: &mut Vec<Error>,
) -> Result<()> {
    for_each_page(tx, page_id, &mut |p, stack| {
        verify_reachable(p, tx.meta.pgid, stack, reachable, freed, errs);
    })?;
    check_key_order(tx, page_id, None, None, &mut vec![page_id], errs)?;
    Ok(())
}

fn for_each_page<F>(tx: &TxInner, pgid: Pgid, fn_: &mut F) -> Result<()>
where
    F: FnMut(&[u8], &[Pgid]),
{
    for_each_page_internal(tx, &mut vec![pgid], fn_)
}

fn for_each_page_internal<F>(tx: &TxInner, stack: &mut Vec<Pgid>, fn_: &mut F) -> Result<()>
where
    F: FnMut(&[u8], &[Pgid]),
{
    let pgid = *stack.last().unwrap();
    let page = tx.db.read_page(pgid)?;
    fn_(&page, stack);
    let hdr = PageHeader::read(&page);
    if hdr.is_branch() {
        for i in 0..hdr.count as usize {
            let (child, _) = branch_at(&page, i);
            stack.push(child);
            for_each_page_internal(tx, stack, fn_)?;
            stack.pop();
        }
    }
    Ok(())
}

fn verify_reachable(
    page: &[u8],
    hwm: Pgid,
    stack: &[Pgid],
    reachable: &mut HashMap<Pgid, bool>,
    freed: &HashMap<Pgid, bool>,
    errs: &mut Vec<Error>,
) {
    let hdr = PageHeader::read(page);
    if hdr.id > hwm {
        errs.push(Error::Corrupt(format!(
            "page {}: out of bounds: {hwm} (stack: {stack:?})",
            hdr.id
        )));
    }
    for i in 0..=hdr.overflow {
        let id = hdr.id + Pgid::from(i);
        if reachable.insert(id, true).is_some() {
            errs.push(Error::Corrupt(format!(
                "page {id}: multiple references (stack: {stack:?})"
            )));
        }
    }
    if freed.get(&hdr.id).copied().unwrap_or(false) {
        errs.push(Error::Corrupt(format!("page {}: reachable freed", hdr.id)));
    } else if hdr.flags != BRANCH_PAGE_FLAG && hdr.flags != LEAF_PAGE_FLAG {
        errs.push(Error::Corrupt(format!(
            "page {}: invalid type: {} (stack: {stack:?})",
            hdr.id,
            hdr.typ()
        )));
    }
}

fn check_key_order(
    tx: &TxInner,
    pgid: Pgid,
    min_closed: Option<&[u8]>,
    max_open: Option<&[u8]>,
    stack: &mut Vec<Pgid>,
    errs: &mut Vec<Error>,
) -> Result<Option<Vec<u8>>> {
    let page = tx.db.read_page(pgid)?;
    let hdr = PageHeader::read(&page);
    if hdr.is_branch() {
        let mut running_min = min_closed.map(|s| s.to_vec());
        let mut max_in_subtree: Option<Vec<u8>> = None;
        for i in 0..hdr.count as usize {
            let (child, key) = branch_at(&page, i);
            let next_max = if i + 1 < hdr.count as usize {
                Some(branch_at(&page, i + 1).1.to_vec())
            } else {
                max_open.map(|s| s.to_vec())
            };
            stack.push(child);
            let subtree_max = check_key_order(
                tx,
                child,
                running_min.as_deref(),
                next_max.as_deref(),
                stack,
                errs,
            )?;
            stack.pop();
            if let Some(m) = subtree_max {
                max_in_subtree = Some(m);
            }
            running_min = Some(key.to_vec());
        }
        Ok(max_in_subtree)
    } else if hdr.is_leaf() {
        let mut prev: Option<Vec<u8>> = None;
        let mut last: Option<Vec<u8>> = None;
        for i in 0..hdr.count as usize {
            let (_f, key, _v) = leaf_at(&page, i);
            if let Some(m) = min_closed {
                if key < m {
                    errs.push(Error::Corrupt(format!(
                        "page {pgid}: key {:?} < min (stack: {stack:?})",
                        hex_key(key)
                    )));
                }
            }
            if let Some(m) = max_open {
                if key >= m {
                    errs.push(Error::Corrupt(format!(
                        "page {pgid}: key {:?} >= max (stack: {stack:?})",
                        hex_key(key)
                    )));
                }
            }
            if let Some(ref p) = prev {
                if key <= p.as_slice() {
                    errs.push(Error::Corrupt(format!(
                        "page {pgid}: out of order keys: {:?} >= {:?} (stack: {stack:?})",
                        hex_key(p),
                        hex_key(key)
                    )));
                }
            }
            prev = Some(key.to_vec());
            last = Some(key.to_vec());
        }
        Ok(last)
    } else {
        Ok(None)
    }
}

fn hex_key(k: &[u8]) -> String {
    let mut s = String::with_capacity(k.len() * 2);
    for b in k {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}
