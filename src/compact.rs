//! Database compaction (`bbolt.Compact`).

use crate::db::{Db, Options};
use crate::error::Result;
use crate::tx::Tx;

/// Create a compacted copy of `src` in `dst`.
///
/// `tx_max_size` limits how large a destination write transaction may grow
/// before an intermittent commit (0 = unlimited), matching upstream.
pub fn compact(dst: &Db, src: &Db, tx_max_size: i64) -> Result<()> {
    let mut size: i64 = 0;
    let mut tx = dst.begin(true)?;

    let walk_result = walk(src, |keys, k, v, seq| {
        let sz = (k.len() + v.as_ref().map(|x| x.len()).unwrap_or(0)) as i64;
        if tx_max_size != 0 && size + sz > tx_max_size {
            tx.commit()?;
            tx = dst.begin(true)?;
            size = 0;
        }
        size += sz;

        let nk = keys.len();
        if nk == 0 {
            let bkt = tx.create_bucket(k)?;
            bkt.set_sequence(seq)?;
            return Ok(());
        }

        let b0 = tx
            .bucket(&keys[0])
            .ok_or(crate::error::Error::BucketNotFound)?;
        let mut b = b0;
        if nk > 1 {
            for part in &keys[1..] {
                b = b.bucket(part).ok_or(crate::error::Error::BucketNotFound)?;
            }
        }
        b.set_fill_percent(1.0);

        if v.is_none() {
            let bkt = b.create_bucket(k)?;
            bkt.set_sequence(seq)?;
            return Ok(());
        }
        b.put(k, v.unwrap())
    });

    if let Err(e) = walk_result {
        let _ = tx.rollback();
        return Err(e);
    }
    tx.commit()
}

/// Compact `src_path` into a new database at `dst_path`.
pub fn compact_files(
    dst_path: impl AsRef<std::path::Path>,
    src_path: impl AsRef<std::path::Path>,
    tx_max_size: i64,
    page_size: usize,
) -> Result<()> {
    let src = Db::open(
        src_path,
        0o600,
        Some(Options {
            read_only: true,
            pre_load_freelist: true,
            page_size,
            ..Options::default()
        }),
    )?;
    let dst = Db::open(
        dst_path,
        0o600,
        Some(Options {
            page_size: if page_size == 0 {
                src.page_size()
            } else {
                page_size
            },
            ..Options::default()
        }),
    )?;
    compact(&dst, &src, tx_max_size)
}

type WalkFn<'a> = dyn FnMut(&[Vec<u8>], &[u8], Option<&[u8]>, u64) -> Result<()> + 'a;

fn walk(db: &Db, mut walk_fn: impl FnMut(&[Vec<u8>], &[u8], Option<&[u8]>, u64) -> Result<()>) -> Result<()> {
    db.view(|tx| {
        tx.for_each(|name, b| {
            walk_bucket(b, &[], name, None, b.sequence(), &mut walk_fn)
        })
    })
}

fn walk_bucket(
    b: &crate::bucket::Bucket,
    keypath: &[Vec<u8>],
    k: &[u8],
    v: Option<&[u8]>,
    seq: u64,
    fn_: &mut WalkFn<'_>,
) -> Result<()> {
    fn_(keypath, k, v, seq)?;
    if v.is_some() {
        return Ok(());
    }
    let mut path = keypath.to_vec();
    path.push(k.to_vec());
    b.for_each(|ck, cv| {
        if cv.is_none() {
            let bkt = b.bucket(ck).ok_or(crate::error::Error::BucketNotFound)?;
            walk_bucket(&bkt, &path, ck, None, bkt.sequence(), fn_)
        } else {
            walk_bucket(b, &path, ck, cv, b.sequence(), fn_)
        }
    })
}

/// Re-export for callers that open both DBs themselves.
#[allow(dead_code)]
pub fn compact_tx_hint(_: &Tx) {}
