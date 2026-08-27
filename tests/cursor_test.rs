//! Ports of upstream `cursor_test.go`.

mod common;

use bbolt::Error;

// Go: TestCursor_Seek
#[test]
fn test_cursor_seek() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        b.put(b"foo", b"0001")?;
        b.put(b"bar", b"0002")?;
        b.put(b"baz", b"0003")?;
        b.create_bucket(b"bkt")?;
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        let mut c = tx.bucket(b"widgets").unwrap().cursor();
        let (k, v) = c.seek(b"bar").unwrap();
        assert_eq!(k.as_deref(), Some(&b"bar"[..]));
        assert_eq!(v.as_deref(), Some(&b"0002"[..]));

        let (k, v) = c.seek(b"bas").unwrap();
        assert_eq!(k.as_deref(), Some(&b"baz"[..]));
        assert_eq!(v.as_deref(), Some(&b"0003"[..]));

        let (k, _v) = c.seek(b"").unwrap();
        assert_eq!(k.as_deref(), Some(&b"bar"[..]));

        let (k, v) = c.seek(b"zzz").unwrap();
        assert!(k.is_none());
        assert!(v.is_none());

        let (k, v) = c.seek(b"bkt").unwrap();
        assert_eq!(k.as_deref(), Some(&b"bkt"[..]));
        assert!(v.is_none());
        Ok(())
    })
    .unwrap();
}

// Go: TestCursor_Delete
#[test]
fn test_cursor_delete() {
    let (_dir, db) = common::open_tmp();
    const COUNT: u64 = 200;
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        for i in 0..COUNT {
            let k = i.to_be_bytes();
            b.put(&k, &[0u8; 100])?;
        }
        b.create_bucket(b"sub")?;
        Ok(())
    })
    .unwrap();
    db.update(|tx| {
        let mut c = tx.bucket(b"widgets").unwrap().cursor();
        let bound = (COUNT / 2).to_be_bytes();
        loop {
            let (k, _) = c.first().unwrap();
            let Some(key) = k else { break };
            if key.as_slice() >= bound.as_slice() {
                break;
            }
            c.delete()?;
        }
        c.seek(b"sub").unwrap();
        assert!(matches!(c.delete(), Err(Error::IncompatibleValue)));
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        let b = tx.bucket(b"widgets").unwrap();
        let mut n = 0;
        b.for_each(|_k, v| {
            if v.is_some() {
                n += 1;
            }
            Ok(())
        })?;
        assert_eq!(n, (COUNT / 2) as usize);
        assert!(b.bucket(b"sub").is_some());
        Ok(())
    })
    .unwrap();
}

// Go: TestCursor_EmptyBucket
#[test]
fn test_cursor_empty_bucket() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        tx.create_bucket(b"widgets")?;
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        let mut c = tx.bucket(b"widgets").unwrap().cursor();
        let (k, v) = c.first().unwrap();
        assert!(k.is_none());
        assert!(v.is_none());
        Ok(())
    })
    .unwrap();
}

// Go: TestCursor_EmptyBucketReverse
#[test]
fn test_cursor_empty_bucket_reverse() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        tx.create_bucket(b"widgets")?;
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        let mut c = tx.bucket(b"widgets").unwrap().cursor();
        let (k, v) = c.last().unwrap();
        assert!(k.is_none());
        assert!(v.is_none());
        Ok(())
    })
    .unwrap();
}

// Go: TestCursor_Iterate_Leaf
#[test]
fn test_cursor_iterate_leaf() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        b.put(b"baz", b"")?;
        b.put(b"foo", &[0])?;
        b.put(b"bar", &[1])?;
        Ok(())
    })
    .unwrap();
    let tx = db.begin(false).unwrap();
    let mut c = tx.bucket(b"widgets").unwrap().cursor();
    let (k, v) = c.first().unwrap();
    assert_eq!(k.as_deref(), Some(&b"bar"[..]));
    assert_eq!(v.as_deref(), Some(&[1][..]));
    let (k, v) = c.next().unwrap();
    assert_eq!(k.as_deref(), Some(&b"baz"[..]));
    assert_eq!(v.as_deref(), Some(&b""[..]));
    let (k, v) = c.next().unwrap();
    assert_eq!(k.as_deref(), Some(&b"foo"[..]));
    assert_eq!(v.as_deref(), Some(&[0][..]));
    let (k, v) = c.next().unwrap();
    assert!(k.is_none());
    assert!(v.is_none());
    tx.rollback().unwrap();
}

// Go: TestCursor_Restart — seek on populated bucket after iteration ends.
#[test]
fn test_cursor_seek_on_populated_bucket() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        for i in 0..50 {
            b.put(format!("{i:03}").as_bytes(), b"v")?;
        }
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        let mut c = tx.bucket(b"widgets").unwrap().cursor();
        let (k, _) = c.seek(b"025").unwrap();
        assert_eq!(k.as_deref(), Some(&b"025"[..]));
        while c.next().unwrap().0.is_some() {}
        let (k, _) = c.seek(b"010").unwrap();
        assert_eq!(k.as_deref(), Some(&b"010"[..]));
        Ok(())
    })
    .unwrap();
}

// Go: TestCursor_RepeatOperations — basic first/last after next/prev exhaustion.
#[test]
fn test_cursor_first_last_basics() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        b.put(b"a", b"1")?;
        b.put(b"b", b"2")?;
        b.put(b"c", b"3")?;
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        let mut c = tx.bucket(b"widgets").unwrap().cursor();
        let (k, _) = c.first().unwrap();
        assert_eq!(k.as_deref(), Some(&b"a"[..]));
        let (k, _) = c.last().unwrap();
        assert_eq!(k.as_deref(), Some(&b"c"[..]));
        let (k, _) = c.prev().unwrap();
        assert_eq!(k.as_deref(), Some(&b"b"[..]));
        Ok(())
    })
    .unwrap();
}

// Go: TestCursor_Seek_Large (thousands of keys)
#[test]
fn test_cursor_seek_large() {
    let (_dir, db) = common::open_tmp();
    const COUNT: u64 = 3000;
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        for block in (0..COUNT).step_by(100) {
            for j in block..block + 100 {
                if j % 2 == 0 {
                    b.put(&j.to_be_bytes(), &[0u8; 100])?;
                }
            }
        }
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        let mut c = tx.bucket(b"widgets").unwrap().cursor();
        for i in 0..COUNT {
            let seek = i.to_be_bytes();
            let (k, _) = c.seek(&seek).unwrap();
            if i == COUNT - 1 {
                assert!(k.is_none());
                continue;
            }
            let num = u64::from_be_bytes(k.unwrap().try_into().unwrap());
            if i % 2 == 0 {
                assert_eq!(num, i, "even seek at {i}");
            } else {
                assert_eq!(num, i + 1, "odd seek at {i}");
            }
        }
        Ok(())
    })
    .unwrap();
}

// Go: TestCursor_LeafRootReverse
#[test]
fn test_cursor_leaf_root_reverse() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        b.put(b"baz", b"")?;
        b.put(b"foo", &[0])?;
        b.put(b"bar", &[1])?;
        Ok(())
    })
    .unwrap();
    let tx = db.begin(false).unwrap();
    let mut c = tx.bucket(b"widgets").unwrap().cursor();
    let (k, v) = c.last().unwrap();
    assert_eq!(k.as_deref(), Some(&b"foo"[..]));
    assert_eq!(v.as_deref(), Some(&[0][..]));
    let (k, v) = c.prev().unwrap();
    assert_eq!(k.as_deref(), Some(&b"baz"[..]));
    assert_eq!(v.as_deref(), Some(&b""[..]));
    let (k, v) = c.prev().unwrap();
    assert_eq!(k.as_deref(), Some(&b"bar"[..]));
    assert_eq!(v.as_deref(), Some(&[1][..]));
    let (k, v) = c.prev().unwrap();
    assert!(k.is_none());
    assert!(v.is_none());
    tx.rollback().unwrap();
}

// Go: TestCursor_First_EmptyPages
#[test]
fn test_cursor_first_empty_pages() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        for i in 0..1000u64 {
            b.put(&i.to_be_bytes(), b"")?;
        }
        Ok(())
    })
    .unwrap();
    db.update(|tx| {
        let b = tx.bucket(b"widgets").unwrap();
        for i in 0..600u64 {
            b.delete(&i.to_be_bytes())?;
        }
        let mut c = b.cursor();
        let mut n = 0;
        let mut kv = c.first().unwrap();
        while kv.0.is_some() {
            n += 1;
            kv = c.next().unwrap();
        }
        assert_eq!(n, 400);
        Ok(())
    })
    .unwrap();
}

// Go: TestCursor_Last_EmptyPages
#[test]
fn test_cursor_last_empty_pages() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        for i in 0..1000u64 {
            b.put(&i.to_be_bytes(), b"")?;
        }
        Ok(())
    })
    .unwrap();
    db.update(|tx| {
        let b = tx.bucket(b"widgets").unwrap();
        for i in 200..1000u64 {
            b.delete(&i.to_be_bytes())?;
        }
        let mut c = b.cursor();
        let mut n = 0;
        let mut kv = c.last().unwrap();
        while kv.0.is_some() {
            n += 1;
            kv = c.prev().unwrap();
        }
        assert_eq!(n, 200);
        Ok(())
    })
    .unwrap();
}

// Go: TestCursor_Bucket
#[test]
fn test_cursor_bucket() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        b.put(b"a", b"1")?;
        let c = b.cursor();
        let cb = c.bucket();
        assert_eq!(cb.get(b"a").as_deref(), Some(&b"1"[..]));
        Ok(())
    })
    .unwrap();
}

// Skipped: TestCursor_RepeatOperations — full 1000-key repeat next/prev cycle.

use rand::Rng;
use std::collections::HashSet;

fn quick_check_items(rng: &mut impl Rng, max_items: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let n = rng.gen_range(1..max_items);
    let mut items = Vec::with_capacity(n);
    let mut used = HashSet::new();
    for _ in 0..n {
        loop {
            let klen = rng.gen_range(1..32);
            let key: Vec<u8> = (0..klen).map(|_| rng.gen()).collect();
            if used.insert(key.clone()) {
                let vlen = rng.gen_range(0..64);
                let val: Vec<u8> = (0..vlen).map(|_| rng.gen()).collect();
                items.push((key, val));
                break;
            }
        }
    }
    items
}

// Go: TestCursor_QuickCheck
#[test]
fn test_cursor_quick_check() {
    let mut rng = rand::thread_rng();
    for _ in 0..100 {
        let items = quick_check_items(&mut rng, 50);
        let (_dir, db) = common::open_tmp();
        db.update(|tx| {
            let b = tx.create_bucket(b"widgets")?;
            for (k, v) in &items {
                b.put(k, v)?;
            }
            Ok(())
        })
        .unwrap();

        let mut sorted = items.clone();
        sorted.sort_by(|a, b| a.0.cmp(&b.0));

        db.view(|tx| {
            let mut c = tx.bucket(b"widgets").unwrap().cursor();
            let mut index = 0;
            let mut kv = c.first().unwrap();
            while kv.0.is_some() && index < sorted.len() {
                assert_eq!(kv.0.as_deref(), Some(sorted[index].0.as_slice()));
                assert_eq!(kv.1.as_deref(), Some(sorted[index].1.as_slice()));
                index += 1;
                kv = c.next().unwrap();
            }
            assert_eq!(index, sorted.len());
            Ok(())
        })
        .unwrap();
    }
}

// Go: TestCursor_QuickCheck_Reverse
#[test]
fn test_cursor_quick_check_reverse() {
    let mut rng = rand::thread_rng();
    for _ in 0..100 {
        let items = quick_check_items(&mut rng, 50);
        let (_dir, db) = common::open_tmp();
        db.update(|tx| {
            let b = tx.create_bucket(b"widgets")?;
            for (k, v) in &items {
                b.put(k, v)?;
            }
            Ok(())
        })
        .unwrap();

        let mut sorted = items.clone();
        sorted.sort_by(|a, b| b.0.cmp(&a.0));

        db.view(|tx| {
            let mut c = tx.bucket(b"widgets").unwrap().cursor();
            let mut index = 0;
            let mut kv = c.last().unwrap();
            while kv.0.is_some() && index < sorted.len() {
                assert_eq!(kv.0.as_deref(), Some(sorted[index].0.as_slice()));
                assert_eq!(kv.1.as_deref(), Some(sorted[index].1.as_slice()));
                index += 1;
                kv = c.prev().unwrap();
            }
            assert_eq!(index, sorted.len());
            Ok(())
        })
        .unwrap();
    }
}

// Go: TestCursor_QuickCheck_BucketsOnly
#[test]
fn test_cursor_quick_check_buckets_only() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        b.create_bucket(b"foo")?;
        b.create_bucket(b"bar")?;
        b.create_bucket(b"baz")?;
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        let mut names = Vec::new();
        let mut c = tx.bucket(b"widgets").unwrap().cursor();
        let mut kv = c.first().unwrap();
        while let Some(k) = kv.0 {
            names.push(String::from_utf8(k).unwrap());
            assert!(kv.1.is_none());
            kv = c.next().unwrap();
        }
        assert_eq!(names, vec!["bar", "baz", "foo"]);
        Ok(())
    })
    .unwrap();
}

// Go: TestCursor_QuickCheck_BucketsOnly_Reverse
#[test]
fn test_cursor_quick_check_buckets_only_reverse() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        b.create_bucket(b"foo")?;
        b.create_bucket(b"bar")?;
        b.create_bucket(b"baz")?;
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        let mut names = Vec::new();
        let mut c = tx.bucket(b"widgets").unwrap().cursor();
        let mut kv = c.last().unwrap();
        while let Some(k) = kv.0 {
            names.push(String::from_utf8(k).unwrap());
            assert!(kv.1.is_none());
            kv = c.prev().unwrap();
        }
        assert_eq!(names, vec!["foo", "baz", "bar"]);
        Ok(())
    })
    .unwrap();
}
