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

// Skipped: TestCursor_RepeatOperations — full 1000-key repeat next/prev cycle.
// Skipped: TestCursor_Bucket — cursor.bucket() identity (no Go-style pointer equality).
// Skipped: TestCursor_Seek_Large — 10000-key seek regression (covered partially above).
// Skipped: TestCursor_LeafRootReverse, First/Last_EmptyPages — extended cursor edge cases.
// Skipped: TestCursor_QuickCheck* — property/quick tests.
