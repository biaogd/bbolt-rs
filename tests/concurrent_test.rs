//! Ports of selected upstream cursor/db concurrent tests.

mod common;

use std::sync::Arc;
use std::thread;

use bbolt::Options;

/// Go: TestConcurrentRepeatableRead (simplified) — readers see a stable snapshot
/// while a writer commits a later version.
#[test]
fn test_concurrent_repeatable_read() {
    let (dir, db) = common::open_tmp();
    let db = Arc::new(db);
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        b.put(b"foo", b"bar")?;
        Ok(())
    })
    .unwrap();

    let db_r = Arc::clone(&db);
    let reader = thread::spawn(move || {
        db_r.view(|tx| {
            let b = tx.bucket(b"widgets").unwrap();
            assert_eq!(b.get(b"foo").as_deref(), Some(&b"bar"[..]));
            // Hold the read tx open while writer runs (caller joins after writer start).
            thread::sleep(std::time::Duration::from_millis(50));
            assert_eq!(b.get(b"foo").as_deref(), Some(&b"bar"[..]));
            assert!(b.get(b"new").is_none());
            Ok(())
        })
        .unwrap();
    });

    thread::sleep(std::time::Duration::from_millis(10));
    db.update(|tx| {
        let b = tx.bucket(b"widgets").unwrap();
        b.put(b"new", b"val")?;
        b.put(b"foo", b"baz")?;
        Ok(())
    })
    .unwrap();

    reader.join().unwrap();

    db.view(|tx| {
        let b = tx.bucket(b"widgets").unwrap();
        assert_eq!(b.get(b"foo").as_deref(), Some(&b"baz"[..]));
        assert_eq!(b.get(b"new").as_deref(), Some(&b"val"[..]));
        Ok(())
    })
    .unwrap();
    let _ = dir;
}

/// Go: TestConcurrentGenericReadAndWrite — smaller multi-reader/writer workload.
#[test]
fn test_concurrent_generic_read_and_write() {
    let (_dir, db) = common::open_tmp_with(Options {
        page_size: 4096,
        ..Options::default()
    });
    let db = Arc::new(db);
    db.update(|tx| {
        tx.create_bucket(b"data")?;
        Ok(())
    })
    .unwrap();

    let mut handles = Vec::new();
    for i in 0..8u8 {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for n in 0..50u8 {
                if n % 5 == 0 {
                    db.update(|tx| {
                        let b = tx.bucket(b"data").unwrap();
                        let key = [i, n];
                        b.put(&key, &key)?;
                        Ok(())
                    })
                    .unwrap();
                } else {
                    db.view(|tx| {
                        let b = tx.bucket(b"data").unwrap();
                        let _ = b.get(&[i, n.saturating_sub(1)]);
                        Ok(())
                    })
                    .unwrap();
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    common::must_check(&db);
}
