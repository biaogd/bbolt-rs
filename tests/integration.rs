//! Integration tests for the core bbolt API.

use std::fs;
use std::sync::Arc;
use std::thread;

use bbolt::{Db, Error, Options};

fn open_tmp() -> (tempfile::TempDir, Db) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    let db = Db::open(
        &path,
        0o600,
        Some(Options {
            page_size: 4096,
            ..Options::default()
        }),
    )
    .unwrap();
    (dir, db)
}

#[test]
fn create_open_put_get() {
    let (_dir, db) = open_tmp();
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        b.put(b"foo", b"bar")?;
        assert_eq!(b.get(b"foo").as_deref(), Some(&b"bar"[..]));
        Ok(())
    })
    .unwrap();

    db.view(|tx| {
        let b = tx.bucket(b"widgets").expect("bucket");
        assert_eq!(b.get(b"foo").as_deref(), Some(&b"bar"[..]));
        assert!(b.get(b"missing").is_none());
        Ok(())
    })
    .unwrap();
}

#[test]
fn persist_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("p.db");
    {
        let db = Db::open(
            &path,
            0o600,
            Some(Options {
                page_size: 4096,
                ..Options::default()
            }),
        )
        .unwrap();
        db.update(|tx| {
            let b = tx.create_bucket(b"data")?;
            b.put(b"k", b"v")?;
            b.put(b"k2", b"v2")?;
            Ok(())
        })
        .unwrap();
        db.close().unwrap();
    }
    let db = Db::open(
        &path,
        0o600,
        Some(Options {
            page_size: 4096,
            ..Options::default()
        }),
    )
    .unwrap();
    db.view(|tx| {
        let b = tx.bucket(b"data").unwrap();
        assert_eq!(b.get(b"k").as_deref(), Some(&b"v"[..]));
        assert_eq!(b.get(b"k2").as_deref(), Some(&b"v2"[..]));
        Ok(())
    })
    .unwrap();
}

#[test]
fn delete_and_overwrite() {
    let (_dir, db) = open_tmp();
    db.update(|tx| {
        let b = tx.create_bucket(b"b")?;
        b.put(b"a", b"1")?;
        b.put(b"a", b"2")?;
        assert_eq!(b.get(b"a").as_deref(), Some(&b"2"[..]));
        b.delete(b"a")?;
        assert!(b.get(b"a").is_none());
        b.delete(b"missing")?;
        Ok(())
    })
    .unwrap();
}

#[test]
fn rollback_discards_writes() {
    let (_dir, db) = open_tmp();
    db.update(|tx| {
        tx.create_bucket(b"keep")?.put(b"x", b"y")?;
        Ok(())
    })
    .unwrap();

    let tx = db.begin(true).unwrap();
    tx.create_bucket(b"gone").unwrap().put(b"a", b"b").unwrap();
    tx.rollback().unwrap();

    db.view(|tx| {
        assert!(tx.bucket(b"gone").is_none());
        assert_eq!(
            tx.bucket(b"keep").unwrap().get(b"x").as_deref(),
            Some(&b"y"[..])
        );
        Ok(())
    })
    .unwrap();
}

#[test]
fn update_error_rolls_back() {
    let (_dir, db) = open_tmp();
    let err: Result<(), Error> = db.update(|tx| {
        tx.create_bucket(b"b")?.put(b"k", b"v")?;
        Err(Error::KeyRequired)
    });
    assert!(matches!(err, Err(Error::KeyRequired)));
    db.view(|tx| {
        assert!(tx.bucket(b"b").is_none());
        Ok(())
    })
    .unwrap();
}

#[test]
fn nested_buckets() {
    let (_dir, db) = open_tmp();
    db.update(|tx| {
        let users = tx.create_bucket(b"users")?;
        users.put(b"alice", b"data1")?;
        users.put(b"bob", b"data2")?;
        let nested = users.create_bucket(b"nested")?;
        nested.put(b"x", b"y")?;
        Ok(())
    })
    .unwrap();

    db.view(|tx| {
        let users = tx.bucket(b"users").unwrap();
        assert_eq!(users.get(b"alice").as_deref(), Some(&b"data1"[..]));
        assert_eq!(users.get(b"bob").as_deref(), Some(&b"data2"[..]));
        assert!(users.get(b"nested").is_none());
        let nested = users.bucket(b"nested").unwrap();
        assert_eq!(nested.get(b"x").as_deref(), Some(&b"y"[..]));
        Ok(())
    })
    .unwrap();
}

#[test]
fn cursor_iteration() {
    let (_dir, db) = open_tmp();
    db.update(|tx| {
        let b = tx.create_bucket(b"letters")?;
        b.put(b"b", b"2")?;
        b.put(b"a", b"1")?;
        b.put(b"c", b"3")?;
        Ok(())
    })
    .unwrap();

    db.view(|tx| {
        let b = tx.bucket(b"letters").unwrap();
        let mut c = b.cursor();
        let (k, v) = c.first().unwrap();
        assert_eq!(k.as_deref(), Some(&b"a"[..]));
        assert_eq!(v.as_deref(), Some(&b"1"[..]));
        let (k, v) = c.next().unwrap();
        assert_eq!(k.as_deref(), Some(&b"b"[..]));
        assert_eq!(v.as_deref(), Some(&b"2"[..]));
        let (k, v) = c.next().unwrap();
        assert_eq!(k.as_deref(), Some(&b"c"[..]));
        assert_eq!(v.as_deref(), Some(&b"3"[..]));
        assert!(c.next().unwrap().0.is_none());

        let (k, _) = c.last().unwrap();
        assert_eq!(k.as_deref(), Some(&b"c"[..]));
        let (k, _) = c.prev().unwrap();
        assert_eq!(k.as_deref(), Some(&b"b"[..]));

        let (k, v) = c.seek(b"b").unwrap();
        assert_eq!(k.as_deref(), Some(&b"b"[..]));
        assert_eq!(v.as_deref(), Some(&b"2"[..]));
        let (k, _) = c.seek(b"bb").unwrap();
        assert_eq!(k.as_deref(), Some(&b"c"[..]));
        assert!(c.seek(b"z").unwrap().0.is_none());
        Ok(())
    })
    .unwrap();
}

#[test]
fn sequences() {
    let (_dir, db) = open_tmp();
    db.update(|tx| {
        let b = tx.create_bucket(b"s")?;
        assert_eq!(b.sequence(), 0);
        assert_eq!(b.next_sequence().unwrap(), 1);
        assert_eq!(b.next_sequence().unwrap(), 2);
        b.set_sequence(10)?;
        assert_eq!(b.next_sequence().unwrap(), 11);
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        assert_eq!(tx.bucket(b"s").unwrap().sequence(), 11);
        Ok(())
    })
    .unwrap();
}

#[test]
fn many_keys_split_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("many.db");
    {
        let db = Db::open(
            &path,
            0o600,
            Some(Options {
                page_size: 4096,
                ..Options::default()
            }),
        )
        .unwrap();
        db.update(|tx| {
            let b = tx.create_bucket(b"big")?;
            for i in 0..500u32 {
                let k = format!("key-{i:04}");
                let v = format!("val-{i:04}");
                b.put(k.as_bytes(), v.as_bytes())?;
            }
            Ok(())
        })
        .unwrap();
        db.close().unwrap();
    }
    let db = Db::open(
        &path,
        0o600,
        Some(Options {
            page_size: 4096,
            ..Options::default()
        }),
    )
    .unwrap();
    db.view(|tx| {
        let b = tx.bucket(b"big").unwrap();
        assert_eq!(b.get(b"key-0000").as_deref(), Some(&b"val-0000"[..]));
        assert_eq!(b.get(b"key-0250").as_deref(), Some(&b"val-0250"[..]));
        assert_eq!(b.get(b"key-0499").as_deref(), Some(&b"val-0499"[..]));
        let mut c = b.cursor();
        let mut n = 0;
        let mut prev: Option<Vec<u8>> = None;
        let mut kv = c.first().unwrap();
        while let Some(k) = kv.0.clone() {
            if let Some(p) = &prev {
                assert!(k.as_slice() > p.as_slice(), "keys must be sorted");
            }
            prev = Some(k);
            n += 1;
            kv = c.next().unwrap();
        }
        assert_eq!(n, 500);
        Ok(())
    })
    .unwrap();
}

#[test]
fn delete_many_rebalance() {
    let (_dir, db) = open_tmp();
    db.update(|tx| {
        let b = tx.create_bucket(b"d")?;
        for i in 0..200u32 {
            let k = format!("{i:04}");
            b.put(k.as_bytes(), b"x")?;
        }
        Ok(())
    })
    .unwrap();
    db.update(|tx| {
        let b = tx.bucket(b"d").unwrap();
        for i in 0..200u32 {
            if i % 2 == 0 {
                let k = format!("{i:04}");
                b.delete(k.as_bytes())?;
            }
        }
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        let b = tx.bucket(b"d").unwrap();
        assert!(b.get(b"0000").is_none());
        assert_eq!(b.get(b"0001").as_deref(), Some(&b"x"[..]));
        Ok(())
    })
    .unwrap();
}

#[test]
fn errors_on_incompatible_and_readonly() {
    let (_dir, db) = open_tmp();
    db.update(|tx| {
        let b = tx.create_bucket(b"b")?;
        b.put(b"k", b"v")?;
        assert!(matches!(
            b.create_bucket(b"k"),
            Err(Error::IncompatibleValue)
        ));
        let sub = b.create_bucket(b"sub")?;
        assert!(matches!(b.put(b"sub", b"x"), Err(Error::IncompatibleValue)));
        assert!(matches!(b.delete(b"sub"), Err(Error::IncompatibleValue)));
        assert!(sub.get(b"x").is_none());
        Ok(())
    })
    .unwrap();

    db.view(|tx| {
        let b = tx.bucket(b"b").unwrap();
        assert!(matches!(b.put(b"a", b"b"), Err(Error::TxNotWritable)));
        Ok(())
    })
    .unwrap();

    assert!(db.begin(true).unwrap().rollback().is_ok());
}

#[test]
fn empty_key_rejected() {
    let (_dir, db) = open_tmp();
    db.update(|tx| {
        let b = tx.create_bucket(b"b")?;
        assert!(matches!(b.put(b"", b"v"), Err(Error::KeyRequired)));
        assert!(matches!(
            tx.create_bucket(b""),
            Err(Error::BucketNameRequired)
        ));
        Ok(())
    })
    .unwrap();
}

#[test]
fn create_bucket_if_not_exists() {
    let (_dir, db) = open_tmp();
    db.update(|tx| {
        let b1 = tx.create_bucket_if_not_exists(b"b")?;
        b1.put(b"k", b"v")?;
        let b2 = tx.create_bucket_if_not_exists(b"b")?;
        assert_eq!(b2.get(b"k").as_deref(), Some(&b"v"[..]));
        Ok(())
    })
    .unwrap();
}

#[test]
fn for_each_and_top_level() {
    let (_dir, db) = open_tmp();
    db.update(|tx| {
        tx.create_bucket(b"a")?.put(b"1", b"one")?;
        tx.create_bucket(b"b")?.put(b"2", b"two")?;
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        let mut names = Vec::new();
        tx.for_each(|name, b| {
            names.push(name.to_vec());
            assert!(b.get(if name == b"a" { b"1" } else { b"2" }).is_some());
            Ok(())
        })?;
        names.sort();
        assert_eq!(names, vec![b"a".to_vec(), b"b".to_vec()]);
        Ok(())
    })
    .unwrap();
}

#[test]
fn concurrent_readers() {
    let (_dir, db) = open_tmp();
    db.update(|tx| {
        tx.create_bucket(b"b")?.put(b"k", b"v")?;
        Ok(())
    })
    .unwrap();
    let db = Arc::new(db);
    let mut handles = Vec::new();
    for _ in 0..8 {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            db.view(|tx| {
                let b = tx.bucket(b"b").unwrap();
                assert_eq!(b.get(b"k").as_deref(), Some(&b"v"[..]));
                Ok(())
            })
            .unwrap();
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn open_go_init_fixture() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("go.db");
    fs::copy("tests/fixtures/go_init.db", &path).unwrap();
    let db = Db::open(
        &path,
        0o600,
        Some(Options {
            page_size: 4096,
            ..Options::default()
        }),
    )
    .unwrap();
    db.view(|tx| {
        assert!(tx.bucket(b"missing").is_none());
        Ok(())
    })
    .unwrap();
}

#[test]
fn open_go_sample_with_data() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("go.db");
    fs::copy("tests/fixtures/go_sample.db", &path).unwrap();
    let db = Db::open(
        &path,
        0o600,
        Some(Options {
            page_size: 4096,
            ..Options::default()
        }),
    )
    .unwrap();
    db.view(|tx| {
        let users = tx.bucket(b"users").expect("users bucket");
        assert_eq!(users.get(b"alice").as_deref(), Some(&b"data1"[..]));
        assert_eq!(users.get(b"bob").as_deref(), Some(&b"data2"[..]));
        let nested = users.bucket(b"nested").expect("nested");
        assert_eq!(nested.get(b"x").as_deref(), Some(&b"y"[..]));
        Ok(())
    })
    .unwrap();
}

#[test]
fn delete_bucket() {
    let (_dir, db) = open_tmp();
    db.update(|tx| {
        let b = tx.create_bucket(b"p")?;
        b.create_bucket(b"c")?.put(b"k", b"v")?;
        tx.delete_bucket(b"p")?;
        assert!(tx.bucket(b"p").is_none());
        Ok(())
    })
    .unwrap();
}

#[test]
fn large_value_overflow() {
    let (_dir, db) = open_tmp();
    let big = vec![0xABu8; 10_000];
    db.update(|tx| {
        let b = tx.create_bucket(b"b")?;
        b.put(b"big", &big)?;
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        let got = tx.bucket(b"b").unwrap().get(b"big").unwrap();
        assert_eq!(got, big);
        Ok(())
    })
    .unwrap();
}

#[test]
fn batch_writes() {
    let (_dir, db) = open_tmp();
    let db = Arc::new(db);
    db.update(|tx| {
        tx.create_bucket(b"b")?;
        Ok(())
    })
    .unwrap();
    let mut handles = Vec::new();
    for i in 0..20u8 {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            db.batch(move |tx| {
                let b = tx.bucket(b"b").unwrap();
                b.put(&[i], &[i])?;
                Ok(())
            })
            .unwrap();
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    db.view(|tx| {
        let b = tx.bucket(b"b").unwrap();
        for i in 0..20u8 {
            assert_eq!(b.get(&[i]).as_deref(), Some(&[i][..]), "missing {i}");
        }
        Ok(())
    })
    .unwrap();
}

#[test]
fn magic_and_version_on_disk() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.db");
    let db = Db::open(
        &path,
        0o600,
        Some(Options {
            page_size: 4096,
            ..Options::default()
        }),
    )
    .unwrap();
    db.close().unwrap();
    let bytes = fs::read(&path).unwrap();
    assert_eq!(&bytes[16..20], &bbolt::MAGIC.to_le_bytes());
    assert_eq!(&bytes[20..24], &bbolt::VERSION.to_le_bytes());
}
