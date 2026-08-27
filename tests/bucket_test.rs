//! Ports of upstream `bucket_test.go`.

mod common;

use bbolt::{Error, MAX_KEY_SIZE, MAX_VALUE_SIZE};

fn must_create() -> (tempfile::TempDir, bbolt::Db) {
    common::open_tmp()
}

// Go: TestBucket_Get_NonExistent
#[test]
fn test_bucket_get_non_existent() {
    let (_dir, db) = must_create();
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        assert!(b.get(b"foo").is_none());
        Ok(())
    })
    .unwrap();
}

// Go: TestBucket_Put
#[test]
fn test_bucket_put() {
    let (_dir, db) = must_create();
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        b.put(b"foo", b"bar")?;
        assert_eq!(
            tx.bucket(b"widgets").unwrap().get(b"foo").as_deref(),
            Some(&b"bar"[..])
        );
        Ok(())
    })
    .unwrap();
}

// Go: TestBucket_Put_Repeat
#[test]
fn test_bucket_put_repeat() {
    let (_dir, db) = must_create();
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        b.put(b"foo", b"bar")?;
        b.put(b"foo", b"baz")?;
        assert_eq!(
            tx.bucket(b"widgets").unwrap().get(b"foo").as_deref(),
            Some(&b"baz"[..])
        );
        Ok(())
    })
    .unwrap();
}

// Go: TestBucket_Put_Large
#[test]
fn test_bucket_put_large() {
    let (_dir, db) = must_create();
    let count = 100;
    let factor = 200;
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        for i in 1..count {
            let key = vec![b'0'; i * factor];
            let val = vec![b'X'; (count - i) * factor];
            b.put(&key, &val)?;
        }
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        let b = tx.bucket(b"widgets").unwrap();
        for i in 1..count {
            let key = vec![b'0'; i * factor];
            let want = vec![b'X'; (count - i) * factor];
            assert_eq!(b.get(&key).as_deref(), Some(want.as_slice()));
        }
        Ok(())
    })
    .unwrap();
}

// Go: TestBucket_Put_IncompatibleValue
#[test]
fn test_bucket_put_incompatible_value() {
    let (_dir, db) = must_create();
    db.update(|tx| {
        let b0 = tx.create_bucket(b"widgets")?;
        tx.bucket(b"widgets").unwrap().create_bucket(b"foo")?;
        assert!(matches!(b0.put(b"foo", b"bar"), Err(Error::IncompatibleValue)));
        Ok(())
    })
    .unwrap();
}

// Go: TestBucket_Put_ReadOnly
#[test]
fn test_bucket_put_read_only() {
    let (_dir, db) = must_create();
    db.update(|tx| {
        tx.create_bucket(b"widgets")?;
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        let b = tx.bucket(b"widgets").unwrap();
        assert!(matches!(b.put(b"foo", b"bar"), Err(Error::TxNotWritable)));
        Ok(())
    })
    .unwrap();
}

// Go: TestBucket_Delete
#[test]
fn test_bucket_delete() {
    let (_dir, db) = must_create();
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        b.put(b"foo", b"bar")?;
        b.delete(b"foo")?;
        assert!(b.get(b"foo").is_none());
        Ok(())
    })
    .unwrap();
}

// Go: TestBucket_Delete_Large (moderate size)
#[test]
fn test_bucket_delete_large() {
    let (_dir, db) = must_create();
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        for i in 0..100 {
            b.put(format!("{i}").as_bytes(), &vec![b'*'; 1024])?;
        }
        Ok(())
    })
    .unwrap();
    db.update(|tx| {
        let b = tx.bucket(b"widgets").unwrap();
        for i in 0..100 {
            b.delete(format!("{i}").as_bytes())?;
        }
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        let b = tx.bucket(b"widgets").unwrap();
        for i in 0..100 {
            assert!(b.get(format!("{i}").as_bytes()).is_none());
        }
        Ok(())
    })
    .unwrap();
}

// Go: TestBucket_Delete_NonExisting
#[test]
fn test_bucket_delete_non_existing() {
    let (_dir, db) = must_create();
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        b.create_bucket(b"nested")?;
        Ok(())
    })
    .unwrap();
    db.update(|tx| {
        let b = tx.bucket(b"widgets").unwrap();
        b.delete(b"foo")?;
        assert!(b.bucket(b"nested").is_some());
        Ok(())
    })
    .unwrap();
}

// Go: TestBucket_Nested
#[test]
fn test_bucket_nested() {
    let (_dir, db) = must_create();
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        b.create_bucket(b"foo")?;
        b.put(b"bar", b"0000")?;
        Ok(())
    })
    .unwrap();
    common::must_check(&db);
    db.update(|tx| {
        tx.bucket(b"widgets").unwrap().put(b"bar", b"xxxx")?;
        Ok(())
    })
    .unwrap();
    db.update(|tx| {
        let b = tx.bucket(b"widgets").unwrap();
        for i in 0..1000 {
            b.put(format!("{i}").as_bytes(), format!("{i}").as_bytes())?;
        }
        Ok(())
    })
    .unwrap();
    db.update(|tx| {
        tx.bucket(b"widgets")
            .unwrap()
            .bucket(b"foo")
            .unwrap()
            .put(b"baz", b"yyyy")?;
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        let b = tx.bucket(b"widgets").unwrap();
        assert_eq!(
            b.bucket(b"foo").unwrap().get(b"baz").as_deref(),
            Some(&b"yyyy"[..])
        );
        assert_eq!(b.get(b"bar").as_deref(), Some(&b"xxxx"[..]));
        for i in 0..1000 {
            let s = format!("{i}");
            assert_eq!(b.get(s.as_bytes()).as_deref(), Some(s.as_bytes()));
        }
        Ok(())
    })
    .unwrap();
}

// Go: TestBucket_Delete_Bucket
#[test]
fn test_bucket_delete_bucket() {
    let (_dir, db) = must_create();
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        b.create_bucket(b"foo")?;
        assert!(matches!(b.delete(b"foo"), Err(Error::IncompatibleValue)));
        Ok(())
    })
    .unwrap();
}

// Go: TestBucket_DeleteBucket_Nested
#[test]
fn test_bucket_delete_bucket_nested() {
    let (_dir, db) = must_create();
    db.update(|tx| {
        let widgets = tx.create_bucket(b"widgets")?;
        let foo = widgets.create_bucket(b"foo")?;
        let bar = foo.create_bucket(b"bar")?;
        bar.put(b"baz", b"bat")?;
        widgets.delete_bucket(b"foo")?;
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        assert!(tx.bucket(b"widgets").unwrap().bucket(b"foo").is_none());
        Ok(())
    })
    .unwrap();
}

// Go: TestBucket_Sequence
#[test]
fn test_bucket_sequence() {
    let (_dir, db) = must_create();
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        assert_eq!(b.sequence(), 0);
        assert_eq!(b.next_sequence()?, 1);
        assert_eq!(b.next_sequence()?, 2);
        b.set_sequence(10)?;
        assert_eq!(b.next_sequence()?, 11);
        Ok(())
    })
    .unwrap();
}

// Go: TestBucket_NextSequence
#[test]
fn test_bucket_next_sequence() {
    let (_dir, db) = must_create();
    db.update(|tx| {
        let b = tx.create_bucket(b"s")?;
        for _ in 0..3 {
            b.next_sequence()?;
        }
        assert_eq!(b.sequence(), 3);
        Ok(())
    })
    .unwrap();
}

// Go: TestBucket_NextSequence_Persist
#[test]
fn test_bucket_next_sequence_persist() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = common::must_create_db_in(dir.path());
        db.update(|tx| {
            tx.create_bucket(b"s")?.next_sequence()?;
            Ok(())
        })
        .unwrap();
        db.close().unwrap();
    }
    let db = common::reopen(&dir, None);
    db.view(|tx| {
        assert_eq!(tx.bucket(b"s").unwrap().sequence(), 1);
        Ok(())
    })
    .unwrap();
}

// Go: TestBucket_NextSequence_ReadOnly
#[test]
fn test_bucket_next_sequence_read_only() {
    let (_dir, db) = must_create();
    db.update(|tx| {
        tx.create_bucket(b"s")?;
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        let b = tx.bucket(b"s").unwrap();
        assert!(matches!(b.next_sequence(), Err(Error::TxNotWritable)));
        Ok(())
    })
    .unwrap();
}

// Go: TestBucket_ForEach
#[test]
fn test_bucket_for_each() {
    let (_dir, db) = must_create();
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        b.put(b"a", b"1")?;
        b.put(b"b", b"2")?;
        b.create_bucket(b"nested")?;
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        let b = tx.bucket(b"widgets").unwrap();
        let mut keys = Vec::new();
        b.for_each(|k, v| {
            keys.push(k.to_vec());
            if k == b"nested" {
                assert!(v.is_none());
            } else {
                assert!(v.is_some());
            }
            Ok(())
        })?;
        keys.sort();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"nested".to_vec()]);
        Ok(())
    })
    .unwrap();
}

// Go: TestBucket_ForEachBucket
#[test]
fn test_bucket_for_each_bucket() {
    let (_dir, db) = must_create();
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        b.put(b"k", b"v")?;
        b.create_bucket(b"c1")?;
        b.create_bucket(b"c2")?;
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        let b = tx.bucket(b"widgets").unwrap();
        let mut names = Vec::new();
        b.for_each_bucket(|name| {
            names.push(name.to_vec());
            Ok(())
        })?;
        names.sort();
        assert_eq!(names, vec![b"c1".to_vec(), b"c2".to_vec()]);
        Ok(())
    })
    .unwrap();
}

// Go: TestBucket_ForEach_ShortCircuit
#[test]
fn test_bucket_for_each_short_circuit() {
    let (_dir, db) = must_create();
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        b.put(b"a", b"1")?;
        b.put(b"b", b"2")?;
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        let b = tx.bucket(b"widgets").unwrap();
        let err = b.for_each(|k, _| {
            if k == b"a" {
                return Err(Error::KeyRequired);
            }
            Ok(())
        });
        assert!(matches!(err, Err(Error::KeyRequired)));
        Ok(())
    })
    .unwrap();
}

// Go: TestBucket_Put_EmptyKey
#[test]
fn test_bucket_put_empty_key() {
    let (_dir, db) = must_create();
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        assert!(matches!(b.put(b"", b"v"), Err(Error::KeyRequired)));
        Ok(())
    })
    .unwrap();
}

// Go: TestBucket_Put_KeyTooLarge
#[test]
fn test_bucket_put_key_too_large() {
    let (_dir, db) = must_create();
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        let key = vec![0u8; MAX_KEY_SIZE + 1];
        assert!(matches!(b.put(&key, b"v"), Err(Error::KeyTooLarge)));
        Ok(())
    })
    .unwrap();
}

// Go: TestBucket_Put_ValueTooLarge
#[test]
fn test_bucket_put_value_too_large() {
    let (_dir, db) = must_create();
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        let val = vec![0u8; MAX_VALUE_SIZE + 1];
        assert!(matches!(b.put(b"k", &val), Err(Error::ValueTooLarge)));
        Ok(())
    })
    .unwrap();
}

// Go: TestBucket_Inspect (via tx.inspect)
#[test]
fn test_bucket_inspect() {
    let (_dir, db) = must_create();
    db.update(|tx| {
        let b1 = tx.create_bucket(b"b1")?;
        for i in 0..3 {
            b1.put(format!("{i:02}").as_bytes(), format!("{i:02}").as_bytes())?;
        }
        let b1_1 = b1.create_bucket(b"b1_1")?;
        for i in 0..6 {
            b1_1.put(format!("{i:02}").as_bytes(), b"x")?;
        }
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        let tree = tx.inspect();
        assert_eq!(tree.name, "root");
        assert!(!tree.children.is_empty());
        let b1 = tree.children.iter().find(|c| c.name == "b1").unwrap();
        assert_eq!(b1.key_n, 3);
        assert!(!b1.children.is_empty());
        Ok(())
    })
    .unwrap();
}

// Skipped: TestBucket_Get_FromNode — same as Put+Get in same tx (covered elsewhere).
// Skipped: TestBucket_Get_IncompatibleValue, TestBucket_Get_Capacity — Go slice semantics.
// Skipped: TestDB_Put_VeryLarge — long-running stress test (testing.Short).
// Skipped: TestBucket_Put_Closed, TestBucket_Delete_Closed — closed tx via stale handle.
// Skipped: TestBucket_Delete_FreelistOverflow — long stress test.
// Skipped: TestBucket_Delete_ReadOnly — covered in errors_on_incompatible_and_readonly.
// Skipped: TestBucket_DeleteBucket_Nested2, DeleteBucket_Large — extended delete variants.
// Skipped: TestBucket_*_IncompatibleValue — create/delete bucket incompatible cases.
// Skipped: TestBucket_NextSequence_Closed — closed tx.
// Skipped: TestBucket_ForEachBucket_NoBuckets, ForEach_Closed — edge cases.
// Skipped: TestBucket_Stats* — bucket stats API not ported.
