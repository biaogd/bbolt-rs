//! Ports of upstream `tx_test.go`.

mod common;

use bbolt::{Error, Options};

// Go: TestTx_Check_ReadOnly
#[test]
fn test_tx_check_read_only() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = common::must_create_db_in(dir.path());
        db.update(|tx| {
            tx.create_bucket(b"widgets")?.put(b"foo", b"bar")?;
            Ok(())
        })
        .unwrap();
        db.close().unwrap();
    }
    let db = bbolt::Db::open(
        common::db_path(&dir),
        0o600,
        Some(Options {
            page_size: common::PAGE_SIZE,
            read_only: true,
            ..Options::default()
        }),
    )
    .unwrap();
    let tx = db.begin(false).unwrap();
    for _ in 0..2 {
        let errs = tx.check();
        assert!(errs.is_empty(), "{errs:?}");
    }
    tx.rollback().unwrap();
}

// Go: TestTx_Commit_ErrTxClosed
#[test]
fn test_tx_commit_err_tx_closed() {
    let (_dir, db) = common::open_tmp();
    let tx = db.begin(true).unwrap();
    tx.create_bucket(b"foo").unwrap();
    tx.commit().unwrap();
    assert!(matches!(tx.commit(), Err(Error::TxClosed)));
}

// Go: TestTx_Rollback_ErrTxClosed
#[test]
fn test_tx_rollback_err_tx_closed() {
    let (_dir, db) = common::open_tmp();
    let tx = db.begin(true).unwrap();
    tx.rollback().unwrap();
    assert!(matches!(tx.rollback(), Err(Error::TxClosed)));
}

// Go: TestTx_Commit_ErrTxNotWritable
#[test]
fn test_tx_commit_err_tx_not_writable() {
    let (_dir, db) = common::open_tmp();
    let tx = db.begin(false).unwrap();
    assert!(matches!(tx.commit(), Err(Error::TxNotWritable)));
    tx.rollback().unwrap();
}

// Go: TestTx_Cursor
#[test]
fn test_tx_cursor() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        tx.create_bucket(b"widgets")?;
        tx.create_bucket(b"woojits")?;
        let mut c = tx.cursor();
        let (k, v) = c.first().unwrap();
        assert_eq!(k.as_deref(), Some(&b"widgets"[..]));
        assert!(v.is_none());
        let (k, v) = c.next().unwrap();
        assert_eq!(k.as_deref(), Some(&b"woojits"[..]));
        assert!(v.is_none());
        let (k, v) = c.next().unwrap();
        assert!(k.is_none());
        assert!(v.is_none());
        Ok(())
    })
    .unwrap();
}

// Go: TestTx_CreateBucket_ErrTxNotWritable
#[test]
fn test_tx_create_bucket_err_tx_not_writable() {
    let (_dir, db) = common::open_tmp();
    db.view(|tx| {
        assert!(matches!(tx.create_bucket(b"foo"), Err(Error::TxNotWritable)));
        Ok(())
    })
    .unwrap();
}

// Go: TestTx_Bucket
#[test]
fn test_tx_bucket() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        tx.create_bucket(b"widgets")?;
        assert!(tx.bucket(b"widgets").is_some());
        Ok(())
    })
    .unwrap();
}

// Go: TestTx_CreateBucket
#[test]
fn test_tx_create_bucket() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        tx.create_bucket(b"widgets")?;
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        assert!(tx.bucket(b"widgets").is_some());
        Ok(())
    })
    .unwrap();
}

// Go: TestTx_CreateBucketIfNotExists
#[test]
fn test_tx_create_bucket_if_not_exists() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        tx.create_bucket_if_not_exists(b"widgets")?;
        tx.create_bucket_if_not_exists(b"widgets")?;
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        assert!(tx.bucket(b"widgets").is_some());
        Ok(())
    })
    .unwrap();
}

// Go: TestTx_CreateBucketIfNotExists_ErrBucketNameRequired
#[test]
fn test_tx_create_bucket_if_not_exists_err_bucket_name_required() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        assert!(matches!(
            tx.create_bucket_if_not_exists(b""),
            Err(Error::BucketNameRequired)
        ));
        Ok(())
    })
    .unwrap();
}

// Go: TestTx_CreateBucket_ErrBucketExists
#[test]
fn test_tx_create_bucket_err_bucket_exists() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        tx.create_bucket(b"widgets")?;
        Ok(())
    })
    .unwrap();
    db.update(|tx| {
        assert!(matches!(
            tx.create_bucket(b"widgets"),
            Err(Error::BucketExists)
        ));
        Ok(())
    })
    .unwrap();
}

// Go: TestTx_CreateBucket_ErrBucketNameRequired
#[test]
fn test_tx_create_bucket_err_bucket_name_required() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        assert!(matches!(
            tx.create_bucket(b""),
            Err(Error::BucketNameRequired)
        ));
        Ok(())
    })
    .unwrap();
}

// Go: TestTx_DeleteBucket
#[test]
fn test_tx_delete_bucket() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        tx.create_bucket(b"widgets")?.put(b"foo", b"bar")?;
        Ok(())
    })
    .unwrap();
    db.update(|tx| {
        tx.delete_bucket(b"widgets")?;
        assert!(tx.bucket(b"widgets").is_none());
        Ok(())
    })
    .unwrap();
    db.update(|tx| {
        assert!(tx.create_bucket(b"widgets")?.get(b"foo").is_none());
        Ok(())
    })
    .unwrap();
}

// Go: TestTx_DeleteBucket_ReadOnly
#[test]
fn test_tx_delete_bucket_read_only() {
    let (_dir, db) = common::open_tmp();
    db.view(|tx| {
        assert!(matches!(
            tx.delete_bucket(b"foo"),
            Err(Error::TxNotWritable)
        ));
        Ok(())
    })
    .unwrap();
}

// Go: TestTx_DeleteBucket_NotFound
#[test]
fn test_tx_delete_bucket_not_found() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        assert!(matches!(
            tx.delete_bucket(b"widgets"),
            Err(Error::BucketNotFound)
        ));
        Ok(())
    })
    .unwrap();
}

// Go: TestTx_ForEach_NoError
#[test]
fn test_tx_for_each_no_error() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        tx.create_bucket(b"widgets")?.put(b"foo", b"bar")?;
        tx.for_each(|_, _| Ok(()))?;
        Ok(())
    })
    .unwrap();
}

// Go: TestTx_ForEach_WithError
#[test]
fn test_tx_for_each_with_error() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        tx.create_bucket(b"widgets")?.put(b"foo", b"bar")?;
        let err = tx.for_each(|_, _| Err(Error::KeyRequired));
        assert!(matches!(err, Err(Error::KeyRequired)));
        Ok(())
    })
    .unwrap();
}

// Go: TestTx_CopyFile
#[test]
fn test_tx_copy_file() {
    let dir = tempfile::tempdir().unwrap();
    let db = common::must_create_db_in(dir.path());
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        b.put(b"foo", b"bar")?;
        b.put(b"baz", b"bat")?;
        Ok(())
    })
    .unwrap();
    let copy_path = dir.path().join("copy.db");
    db.view(|tx| {
        tx.copy_file(&copy_path, 0o600)?;
        Ok(())
    })
    .unwrap();
    let db2 = common::reopen_path(&copy_path, None);
    db2.view(|tx| {
        let b = tx.bucket(b"widgets").unwrap();
        assert_eq!(b.get(b"foo").as_deref(), Some(&b"bar"[..]));
        assert_eq!(b.get(b"baz").as_deref(), Some(&b"bat"[..]));
        Ok(())
    })
    .unwrap();
}

// Go: TestTx_Rollback
#[test]
fn test_tx_rollback() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        tx.create_bucket(b"mybucket")?;
        Ok(())
    })
    .unwrap();
    let tx = db.begin(true).unwrap();
    tx.bucket(b"mybucket").unwrap().put(b"k", b"v").unwrap();
    tx.rollback().unwrap();
    db.view(|tx| {
        assert!(tx.bucket(b"mybucket").unwrap().get(b"k").is_none());
        Ok(())
    })
    .unwrap();
}

// Skipped: TestTx_CreateBucket_ErrTxClosed, TestTx_DeleteBucket_ErrTxClosed — stale tx handle.
// Skipped: TestTx_Get_NotFound — bucket-level get covered in bucket tests.
// Skipped: TestTx_OnCommit, TestTx_OnCommit_Rollback — OnCommit API not implemented.
// Skipped: TestTx_CopyFile_Error_Meta, TestTx_CopyFile_Error_Normal — failWriter injection.
// Skipped: TestTx_releaseRange — complex freelist releaseRange integration test.
// Skipped: TestTxStats_GetAndIncAtomically, TestTxStats_Sub — TxStats counters not instrumented.
// Skipped: TestTx_TruncateBeforeWrite — Unix-specific grow/truncate behavior.
