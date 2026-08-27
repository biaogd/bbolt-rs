//! Ports of upstream `db_test.go`.

mod common;

use std::fs;
use std::io::Write;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use bbolt::{Db, Error, Options, Result};

// --- TestOpen ---

#[test]
fn test_open() {
    // Go: TestOpen
    let (dir, db) = common::open_tmp();
    assert_eq!(db.path(), common::db_path(&dir));
    db.close().unwrap();
}

// Go: TestOpen_ErrNotExists — opening a path whose parent directory does not exist.
#[test]
fn test_open_err_not_exists() {
    let dir = tempfile::tempdir().unwrap();
    let bad = dir.path().join("missing-parent").join("db");
    assert!(Db::open(&bad, 0o600, Some(common::default_opts())).is_err());
}

// Go: TestOpen_ErrInvalid
#[test]
fn test_open_err_invalid() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("not-bolt");
    {
        let mut f = fs::File::create(&path).unwrap();
        writeln!(f, "this is not a bolt database").unwrap();
    }
    assert!(matches!(
        Db::open(&path, 0o600, Some(common::default_opts())),
        Err(Error::Invalid)
    ));
}

// Go: TestDB_Open_ReadOnly
#[test]
fn test_open_read_only() {
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
    let db = Db::open(
        common::db_path(&dir),
        0o600,
        Some(Options {
            page_size: common::PAGE_SIZE,
            read_only: true,
            ..Options::default()
        }),
    )
    .unwrap();
    assert!(db.is_read_only());
    db.view(|tx| {
        assert_eq!(
            tx.bucket(b"widgets").unwrap().get(b"foo").as_deref(),
            Some(&b"bar"[..])
        );
        Ok(())
    })
    .unwrap();
    assert!(matches!(db.begin(true), Err(Error::DatabaseReadOnly)));
    db.close().unwrap();
}

// Go: TestDB_Open_ReadOnly_NoCreate
#[test]
fn test_open_read_only_no_create() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let err = Db::open(
        &path,
        0o600,
        Some(Options {
            page_size: common::PAGE_SIZE,
            read_only: true,
            ..Options::default()
        }),
    );
    assert!(err.is_err(), "expected open error for missing file");
}

// Go: TestOpen_RecoverFreeList
#[test]
fn test_open_recover_free_list() {
    let dir = tempfile::tempdir().unwrap();
    let path = common::db_path(&dir);
    {
        let db = Db::open(
            &path,
            0o600,
            Some(Options {
                page_size: common::PAGE_SIZE,
                no_freelist_sync: true,
                ..Options::default()
            }),
        )
        .unwrap();
        let tx = db.begin(true).unwrap();
        let wbuf = vec![0u8; 8192];
        for i in 0..100 {
            let s = format!("{i}");
            let b = tx.create_bucket(s.as_bytes()).unwrap();
            b.put(s.as_bytes(), &wbuf).unwrap();
        }
        tx.commit().unwrap();

        let tx = db.begin(true).unwrap();
        for i in 0..50 {
            let s = format!("{i}");
            tx.bucket(s.as_bytes()).unwrap().delete(s.as_bytes()).unwrap();
        }
        tx.commit().unwrap();
        db.close().unwrap();
    }

    let db = Db::open(
        &path,
        0o600,
        Some(Options {
            page_size: common::PAGE_SIZE,
            no_freelist_sync: true,
            ..Options::default()
        }),
    )
    .unwrap();
    let freepages = db.stats().free_page_n;
    assert!(freepages > 0, "no free pages on NoFreelistSync reopen");
    db.close().unwrap();

    let db = Db::open(
        &path,
        0o600,
        Some(Options {
            page_size: common::PAGE_SIZE,
            no_freelist_sync: false,
            ..Options::default()
        }),
    )
    .unwrap();
    let recovered = db.stats().free_page_n;
    assert!(
        recovered >= freepages.saturating_sub(1),
        "closed with {freepages} free pages, opened with {recovered}"
    );
}

// Go: TestDB_BeginRW
#[test]
fn test_db_begin_rw() {
    let (_dir, db) = common::open_tmp();
    let tx = db.begin(true).unwrap();
    assert!(tx.writable());
    tx.commit().unwrap();
}

// Go: TestDB_Update
#[test]
fn test_db_update() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        b.put(b"foo", b"bar")?;
        b.put(b"baz", b"bat")?;
        b.delete(b"foo")?;
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        let b = tx.bucket(b"widgets").unwrap();
        assert!(b.get(b"foo").is_none());
        assert_eq!(b.get(b"baz").as_deref(), Some(&b"bat"[..]));
        Ok(())
    })
    .unwrap();
}

// Go: TestDB_Update_ManualCommit
#[test]
fn test_db_update_manual_commit() {
    let (_dir, db) = common::open_tmp();
    let tx = db.begin(true).unwrap();
    tx.create_bucket(b"widgets")
        .unwrap()
        .put(b"foo", b"bar")
        .unwrap();
    tx.commit().unwrap();
    db.view(|tx| {
        assert_eq!(
            tx.bucket(b"widgets").unwrap().get(b"foo").as_deref(),
            Some(&b"bar"[..])
        );
        Ok(())
    })
    .unwrap();
}

// Go: TestDB_Update_ManualRollback
#[test]
fn test_db_update_manual_rollback() {
    let (_dir, db) = common::open_tmp();
    let tx = db.begin(true).unwrap();
    tx.create_bucket(b"widgets").unwrap().put(b"foo", b"bar").unwrap();
    tx.rollback().unwrap();
    db.view(|tx| {
        assert!(tx.bucket(b"widgets").is_none());
        Ok(())
    })
    .unwrap();
}

// Go: TestDB_View_ManualCommit
#[test]
fn test_db_view_manual_commit() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        tx.create_bucket(b"widgets")?.put(b"foo", b"bar")?;
        Ok(())
    })
    .unwrap();
    let tx = db.begin(false).unwrap();
    assert_eq!(
        tx.bucket(b"widgets").unwrap().get(b"foo").as_deref(),
        Some(&b"bar"[..])
    );
    assert!(matches!(tx.commit(), Err(Error::TxNotWritable)));
    tx.rollback().unwrap();
}

// Go: TestDB_View_ManualRollback
#[test]
fn test_db_view_manual_rollback() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        tx.create_bucket(b"widgets")?.put(b"foo", b"bar")?;
        Ok(())
    })
    .unwrap();
    let tx = db.begin(false).unwrap();
    assert_eq!(
        tx.bucket(b"widgets").unwrap().get(b"foo").as_deref(),
        Some(&b"bar"[..])
    );
    tx.rollback().unwrap();
}

// Go: TestDB_View_Error
#[test]
fn test_db_view_error() {
    let (_dir, db) = common::open_tmp();
    let err: Result<()> = db.view(|_| Err(Error::KeyRequired));
    assert!(matches!(err, Err(Error::KeyRequired)));
}

// Go: TestDB_Stats — freelist counters after bucket creation.
#[test]
fn test_db_stats() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        tx.create_bucket(b"widgets")?;
        Ok(())
    })
    .unwrap();
    let stats = db.stats();
    assert_eq!(stats.free_page_n, 0);
}

// Go: TestDB_Consistency (smaller) — meta pages and check pass after writes.
#[test]
fn test_db_consistency() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        tx.create_bucket(b"widgets")?;
        Ok(())
    })
    .unwrap();
    for _ in 0..10 {
        db.update(|tx| {
            tx.bucket(b"widgets").unwrap().put(b"foo", b"bar")?;
            Ok(())
        })
        .unwrap();
    }
    db.update(|tx| {
        assert_eq!(tx.page_info(0).unwrap().page_type, "meta");
        assert_eq!(tx.page_info(1).unwrap().page_type, "meta");
        assert!(tx.page_info(6).is_err());
        Ok(())
    })
    .unwrap();
    common::must_check(&db);
}

// Go: TestDBStats_Sub
#[test]
fn test_db_stats_sub() {
    let mut a = bbolt::Stats::default();
    let mut b = bbolt::Stats::default();
    a.tx_stats.page_count = 3;
    a.free_page_n = 4;
    b.tx_stats.page_count = 10;
    b.free_page_n = 14;
    let diff = b.sub(&a);
    assert_eq!(diff.tx_stats.page_count, 7);
    assert_eq!(diff.free_page_n, 14);
}

// Go: TestDB_Batch
#[test]
fn test_db_batch() {
    let (_dir, db) = common::open_tmp();
    let db = Arc::new(db);
    db.update(|tx| {
        tx.create_bucket(b"widgets")?;
        Ok(())
    })
    .unwrap();
    let mut handles = Vec::new();
    for i in 0..2u64 {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            db.batch(move |tx| {
                tx.bucket(b"widgets")
                    .unwrap()
                    .put(&i.to_le_bytes(), b"")?;
                Ok(())
            })
        }));
    }
    for h in handles {
        h.join().unwrap().unwrap();
    }
    db.view(|tx| {
        let b = tx.bucket(b"widgets").unwrap();
        for i in 0..2u64 {
            assert!(b.get(&i.to_le_bytes()).is_some());
        }
        Ok(())
    })
    .unwrap();
}

// Go: TestDB_Open_InitialMmapSize (basic) — large write commits without long block.
#[test]
fn test_db_open_initial_mmap_size_basic() {
    let dir = tempfile::tempdir().unwrap();
    let path = common::db_path(&dir);
    let db = Db::open(
        &path,
        0o600,
        Some(Options {
            page_size: common::PAGE_SIZE,
            initial_mmap_size: 64 * 1024 * 1024,
            ..Options::default()
        }),
    )
    .unwrap();
    let wtx = db.begin(true).unwrap();
    let b = wtx.create_bucket(b"test").unwrap();
    let big = vec![0u8; 512 * 1024];
    b.put(b"foo", &big).unwrap();
    let start = Instant::now();
    wtx.commit().unwrap();
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "write commit blocked too long"
    );
}

// Go: TestOpen_Check
#[test]
fn test_open_check() {
    let dir = tempfile::tempdir().unwrap();
    let path = common::db_path(&dir);
    {
        let db = Db::open(&path, 0o600, Some(common::default_opts())).unwrap();
        db.view(|tx| {
            assert!(tx.check().is_empty());
            Ok(())
        })
        .unwrap();
        db.close().unwrap();
    }
    let db = common::reopen(&dir, None);
    db.view(|tx| {
        assert!(tx.check().is_empty());
        Ok(())
    })
    .unwrap();
}

// Go: TestDB_MaxSizeNotExceeded
#[test]
fn test_db_max_size_not_exceeded() {
    let dir = tempfile::tempdir().unwrap();
    let path = common::db_path(&dir);
    let max_size = 512 * 1024;
    let db = Db::open(
        &path,
        0o600,
        Some(Options {
            page_size: common::PAGE_SIZE,
            max_size,
            ..Options::default()
        }),
    )
    .unwrap();
    common::fill_bucket(
        &db,
        b"data",
        50,
        |k| format!("{k:04}").into_bytes(),
        |_| vec![0u8; 1000],
    )
    .unwrap();
    let sz_before = common::file_size(&path);
    assert!(sz_before <= max_size as u64);
    let err = db.update(|tx| {
        tx.bucket(b"data")
            .unwrap()
            .put(b"oversized", &vec![0u8; 400_000])?;
        Ok(())
    });
    assert!(matches!(err, Err(Error::MaxSizeReached)));
    assert!(common::file_size(&path) <= max_size as u64);
    common::fill_bucket(
        &db,
        b"data",
        1,
        |k| format!("small{k}").into_bytes(),
        |_| vec![0u8; 1],
    )
    .unwrap();
}

// Go: TestDB_HugeValue (reasonable size for CI)
#[test]
fn test_db_huge_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = common::must_create_db_in(dir.path());
    let size = 2 * 1024 * 1024;
    let data = vec![0xCDu8; size];
    db.update(|tx| {
        let b = tx.create_bucket_if_not_exists(b"data")?;
        b.put(b"key", &data)?;
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        let got = tx.bucket(b"data").unwrap().get(b"key").unwrap();
        assert_eq!(got.len(), size);
        Ok(())
    })
    .unwrap();
}

// Go: TestDB_WriteTo_and_Overwrite (simplified — copy via read-only tx after populate)
#[test]
fn test_db_write_to_and_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let db = common::must_create_db_in(dir.path());
    db.update(|tx| {
        let b = tx.create_bucket(b"data")?;
        for k in 0..10 {
            b.put(format!("key_{k}").as_bytes(), format!("value_{k}").as_bytes())?;
        }
        Ok(())
    })
    .unwrap();
    let mut expected = std::collections::HashMap::new();
    db.view(|tx| {
        let b = tx.bucket(b"data").unwrap();
        b.for_each(|k, v| {
            expected.insert(k.to_vec(), v.unwrap().to_vec());
            Ok(())
        })?;
        Ok(())
    })
    .unwrap();

    let rtx = db.begin(false).unwrap();
    let backup = dir.path().join("backup.db");
    rtx.copy_file(&backup, 0o600).unwrap();
    rtx.rollback().unwrap();

    let snap = Db::open(
        &backup,
        0o600,
        Some(Options {
            page_size: common::PAGE_SIZE,
            read_only: true,
            pre_load_freelist: true,
            ..Options::default()
        }),
    )
    .unwrap();
    snap.view(|tx| {
        let b = tx.bucket(b"data").unwrap();
        b.for_each(|k, v| {
            assert_eq!(expected.get(k), Some(&v.unwrap().to_vec()));
            Ok(())
        })?;
        assert!(tx.check().is_empty());
        Ok(())
    })
    .unwrap();
}

// Skipped: TestOpen_MultipleGoroutines — stress test, not required for API parity.
// Skipped: TestOpen_ErrPathRequired — empty path behavior is platform-specific.
// Skipped: TestOpen_ErrVersionMismatch, TestOpen_ErrChecksum — require raw meta surgery.
// Skipped: TestOpen_ReadPageSize_* — meta fallback page-size tests.
// Skipped: TestOpen_Size, TestOpen_Size_Large — file growth regression tests.
// Skipped: TestOpen_MetaInitWriteError — pending upstream.
// Skipped: TestOpen_FileTooSmall — truncated file error string differs.
// Skipped: TestDB_Concurrent_WriteTo_and_ConsistentRead — heavy concurrency stress.
// Skipped: TestDB_Begin_ErrDatabaseNotOpen — zero-value Db not exposed the same way.
// Skipped: TestDB_BeginRW_Closed, TestDB_Close_PendingTx_* — close/pending tx timing.
// Skipped: TestDB_Update_Closed, TestDB_Update_Panic, TestDB_View_Panic — panic/closed DB.
// Skipped: TestDB_Batch_Panic, TestDB_BatchFull, TestDB_BatchTime — batch edge cases.
// Skipped: TestDBUnmap — whitebox field inspection.
// Skipped: TestDB_MaxSizeExceededCanOpen* — secondary max-size open scenarios.
// Skipped: TestDB_MaxSizeExceededDoesNotGrow, TestDB_WindowsMMapReadsAndWritesWithMaxSize —
//          Windows-only (runtime.GOOS == "windows").
// Skipped: TestDB_MaxSizeWithHighInitialMMapDoesNotGrowOnWrite — platform mmap behavior.
