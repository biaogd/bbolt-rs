//! Per-database, test-only failure injection: no process-global fault state.
use crate::{db::DbInner, platform, Db, Error, Options, Result};
use std::sync::atomic::Ordering;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Stage {
    DataWrite,
    DataSync,
    MetaWrite,
    MetaSync,
    Remap,
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum Failure {
    ShortWrite,
    Eio,
    Enospc,
    Exit,
}
#[derive(Clone, Copy)]
pub(crate) struct Fault {
    pub stage: Stage,
    pub failure: Failure,
}

impl DbInner {
    pub(crate) fn inject(&self, stage: Stage, write: Option<(&[u8], u64)>) -> Result<()> {
        let mut fault = self.fault.lock();
        if fault.as_ref().map(|f| f.stage) != Some(stage) {
            return Ok(());
        }
        let failure = fault.take().unwrap().failure;
        drop(fault);
        let error = match failure {
            Failure::Exit => std::process::exit(77),
            Failure::ShortWrite => {
                if let Some((buf, off)) = write {
                    // Actually persist a prefix, then fail the remainder of write_all_at.
                    let prefix = if stage == Stage::MetaWrite {
                        40
                    } else {
                        buf.len() / 2
                    };
                    platform::write_at(&self.file, &buf[..prefix], off).unwrap();
                }
                std::io::Error::new(std::io::ErrorKind::WriteZero, "injected short write")
            }
            Failure::Eio => std::io::Error::from_raw_os_error(libc::EIO),
            Failure::Enospc => std::io::Error::from_raw_os_error(libc::ENOSPC),
        };
        Err(self.io(error))
    }
}

fn open(path: &std::path::Path) -> Db {
    Db::open(
        path,
        0o600,
        Some(Options {
            page_size: 4096,
            ..Options::default()
        }),
    )
    .unwrap()
}
fn seed(db: &Db) {
    db.update(|tx| {
        let b = tx.create_bucket(b"data")?;
        b.put(b"a", b"old")?;
        b.put(b"b", b"old")
    })
    .unwrap();
}
fn change(db: &Db) -> Result<()> {
    db.update(|tx| {
        let b = tx.bucket(b"data")?.unwrap();
        b.put(b"a", &vec![b'n'; 100_000])?;
        b.put(b"b", &vec![b'n'; 100_000])
    })
}
fn check_complete(db: &Db, expected: Option<bool>) {
    db.view(|tx| {
        let b = tx.bucket(b"data")?.unwrap();
        let a = b.get(b"a")?.unwrap();
        let b = b.get(b"b")?.unwrap();
        assert_eq!(a, b, "torn transaction");
        assert!(a == b"old" || a == vec![b'n'; 100_000]);
        if let Some(new) = expected {
            assert_eq!(a.len() == 100_000, new);
        }
        assert!(tx.check().is_empty(), "{:?}", tx.check());
        Ok(())
    })
    .unwrap();
}

#[test]
fn commit_io_failures_require_reopen_and_recover_complete_transaction() {
    for no_freelist_sync in [false, true] {
        for freelist_type in [crate::FreelistType::Array, crate::FreelistType::HashMap] {
            for stage in [
                Stage::DataWrite,
                Stage::DataSync,
                Stage::MetaWrite,
                Stage::MetaSync,
            ] {
                for failure in [Failure::ShortWrite, Failure::Eio, Failure::Enospc] {
                    // Synchronization has no short-write result; WriteZero here tests error propagation.
                    let dir = tempfile::tempdir().unwrap();
                    let path = dir.path().join("db");
                    let db = Db::open(
                        &path,
                        0o600,
                        Some(Options {
                            page_size: 4096,
                            no_freelist_sync,
                            freelist_type,
                            ..Options::default()
                        }),
                    )
                    .unwrap();
                    seed(&db);
                    let reader = db.begin(false).unwrap();
                    *db.inner.fault.lock() = Some(Fault { stage, failure });
                    let e = change(&db).unwrap_err();
                    if matches!(stage, Stage::MetaWrite | Stage::MetaSync) {
                        assert!(
                            matches!(e, Error::CommitUncertain(_)),
                            "{stage:?} {failure:?}: {e}"
                        );
                    } else {
                        assert!(matches!(e, Error::Io { .. }));
                    }
                    assert!(db.inner.fault.lock().is_none(), "fault not exercised");
                    assert!(matches!(db.begin(true), Err(Error::RecoveryRequired)));
                    assert!(matches!(db.begin(false), Err(Error::RecoveryRequired)));
                    let b = reader.bucket(b"data").unwrap().unwrap();
                    assert_eq!(b.get(b"a").unwrap().unwrap(), b"old");
                    reader.rollback().unwrap();
                    db.close().unwrap();
                    drop(db);
                    let db = Db::open(
                        &path,
                        0o600,
                        Some(Options {
                            page_size: 4096,
                            no_freelist_sync,
                            freelist_type,
                            ..Options::default()
                        }),
                    )
                    .unwrap();
                    check_complete(
                        &db,
                        if matches!(stage, Stage::DataWrite | Stage::DataSync) {
                            Some(false)
                        } else {
                            None
                        },
                    );
                    db.update(|tx| tx.bucket(b"data")?.unwrap().put(b"after", b"reopen"))
                        .unwrap();
                    check_complete(&db, None);
                    db.close().unwrap();
                }
            }
        }
    }
}

#[test]
fn committed_remap_failure_preserves_mapping_and_durable_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let db = open(&path);
    seed(&db);
    let pin = db.inner.pin_mmap().unwrap();
    let reader = db.begin(false).unwrap();
    let tx = db.begin(true).unwrap();
    let b = tx.bucket(b"data").unwrap().unwrap();
    b.put(b"a", &vec![b'n'; 100_000]).unwrap();
    b.put(b"b", &vec![b'n'; 100_000]).unwrap();
    let called = std::rc::Rc::new(std::cell::Cell::new(false));
    let c = called.clone();
    tx.on_commit(move || c.set(true));
    *db.inner.fault.lock() = Some(Fault {
        stage: Stage::Remap,
        failure: Failure::Eio,
    });
    assert!(matches!(tx.commit(), Err(Error::CommittedMappingFailed(_))));
    assert!(called.get());
    assert!(matches!(tx.rollback(), Err(Error::TxClosed)));
    assert!(std::sync::Arc::ptr_eq(&pin, &db.inner.pin_mmap().unwrap()));
    assert!(matches!(db.begin(true), Err(Error::RecoveryRequired)));
    assert!(matches!(db.begin(false), Err(Error::RecoveryRequired)));
    assert_eq!(
        reader
            .bucket(b"data")
            .unwrap()
            .unwrap()
            .get(b"a")
            .unwrap()
            .unwrap(),
        b"old"
    );
    reader.rollback().unwrap();
    db.close().unwrap();
    let db = open(&path);
    check_complete(&db, Some(true));
    db.update(|tx| tx.bucket(b"data")?.unwrap().put(b"after", b"ok"))
        .unwrap();
    db.close().unwrap();
}

#[test]
fn ordinary_rollback_restores_allocated_free_pages() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().join("db"));
    seed(&db);
    change(&db).unwrap();
    change(&db).unwrap();
    db.update(|_| Ok(())).unwrap(); // release pending pages

    for _ in 0..10 {
        let tx = db.begin(true).unwrap();
        // Allocate directly to cover failure/drop after spill has consumed free IDs.
        let before = db.inner.freelist.lock().free_count();
        assert!(before > 0);
        {
            let mut inner = tx.inner.borrow_mut();
            db.inner
                .allocate(inner.meta.txid, 1, &mut inner.meta.pgid)
                .unwrap();
        }
        drop(tx);
        assert_eq!(db.inner.freelist.lock().free_count(), before);
        assert!(!db.inner.recovery_required.load(Ordering::SeqCst));
    }
    check_complete(&db, Some(true));
}

#[test]
fn crash_child() {
    let Ok(path) = std::env::var("BBOLT_CRASH_TEST_PATH") else {
        return;
    };
    let stage: usize = std::env::var("BBOLT_CRASH_STAGE").unwrap().parse().unwrap();
    let db = open(std::path::Path::new(&path));
    *db.inner.fault.lock() = Some(Fault {
        stage: [
            Stage::DataWrite,
            Stage::DataSync,
            Stage::MetaWrite,
            Stage::MetaSync,
            Stage::Remap,
        ][stage],
        failure: Failure::Exit,
    });
    change(&db).unwrap();
    panic!("crash hook not reached");
}

#[test]
fn process_exit_at_commit_stages_recovers_complete_transaction() {
    for stage in 0..5 {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let db = open(&path);
        seed(&db);
        db.close().unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "recovery_tests::crash_child", "--nocapture"])
            .env("BBOLT_CRASH_TEST_PATH", &path)
            .env("BBOLT_CRASH_STAGE", stage.to_string())
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(77), "stage {stage}");
        let db = open(&path);
        check_complete(
            &db,
            if stage < 3 {
                Some(false)
            } else if stage == 4 {
                Some(true)
            } else {
                None
            },
        );
        change(&db).unwrap();
        check_complete(&db, Some(true));
        db.close().unwrap();
    }
}

#[test]
fn read_results_distinguish_missing_closed_and_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().join("db"));
    let tx = db.begin(true).unwrap();
    let b = tx.create_bucket(b"data").unwrap();
    assert!(tx.bucket(b"missing").unwrap().is_none());
    assert!(b.bucket(b"missing").unwrap().is_none());
    assert!(b.get(b"missing").unwrap().is_none());
    assert!(!b.has_value(b"missing").unwrap());
    // Corrupt an inline page after opening its bucket, covering Bucket read paths.
    {
        let mut inner = tx.inner.borrow_mut();
        let page = inner
            .buckets
            .get_mut(&b.id)
            .unwrap()
            .inline_page
            .as_mut()
            .unwrap();
        crate::page::set_page_count(page, 1); // element header is absent
    }
    assert!(matches!(b.get(b"missing"), Err(Error::Corrupt(_))));
    assert!(matches!(b.bucket(b"missing"), Err(Error::Corrupt(_))));
    assert!(matches!(b.has_value(b"missing"), Err(Error::Corrupt(_))));
    tx.rollback().unwrap();
    assert!(matches!(tx.bucket(b"data"), Err(Error::TxClosed)));
    assert!(matches!(b.bucket(b"data"), Err(Error::TxClosed)));
    assert!(matches!(b.get(b"data"), Err(Error::TxClosed)));
    assert!(matches!(b.has_value(b"data"), Err(Error::TxClosed)));
    db.close().unwrap();
}

#[test]
fn corrupt_root_propagates_through_transaction_bucket_lookup() {
    use std::io::{Seek, SeekFrom, Write};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let db = open(&path);
    seed(&db);
    let tx = db.begin(false).unwrap();
    let root = tx.meta().root.root;
    tx.rollback().unwrap();
    db.close().unwrap();
    drop(tx);
    drop(db);
    let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.seek(SeekFrom::Start(root * 4096 + 16 + 4)).unwrap();
    file.write_all(&u32::MAX.to_le_bytes()).unwrap();
    file.sync_all().unwrap();
    let db = open(&path);
    let tx = db.begin(false).unwrap();
    assert!(matches!(tx.bucket(b"data"), Err(Error::Corrupt(_))));
    tx.rollback().unwrap();
    db.close().unwrap();
}

#[test]
fn long_reader_pins_pages_then_space_is_reused() {
    for freelist_type in [crate::FreelistType::Array, crate::FreelistType::HashMap] {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(
            dir.path().join("db"),
            0o600,
            Some(Options {
                page_size: 4096,
                freelist_type,
                ..Options::default()
            }),
        )
        .unwrap();
        seed(&db);
        let reader = db.begin(false).unwrap();
        for _ in 0..12 {
            change(&db).unwrap();
        }
        assert!(db.inner.freelist.lock().pending_count() > 0);
        assert_eq!(
            reader
                .bucket(b"data")
                .unwrap()
                .unwrap()
                .get(b"a")
                .unwrap()
                .unwrap(),
            b"old"
        );
        reader.rollback().unwrap();
        let high = db.begin(false).unwrap();
        let hwm = high.high_water_mark();
        high.rollback().unwrap();
        for _ in 0..24 {
            change(&db).unwrap();
        }
        let tx = db.begin(false).unwrap();
        // Two alternating transactions need at most a small new freelist allocation.
        assert!(tx.high_water_mark() <= hwm + 4, "space was not reused");
        assert!(tx.check().is_empty());
        tx.rollback().unwrap();
        check_complete(&db, Some(true));
        db.close().unwrap();
    }
}
