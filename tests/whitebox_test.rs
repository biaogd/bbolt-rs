//! Ports of upstream `db_whitebox_test.go` / `allocate_test.go` (subset).

mod common;

use bbolt::{Db, Options};

// Go: TestOpenWithPreLoadFreelist — writable always loads; RO respects the flag.
#[test]
fn test_open_with_pre_load_freelist() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.db");
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
            let b = tx.create_bucket(b"b")?;
            b.put(b"k", b"v")?;
            Ok(())
        })
        .unwrap();
        db.close().unwrap();
    }

    // Read-only without preload: freelist not required for reads.
    let db = Db::open(
        &path,
        0o600,
        Some(Options {
            page_size: 4096,
            read_only: true,
            pre_load_freelist: false,
            ..Options::default()
        }),
    )
    .unwrap();
    db.view(|tx| {
        assert_eq!(
            tx.bucket(b"b").unwrap().get(b"k").as_deref(),
            Some(&b"v"[..])
        );
        Ok(())
    })
    .unwrap();
    db.close().unwrap();

    // Read-only with preload.
    let db = Db::open(
        &path,
        0o600,
        Some(Options {
            page_size: 4096,
            read_only: true,
            pre_load_freelist: true,
            ..Options::default()
        }),
    )
    .unwrap();
    let st = db.stats();
    let _ = st.free_page_n;
    db.close().unwrap();
}

// Go: TestTx_allocatePageStats — allocating pages bumps high-water / freelist usage.
#[test]
fn test_tx_allocate_page_growth() {
    let (_dir, db) = common::open_tmp();
    let before = db.view(|tx| Ok(tx.high_water_mark())).unwrap();
    db.update(|tx| {
        let b = tx.create_bucket(b"big")?;
        // Force page growth with many keys.
        for i in 0..500u32 {
            b.put(&i.to_be_bytes(), &i.to_be_bytes())?;
        }
        Ok(())
    })
    .unwrap();
    let after = db.view(|tx| Ok(tx.high_water_mark())).unwrap();
    assert!(after > before, "hwm {before} -> {after}");
    common::must_check(&db);
}
