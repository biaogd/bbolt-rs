//! Ports of upstream `tx_check_test.go`.

mod common;

use bbolt::{CheckOptions, PageHeader, branch_at, leaf_at, PAGE_HEADER_SIZE, LEAF_PAGE_ELEMENT_SIZE};

fn must_bucket_root(db: &bbolt::Db, bucket: &[u8]) -> u64 {
    db.view(|tx| Ok(tx.bucket(bucket).unwrap().root()))
        .unwrap()
}

fn leaf_pages_under_root(file: &[u8], root: u64, page_size: usize) -> Vec<u64> {
    let off = root as usize * page_size;
    let page = &file[off..off + page_size];
    let hdr = PageHeader::read(page);
    if hdr.is_branch() {
        (0..hdr.count as usize)
            .map(|i| branch_at(page, i).0)
            .collect()
    } else {
        vec![root]
    }
}

fn corrupt_leaf_key_order(path: &std::path::Path, victim: u64, page_size: usize) {
    let mut file = std::fs::read(path).unwrap();
    let off = victim as usize * page_size;
    let page = &mut file[off..off + page_size];
    let hdr = PageHeader::read(page);
    assert!(hdr.count > 1, "need multi-element leaf");
    let (_, _key, _) = leaf_at(page, 1);
    let elem_off = PAGE_HEADER_SIZE + LEAF_PAGE_ELEMENT_SIZE;
    let pos = u32::from_le_bytes(page[elem_off + 4..elem_off + 8].try_into().unwrap()) as usize;
    let key_off = off + elem_off + pos;
    file[key_off] = 0;
    std::fs::write(path, file).unwrap();
}

// Go: TestTx_Check_WithNestBucket
#[test]
fn test_tx_check_with_nest_bucket() {
    let dir = tempfile::tempdir().unwrap();
    let db = common::must_create_db_in(dir.path());
    db.update(|tx| {
        let pb = tx.create_bucket(b"parentBucket")?;
        for i in 0..10 {
            pb.put(format!("{i:04}").as_bytes(), format!("value_{i:04}").as_bytes())?;
        }
        let cb = pb.create_bucket(b"nestedBucket")?;
        for i in 0..2000 {
            cb.put(format!("{i:04}").as_bytes(), format!("value_{i:04}").as_bytes())?;
        }
        Ok(())
    })
    .unwrap();
    let root = must_bucket_root(&db, b"parentBucket");
    db.view(|tx| {
        let errs = tx.check_with(CheckOptions { page_id: root });
        assert!(errs.is_empty(), "{errs:?}");
        Ok(())
    })
    .unwrap();
}

// Go: TestTx_Check_CorruptPage
#[test]
fn test_tx_check_corrupt_page() {
    let dir = tempfile::tempdir().unwrap();
    let path = common::db_path(&dir);
    let db = common::must_create_db_in(dir.path());
    common::fill_bucket(
        &db,
        b"data",
        100,
        |k| format!("{k:04}").into_bytes(),
        |_| vec![0u8; 100],
    )
    .unwrap();
    db.close().unwrap();

    let root = {
        let db = common::reopen(&dir, None);
        let r = must_bucket_root(&db, b"data");
        db.close().unwrap();
        r
    };

    let file = std::fs::read(&path).unwrap();
    let leaves = leaf_pages_under_root(&file, root, common::PAGE_SIZE);
    assert!(!leaves.is_empty());
    let victim = leaves[0];
    corrupt_leaf_key_order(&path, victim, common::PAGE_SIZE);

    let db = common::reopen(&dir, None);
    db.view(|tx| {
        let errs = tx.check_with(CheckOptions { page_id: victim });
        assert!(!errs.is_empty(), "expected corruption errors");
        for &pg in &leaves[1..] {
            let ok = tx.check_with(CheckOptions { page_id: pg });
            assert!(ok.is_empty(), "page {pg} should be valid: {ok:?}");
        }
        Ok(())
    })
    .unwrap();
}

// Skipped: TestTx_Check_Panic — requires corrupt root page flags and channel-based Check API.
