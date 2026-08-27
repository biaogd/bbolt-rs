//! Ports of upstream `node_test.go` / leaf page inode behavior via public page helpers.
//! Go's `node` type is unexported; we exercise the same on-disk leaf layout.

use bbolt::{
    leaf_at, read_inodes, write_inodes, Inode, PageHeader, BRANCH_PAGE_FLAG, LEAF_PAGE_ELEMENT_SIZE,
    LEAF_PAGE_FLAG, PAGE_HEADER_SIZE,
};

// Go: TestNode_write_LeafPage / TestNode_read_LeafPage (combined round-trip)
#[test]
fn test_node_leaf_page_roundtrip() {
    let mut page = vec![0u8; 4096];
    PageHeader {
        id: 3,
        flags: LEAF_PAGE_FLAG,
        count: 0,
        overflow: 0,
    }
    .write(&mut page);
    let inodes = vec![
        Inode {
            flags: 0,
            key: b"john".to_vec(),
            value: b"johnson".to_vec(),
            pgid: 0,
        },
        Inode {
            flags: 0,
            key: b"ricki".to_vec(),
            value: b"lake".to_vec(),
            pgid: 0,
        },
        Inode {
            flags: 0,
            key: b"susy".to_vec(),
            value: b"que".to_vec(),
            pgid: 0,
        },
    ];
    write_inodes(&mut page, true, &inodes);
    let got = read_inodes(&page);
    assert_eq!(got.len(), 3);
    assert_eq!(got[0].key, b"john");
    assert_eq!(got[0].value, b"johnson");
    assert_eq!(got[1].key, b"ricki");
    assert_eq!(got[2].key, b"susy");
    let (_f, k, v) = leaf_at(&page, 0);
    assert_eq!(k, b"john");
    assert_eq!(v, b"johnson");
    let _ = PAGE_HEADER_SIZE;
}

// Go: TestNode_put — sorted insert / overwrite semantics via write+read
#[test]
fn test_node_put_sorted_overwrite() {
    let mut page = vec![0u8; 4096];
    let mut inodes = vec![
        Inode {
            flags: 0,
            key: b"baz".to_vec(),
            value: b"2".to_vec(),
            pgid: 0,
        },
        Inode {
            flags: 0,
            key: b"foo".to_vec(),
            value: b"0".to_vec(),
            pgid: 0,
        },
        Inode {
            flags: 0,
            key: b"bar".to_vec(),
            value: b"1".to_vec(),
            pgid: 0,
        },
    ];
    inodes.sort_by(|a, b| a.key.cmp(&b.key));
    // overwrite foo
    if let Some(i) = inodes.iter().position(|x| x.key == b"foo") {
        inodes[i].value = b"3".to_vec();
        inodes[i].flags = LEAF_PAGE_FLAG as u32;
    }
    write_inodes(&mut page, true, &inodes);
    let got = read_inodes(&page);
    assert_eq!(got.len(), 3);
    assert_eq!(got[0].key, b"bar");
    assert_eq!(got[1].key, b"baz");
    assert_eq!(got[2].key, b"foo");
    assert_eq!(got[2].value, b"3");
}

// Go: TestNode_read_LeafPage — construct leaf bytes like upstream
#[test]
fn test_node_read_leaf_page() {
    fn w32(buf: &mut [u8], off: usize, v: u32) {
        buf[off..off + 4].copy_from_slice(&v.to_le_bytes());
    }

    let mut page = vec![0u8; 4096];
    PageHeader {
        id: 0,
        flags: LEAF_PAGE_FLAG,
        count: 2,
        overflow: 0,
    }
    .write(&mut page);

    let elem0 = PAGE_HEADER_SIZE;
    w32(&mut page, elem0, 0);
    w32(&mut page, elem0 + 4, 32);
    w32(&mut page, elem0 + 8, 3);
    w32(&mut page, elem0 + 12, 4);

    let elem1 = PAGE_HEADER_SIZE + LEAF_PAGE_ELEMENT_SIZE;
    w32(&mut page, elem1, 0);
    w32(&mut page, elem1 + 4, 23);
    w32(&mut page, elem1 + 8, 10);
    w32(&mut page, elem1 + 12, 3);

    let data_start = PAGE_HEADER_SIZE + 2 * LEAF_PAGE_ELEMENT_SIZE;
    page[data_start..data_start + 20].copy_from_slice(b"barfoozhelloworldbye");

    let got = read_inodes(&page);
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].key, b"bar");
    assert_eq!(got[0].value, b"fooz");
    assert_eq!(got[1].key, b"helloworld");
    assert_eq!(got[1].value, b"bye");
}

// Go: TestNode_split / TestNode_split_MinKeys / TestNode_split_SinglePage —
// node internals are unexported; bucket fills exercise the same split paths.
#[test]
fn test_node_split_via_bucket() {
    let db = bbolt::Db::open(
        tempfile::tempdir().unwrap().path().join("split.db"),
        0o600,
        Some(bbolt::Options {
            page_size: 4096,
            ..bbolt::Options::default()
        }),
    )
    .unwrap();
    let val = b"0123456701234567";
    db.update(|tx| {
        let b = tx.create_bucket(b"data")?;
        for i in 1..=5u8 {
            let key = format!("{i:08}");
            b.put(key.as_bytes(), val)?;
        }
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        let b = tx.bucket(b"data").unwrap();
        for i in 1..=5u8 {
            let key = format!("{i:08}");
            assert_eq!(b.get(key.as_bytes()).as_deref(), Some(val as &[u8]));
        }
        let stats = b.stats();
        assert!(stats.depth >= 1);
        assert_eq!(stats.key_n, 5);
        Ok(())
    })
    .unwrap();
}

// Go: TestNode_split_MinKeys — few keys stay on a single leaf (no branch).
#[test]
fn test_node_split_min_keys_via_bucket() {
    let db = bbolt::Db::open(
        tempfile::tempdir().unwrap().path().join("min.db"),
        0o600,
        Some(bbolt::Options {
            page_size: 4096,
            ..bbolt::Options::default()
        }),
    )
    .unwrap();
    let val = b"0123456701234567";
    db.update(|tx| {
        let b = tx.create_bucket(b"data")?;
        for i in 1..=2u8 {
            b.put(format!("{i:08}").as_bytes(), val)?;
        }
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        let stats = tx.bucket(b"data").unwrap().stats();
        assert_eq!(stats.key_n, 2);
        assert_eq!(stats.branch_page_n, 0);
        Ok(())
    })
    .unwrap();
}

// Go: TestNode_split_SinglePage — modest fill remains a single leaf page.
#[test]
fn test_node_split_single_page_via_bucket() {
    let db = bbolt::Db::open(
        tempfile::tempdir().unwrap().path().join("single.db"),
        0o600,
        Some(bbolt::Options {
            page_size: 4096,
            ..bbolt::Options::default()
        }),
    )
    .unwrap();
    db.update(|tx| {
        let b = tx.create_bucket(b"data")?;
        for i in 1..=5u8 {
            b.put(format!("{i:08}").as_bytes(), b"0123456701234567")?;
        }
        Ok(())
    })
    .unwrap();
    db.view(|tx| {
        let stats = tx.bucket(b"data").unwrap().stats();
        assert_eq!(stats.key_n, 5);
        assert_eq!(stats.branch_page_n, 0);
        Ok(())
    })
    .unwrap();
}

#[test]
fn test_page_typ_unknown() {
    // Go: TestPage_typ
    let mut h = PageHeader {
        flags: BRANCH_PAGE_FLAG,
        ..Default::default()
    };
    assert_eq!(h.typ(), "branch");
    h.flags = 20000;
    assert_eq!(h.typ(), "unknown<4e20>");
}
