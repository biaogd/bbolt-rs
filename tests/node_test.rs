//! Ports of upstream `node_test.go` / leaf page inode behavior via public page helpers.
//! Go's `node` type is unexported; we exercise the same on-disk leaf layout.

use bbolt::{
    leaf_at, read_inodes, write_inodes, Inode, PageHeader, BRANCH_PAGE_FLAG, LEAF_PAGE_FLAG,
    PAGE_HEADER_SIZE,
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
