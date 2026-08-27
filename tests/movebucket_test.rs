//! Ports of upstream `movebucket_test.go`.

mod common;

use bbolt::{Bucket, Error, Tx};

struct MoveCase {
    name: &'static str,
    src_path: &'static [&'static [u8]],
    dst_path: &'static [&'static [u8]],
    bucket_to_move: &'static [u8],
    exist_in_src: bool,
    exist_in_dst: bool,
    incompatible_in_src: bool,
    incompatible_in_dst: bool,
    expected: Option<Error>,
}

fn open_chain(tx: &Tx, path: &[&[u8]]) -> Option<Bucket> {
    if path.is_empty() {
        return None;
    }
    let mut b = if let Some(existing) = tx.bucket(path[0]) {
        existing
    } else {
        let created = tx.create_bucket(path[0]).unwrap();
        populate_sample(&created, 5);
        created
    };
    for &name in &path[1..] {
        b = if let Some(child) = b.bucket(name) {
            child
        } else {
            let child = b.create_bucket(name).unwrap();
            populate_sample(&child, 5);
            child
        };
    }
    Some(b)
}

fn populate_sample(b: &Bucket, n: usize) {
    for i in 0..n {
        b.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes())
            .unwrap();
    }
}

fn snapshot_bucket(b: &Bucket) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    b.for_each(|k, v| {
        out.push((k.to_vec(), v.unwrap().to_vec()));
        Ok(())
    })
    .unwrap();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn create_named_bucket(parent: Option<&Bucket>, tx: &Tx, name: &[u8]) -> Bucket {
    let b = if let Some(p) = parent {
        p.create_bucket(name).unwrap()
    } else {
        tx.create_bucket(name).unwrap()
    };
    populate_sample(&b, 8);
    b
}

fn moved_bucket(tx: &Tx, dst_path: &[&[u8]], name: &[u8]) -> Bucket {
    if dst_path.is_empty() {
        tx.bucket(name).unwrap()
    } else {
        open_chain(tx, dst_path)
            .unwrap()
            .bucket(name)
            .unwrap()
    }
}

fn source_bucket(tx: &Tx, src_path: &[&[u8]], name: &[u8]) -> Bucket {
    if src_path.is_empty() {
        tx.bucket(name).unwrap()
    } else {
        open_chain(tx, src_path).unwrap().bucket(name).unwrap()
    }
}

// Go: TestTx_MoveBucket
#[test]
fn test_tx_move_bucket() {
    let cases = [
        MoveCase {
            name: "normal case",
            src_path: &[b"sb1", b"sb2"],
            dst_path: &[b"db1", b"db2"],
            bucket_to_move: b"bucketToMove",
            exist_in_src: true,
            exist_in_dst: false,
            incompatible_in_src: false,
            incompatible_in_dst: false,
            expected: None,
        },
        MoveCase {
            name: "shared grandparent",
            src_path: &[b"grandparent", b"sb2"],
            dst_path: &[b"grandparent", b"db2"],
            bucket_to_move: b"bucketToMove",
            exist_in_src: true,
            exist_in_dst: false,
            incompatible_in_src: false,
            incompatible_in_dst: false,
            expected: None,
        },
        MoveCase {
            name: "top level bucket",
            src_path: &[],
            dst_path: &[b"db1", b"db2"],
            bucket_to_move: b"bucketToMove",
            exist_in_src: true,
            exist_in_dst: false,
            incompatible_in_src: false,
            incompatible_in_dst: false,
            expected: None,
        },
        MoveCase {
            name: "convert to top level",
            src_path: &[b"sb1", b"sb2"],
            dst_path: &[],
            bucket_to_move: b"bucketToMove",
            exist_in_src: true,
            exist_in_dst: false,
            incompatible_in_src: false,
            incompatible_in_dst: false,
            expected: None,
        },
        MoveCase {
            name: "not exist in source",
            src_path: &[b"sb1", b"sb2"],
            dst_path: &[b"db1", b"db2"],
            bucket_to_move: b"bucketToMove",
            exist_in_src: false,
            exist_in_dst: false,
            incompatible_in_src: false,
            incompatible_in_dst: false,
            expected: Some(Error::BucketNotFound),
        },
        MoveCase {
            name: "exist in target",
            src_path: &[b"sb1", b"sb2"],
            dst_path: &[b"db1", b"db2"],
            bucket_to_move: b"bucketToMove",
            exist_in_src: true,
            exist_in_dst: true,
            incompatible_in_src: false,
            incompatible_in_dst: false,
            expected: Some(Error::BucketExists),
        },
        MoveCase {
            name: "incompatible key in source",
            src_path: &[b"sb1", b"sb2"],
            dst_path: &[b"db1", b"db2"],
            bucket_to_move: b"bucketToMove",
            exist_in_src: false,
            exist_in_dst: false,
            incompatible_in_src: true,
            incompatible_in_dst: false,
            expected: Some(Error::IncompatibleValue),
        },
        MoveCase {
            name: "incompatible key in target",
            src_path: &[b"sb1", b"sb2"],
            dst_path: &[b"db1", b"db2"],
            bucket_to_move: b"bucketToMove",
            exist_in_src: true,
            exist_in_dst: false,
            incompatible_in_src: false,
            incompatible_in_dst: true,
            expected: Some(Error::IncompatibleValue),
        },
        MoveCase {
            name: "same bucket",
            src_path: &[b"sb1", b"sb2"],
            dst_path: &[b"sb1", b"sb2"],
            bucket_to_move: b"bucketToMove",
            exist_in_src: true,
            exist_in_dst: false,
            incompatible_in_src: false,
            incompatible_in_dst: false,
            expected: Some(Error::SameBuckets),
        },
        MoveCase {
            name: "both root",
            src_path: &[],
            dst_path: &[],
            bucket_to_move: b"bucketToMove",
            exist_in_src: true,
            exist_in_dst: false,
            incompatible_in_src: false,
            incompatible_in_dst: false,
            expected: Some(Error::SameBuckets),
        },
    ];

    for tc in cases {
        let (_dir, db) = common::open_tmp();
        db.update(|tx| {
            let src = open_chain(tx, tc.src_path);
            let dst = open_chain(tx, tc.dst_path);
            if tc.exist_in_src {
                create_named_bucket(src.as_ref(), tx, tc.bucket_to_move);
            }
            if tc.exist_in_dst {
                create_named_bucket(dst.as_ref(), tx, tc.bucket_to_move);
            }
            if tc.incompatible_in_src {
                open_chain(tx, tc.src_path)
                    .unwrap()
                    .put(tc.bucket_to_move, b"bar")?;
            }
            if tc.incompatible_in_dst {
                open_chain(tx, tc.dst_path)
                    .unwrap()
                    .put(tc.bucket_to_move, b"bar")?;
            }
            Ok(())
        })
        .unwrap();

        let before = db
            .view(|tx| {
                if tc.expected.is_none() && tc.exist_in_src {
                    Ok(Some(snapshot_bucket(&source_bucket(
                        tx,
                        tc.src_path,
                        tc.bucket_to_move,
                    ))))
                } else {
                    Ok(None)
                }
            })
            .unwrap();

        db.update(|tx| {
            let src = open_chain(tx, tc.src_path);
            let dst = open_chain(tx, tc.dst_path);
            let err = tx.move_bucket(
                tc.bucket_to_move,
                src.as_ref(),
                dst.as_ref(),
            );
            match (&tc.expected, err) {
                (None, Ok(())) => {}
                (Some(exp), Err(got)) if got.to_string() == exp.to_string() => {}
                (other, got) => panic!("case {}: expected {:?}, got {:?}", tc.name, other, got),
            }
            Ok(())
        })
        .unwrap();

        if tc.expected.is_none() {
            let after = db
                .view(|tx| Ok(snapshot_bucket(&moved_bucket(
                    tx,
                    tc.dst_path,
                    tc.bucket_to_move,
                ))))
                .unwrap();
            assert_eq!(before.unwrap(), after, "case {}", tc.name);
        }
    }
}

// Go: TestBucket_MoveBucket_DiffDB
#[test]
fn test_bucket_move_bucket_diff_db() {
    let dir = tempfile::tempdir().unwrap();
    let src_path = dir.path().join("src.db");
    let dst_path = dir.path().join("dst.db");
    {
        let db = bbolt::Db::open(&src_path, 0o600, Some(common::default_opts())).unwrap();
        db.update(|tx| {
            open_chain(tx, &[b"sb1", b"sb2"]);
            Ok(())
        })
        .unwrap();
    }
    {
        let db = bbolt::Db::open(&dst_path, 0o600, Some(common::default_opts())).unwrap();
        db.update(|tx| {
            open_chain(tx, &[b"db1", b"db2"]);
            Ok(())
        })
        .unwrap();
    }
    let src_db = bbolt::Db::open(&src_path, 0o600, Some(common::default_opts())).unwrap();
    let dst_db = bbolt::Db::open(&dst_path, 0o600, Some(common::default_opts())).unwrap();
    let s_tx = src_db.begin(true).unwrap();
    let src = open_chain(&s_tx, &[b"sb1", b"sb2"]).unwrap();
    let d_tx = dst_db.begin(true).unwrap();
    let dst = open_chain(&d_tx, &[b"db1", b"db2"]).unwrap();
    assert!(matches!(
        src.move_bucket(b"bucketToMove", &dst),
        Err(Error::DifferentDb)
    ));
}

// Go: TestBucket_MoveBucket_DiffTx
#[test]
fn test_bucket_move_bucket_diff_tx() {
    let (_dir, db) = common::open_tmp();
    db.update(|tx| {
        open_chain(tx, &[b"sb1", b"sb2"]);
        open_chain(tx, &[b"db1", b"db2"]);
        Ok(())
    })
    .unwrap();
    let s_tx = db.begin(false).unwrap();
    let src = open_chain(&s_tx, &[b"sb1", b"sb2"]).unwrap();
    let d_tx = db.begin(true).unwrap();
    let dst = open_chain(&d_tx, &[b"db1", b"db2"]).unwrap();
    assert!(matches!(
        src.move_bucket(b"bucketToMove", &dst),
        Err(Error::TxNotWritable)
    ));
}
