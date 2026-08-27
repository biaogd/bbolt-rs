//! Cross-implementation tests: Rust bbolt ↔ upstream Go bbolt (oracle).
//!
//! Requires `go` on PATH. `GOTOOLCHAIN=auto` downloads the toolchain required
//! by `go.etcd.io/bbolt` (see `tests/go_oracle/go.mod`).

mod common;

use std::fs;
use std::path::Path;

use bbolt::{Db, Error, FreelistType, Options};

use common::go_oracle::{
    go_check, go_compact, go_inspect, go_init, go_mutate, go_write, go_writeto,
};

const PAGE: usize = 4096;

fn hx(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        use std::fmt::Write;
        let _ = write!(s, "{x:02x}");
    }
    s
}

fn open_rust(path: &Path, freelist: FreelistType) -> Db {
    Db::open(
        path,
        0o600,
        Some(Options {
            page_size: PAGE,
            freelist_type: freelist,
            ..Options::default()
        }),
    )
    .unwrap()
}

fn freelist_str(ft: FreelistType) -> &'static str {
    match ft {
        FreelistType::Array => "array",
        FreelistType::HashMap => "hashmap",
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DumpKv {
    k: String,
    v: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DumpBucket {
    name: String,
    sequence: u64,
    keys: Vec<DumpKv>,
    buckets: Vec<DumpBucket>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DumpDb {
    page_size: usize,
    buckets: Vec<DumpBucket>,
}

fn rust_inspect(db: &Db) -> DumpDb {
    let page_size = db.info().page_size;
    let mut buckets = Vec::new();
    db.view(|tx| {
        let mut names = Vec::new();
        tx.for_each(|name, _| {
            names.push(name.to_vec());
            Ok(())
        })?;
        names.sort();
        for name in names {
            let b = tx.bucket(&name).unwrap();
            buckets.push(dump_bucket(&b, &hx(&name)));
        }
        Ok(())
    })
    .unwrap();
    DumpDb { page_size, buckets }
}

fn dump_bucket(b: &bbolt::Bucket, name: &str) -> DumpBucket {
    let mut keys = Vec::new();
    let mut nested = Vec::new();
    b.for_each(|k, v| {
        match v {
            None => {
                let sub = b.bucket(k).expect("nested bucket");
                nested.push(dump_bucket(&sub, &hx(k)));
            }
            Some(val) => keys.push(DumpKv {
                k: hx(k),
                v: hx(val),
            }),
        }
        Ok(())
    })
    .unwrap();
    keys.sort_by(|a, b| a.k.cmp(&b.k));
    nested.sort_by(|a, b| a.name.cmp(&b.name));
    DumpBucket {
        name: name.to_string(),
        sequence: b.sequence(),
        keys,
        buckets: nested,
    }
}

fn dump_db_json(d: &DumpDb) -> String {
    fn buckets_json(bs: &[DumpBucket]) -> String {
        let parts: Vec<String> = bs
            .iter()
            .map(|b| {
                format!(
                    "{{\"name\":{},\"sequence\":{},\"keys\":{},\"buckets\":{}}}",
                    json_str(&b.name),
                    b.sequence,
                    keys_json(&b.keys),
                    buckets_json(&b.buckets)
                )
            })
            .collect();
        format!("[{}]", parts.join(","))
    }
    fn keys_json(ks: &[DumpKv]) -> String {
        let parts: Vec<String> = ks
            .iter()
            .map(|kv| {
                format!(
                    "{{\"k\":{},\"v\":{}}}",
                    json_str(&kv.k),
                    json_str(&kv.v)
                )
            })
            .collect();
        format!("[{}]", parts.join(","))
    }
    fn json_str(s: &str) -> String {
        format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
    }
    format!(
        "{{\"page_size\":{},\"buckets\":{}}}",
        d.page_size,
        buckets_json(&d.buckets)
    )
}

fn assert_inspect_eq(go_json: &str, rust_json: &str) {
    let dir = tempfile::tempdir().unwrap();
    let go_path = dir.path().join("go.json");
    let rust_path = dir.path().join("rust.json");
    fs::write(&go_path, go_json).unwrap();
    fs::write(&rust_path, rust_json).unwrap();
    let status = std::process::Command::new("python3")
        .args([
            "-c",
            &format!(
                "import json,sys; a=json.load(open({go_path:?})); b=json.load(open({rust_path:?}));\n\
                 sys.exit(0 if a==b else 1)"
            ),
        ])
        .status()
        .unwrap();
    if !status.success() {
        panic!("inspect mismatch\n=== go ===\n{go_json}\n=== rust ===\n{rust_json}");
    }
}

fn apply_rust_scenario(db: &Db, scenario: &str) {
    match scenario {
        "sample" => {
            db.update(|tx| {
                let b = tx.create_bucket_if_not_exists(b"users")?;
                b.put(b"alice", b"data1")?;
                b.put(b"bob", b"data2")?;
                let nb = b.create_bucket_if_not_exists(b"nested")?;
                nb.put(b"x", b"y")?;
                Ok(())
            })
            .unwrap();
        }
        "nested_deep" => {
            db.update(|tx| {
                let a = tx.create_bucket_if_not_exists(b"a")?;
                let b = a.create_bucket_if_not_exists(b"b")?;
                let c = b.create_bucket_if_not_exists(b"c")?;
                c.put(b"leaf", b"value")?;
                a.put(b"sibling", b"s")?;
                Ok(())
            })
            .unwrap();
        }
        "sequences" => {
            db.update(|tx| {
                let b = tx.create_bucket_if_not_exists(b"seq")?;
                b.set_sequence(10)?;
                let id = b.next_sequence()?;
                assert_eq!(id, 11);
                b.put(b"last", format!("{id}").as_bytes())?;
                Ok(())
            })
            .unwrap();
        }
        "overflow" => {
            db.update(|tx| {
                let b = tx.create_bucket_if_not_exists(b"big")?;
                b.put(b"v", &vec![0xABu8; 10_000])?;
                b.put(b"small", b"ok")?;
                Ok(())
            })
            .unwrap();
        }
        "split" => {
            db.update(|tx| {
                let b = tx.create_bucket_if_not_exists(b"split")?;
                for i in 0..500 {
                    let k = format!("k{i:04}");
                    let v = format!("v{i:04}-{}", "x".repeat(32));
                    b.put(k.as_bytes(), v.as_bytes())?;
                }
                Ok(())
            })
            .unwrap();
        }
        "deletes" => {
            db.update(|tx| {
                let b = tx.create_bucket_if_not_exists(b"del")?;
                for i in 0..200 {
                    b.put(format!("{i:04}").as_bytes(), b"x")?;
                }
                Ok(())
            })
            .unwrap();
            db.update(|tx| {
                let b = tx.bucket(b"del").unwrap();
                for i in (0..200).step_by(2) {
                    b.delete(format!("{i:04}").as_bytes())?;
                }
                Ok(())
            })
            .unwrap();
        }
        "multi_tx" => {
            for i in 0..5 {
                db.update(|tx| {
                    let b = tx.create_bucket_if_not_exists(b"multi")?;
                    b.put(format!("t{i}").as_bytes(), format!("v{i}").as_bytes())?;
                    Ok(())
                })
                .unwrap();
            }
        }
        other => panic!("unknown apply scenario {other}"),
    }
}

fn write_rust_scenario(path: &Path, scenario: &str, freelist: FreelistType) {
    let _ = fs::remove_file(path);
    let db = open_rust(path, freelist);
    match scenario {
        "empty" => {}
        "mixed" => {
            for s in [
                "sample",
                "sequences",
                "overflow",
                "split",
                "deletes",
                "nested_deep",
                "multi_tx",
            ] {
                apply_rust_scenario(&db, s);
            }
        }
        other => apply_rust_scenario(&db, other),
    }
    db.close().unwrap();
}

fn mutate_rust(path: &Path, scenario: &str, freelist: FreelistType) {
    let db = open_rust(path, freelist);
    match scenario {
        "add_keys" => {
            db.update(|tx| {
                let b = tx.create_bucket_if_not_exists(b"users")?;
                b.put(b"carol", b"data3")?;
                b.put(b"dave", b"data4")?;
                Ok(())
            })
            .unwrap();
        }
        "delete_alice" => {
            db.update(|tx| {
                tx.bucket(b"users").unwrap().delete(b"alice")?;
                Ok(())
            })
            .unwrap();
        }
        "bump_seq" => {
            db.update(|tx| {
                let b = tx.create_bucket_if_not_exists(b"seq")?;
                b.next_sequence()?;
                Ok(())
            })
            .unwrap();
        }
        other => panic!("unknown mutate {other}"),
    }
    db.close().unwrap();
}

#[test]
fn fresh_init_bytes_match_go() {
    let dir = tempfile::tempdir().unwrap();
    let go_path = dir.path().join("go.db");
    let rust_path = dir.path().join("rust.db");
    go_init(&go_path, PAGE, "array");
    {
        let db = open_rust(&rust_path, FreelistType::Array);
        db.close().unwrap();
    }
    let go_bytes = fs::read(&go_path).unwrap();
    let rust_bytes = fs::read(&rust_path).unwrap();
    assert_eq!(
        go_bytes.len(),
        rust_bytes.len(),
        "init size diverge: go={} rust={}",
        go_bytes.len(),
        rust_bytes.len()
    );
    if go_bytes != rust_bytes {
        let mut diffs = Vec::new();
        for (i, (a, b)) in go_bytes.iter().zip(rust_bytes.iter()).enumerate() {
            if a != b {
                diffs.push(i);
                if diffs.len() >= 16 {
                    break;
                }
            }
        }
        panic!("fresh init byte divergence at offsets {diffs:?}");
    }
    let fixture = fs::read("tests/fixtures/go_init.db").unwrap();
    assert_eq!(
        rust_bytes, fixture,
        "Rust init must match tests/fixtures/go_init.db"
    );
}

#[test]
fn go_write_rust_read_scenarios() {
    for scenario in [
        "sample",
        "nested_deep",
        "sequences",
        "overflow",
        "split",
        "deletes",
        "multi_tx",
        "mixed",
    ] {
        for ft in [FreelistType::Array, FreelistType::HashMap] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("g.db");
            go_write(&path, scenario, PAGE, freelist_str(ft));
            go_check(&path, freelist_str(ft));
            let rust_json = {
                let db = open_rust(&path, ft);
                let errs = db.view(|tx| Ok(tx.check())).unwrap();
                assert!(errs.is_empty(), "{scenario}/{ft:?}: rust check {errs:?}");
                let j = dump_db_json(&rust_inspect(&db));
                db.close().unwrap();
                j
            };
            let go_json = go_inspect(&path, freelist_str(ft));
            assert_inspect_eq(&go_json, &rust_json);
        }
    }
}

#[test]
fn rust_write_go_read_scenarios() {
    for scenario in [
        "sample",
        "nested_deep",
        "sequences",
        "overflow",
        "split",
        "deletes",
        "multi_tx",
        "mixed",
    ] {
        for ft in [FreelistType::Array, FreelistType::HashMap] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("r.db");
            write_rust_scenario(&path, scenario, ft);
            go_check(&path, freelist_str(ft));
            let rust_json = {
                let db = open_rust(&path, ft);
                let errs = db.view(|tx| Ok(tx.check())).unwrap();
                assert!(errs.is_empty(), "{scenario}/{ft:?}: {errs:?}");
                let j = dump_db_json(&rust_inspect(&db));
                db.close().unwrap();
                j
            };
            let go_json = go_inspect(&path, freelist_str(ft));
            assert_inspect_eq(&go_json, &rust_json);
        }
    }
}

#[test]
fn round_trip_go_rust_go() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rt.db");
    go_write(&path, "sample", PAGE, "array");
    mutate_rust(&path, "add_keys", FreelistType::Array);
    mutate_rust(&path, "delete_alice", FreelistType::Array);
    go_check(&path, "array");
    let rust_json = {
        let db = open_rust(&path, FreelistType::Array);
        db.view(|tx| {
            let u = tx.bucket(b"users").unwrap();
            assert!(u.get(b"alice").is_none());
            assert_eq!(u.get(b"bob").as_deref(), Some(&b"data2"[..]));
            assert_eq!(u.get(b"carol").as_deref(), Some(&b"data3"[..]));
            assert_eq!(u.get(b"dave").as_deref(), Some(&b"data4"[..]));
            Ok(())
        })
        .unwrap();
        let j = dump_db_json(&rust_inspect(&db));
        db.close().unwrap();
        j
    };
    let go_json = go_inspect(&path, "array");
    assert_inspect_eq(&go_json, &rust_json);
}

#[test]
fn round_trip_rust_go_rust() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rt.db");
    write_rust_scenario(&path, "sample", FreelistType::Array);
    go_mutate(&path, "add_keys", "array");
    go_mutate(&path, "delete_alice", "array");
    let rust_json = {
        let db = open_rust(&path, FreelistType::Array);
        db.view(|tx| {
            let u = tx.bucket(b"users").unwrap();
            assert!(u.get(b"alice").is_none());
            assert_eq!(u.get(b"carol").as_deref(), Some(&b"data3"[..]));
            Ok(())
        })
        .unwrap();
        let errs = db.view(|tx| Ok(tx.check())).unwrap();
        assert!(errs.is_empty());
        let j = dump_db_json(&rust_inspect(&db));
        db.close().unwrap();
        j
    };
    let go_json = go_inspect(&path, "array");
    assert_inspect_eq(&go_json, &rust_json);
}

#[test]
fn compact_and_writeto_cross() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.db");
    go_write(&src, "mixed", PAGE, "array");

    let go_snap = dir.path().join("go_writeto.db");
    go_writeto(&src, &go_snap, "array");
    {
        let rust_json = {
            let db = open_rust(&go_snap, FreelistType::Array);
            assert!(db.view(|tx| Ok(tx.check())).unwrap().is_empty());
            let j = dump_db_json(&rust_inspect(&db));
            db.close().unwrap();
            j
        };
        let go_json = go_inspect(&go_snap, "array");
        assert_inspect_eq(&go_json, &rust_json);
    }

    let rust_snap = dir.path().join("rust_writeto.db");
    {
        let db = open_rust(&src, FreelistType::Array);
        let mut f = fs::File::create(&rust_snap).unwrap();
        db.view(|tx| {
            tx.write_to(&mut f)?;
            Ok(())
        })
        .unwrap();
        db.close().unwrap();
    }
    go_check(&rust_snap, "array");

    let go_dst = dir.path().join("go_compact.db");
    go_compact(&src, &go_dst, PAGE, "array");
    {
        let rust_json = {
            let db = open_rust(&go_dst, FreelistType::Array);
            assert!(db.view(|tx| Ok(tx.check())).unwrap().is_empty());
            let j = dump_db_json(&rust_inspect(&db));
            db.close().unwrap();
            j
        };
        let go_json = go_inspect(&go_dst, "array");
        assert_inspect_eq(&go_json, &rust_json);
    }

    let rust_dst = dir.path().join("rust_compact.db");
    {
        let src_db = open_rust(&src, FreelistType::Array);
        let dst_db = open_rust(&rust_dst, FreelistType::Array);
        src_db.compact_into(&dst_db, 64 * 1024 * 1024).unwrap();
        dst_db.close().unwrap();
        src_db.close().unwrap();
    }
    go_check(&rust_dst, "array");
    let rust_json = {
        let db = open_rust(&rust_dst, FreelistType::Array);
        let j = dump_db_json(&rust_inspect(&db));
        db.close().unwrap();
        j
    };
    let go_json = go_inspect(&rust_dst, "array");
    assert_inspect_eq(&go_json, &rust_json);
}

#[test]
fn behavioral_cursor_order_and_errors() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("b.db");
    go_write(&path, "split", PAGE, "array");
    let db = open_rust(&path, FreelistType::Array);
    db.view(|tx| {
        let b = tx.bucket(b"split").unwrap();
        let mut c = b.cursor();
        let mut prev: Option<Vec<u8>> = None;
        let mut kv = c.first()?;
        let mut n = 0;
        while let Some(k) = kv.0.clone() {
            if let Some(p) = &prev {
                assert!(p.as_slice() < k.as_slice(), "cursor order");
            }
            prev = Some(k);
            n += 1;
            kv = c.next()?;
        }
        assert_eq!(n, 500);
        Ok(())
    })
    .unwrap();

    db.update(|tx| {
        let b = tx.create_bucket(b"widgets")?;
        b.create_bucket(b"sub")?;
        let err = b.put(b"sub", b"nope").unwrap_err();
        assert!(matches!(err, Error::IncompatibleValue));
        Ok(())
    })
    .unwrap();

    db.view(|tx| {
        match tx.create_bucket(b"nope") {
            Err(Error::TxNotWritable) => {}
            Err(e) => panic!("expected TxNotWritable, got {e}"),
            Ok(_) => panic!("expected TxNotWritable, got Ok"),
        }
        Ok(())
    })
    .unwrap();

    {
        let tx = db.begin(true).unwrap();
        tx.create_bucket(b"gone").unwrap().put(b"a", b"b").unwrap();
        tx.rollback().unwrap();
    }
    db.view(|tx| {
        assert!(tx.bucket(b"gone").is_none());
        Ok(())
    })
    .unwrap();

    db.update(|tx| {
        let b = tx.create_bucket(b"seq2")?;
        b.set_sequence(41)?;
        assert_eq!(b.next_sequence()?, 42);
        Ok(())
    })
    .unwrap();
    db.close().unwrap();
    {
        let db = open_rust(&path, FreelistType::Array);
        db.view(|tx| {
            assert_eq!(tx.bucket(b"seq2").unwrap().sequence(), 42);
            Ok(())
        })
        .unwrap();
        db.close().unwrap();
    }
    go_check(&path, "array");
}

#[test]
fn hashmap_freelist_file_opens_in_both() {
    let dir = tempfile::tempdir().unwrap();
    let go_path = dir.path().join("go_hm.db");
    let rust_path = dir.path().join("rust_hm.db");
    go_write(&go_path, "deletes", PAGE, "hashmap");
    write_rust_scenario(&rust_path, "deletes", FreelistType::HashMap);

    go_check(&go_path, "hashmap");
    go_check(&rust_path, "hashmap");
    for path in [&go_path, &rust_path] {
        let rust_json = {
            let db = open_rust(path, FreelistType::HashMap);
            assert!(db.view(|tx| Ok(tx.check())).unwrap().is_empty());
            let j = dump_db_json(&rust_inspect(&db));
            db.close().unwrap();
            j
        };
        let go_json = go_inspect(path, "hashmap");
        assert_inspect_eq(&go_json, &rust_json);
    }
}

#[test]
fn go_incompatible_scenario_matches_rust() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("inc.db");
    go_write(&path, "incompatible_put", PAGE, "array");
    let db = open_rust(&path, FreelistType::Array);
    db.view(|tx| {
        let w = tx.bucket(b"widgets").unwrap();
        assert!(w.bucket(b"sub").is_some());
        assert!(w.get(b"sub").is_none());
        Ok(())
    })
    .unwrap();
}
