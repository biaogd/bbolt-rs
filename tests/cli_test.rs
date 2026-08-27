//! CLI smoke tests corresponding to upstream `cmd/bbolt/command/*_test.go` basics.

use std::process::Command;

fn bbolt_bin() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_bbolt"));
    c.env_remove("RUST_BACKTRACE");
    c
}

#[test]
fn cli_version() {
    // Go: command_version
    let out = bbolt_bin().arg("version").output().unwrap();
    assert!(out.status.success());
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(s.contains("bbolt"));
}

#[test]
fn cli_info_buckets_get_check() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.db");
    {
        let db = bbolt::Db::open(
            &path,
            0o600,
            Some(bbolt::Options {
                page_size: 4096,
                ..bbolt::Options::default()
            }),
        )
        .unwrap();
        db.update(|tx| {
            let b = tx.create_bucket(b"widgets")?;
            b.put(b"foo", b"bar")?;
            Ok(())
        })
        .unwrap();
        db.close().unwrap();
    }

    let info = bbolt_bin().args(["info", path.to_str().unwrap()]).output().unwrap();
    assert!(info.status.success(), "{:?}", String::from_utf8_lossy(&info.stderr));
    assert!(String::from_utf8_lossy(&info.stdout).contains("PageSize"));

    let buckets = bbolt_bin()
        .args(["buckets", path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(buckets.status.success());
    assert!(String::from_utf8_lossy(&buckets.stdout).contains("widgets"));

    let get = bbolt_bin()
        .args(["get", path.to_str().unwrap(), "widgets", "foo"])
        .output()
        .unwrap();
    assert!(get.status.success());
    assert!(String::from_utf8_lossy(&get.stdout).contains("bar"));

    let check = bbolt_bin()
        .args(["check", path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(check.status.success());
    assert!(String::from_utf8_lossy(&check.stdout).contains("OK"));
}

#[test]
fn cli_compact() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.db");
    let dst = dir.path().join("dst.db");
    {
        let db = bbolt::Db::open(
            &src,
            0o600,
            Some(bbolt::Options {
                page_size: 4096,
                ..bbolt::Options::default()
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
    let out = bbolt_bin()
        .args([
            "compact",
            "-o",
            dst.to_str().unwrap(),
            src.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(out.status.success(), "{:?}", String::from_utf8_lossy(&out.stderr));
    assert!(dst.exists());
}
