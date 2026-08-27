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

#[test]
fn cli_keys() {
    // Go: TestKeysCommand_Run (printable keys smoke)
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
            let b = tx.create_bucket(b"foo")?;
            for i in 0..3 {
                b.put(format!("foo-{i}").as_bytes(), b"")?;
            }
            Ok(())
        })
        .unwrap();
        db.close().unwrap();
    }
    let out = bbolt_bin()
        .args(["keys", path.to_str().unwrap(), "foo"])
        .output()
        .unwrap();
    assert!(out.status.success(), "{:?}", String::from_utf8_lossy(&out.stderr));
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(s.contains("foo-0"));
    assert!(s.contains("foo-2"));
}

#[test]
fn cli_pages() {
    // Go: TestPagesCommand_Run
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
            let b = tx.create_bucket(b"foo")?;
            b.put(b"foo-0", b"val")?;
            Ok(())
        })
        .unwrap();
        db.close().unwrap();
    }
    let out = bbolt_bin()
        .args(["pages", path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(out.status.success(), "{:?}", String::from_utf8_lossy(&out.stderr));
}

#[test]
fn cli_inspect() {
    // Go: TestInspect
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
            tx.create_bucket(b"widgets")?.put(b"k", b"v")?;
            Ok(())
        })
        .unwrap();
        db.close().unwrap();
    }
    let out = bbolt_bin()
        .args(["inspect", path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(out.status.success(), "{:?}", String::from_utf8_lossy(&out.stderr));
    assert!(String::from_utf8_lossy(&out.stdout).contains("root"));
}

#[test]
fn cli_stats() {
    // Go: TestStatsCommand_Run_EmptyDatabase (smoke — Rust CLI prints freelist counters)
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
        db.close().unwrap();
    }
    let out = bbolt_bin()
        .args(["stats", path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(out.status.success(), "{:?}", String::from_utf8_lossy(&out.stderr));
    assert!(String::from_utf8_lossy(&out.stdout).contains("FreePageN"));
}

#[test]
fn cli_info_command_run() {
    // Go: TestInfoCommand_Run
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
        db.close().unwrap();
    }
    let out = bbolt_bin()
        .args(["info", path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(out.status.success());
    assert!(String::from_utf8_lossy(&out.stdout).contains("PageSize"));
}

#[test]
fn cli_buckets_command_run() {
    // Go: TestBucketsCommand_Run
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
            tx.create_bucket(b"foo")?;
            tx.create_bucket(b"bar")?;
            Ok(())
        })
        .unwrap();
        db.close().unwrap();
    }
    let out = bbolt_bin()
        .args(["buckets", path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(out.status.success());
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(s.contains("foo"));
    assert!(s.contains("bar"));
}

#[test]
fn cli_get_command_run() {
    // Go: TestGetCommand_Run
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
            tx.create_bucket(b"widgets")?.put(b"foo", b"bar")?;
            Ok(())
        })
        .unwrap();
        db.close().unwrap();
    }
    let out = bbolt_bin()
        .args(["get", path.to_str().unwrap(), "widgets", "foo"])
        .output()
        .unwrap();
    assert!(out.status.success());
    assert!(String::from_utf8_lossy(&out.stdout).contains("bar"));
}

#[test]
fn cli_check_command_run() {
    // Go: TestCheckCommand_Run
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
            tx.create_bucket(b"widgets")?.put(b"k", b"v")?;
            Ok(())
        })
        .unwrap();
        db.close().unwrap();
    }
    let out = bbolt_bin()
        .args(["check", path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(out.status.success());
    assert!(String::from_utf8_lossy(&out.stdout).contains("OK"));
}

#[test]
fn cli_no_args_fail() {
    // Go: TestInfoCommand_NoArgs / TestGetCommand_NoArgs / TestPagesCommand_NoArgs (smoke)
    for args in ["info", "get", "pages", "buckets", "check"] {
        let out = bbolt_bin().arg(args).output().unwrap();
        assert!(!out.status.success(), "expected failure for `{args}` with no path");
    }
}
