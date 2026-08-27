//! Helpers to invoke the Go bbolt oracle (`tests/go_oracle`).

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

static ORACLE: OnceLock<PathBuf> = OnceLock::new();

/// Build (once) and return the path to `tests/go_oracle/go-oracle`.
pub fn oracle_bin() -> &'static Path {
    ORACLE.get_or_init(|| {
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let dir = manifest.join("tests/go_oracle");
        let bin = dir.join("go-oracle");
        let status = Command::new("go")
            .args(["build", "-o"])
            .arg(&bin)
            .arg(".")
            .current_dir(&dir)
            .env("GOTOOLCHAIN", "auto")
            .status()
            .unwrap_or_else(|e| panic!("failed to spawn `go` for oracle build: {e}"));
        assert!(
            status.success(),
            "go build of tests/go_oracle failed; ensure Go is installed (GOTOOLCHAIN=auto)"
        );
        bin
    })
}

pub fn oracle() -> Command {
    let mut c = Command::new(oracle_bin());
    c.env("GOTOOLCHAIN", "auto");
    c
}

pub fn run_ok(mut c: Command) -> String {
    let out = c
        .output()
        .unwrap_or_else(|e| panic!("oracle spawn failed: {e}"));
    if !out.status.success() {
        panic!(
            "oracle failed ({:?})\nstdout:\n{}\nstderr:\n{}",
            out.status,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    String::from_utf8_lossy(&out.stdout).into_owned()
}

pub fn go_init(path: &Path, page_size: usize, freelist: &str) {
    let mut c = oracle();
    c.args([
        "init",
        "-o",
        path.to_str().unwrap(),
        "-pagesize",
        &page_size.to_string(),
        "-freelist",
        freelist,
    ]);
    run_ok(c);
}

pub fn go_write(path: &Path, scenario: &str, page_size: usize, freelist: &str) {
    let mut c = oracle();
    c.args([
        "write",
        "-o",
        path.to_str().unwrap(),
        "-scenario",
        scenario,
        "-pagesize",
        &page_size.to_string(),
        "-freelist",
        freelist,
    ]);
    run_ok(c);
}

pub fn go_mutate(path: &Path, scenario: &str, freelist: &str) {
    let mut c = oracle();
    c.args([
        "mutate",
        "-db",
        path.to_str().unwrap(),
        "-scenario",
        scenario,
        "-freelist",
        freelist,
    ]);
    run_ok(c);
}

pub fn go_inspect(path: &Path, freelist: &str) -> String {
    let mut c = oracle();
    c.args([
        "inspect",
        "-db",
        path.to_str().unwrap(),
        "-freelist",
        freelist,
    ]);
    run_ok(c)
}

pub fn go_check(path: &Path, freelist: &str) {
    let mut c = oracle();
    c.args([
        "check",
        "-db",
        path.to_str().unwrap(),
        "-freelist",
        freelist,
    ]);
    let out = run_ok(c);
    assert!(out.contains("OK"), "go check: {out}");
}

pub fn go_compact(src: &Path, dst: &Path, page_size: usize, freelist: &str) {
    let mut c = oracle();
    c.args([
        "compact",
        "-src",
        src.to_str().unwrap(),
        "-dst",
        dst.to_str().unwrap(),
        "-pagesize",
        &page_size.to_string(),
        "-freelist",
        freelist,
    ]);
    run_ok(c);
}

pub fn go_writeto(src: &Path, dst: &Path, freelist: &str) {
    let mut c = oracle();
    c.args([
        "writeto",
        "-db",
        src.to_str().unwrap(),
        "-o",
        dst.to_str().unwrap(),
        "-freelist",
        freelist,
    ]);
    run_ok(c);
}
