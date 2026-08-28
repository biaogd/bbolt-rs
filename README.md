# bbolt

A Rust port of [etcd-io/bbolt](https://github.com/etcd-io/bbolt): an embedded, mmap-backed B+tree key/value store in a single file. Single-writer / multiple-reader, ACID, serializable transactions. On-disk layout matches Go bbolt **version 2** (magic `0xED0CDAED`); files written by either implementation can be opened by the other — exercised by `tests/cross_go_test.rs` and `tests/fixtures/`.

## Status

- **Works:** open/create, RW/RO txs, buckets (nested/inline), cursors, freelist (array + hashmap), `NoFreelistSync` recovery, batch, `Check`, `Compact`, `WriteTo`/`CopyFile`, CLI (`version`, `info`, `buckets`, `keys`, `get`, `check`, `compact`, `pages`, `inspect`, `stats`).
- **Windows:** paths compile behind `cfg(windows)`; not tested on this Linux CI/agent.
- **Skipped:** failpoint/dmflakey/powerfailure suites, surgery/bench upstream CLIs, multi-GB stress.

## API sketch

```rust
use bbolt::{Db, FreelistType, Options};

let db = Db::open("my.db", 0o600, Some(Options {
    page_size: 4096,
    freelist_type: FreelistType::HashMap,
    ..Options::default()
}))?;

db.update(|tx| {
    let users = tx.create_bucket(b"users")?;
    users.put(b"alice", b"data")?;
    Ok(())
})?;

db.view(|tx| {
    let users = tx.bucket(b"users").unwrap();
    assert_eq!(users.get(b"alice").as_deref(), Some(&b"data"[..]));
    Ok(())
})?;
```

Managed transactions: `Ok` from `update` commits, `Err` rolls back. `Db` is `Clone` / `Send` / `Sync`.

## CLI & tests

```sh
cargo run --bin bbolt -- info path/to.db
cargo test
cargo test --test cross_go_test   # Go↔Rust oracle (needs `go` + `python3`)
```

## Performance vs Go bbolt

Same cloud VM, `n=100000`, key=8, value=32, page=4096, fsync on, fill 0.5, 1 warmup + 5 trials (median). [ambaxter/bbolt-rs](https://github.com/ambaxter/bbolt-rs) targets Bolt **v1.3.10** on-disk (not interchangeable with this **v1.5.0** port); their published MacBook numbers use a different harness. See **[BENCHMARKS.md](BENCHMARKS.md)** for the fuller two-way vs-Go table (e.g. seq_put **2.70×** from a separate run).

| Workload | Go (bbolt v1.5.0) | this crate | ambaxter/bbolt-rs 1.3.10 | this/Go | bbolt-rs/Go | this/bbolt-rs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| seq_put | 846,040 | 2,183,226 | 1,223,120 | 2.58× | 1.45× | 1.78× |
| random_put | 4,865 | 51,332 | 24,820 | 10.55× | 5.10× | 2.07× |
| cursor_scan | 83,133,812 | 100,702,399 | 97,322,654 | 1.21× | 1.17× | 1.03× |
| random_get | 2,592,914 | 3,252,083 | 2,110,585 | 1.25× | 0.81× | 1.54× |
| deletes | 2,907,090 | 6,414,485 | 3,130,763 | 2.21× | 1.08× | 2.05× |

```sh
./benches/run_compare.sh
cargo run --release --bin bench_compare -- --dir /tmp/r --workload seq_put --n 100000
```

## Compatibility

- On-disk **format v2**: meta checksum (FNV-1a-64), page types (meta/leaf/branch/freelist), inline buckets, dual meta pages, copy-on-write commits.
- Targets **`go.etcd.io/bbolt` v1.5.0** — same family as current etcd-io/bbolt main.
- Go write → Rust read and Rust write → Go read across mixed workloads (nested buckets, overflow, freelist backends, compact/WriteTo); see `tests/cross_go_test.rs`.
- Static fixtures in `tests/fixtures/`; live oracle needs `go` on `PATH` (one-time module fetch) and `python3`.
- `MmapFlags` / `Mlock` accepted for API parity but not fully applied; per-op TxStats counters not incremented on every internal path yet.
- Do not interchange files with [ambaxter/bbolt-rs](https://github.com/ambaxter/bbolt-rs) (Bolt v1.3.10). Full bench notes: **[BENCHMARKS.md](BENCHMARKS.md)**.
