# bbolt

A Rust port of [etcd-io/bbolt](https://github.com/etcd-io/bbolt): an embedded, mmap-backed B+tree key/value store.

bbolt is a single-writer / multiple-reader, ACID, serializable database in one file. This crate keeps the on-disk layout of Go bbolt (magic `0xED0CDAED`, version 2) so files can be opened by either implementation.

## Status

Core operations and most of the public Go surface are implemented with on-disk format compatibility. Fresh databases are byte-identical to Go bbolt at init; Go can read files this crate writes, and this crate can read files Go writes (`tests/fixtures/`, `cargo test`).

### At parity (practical)

| Area | Notes |
| --- | --- |
| Open / create, mmap, flock | Unix exercised; Windows paths compile via `cfg` (untested here) |
| RW / RO txs, `update` / `view` / `begin`, `batch` | Single writer, serializable snapshots |
| Buckets, nested buckets, inline buckets | `Put` / `Get` / `Delete`, `ForEach`, `MoveBucket` |
| Sequences, `FillPercent` | |
| Cursors | `first` / `last` / `next` / `prev` / `seek` / `delete` |
| Freelist | **array** (default) and **hashmap** backends; `NoFreelistSync` + reachable-page recovery |
| Options | `Timeout`, `NoSync`, `NoGrowSync`, `NoFreelistSync`, `PreLoadFreelist`, `FreelistType`, `ReadOnly`, `PageSize`, `MaxSize`, `InitialMmapSize`, `MmapFlags` (accepted), `Mlock` (accepted, not applied), `NoStatistics` |
| `Tx.WriteTo` / `Copy` / `CopyFile` | Snapshot backup |
| `Compact` | Library + CLI |
| `Tx.Check` | Consistency checker |
| `Tx.Inspect`, `Db.Stats`, `Db.Info`, `Bucket.Stats` | |
| CLI | `bbolt` binary: `version`, `info`, `buckets`, `keys`, `get`, `check`, `compact`, `pages`, `inspect`, `stats` |

### Remaining gaps / environment limits

- **Windows**: flock / fdatasync / truncate-for-mmap implemented behind `cfg(windows)` but **not tested** on this Linux agent.
- **`MmapFlags` / `Mlock`**: fields exist for API parity; memmap2 mapping does not apply arbitrary Linux `MAP_*` flags or `mlock`.
- **Logger / StrictMode / full TxStats instrumentation**: not ported (StrictMode can be approximated by calling `tx.check()` yourself). `OnCommit` is supported.
- **Surgery / bench CLI subcommands**: not included (upstream-only maintenance tools).
- Keys/values are returned as owned `Vec<u8>` (copied out of the mmap), not zero-copy slices.

## Upstream test suite coverage

Go bbolt has ~283 `Test*` functions across ~46 `*_test.go` files. This crate maps **150+** of those names via `// Go: TestX` comments (grep `Go:`). `cargo test` runs ~200 library/CLI tests (a few large simulations are `#[ignore]`).

| Upstream file | Status |
| --- | --- |
| `db_test.go` | Most portable cases in `tests/db_test.rs` (open errors, size, batch-full, max-size reopen, concurrent WriteTo) |
| `bucket_test.go` | Most portable cases + `Bucket::stats()` Empty/Small/Nested; closed-tx Put/Delete/ForEach/NextSequence |
| `tx_test.go` | Closed-tx errors, CopyFile, OnCommit, `TxStats::sub` |
| `cursor_test.go` | Seek/delete/iterate, seek-large, leaf-root-reverse, empty-page skips, `cursor.bucket()` |
| `movebucket_test.go` | Full table + DiffDB/DiffTx |
| `tx_check_test.go` | Nest-bucket + corrupt page |
| `concurrent_test.go` | Repeatable-read + generic R/W (simplified) |
| `simulation_test.go` / `simulation_no_freelist_sync_test.go` | Through 1000op/10p (+ nfs 100op_10p); 100op_100p and 10000op monsters `#[ignore]` |
| `internal/freelist/*_test.go` | Unit tests in `src/freelist.rs` (allocate/free/release/reload/write/read/rollback/init panics) |
| `internal/common/page_test.go` | `page_type_names`, `merge_pgids` / `merge_pgids_quick` |
| `node_test.go` | Leaf read/write + split MinKeys/SinglePage via bucket fills |
| `db_whitebox_test.go` | PreLoadFreelist (+ allocate growth) |
| `cmd/bbolt/command/*_test.go` | Smoke: version/info/buckets/keys/get/check/compact/pages/inspect/stats + no-args failures |
| failpoint / dmflakey / powerfailure / surgeon / Windows-only / QuickCheck / multi-GB | **Skipped** (environment or extreme runtime) |

Also: `tests/integration.rs` (format compatibility + feature smoke).

## Crate overview

| Type | Role |
| --- | --- |
| [`Db`](src/db.rs) | One file. `open`, `update` / `view`, `begin`, `batch`, `compact_into`, `stats`, `info`, `close` |
| [`Tx`](src/tx.rs) | Read-only or read-write transaction; `write_to`, `check`, `inspect` |
| [`Bucket`](src/bucket.rs) | Collection of keys; may contain nested buckets |
| [`Cursor`](src/cursor.rs) | Lexicographic iteration |
| [`FreelistType`](src/freelist.rs) | `Array` or `HashMap` |

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
    let nested = users.create_bucket(b"nested")?;
    nested.put(b"x", b"y")?;
    let _id = users.next_sequence()?;
    Ok(())
})?;

db.view(|tx| {
    assert!(tx.check().is_empty());
    let users = tx.bucket(b"users").unwrap();
    assert_eq!(users.get(b"alice").as_deref(), Some(&b"data"[..]));
    Ok(())
})?;
```

Managed transactions: return `Ok` from `update` to commit, `Err` to roll back. `Db` is `Clone` / `Send` / `Sync`. A `Tx` is not thread-safe.

## CLI

```sh
cargo run --bin bbolt -- version
cargo run --bin bbolt -- info path/to.db
cargo run --bin bbolt -- buckets path/to.db
cargo run --bin bbolt -- keys path/to.db mybucket
cargo run --bin bbolt -- get path/to.db mybucket mykey
cargo run --bin bbolt -- check path/to.db
cargo run --bin bbolt -- compact -o out.db path/to.db
cargo run --bin bbolt -- pages path/to.db
cargo run --bin bbolt -- inspect path/to.db
cargo run --bin bbolt -- stats path/to.db
```

## How to run tests

```sh
cargo test
cargo clippy --all-targets -- -D warnings
```

Integration coverage includes create/open, put/get/delete, nested buckets, cursors, commit/rollback, sequences, splits/overflow, batch, Go fixtures, **hashmap freelist**, **NoFreelistSync recovery**, **check**, **compact**, and **WriteTo/CopyFile**.

## Compatibility with upstream bbolt

**On-disk format (version 2)** — intended to match:

- Magic `0xED0CDAED`, FNV-1a-64 meta checksum, little-endian page header (16 bytes)
- Page types: meta (`0x04`), leaf (`0x02`), branch (`0x01`), freelist (`0x10`)
- Leaf element flags: `bucketLeafFlag = 0x01`
- Inline buckets: 16-byte `InBucket` header (`root`, `sequence`) plus an embedded page when `root == 0`
- Dual meta pages at pgid 0/1; commit writes meta for `txid % 2` after data pages
- Copy-on-write pages, single writer, serializable snapshots for readers
- Freelist may be omitted (`pgid = 0xffffffffffffffff`) when `NoFreelistSync` is set; reopen reconstructs free pages by scanning reachability from the root

Verified: empty init file is byte-identical to Go; `tests/fixtures/go_sample.db` opens correctly; files written here open in Go bbolt.
