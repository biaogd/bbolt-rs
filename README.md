# bbolt

A Rust port of [etcd-io/bbolt](https://github.com/etcd-io/bbolt): an embedded, mmap-backed B+tree key/value store.

bbolt is a single-writer / multiple-reader, ACID, serializable database in one file. This crate keeps the on-disk layout of Go bbolt (magic `0xED0CDAED`, version 2) so files can be opened by either implementation.

## Status

Core KV operations work and persist. Fresh databases are byte-identical to Go bbolt at init, Go can read files this crate writes, and this crate can read files Go writes (see `tests/fixtures/` and `cargo test`).

This is a usable core, not a line-by-line clone of every helper in upstream.

## Crate overview

| Type | Role |
| --- | --- |
| [`Db`](src/db.rs) | One file. `open`, `update` / `view`, `begin`, `batch`, `close` |
| [`Tx`](src/tx.rs) | Read-only or read-write transaction |
| [`Bucket`](src/bucket.rs) | Collection of keys; may contain nested buckets |
| [`Cursor`](src/cursor.rs) | Lexicographic iteration (`first` / `next` / `prev` / `seek`) |

Internals follow the same pieces as upstream: page headers, meta pages, B+tree nodes, copy-on-write spills, a freelist, and mmap I/O.

## API sketch

```rust
use bbolt::{Db, Options};

let db = Db::open("my.db", 0o600, Some(Options {
    page_size: 4096,
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
    let users = tx.bucket(b"users").unwrap();
    assert_eq!(users.get(b"alice").as_deref(), Some(&b"data"[..]));
    let mut c = users.cursor();
    let (k, v) = c.first()?;
    let _ = (k, v);
    Ok(())
})?;
```

Managed transactions: return `Ok` from `update` to commit, `Err` to roll back. `view` is always read-only. Manual `begin(true|false)` plus `commit` / `rollback` is also supported.

Keys and values are returned as owned `Vec<u8>` (copied out of the mmap). Nested buckets appear in cursors with `value == None`, matching Go’s nil value.

`Db` is `Clone` (shared handle), `Send`, and `Sync`. A `Tx` is not thread-safe.

## How to run tests

```sh
cargo test
cargo clippy --all-targets -- -D warnings
```

Integration tests cover create/open, put/get/delete, nested buckets, cursor iteration, commit vs rollback, persistence across reopen, sequences, page splits, overflow values, concurrent readers, batch writes, and opening fixtures produced by Go bbolt.

## Compatibility with upstream bbolt

**On-disk format (version 2)** — intended to match:

- Magic `0xED0CDAED`, FNV-1a-64 meta checksum, little-endian page header (16 bytes)
- Page types: meta (`0x04`), leaf (`0x02`), branch (`0x01`), freelist (`0x10`)
- Leaf element flags: `bucketLeafFlag = 0x01`
- Inline buckets: 16-byte `InBucket` header (`root`, `sequence`) plus an embedded page when `root == 0`
- Dual meta pages at pgid 0/1; commit writes meta for `txid % 2` after data pages
- Copy-on-write pages, single writer, serializable snapshots for readers

Verified here: empty init file is byte-identical to Go; `tests/fixtures/go_sample.db` (written by Go) opens and reads correctly; a file written by this crate opens in Go bbolt.

**Ported**

- `Open` / create, mmap, advisory `flock`, `fdatasync`
- Read-write and read-only transactions (`Update`, `View`, `Begin`)
- Buckets, nested buckets, inline buckets, `CreateBucketIfNotExists`, `DeleteBucket`, `MoveBucket`
- `Put` / `Get` / `Delete`, sequences
- Cursors: first, last, next, prev, seek, delete
- B+tree split / spill / rebalance, overflow pages
- Array freelist (upstream default), pending pages vs open readers
- `Batch` (combined writers; closures must be `Send + 'static`)
- `NoSync`, `ReadOnly`, `Timeout`, `PageSize`, `MaxSize`

**Not ported yet (gaps vs upstream)**

- Hashmap freelist backend
- `NoFreelistSync` recovery by scanning reachable pages
- `Tx.WriteTo` / `CopyFile` / `Compact`
- `Tx.Check` / `Inspect`, stats structs
- Logger, `Mlock`, `StrictMode`
- Windows mmap/lock paths (Unix only)
- CLI (`cmd/bbolt`)
- Zero-copy mmap slices (this crate copies into `Vec<u8>`)
- Full historical test suite and every public helper

Known API differences: values are owned copies; `Batch` requires `Send + 'static` closures so they can run on a combiner thread.

## License

MIT, same as [etcd-io/bbolt](https://github.com/etcd-io/bbolt/blob/main/LICENSE) (originally Ben Johnson’s Bolt).
