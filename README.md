# bbolt

A Rust port of [etcd-io/bbolt](https://github.com/etcd-io/bbolt): an embedded, mmap-backed B+tree key/value store.

bbolt is a single-writer / multiple-reader, ACID, serializable database in one file. This crate keeps the on-disk layout of Go bbolt (magic `0xED0CDAED`, version 2) so files can be opened by either implementation.

## Status

Core operations and most of the public Go surface are implemented with on-disk format compatibility. Fresh databases are byte-identical to Go bbolt at init; Go can read files this crate writes, and this crate can read files Go writes — proven by the live Go oracle suite (`cargo test --test cross_go_test`) plus `tests/fixtures/`.

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
- **Logger / StrictMode / full TxStats instrumentation**: TxStats Inc/Get/`add` API is ported; per-tx counters are stored on `TxInner` but not yet incremented on every spill/rebalance path. `OnCommit` is supported.
- **Surgery / bench CLI subcommands**: not included (upstream-only maintenance tools).
- Keys/values from `get` / `Cursor::{first,next,...}` are owned `Vec<u8>`. Prefer `Cursor::{first_view,next_view,key,value}` or `Bucket::has_value` for Go-style zero-copy views into the pinned mmap.

## Upstream test suite coverage

Go bbolt has ~281 `Test*` functions across ~46 `*_test.go` files. This crate maps **~233** of those names via `// Go: TestX` comments (grep `Go:`), including freelist E2E/SerDe, TxStats add/Inc/Get, Copy failWriter, releaseRange, QuickCheck, panic lifecycle, Check corruption, meta1 page-size fallback, ManyDBs, and bucket stress (moderated). `cargo test` runs **261** tests by default (**18** are `#[ignore]`), including **9** live Go↔Rust oracle tests.

| Upstream file | Status |
| --- | --- |
| `db_test.go` | Most portable cases + panic lifecycle, close-pending-RW, batch panic/time, max-size reopen + high mmap, BigPage, meta1 page-size, concurrent WriteTo |
| `bucket_test.go` | Most portable cases + Get_FromNode, DeleteBucket_Large, moderated VeryLarge / FreelistOverflow; `Bucket::stats()` Empty/Small/Nested/**Stats(4096)**; closed-tx Put/Delete/ForEach/NextSequence |
| `tx_test.go` | CopyFile, OnCommit, `TxStats` (Sub/Inc/add), failWriter Copy errors, `releaseRange`, closed-tx errors |
| `tx_stats_test.go` | `TestTxStats_add` |
| `cursor_test.go` | Seek/delete/iterate, seek-large, QuickCheck (forward/reverse/buckets), empty-page skips |
| `movebucket_test.go` | Full table + DiffDB/DiffTx |
| `tx_check_test.go` | Nest-bucket, corrupt page, Check_Panic, RecursivelyCheck (leaf / misplaced) |
| `manydbs_test.go` | Moderated parallel open/put |
| `concurrent_test.go` | Repeatable-read + generic R/W (simplified) |
| `simulation_test.go` / `simulation_no_freelist_sync_test.go` | Through 1000op/10p (+ nfs 100op_10p); 100op_100p and 10000op monsters `#[ignore]` |
| `internal/freelist/*_test.go` | Unit tests in `src/freelist.rs` including E2E happy path, SerDe across backends, `releaseRange` table |
| `internal/common/page_test.go` | `page_type_names`, `page_dump`, `merge_pgids` / `merge_pgids_quick` |
| `node_test.go` | Leaf read/write + split MinKeys/SinglePage via bucket fills |
| `db_whitebox_test.go` | PreLoadFreelist (+ allocate growth) |
| `cmd/bbolt/command/*_test.go` | Smoke: version/info/buckets/keys/get/check/compact/pages/inspect/stats + no-args failures |
| failpoint / dmflakey / powerfailure / surgeon / Windows-only / multi-GB | **Skipped** (environment or extreme runtime) |

**Still `#[ignore]` / skipped (with reason):**
- `TestFreelist_E2E_SerDe_AcrossImplementations` at `n=0xFFFF*2` — memory/slow
- `TestTx_TruncateBeforeWrite` — Rust `grow_size` stepping differs on Unix
- `TestDB_Close_PendingTx_RO` — close does not block on open read-only txs
- `TestBucket_Stats_Large` / full `TestDB_Put_VeryLarge` / `TestBucket_Delete_FreelistOverflow` — long-running (moderated variants run by default)
- Simulation monsters (`100op_100p`, `10000op_*`) — runtime
- `TestBucket_Get_Capacity` — Go slice `cap` (Rust returns owned `Vec`)
- `TestDBUnmap` / `TestMethodPage` — whitebox reflect / unexported API
- `TestOpen_MetaInitWriteError` — upstream pending
- `TestOpen_Size_Large` / Windows MaxSize mmap variants — multi-GB or OS-specific

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

## Performance vs Go bbolt

See **[BENCHMARKS.md](BENCHMARKS.md)** for an apples-to-apples comparison against `go.etcd.io/bbolt` v1.5.0 on this VM (same page size, freelist array, fsync defaults). Summary: Rust is **ahead** on sequential put (~1.5×), cursor scan (~1.3×), random get (~1.3×), and random put (~5×); `many_small_tx` stays fsync-bound (~0.8×); deletes still lag (~0.45×).

```sh
./benches/run_compare.sh
cargo run --release --bin bench_compare -- --dir /tmp/r --workload seq_put --n 100000
```

## Compatibility with upstream bbolt

**On-disk format (version 2)** — intended to match:

- Magic `0xED0CDAED`, FNV-1a-64 meta checksum, little-endian page header (16 bytes)
- Page types: meta (`0x04`), leaf (`0x02`), branch (`0x01`), freelist (`0x10`)
- Leaf element flags: `bucketLeafFlag = 0x01`
- Inline buckets: 16-byte `InBucket` header (`root`, `sequence`) plus an embedded page when `root == 0`
- Dual meta pages at pgid 0/1; commit writes meta for `txid % 2` after data pages
- Copy-on-write pages, single writer, serializable snapshots for readers
- Freelist may be omitted (`pgid = 0xffffffffffffffff`) when `NoFreelistSync` is set; reopen reconstructs free pages by scanning reachability from the root

### Go↔Rust interchange (oracle suite)

Automated cross-implementation tests live in `tests/cross_go_test.rs` and drive a small Go helper at `tests/go_oracle/` against **`go.etcd.io/bbolt` v1.5.0** (on-disk format v2; same family as current etcd-io/bbolt main).

**Requirements:** `go` on `PATH` (with network once for module/`GOTOOLCHAIN=auto` download) and `python3` for JSON equality.

```bash
# Builds tests/go_oracle/go-oracle on first run, then exercises Go↔Rust.
cargo test --test cross_go_test

# Or build the oracle alone:
cd tests/go_oracle && GOTOOLCHAIN=auto go build -o go-oracle .
./go-oracle write -o /tmp/t.db -scenario mixed -pagesize 4096
./go-oracle inspect -db /tmp/t.db
./go-oracle check -db /tmp/t.db
```

**Proven compatible (tests fail on divergence):**

| Check | Coverage |
| --- | --- |
| Fresh init bytes (page size 4096) | Rust empty DB == live Go init == `tests/fixtures/go_init.db` |
| Go write → Rust read | sample, nested, sequences, overflow, page-split, deletes/freelist, multi-tx, mixed; array + hashmap freelist |
| Rust write → Go read | same scenarios; Go `Check` + inspect tree match Rust |
| Round-trip | Go→Rust mutate/commit→Go read; Rust→Go mutate→Rust read |
| Compact / WriteTo | Go snapshot opens in Rust; Rust snapshot/`compact_into` opens in Go; inspect trees match |
| Check | Healthy files pass both Go and Rust `Check` |
| Behavior | Cursor lexicographic order, `ErrIncompatibleValue`, read-only tx rules, rollback, `NextSequence` persistence |

Static fixtures `tests/fixtures/go_init.db` / `go_sample.db` remain as smoke coverage; the oracle suite regenerates live DBs each run.

**Remaining holes (not claimed interchangeable here):**

- Windows flock / mmap MaxSize speculative growth (OS-specific)
- failpoint / dm-flakey / powerfailure injection
- Surgery / bench CLI internals
- Multi-GB `Open_Size_Large` stress
- Exact freelist page byte layout after complex free/allocate churn may differ between array vs hashmap backends (logical free set and user data still match; both sides open each other’s files)
- Live per-op TxStats counter instrumentation parity (API exists; not every spill path increments yet)

