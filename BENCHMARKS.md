# Benchmarks: Rust bbolt vs Go bbolt v1.5.0

Apples-to-apples workloads on the **same cloud VM**, same options, same on-disk path.

| Side | Implementation |
| --- | --- |
| Go | `go.etcd.io/bbolt` **v1.5.0** (`benches/go`) |
| Rust | this crate, `--release` (`bench_compare` binary) |

## Environment (captured run)

| Item | Value |
| --- | --- |
| Date (UTC) | 2026-08-27 |
| CPUs | 4 (`nproc`) |
| RAM | ~16 GiB |
| Disk path | `/workspace/bench-data` on **overlayfs** backed by the VM disk image (**not** tmpfs / `shm`) |
| Page size | 4096 |
| Freelist | **array** (both) |
| `NoSync` / `NoGrowSync` | false unless row labeled `_nosync` |
| Fill percent | library defaults (0.5) |
| Rustc | 1.83.0 (`lto = true`, `codegen-units = 1`, `RUSTFLAGS=-C target-cpu=native`, jemalloc) |
| Go | go1.24.0 host / `GOTOOLCHAIN=auto` pulls toolchain required by bbolt v1.5.0 |
| Method | 1 warmup + **5 trials**, **median** by `elapsed_ns` |

## How to run

```bash
# Builds both harnesses, runs all workloads, writes bench-data/results.tsv
./benches/run_compare.sh

# Or single shot:
cargo build --release --bin bench_compare
(cd benches/go && GOTOOLCHAIN=auto go build -o go-bench .)

./benches/go/go-bench -dir /tmp/g -workload seq_put -n 100000 -key-size 8 -value-size 32
./target/release/bench_compare --dir /tmp/r --workload seq_put --n 100000 --key-size 8 --value-size 32
```

Optional env: `TRIALS=5`, `WARMUP=1`, `BENCH_DIR=/path/to/dir`, `RUSTFLAGS` (script defaults to `-C target-cpu=native`).

Workloads time **only the measured phase** (prepare/load for gets/scans/deletes is outside the timer). Keys are 8-byte big-endian counters (left-padded to `key-size`); values are deterministic bytes of `value-size`.

## Results (ops/sec, median)

Ratio = **Rust / Go** (higher is better for Rust; `< 1` means Rust is slower).

| Workload | Go ops/s | Rust ops/s | Ratio | Go ms | Rust ms | Go file B | Rust file B | Notes |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| seq_put | 861,735 | 2,323,958 | **2.70** | 116 | 43 | 16,777,216 | 11,542,528 | 100k keys, 32 B values, **fsync on** |
| random_get | 2,397,881 | 3,174,079 | **1.32** | 42 | 32 | 16,777,216 | 11,542,528 | 100k random gets after load |
| cursor_scan | 75,003,000 | 97,523,964 | **1.30** | 1.3 | 1.0 | 16,777,216 | 11,542,528 | full forward cursor |
| random_put | 4,897 | 51,331 | **10.48** | 20,422 | 1,948 | 16,777,216 | 11,542,528 | 100k scrambled keys, one txn |
| deletes | 2,901,479 | 6,195,461 | **2.13** | 17 | 8 | 16,777,216 | 11,563,008 | delete 50% of 100k (50k ops) |
| one_large_tx | 916,612 | 1,636,660 | **1.79** | 11 | 6 | 2,097,152 | 1,179,648 | 10k puts, one Update |
| many_small_tx | 11,875 | 10,973 | **0.92** | 842 | 911 | 2,097,152 | 1,191,936 | 10k Updates × 1 put |
| large_value | 25,965 | 52,295 | **2.01** | 385 | 191 | 139,927,552 | 123,150,336 | 10k × 10 KiB values |
| seq_put_nosync | 1,034,216 | 3,214,051 | **3.11** | 97 | 31 | 16,777,216 | 11,542,528 | same as seq_put with **NoSync=true** |

Raw TSV from the harness: `benches/results.tsv` (also regenerated under `bench-data/` by the script).

## What changed (conventional perf)

Wins come from ordinary layout/alloc and algorithm work — not raw-pointer inode types or cached `*const u8` on the cursor:

1. **Spill / seq put** — `inodes_size_less_than` early-exit (Go `sizeLessThan`), O(n) single-pass `split_node`, sequential inode **append** + `put_hint`, `VecDeque` inodes.
2. **Node storage** — dense `Vec`-backed `NodeMap` indexed by monotonic `NodeId` (no `HashMap` on the node hot path).
3. **Safe mmap reads** — transactions pin `Arc<Mmap>`; `page_bytes` / `with_page` return `&[u8]` into the mapping.
4. **Cursor views** — Go-style key/value via **byte offsets** into the pinned mmap (or owned buffers for materialized/inline nodes); leaf Next + sibling advance without RefCell.
5. **Get** — stackless `has_value` (`&self`) matching Go `Get(key) != nil` without allocating value bytes.
6. **Deletes** — leaf delete hint + O(1) front/back inode remove.
7. **Release build** — LTO, `codegen-units = 1`, `target-cpu=native`, jemalloc (non-MSVC).

Page-element decoding still uses the existing small `unsafe` blocks in `leaf_at` / `branch_at` only.

## Reading the numbers

- **CPU-bound**: seq_put (~2.7×), deletes (~2.1×), cursor_scan (~1.3×), random_get (~1.3×), random_put (~10×), large_value (~2×).
- **many_small_tx** stays near parity (~0.92×) — both sides fsync every one-key Update.
- **File sizes differ** after the same logical load (Go often lands on larger power-of-two growth steps). Cross-impl oracle tests cover data interchange separately.

## Fairness notes

- Same N, key/value sizes, page size 4096, freelist **array**, default fill percent.
- Same fsync policy on both sides (`NoSync` only on `_nosync` rows).
- Cursor scan harness still materializes key views each step (equivalent to Go using the returned `k`).
- Get workload uses real lookups (`has_value` / Go `Get != nil`), not a stub.
