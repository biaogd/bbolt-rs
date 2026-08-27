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
| Rustc | 1.83.0 (`lto = true`, `codegen-units = 1`, `RUSTFLAGS=-C target-cpu=native`) |
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
| seq_put | 871,325 | 1,202,815 | **1.38** | 115 | 83 | 16,777,216 | 11,542,528 | 100k keys, 32 B values, **fsync on** |
| random_get | 2,394,532 | 2,908,647 | **1.22** | 42 | 34 | 16,777,216 | 11,542,528 | 100k random gets after load |
| cursor_scan | 76,833,595 | 90,986,098 | **1.18** | 1.3 | 1.1 | 16,777,216 | 11,542,528 | full forward cursor |
| random_put | 4,797 | 49,535 | **10.3** | 20,847 | 2,019 | 16,777,216 | 11,542,528 | 100k scrambled keys, one txn |
| deletes | 2,836,231 | 4,003,235 | **1.41** | 18 | 12 | 16,777,216 | 11,563,008 | delete 50% of 100k (50k ops) |
| one_large_tx | 919,975 | 1,130,556 | **1.23** | 11 | 9 | 2,097,152 | 1,179,648 | 10k puts, one Update |
| many_small_tx | 11,956 | 9,578 | **0.80** | 836 | 1,044 | 2,097,152 | 1,191,936 | 10k Updates × 1 put |
| large_value | 26,695 | 54,791 | **2.05** | 375 | 183 | 139,927,552 | 123,150,336 | 10k × 10 KiB values |
| seq_put_nosync | 1,030,058 | 1,452,746 | **1.41** | 97 | 69 | 16,777,216 | 11,542,528 | same as seq_put with **NoSync=true** |

Raw TSV from the harness: `benches/results.tsv` (also regenerated under `bench-data/` by the script).

## What changed (perf pass)

Previously Rust collapsed on bulk sequential put (~0.027×) and cursor scan (~0.036×) because of superlinear spill work and per-step page/key copies. This pass:

1. **Spill / seq put** — `inodes_size_less_than` early-exit (Go `sizeLessThan`), O(n) single-pass `split_node`, sequential inode **append** fast path, `VecDeque` inodes (O(1) front/back remove).
2. **Zero-copy reads** — transactions pin `Arc<Mmap>`; page access is a slice into the mapping (no 4 KiB `to_vec` on every cursor/search step).
3. **Cursor** — Go-style key/value views with a **RefCell-free leaf Next** hot path; unchecked `leaf_at` / `branch_at`.
4. **Get** — stackless `has_value` matching Go `Get(key) != nil` without allocating value bytes.
5. **Deletes** — leaf delete hint for sequential keys + `node_del_at` / `pop_front` (was ~0.45×, now ~1.4×).

## Reading the numbers

- **CPU-bound wins**: seq_put (~1.4×), cursor_scan (~1.2×), random_get (~1.2×), deletes (~1.4×), random_put (~10×), large_value (~2×).
- **many_small_tx** stays near parity (~0.80×) because both sides fsync every one-key Update.
- **File sizes differ** after the same logical load (Go often lands on larger power-of-two growth steps). Cross-impl oracle tests cover data interchange separately.

## Fairness notes

- Same N, key/value sizes, page size 4096, freelist **array**, default fill percent.
- `NoSync` appears only on the explicitly labeled `seq_put_nosync` row.
- Disk is the workspace overlay (VM disk), not `/dev/shm`.
- Fsync defaults are identical on both sides for non-`_nosync` rows.
