#!/usr/bin/env bash
# Run apples-to-apples Go vs Rust bbolt workloads; print median results.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GO_BIN="${ROOT}/benches/go/go-bench"
RUST_BIN="${ROOT}/target/release/bench_compare"
DATA_ROOT="${BENCH_DIR:-${ROOT}/bench-data}"
TRIALS="${TRIALS:-5}"
WARMUP="${WARMUP:-1}"
GOTOOLCHAIN="${GOTOOLCHAIN:-auto}"
export GOTOOLCHAIN

mkdir -p "${DATA_ROOT}"
df -T "${DATA_ROOT}" | tee "${DATA_ROOT}/disk.txt"

echo "Building harnesses..."
(cd "${ROOT}/benches/go" && go build -o go-bench .)
(cd "${ROOT}" && cargo build --release --bin bench_compare)

run_one() {
  local impl="$1" workload="$2" dir="$3"
  shift 3
  if [[ "${impl}" == "go" ]]; then
    "${GO_BIN}" -dir "${dir}" -workload "${workload}" "$@"
  else
    local -a args=(--dir "${dir}" --workload "${workload}")
    while [[ $# -gt 0 ]]; do
      case "$1" in
        -n|-key-size|-value-size|-page-size|-batch-size|-delete-frac|-seed)
          args+=("${1/#-/--}" "$2")
          shift 2
          ;;
        -no-sync)
          args+=(--no-sync)
          shift
          ;;
        *)
          echo "unknown arg: $1" >&2
          exit 1
          ;;
      esac
    done
    "${RUST_BIN}" "${args[@]}"
  fi
}

# Collect elapsed_ns values; print median ops_sec and filesize from median trial.
median_run() {
  local impl="$1" workload="$2"
  shift 2
  local base="${DATA_ROOT}/${impl}_${workload}"
  rm -rf "${base}"
  mkdir -p "${base}"

  for ((w = 0; w < WARMUP; w++)); do
    run_one "${impl}" "${workload}" "${base}/warmup_${w}" "$@" >/dev/null
  done

  local -a lines=()
  for ((t = 0; t < TRIALS; t++)); do
    local out
    out="$(run_one "${impl}" "${workload}" "${base}/trial_${t}" "$@")"
    echo "${out}" | tee -a "${DATA_ROOT}/raw.log" >/dev/null
    lines+=("${out}")
  done

  # Sort by elapsed_ns and pick middle
  local median
  median="$(printf '%s\n' "${lines[@]}" | awk '{
    # elapsed_ns= field
    for (i=1;i<=NF;i++) if ($i ~ /^elapsed_ns=/) { split($i,a,"="); print a[2], $0 }
  }' | sort -n | awk -v n="${TRIALS}" 'NR==int((n+1)/2){ $1=""; sub(/^ /,""); print }')"
  echo "${median}"
}

echo "=== hardware ==="
{
  echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "nproc: $(nproc)"
  echo "mem_total_kb: $(awk '/MemTotal/ {print $2}' /proc/meminfo)"
  echo "rustc: $(rustc --version)"
  echo "go: $(go version)"
  echo "cargo: $(cargo --version)"
  echo "bbolt_go: go.etcd.io/bbolt v1.5.0"
  echo "bbolt_rust: workspace crate"
  echo "bench_dir: ${DATA_ROOT}"
  df -T "${DATA_ROOT}" | tail -1
} | tee "${DATA_ROOT}/env.txt"

RESULTS="${DATA_ROOT}/results.tsv"
echo -e "workload\tgo_ops_sec\trust_ops_sec\tratio_rust_over_go\tgo_elapsed_ms\trust_elapsed_ms\tgo_filesize\trust_filesize\tnosync\tnotes" >"${RESULTS}"

record_pair() {
  local label="$1"
  local workload="$2"
  shift 2
  echo "--- ${label} (workload=${workload}) ---"
  local go_line rust_line
  go_line="$(median_run go "${workload}" "$@")"
  rust_line="$(median_run rust "${workload}" "$@")"
  # Rewrite workload field in output lines to the label for the table
  go_line="$(echo "${go_line}" | sed "s/workload=${workload}/workload=${label}/")"
  rust_line="$(echo "${rust_line}" | sed "s/workload=${workload}/workload=${label}/")"
  echo "GO   ${go_line}"
  echo "RUST ${rust_line}"

  python3 - "${label}" "${go_line}" "${rust_line}" "${RESULTS}" <<'PY'
import sys
name, go, rust, path = sys.argv[1:5]

def parse(line):
    d = {}
    for tok in line.split():
        if "=" in tok:
            k, v = tok.split("=", 1)
            d[k] = v
    return d

g, r = parse(go), parse(rust)
g_ops, r_ops = float(g["ops_sec"]), float(r["ops_sec"])
ratio = r_ops / g_ops if g_ops else float("nan")
g_ms = int(g["elapsed_ns"]) / 1e6
r_ms = int(r["elapsed_ns"]) / 1e6
nosync = g.get("nosync", "false")
notes = f"n={g.get('n')} key={g.get('key_size')} val={g.get('value_size')}"
with open(path, "a") as f:
    f.write(f"{name}\t{g_ops:.2f}\t{r_ops:.2f}\t{ratio:.3f}\t{g_ms:.2f}\t{r_ms:.2f}\t{g['filesize']}\t{r['filesize']}\t{nosync}\t{notes}\n")
print(f"ratio Rust/Go ops/sec = {ratio:.3f}")
PY
}

# Default small-value suite
COMMON=(-n 100000 -key-size 8 -value-size 32 -page-size 4096 -seed 1)

record_pair seq_put seq_put "${COMMON[@]}"
record_pair random_get random_get "${COMMON[@]}"
record_pair cursor_scan cursor_scan "${COMMON[@]}"
record_pair random_put random_put "${COMMON[@]}"
record_pair deletes deletes "${COMMON[@]}" -delete-frac 0.5

# Same N for fair large-vs-many comparison
TXN_N=(-n 10000 -key-size 8 -value-size 32 -page-size 4096 -seed 1)
record_pair one_large_tx one_large_tx "${TXN_N[@]}"
record_pair many_small_tx many_small_tx "${TXN_N[@]}" -batch-size 1

# Overflow path
record_pair large_value large_value -n 10000 -key-size 8 -value-size 10240 -page-size 4096 -seed 1

# Clearly labeled NoSync variant (seq_put only)
record_pair seq_put_nosync seq_put "${COMMON[@]}" -no-sync

echo
echo "Wrote ${RESULTS}"
column -t -s $'\t' "${RESULTS}" || cat "${RESULTS}"
