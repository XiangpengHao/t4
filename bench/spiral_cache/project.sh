#!/usr/bin/env bash
# Runs on the dev box: Spiral's local I/O cache, Kismet against t4, on a Spiral project's
# tables read as desktop Studio reads them (doc/10-spiral-integration.md, plan step 2):
# spiraldb's engine-bench with `--spiral-project`, DataFusion over the project's Arbor tables
# through an embedded scandal and its disk cache, at a desktop's settings (a 20 GiB budget),
# each cache in its own directory on the same RAID0 of all the box's local SSDs, OS defaults
# throughout.
#
#   bench/spiral_cache/project.sh <results dir>
#
# For each benchmark, a process a pass, as run.sh runs them, the caches taking turns:
#
#   cold     the cache empty: every byte from the project's storage
#   disk-N   the page cache dropped before each query
#   warm-N   the page cache left as the queries leave it
#
# WRITE names the benchmarks whose tables to write into the project first, from the Parquet
# engine-bench generates (TPC-H) or downloads (ClickBench), in place of tables of the same names.
#
# PROJECT is the Spiral project; BENCHMARKS the benchmarks (default tpch clickbench); SCALE
# TPC-H's scale factor (default 100); CACHES the caches (default kismet t4), which may name none,
# reads straight from the project's storage; DISK_PASSES and WARM_PASSES how many of each (default
# 2 and 3); CAPACITY, if set, the caches' budget in bytes in place of a desktop's, which
# settings.txt records. TPC-H leaves out q15, whose view the Spiral
# catalog refuses. The deployment's sign-in comes from ~/spiral.env, which drive_gcp.sh writes.
#
# Writes <benchmark>-<cache>-<pass>.jsonl, each query run's wall time and CPU (engine-bench's
# --io-usage-out), with .log and .json beside it; .diskstats and .netdev, /proc/diskstats's lines
# for the RAID and its SSDs and /proc/net/dev's for the NIC, before each query run and once more
# at the end; .iostat, iostat's each second; write-<benchmark>.log; drives.txt and build.log;
# and done at the end.
set -euo pipefail

OUT=${1:?results dir}
PROJECT=${PROJECT:?the Spiral project}
SPIRALDB=${SPIRALDB:-$HOME/spiraldb}
read -r -a BENCHMARKS <<< "${BENCHMARKS:-tpch clickbench}"
SCALE=${SCALE:-100}
read -r -a CACHES <<< "${CACHES:-kismet t4}"
DISK_PASSES=${DISK_PASSES:-2}
WARM_PASSES=${WARM_PASSES:-3}
CAPACITY=${CAPACITY:-}
read -r -a WRITE <<< "${WRITE:-}"
MOUNT=/mnt/raid
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)

# The boot's startup script formats and mounts each local SSD; the RAID takes them once it ends.
for _ in $(seq 120); do
    [ "$(systemctl show -p ActiveState --value google-startup-scripts.service)" = activating ] || break
    sleep 5
done
"$(dirname "$0")/../cold_reads/raid.sh" "$OUT"

# spiraldb's build: a C toolchain, protoc and its well-known types; Rust through rustup, which takes the toolchain
# spiraldb's rust-toolchain.toml pins. sysstat for iostat.
sudo apt-get update -q && sudo apt-get install -y -q build-essential pkg-config protobuf-compiler libprotobuf-dev clang cmake libssl-dev sysstat > /dev/null
command -v rustup >/dev/null || curl -sSf https://sh.rustup.rs | sh -s -- -y -q --default-toolchain none
# shellcheck disable=SC1091
source "$HOME/.cargo/env"
# Touched so that the build log shows engine-bench built from this tree on a box that built it
# before too.
touch "$SPIRALDB/engine/benches/engine-bench/src/main.rs"
(cd "$SPIRALDB" && timeout -k 30 3600 cargo build --release --locked -p engine-bench --bin engine-bench) 2>&1 | tee "$OUT/build.log"
grep -q 'Compiling engine-bench' "$OUT/build.log" || { echo "engine-bench was not built from this tree" >&2; exit 1; }
BIN=$SPIRALDB/target/release/engine-bench
# shellcheck disable=SC1091
set -a; source "$HOME/spiral.env"; set +a
echo "cache budget: ${CAPACITY:-the desktop default, 20 GiB}" > "$OUT/settings.txt"

# bench_args <benchmark>: engine-bench's arguments for it.
bench_args() {
    case $1 in
        tpch) echo "tpch --opt scale-factor=$SCALE -e 15" ;;
        clickbench) echo "clickbench" ;;
        *) echo "no benchmark $1" >&2; return 1 ;;
    esac
}

for bench in "${WRITE[@]}"; do
    read -r -a args <<< "$(bench_args "$bench")"
    (cd "$SPIRALDB" && timeout -k 30 14400 "$BIN" "${args[@]}" --spiral-project "$PROJECT" --write-project) \
        > "$OUT/write-$bench.log" 2>&1
done

DROP='sync && echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null'
DRIVES=$(lsblk -dno NAME | grep -E '^(md|nvme)' | tr '\n' ' ')

# run_pass <benchmark> <cache> <pass> [<command before each query>]
run_pass() {
    local bench=$1 cache=$2 pass=$3 before=${4:-}
    local name=$bench-$cache-$pass args
    read -r -a args <<< "$(bench_args "$bench")"
    [ -n "$CAPACITY" ] && args+=(--io-cache-capacity-bytes "$CAPACITY")
    local snapshot='stamp="# $(date +%s.%N)"; { echo "$stamp"; grep -E " (md|nvme)[0-9]" /proc/diskstats; } >> '"$OUT/$name.diskstats"'; { echo "$stamp"; grep -E "^ *(ens|eth)" /proc/net/dev; } >> '"$OUT/$name.netdev"
    # shellcheck disable=SC2086
    iostat -dxy -t $DRIVES 1 > "$OUT/$name.iostat" &
    local iostat=$!
    # A pass is a few minutes; an hour is a hang. A cold query can take minutes, past
    # engine-bench's default of 60 s, which would drop it from both caches' results.
    (cd "$SPIRALDB" && timeout -k 30 3600 "$BIN" "${args[@]}" -i 1 --hide-progress-bar --sql-timeout-secs 900 \
        --spiral-project "$PROJECT" --io-cache "$cache" --io-cache-dir "$MOUNT/cache/$bench-$cache" \
        --io-usage-out "$OUT/$name.jsonl" --io-usage-label "$pass" -o "$OUT/$name.json" \
        --before-each-run "${before:+$before && }$snapshot") > "$OUT/$name.log" 2>&1 ||
        echo "$name failed: exit $?" | tee -a "$OUT/$name.log" "$OUT/failures.txt"
    sh -c "$snapshot"
    kill "$iostat" 2>/dev/null || true; wait "$iostat" 2>/dev/null || true
}

sudo rm -rf "$MOUNT/cache"
mkdir -p "$MOUNT/cache"
for bench in "${BENCHMARKS[@]}"; do
    for cache in "${CACHES[@]}"; do run_pass "$bench" "$cache" cold "$DROP"; done
    for n in $(seq "$DISK_PASSES"); do
        for cache in "${CACHES[@]}"; do run_pass "$bench" "$cache" "disk-$n" "$DROP"; done
    done
    for n in $(seq "$WARM_PASSES"); do
        for cache in "${CACHES[@]}"; do run_pass "$bench" "$cache" "warm-$n"; done
    done
done
du -sh "$MOUNT/cache"/* > "$OUT/cache-sizes.txt" 2>&1 || true
touch "$OUT/done"
