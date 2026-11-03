#!/usr/bin/env bash
# Runs on the dev box: Spiral's local I/O cache, Kismet against t4, as a
# desktop host runs it (doc/10-spiral-integration.md, plan step 2). ClickBench's
# 100 Vortex files with 2 MiB dictionaries, read from S3 through spiral-io's
# managed stores by Spiral's engine (spiraldb's engine-bench, `--data-uri`),
# each cache in its own directory on the same RAID0 of all the box's
# instance-store SSDs, OS defaults throughout.
#
#   bench/spiral_cache/run.sh <results dir>
#
# Each pass runs every query engine-bench plans natively, in one process, as a
# desktop host runs queries; a process a pass, so that each pass starts its
# cache from what the last one kept (t4's snapshot, Kismet's files):
#
#   cold     the cache empty: every byte from S3
#   disk-N   the page cache dropped before each query: from the cache's drive
#   warm-N   the page cache left as the queries leave it, as a desktop's is;
#            warm-1 fills it, from the passes before
#
# The caches take turns within each pass. Writes <cache>-<pass>.jsonl, each
# query run's wall time, CPU, GETs and slowest GET (engine-bench's
# --io-usage-out), with <cache>-<pass>.log and .json beside it, and the drives'
# work: <cache>-<pass>.diskstats, /proc/diskstats's lines for the RAID and its
# SSDs before each query run and once more at the end, and .iostat, iostat's
# each second; drives.txt and build.log; and done at the end.
#
# SPIRALDB is the spiraldb checkout to build (default ~/spiraldb); DATA the S3
# prefix; DISK_PASSES and WARM_PASSES how many of each (default 5 and 6);
# EXCLUDE the queries left out (default 36,37,38,40,41: they filter on
# EventDate, which engine-bench's plans take as vortex-bench's date extension
# type, and to_vortex wrote these files with plain dates).
# CACHES the caches (default kismet t4).
# PERF=1 adds a disk-perf and a warm-perf pass, each under perf record, for
# <cache>-<pass>.threads.txt (each thread's CPU time and its busiest second)
# and .symbols.txt (each thread's CPU by function); their timings are not the
# other passes' to compare with.
# S3's credentials come from ~/s3.env, read before each pass: drive.sh writes
# it, and writes it again as its session's credentials renew.
set -euo pipefail

OUT=${1:?results dir}
SPIRALDB=${SPIRALDB:-$HOME/spiraldb}
DATA=${DATA:-s3://t4-bench-xiangpeng-use1/clickbench/partitioned-vortex-dict2m}
DISK_PASSES=${DISK_PASSES:-5}
WARM_PASSES=${WARM_PASSES:-6}
EXCLUDE=${EXCLUDE:-36,37,38,40,41}
PERF=${PERF:-0}
read -r -a CACHES <<< "${CACHES:-kismet t4}"
MOUNT=/mnt/raid
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)

"$(dirname "$0")/../cold_reads/raid.sh" "$OUT"

# spiraldb's build: a C toolchain and protoc; Rust through rustup, which takes
# the toolchain spiraldb's rust-toolchain.toml pins. sysstat for iostat; perf
# for the kernel running.
packages=(build-essential pkg-config protobuf-compiler clang cmake libssl-dev sysstat)
[ "$PERF" = 1 ] && packages+=(linux-tools-common "linux-tools-$(uname -r)")
sudo apt-get update -q && sudo apt-get install -y -q "${packages[@]}" > /dev/null
command -v rustup >/dev/null || curl -sSf https://sh.rustup.rs | sh -s -- -y -q --default-toolchain none
# shellcheck disable=SC1091
source "$HOME/.cargo/env"

(cd "$SPIRALDB" && timeout -k 30 3600 cargo build --release --locked -p engine-bench --bin engine-bench) 2>&1 | tee "$OUT/build.log"
grep -q 'Compiling engine-bench' "$OUT/build.log" || { echo "engine-bench was not built from this tree" >&2; exit 1; }
BIN=$SPIRALDB/target/release/engine-bench

DROP='sync && echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null'
DRIVES=$(lsblk -dno NAME | grep -E '^(md|nvme)' | tr '\n' ' ')
if [ "$PERF" = 1 ]; then
    sudo sysctl -q -w kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0
    # perf's ring buffers count against the user's locked memory, which t4's
    # io_uring needs too: under the default 8 MiB, t4 fails to start.
    sudo prlimit --pid $$ --memlock=unlimited:unlimited
    mkdir -p "$HOME/perf"
fi

# perf_report <name>: from ~/perf/<name>.perf.data, sampled at 499 Hz, each
# thread's CPU seconds, its busiest second's share of a CPU and how many
# seconds it was over 90% busy, busiest first; the process's CPU by function,
# then that of the busiest four threads outside the engine's own pool
# (vortex-cpu-N), where the caches' I/O runs. The data stays on the box.
perf_report() {
    local data=$HOME/perf/$1.perf.data
    echo "tid comm cpu_s busiest_second seconds_over_90%" > "$OUT/$1.threads.txt"
    perf script -i "$data" -F comm,tid,time 2>/dev/null | awk -v hz=499 '
        { t = $NF; sub(/:$/, "", t); tid = $(NF - 1); comm = $1
          for (i = 2; i < NF - 1; i++) comm = comm "_" $i
          name[tid] = comm; samples[tid]++; second[tid " " int(t)]++ }
        END { for (k in second) { split(k, key, " ")
                  if (second[k] > busiest[key[1]]) busiest[key[1]] = second[k]
                  if (second[k] >= 0.9 * hz) over[key[1]]++ }
              for (tid in samples)
                  printf "%s %s %.2f %.2f %d\n", tid, name[tid], samples[tid] / hz, busiest[tid] / hz, over[tid] }' |
        sort -k3 -nr | awk 'NR <= 60' >> "$OUT/$1.threads.txt"
    perf report -i "$data" --no-children --sort sym --stdio --percent-limit 0.5 2>/dev/null > "$OUT/$1.symbols.txt"
    awk 'NR > 1 && $2 !~ /^vortex-cpu/ { print $1 }' "$OUT/$1.threads.txt" | awk 'NR <= 4' | while read -r tid; do
        echo "== thread $tid"
        perf report -i "$data" --no-children --tid "$tid" --percentage relative --sort sym --stdio --percent-limit 1 2>/dev/null | grep -v -e '^#' -e '^$'
    done >> "$OUT/$1.symbols.txt"
}

# run_pass <cache> <pass> [<command before each query>]
run_pass() {
    local cache=$1 pass=$2 before=${3:-}
    local name=$cache-$pass
    local snapshot='{ echo "# $(date +%s.%N)"; grep -E " (md|nvme)[0-9]" /proc/diskstats; } >> '"$OUT/$name.diskstats"
    local flags=(clickbench --formats vortex -i 1 --hide-progress-bar -e "$EXCLUDE"
        --data-uri "$DATA" --io-cache "$cache" --io-cache-dir "$MOUNT/cache/$cache"
        --io-usage-out "$OUT/$name.jsonl" --io-usage-label "$pass" -o "$OUT/$name.json"
        --before-each-run "${before:+$before && }$snapshot")
    local run=("$BIN" "${flags[@]}")
    [[ $pass == *-perf ]] && run=(perf record -q -e cpu-clock -F 499 -o "$HOME/perf/$name.perf.data" -- "${run[@]}")
    # shellcheck disable=SC1091
    set -a; source "$HOME/s3.env"; set +a
    # shellcheck disable=SC2086
    iostat -dxy -t $DRIVES 1 > "$OUT/$name.iostat" &
    local iostat=$!
    # A pass is a minute or two; twenty is a hang.
    timeout -k 30 1200 "${run[@]}" > "$OUT/$name.log" 2>&1 ||
        echo "$name failed: exit $?" | tee -a "$OUT/$name.log" "$OUT/failures.txt"
    sh -c "$snapshot"
    kill "$iostat" 2>/dev/null || true; wait "$iostat" 2>/dev/null || true
    if [[ $pass == *-perf ]]; then
        perf_report "$name" || echo "$name: no perf report" | tee -a "$OUT/failures.txt"
    fi
}

sudo rm -rf "$MOUNT/cache"
mkdir -p "$MOUNT/cache"
for cache in "${CACHES[@]}"; do run_pass "$cache" cold "$DROP"; done
for n in $(seq "$DISK_PASSES"); do
    for cache in "${CACHES[@]}"; do run_pass "$cache" "disk-$n" "$DROP"; done
done
if [ "$PERF" = 1 ]; then
    for cache in "${CACHES[@]}"; do run_pass "$cache" disk-perf "$DROP"; done
fi
for n in $(seq "$WARM_PASSES"); do
    for cache in "${CACHES[@]}"; do run_pass "$cache" "warm-$n"; done
done
if [ "$PERF" = 1 ]; then
    for cache in "${CACHES[@]}"; do run_pass "$cache" warm-perf; done
fi
du -sh "$MOUNT/cache"/* > "$OUT/cache-sizes.txt" 2>&1 || true
touch "$OUT/done"
