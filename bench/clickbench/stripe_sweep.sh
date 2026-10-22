#!/usr/bin/env bash
# Sweeps how many SSDs t4 stripes its disk tier over (doc/04-multi-device.md)
# on the dev box (bench/devbox/), against the page cache: TRACE's calls
# replayed with nothing between them, every block of t4's on disk, and
# the local copy of the file, all in the page cache. Each run's pass 1 is
# the warm one; pass 0 fills the cache from the local copy, not from GCS.
# Writes one CSV a run to ~/t4-results/stripe-sweep-<time>/.
#
# SSD_COUNTS (default "1 2 3 4 6 8 12") and QUEUE_DEPTHS (default "64
# 256") are swept; COPIES (default "1 4") runs each query's tasks that
# many times at once, the page cache and t4 on every SSD alike. The
# binaries run on mimalloc (bench/Cargo.toml).
set -euo pipefail

cd "$(dirname "$0")/../.."
TRACE=${TRACE:-$HOME/t4-results/trace-single.csv}
SSD_COUNTS=${SSD_COUNTS:-1 2 3 4 6 8 12}
QUEUE_DEPTHS=${QUEUE_DEPTHS:-64 256}
COPIES=${COPIES:-1 4}
mounts=(/mnt/t4/[0-9]*)
LOCAL=${mounts[0]}/data/hits.parquet
OUT=${OUT:-$HOME/t4-results/stripe-sweep-$(date +%Y%m%d-%H%M%S)}

cargo build --release -p t4-bench --bin replay_reads
mkdir -p "$OUT" "$(dirname "$LOCAL")"
[ -e "$LOCAL" ] || gcloud storage cp gs://t4-bench-us-east4/clickbench/hits.parquet "$LOCAL"
drives=()
for ssd in "${mounts[@]}"; do drives+=(--drive-dir "$ssd"); done

# Replays TRACE into $OUT/$1.csv, after the page cache is dropped.
replay() {
    local name=$1
    shift
    rm -f /mnt/t4/*/t4.cache
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null
    echo "== $name"
    target/release/replay_reads --trace "$TRACE" --location "$LOCAL" --passes 2 \
        "${drives[@]}" --out "$OUT/$name.csv" "$@" | grep -E "^pass 1"
    rm -f /mnt/t4/*/t4.cache
}

for copies in $COPIES; do
    replay "page-cache-x$copies" --copies "$copies"
done
for queue_depth in $QUEUE_DEPTHS; do
    for count in $SSD_COUNTS; do
        files=()
        for ssd in "${mounts[@]:0:$count}"; do files+=(--cache-path "$ssd/t4.cache"); done
        replay "t4-${count}ssd-qd$queue_depth-x1" --cache-policy disk --cache-memory-mib 1024 \
            --cache-disk-gib 16 --cache-queue-depth "$queue_depth" "${files[@]}"
    done
done
all=()
for ssd in "${mounts[@]}"; do all+=(--cache-path "$ssd/t4.cache"); done
for copies in $COPIES; do
    [ "$copies" = 1 ] && continue
    for queue_depth in $QUEUE_DEPTHS; do
        replay "t4-${#mounts[@]}ssd-qd$queue_depth-x$copies" --copies "$copies" \
            --cache-policy disk --cache-memory-mib 1024 --cache-disk-gib 16 \
            --cache-queue-depth "$queue_depth" "${all[@]}"
    done
done
echo "results in $OUT"
