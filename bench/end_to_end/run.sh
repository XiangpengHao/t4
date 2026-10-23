#!/usr/bin/env bash
# Runs a workload's queries end to end, DataFusion reading S3, each query in
# sessions of its own that start cold (`run_queries --cold-runs`), and
# writes one CSV a store to ~/t4-results/end-to-end-<workload>-<format>-<time>/:
#
#   s3.csv         straight from S3: each run cold, as no cache keeps a byte
#   t4-none.csv    through t4, keeping no block: what going through t4 costs,
#                  its whole-block GETs and its copies, with no tier's work
#   t4-memory.csv  through t4, every block in memory: each run cold from S3,
#                  then warm from memory
#   t4-disk.csv    through t4, every block on the SSDs: cold, then warm
#                  from the drives
#
# Each row is a session's setup (the tables registered, their footers read)
# or a query's run: its time; the GETs, HEADs and list pages sent to S3 and
# their cost; the machine's time priced at DOLLARS_PER_HOUR, if given; and,
# through t4, each GET's and disk read's latency (p50, p99, max). A query's
# cold times over its runs are its spread; its cold time less its warm in
# memory is what it waits on S3 at most, all any read policy can win back.
#
# A run should take under five minutes: pick QUERIES so it does, from a
# first run of RUNS=1.
#
#   WORKLOAD   clickbench (default; its 100 files) or tpch (scale factor SF,
#              default 100)
#   FORMAT     parquet (default) or vortex
#   STORES     which of the above (default "s3 t4-none t4-memory t4-disk")
#   RUNS       cold runs of each query (default 5)
#   QUERIES    the queries' numbers, from 0 (default all)
#   MEMORY_GIB t4's memory tier (default 128)
#   DISK_GIB   t4's cache file on each SSD (default 64)
#   DOLLARS_PER_HOUR  what the box costs
#   BUCKET     where prepare.sh put the datasets
#   MOUNTS     where the SSDs are mounted, one a directory (default
#              /mnt/t4, as bench/devbox/startup.sh mounts them)
#
# S3's credentials and region come from the environment, as object_store's
# `AmazonS3Builder::from_env` reads them (AWS_REGION, AWS_ACCESS_KEY_ID, ...).
# Run it in the dev shell (`nix develop -c`), for cargo. Arguments go to
# every run_queries.
set -euo pipefail

cd "$(dirname "$0")/../.."
WORKLOAD=${WORKLOAD:-clickbench}
FORMAT=${FORMAT:-parquet}
STORES=${STORES:-s3 t4-none t4-memory t4-disk}
RUNS=${RUNS:-5}
QUERIES=${QUERIES:-}
SF=${SF:-100}
MEMORY_GIB=${MEMORY_GIB:-128}
DISK_GIB=${DISK_GIB:-64}
DOLLARS_PER_HOUR=${DOLLARS_PER_HOUR:-}
BUCKET=${BUCKET:-s3://t4-bench-xiangpeng-use1}
MOUNTS=${MOUNTS:-/mnt/t4}

tables=()
case "$WORKLOAD/$FORMAT" in
clickbench/parquet) tables=(--table "hits=$BUCKET/clickbench/partitioned/") ;;
clickbench/vortex) tables=(--table "hits=$BUCKET/clickbench/partitioned-vortex/") ;;
tpch/parquet | tpch/vortex)
    for table in region nation supplier customer part partsupp orders lineitem; do
        tables+=(--table "$table=$BUCKET/tpch/sf$SF/$FORMAT/$table/")
    done
    ;;
*)
    echo "WORKLOAD is clickbench or tpch, FORMAT parquet or vortex, not $WORKLOAD/$FORMAT" >&2
    exit 1
    ;;
esac
common=(--format "$FORMAT" "${tables[@]}" --queries "bench/$WORKLOAD/queries.sql"
    --cold-runs "$RUNS")
for query in $QUERIES; do
    common+=(--query "$query")
done
[ -z "$DOLLARS_PER_HOUR" ] || common+=(--dollars-per-hour "$DOLLARS_PER_HOUR")

mounts=("$MOUNTS"/[0-9]*)
drives=()
disk_files=()
for mount in "${mounts[@]}"; do
    drives+=(--drive-dir "$mount")
    disk_files+=(--cache-path "$mount/t4.cache")
done
OUT=${OUT:-$HOME/t4-results/end-to-end-$WORKLOAD-$FORMAT-$(date +%Y%m%d-%H%M%S)}

cargo build --release -p t4-bench --bin run_queries
mkdir -p "$OUT"

for store in $STORES; do
    case "$store" in
    s3) cache=(--passes 1) ;;
    t4-none)
        cache=(--passes 1 --cache-policy none --cache-memory-mib 1024
            --cache-path "${mounts[0]}/t4.cache" --cache-disk-gib 1)
        ;;
    t4-memory)
        cache=(--passes 2 --cache-policy memory --cache-memory-mib $((MEMORY_GIB * 1024))
            --cache-path "${mounts[0]}/t4.cache" --cache-disk-gib 1)
        ;;
    t4-disk)
        cache=(--passes 2 --cache-policy disk --cache-memory-mib 1024
            "${disk_files[@]}" --cache-disk-gib "$DISK_GIB")
        ;;
    *)
        echo "STORES holds s3, t4-none, t4-memory and t4-disk, not $store" >&2
        exit 1
        ;;
    esac
    echo "== $store"
    started=$SECONDS
    target/release/run_queries "${common[@]}" "${cache[@]}" "${drives[@]}" \
        --out "$OUT/$store.csv" "$@" | tee "$OUT/$store.log"
    rm -f "${mounts[@]/%//t4.cache}"
    echo "== $store took $((SECONDS - started)) s"
done

echo "results in $OUT"
