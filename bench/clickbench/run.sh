#!/usr/bin/env bash
# Runs ClickBench on the dev box (bench/devbox/) every way the benchmark
# compares, each in a process of its own, and writes one CSV a run to
# ~/t4-results/<tool>-<dataset>-<time>/:
#
#   gcs.csv               the dataset from GCS, no cache
#   local-<n>g.csv        the copy on the SSD through the page cache, in a
#                         cgroup of n GiB and the tool's working memory:
#                         pass 0 cold, pass 1 warm
#   t4-<n>g.csv           through t4, its default CLOCK policy, the SSD, and
#                         a memory tier the size the page cache was in
#                         local-<n>g: pass 0 cold, pass 1 warm
#   t4-pinned-disk.csv    through t4, every block on the SSD: disk hits alone
#   t4-pinned-memory.csv  through t4, every block in memory: memory hits alone
#
# Every run starts with the page cache dropped (it outlives a cgroup), and
# runs in a cgroup whose memory holds its cache and its working memory.
# Working memory varies by query, and the page cache takes whatever the
# working memory leaves, so local-<n>g caches in more than n GiB at times;
# t4 then gets, for its memory tier, the median page cache across
# local-<n>g's warm queries. Both cache in the same memory.
#
# TOOL picks what reads: `queries` (the default), DataFusion running the
# SQL, or `replay`, replay_reads making the calls of TRACE (a trace of the
# same dataset) with nothing between them. DATASET picks ClickBench's single
# file (`single`, the default) or its 100 files (`partitioned`). BUDGETS
# lists the memory budgets in GiB (default "1 32"). Arguments go to every
# run, e.g. `--query 0 --query 1` for a smoke test.
set -euo pipefail

cd "$(dirname "$0")/../.."
TOOL=${TOOL:-queries}
DATASET=${DATASET:-single}
BUDGETS=${BUDGETS:-1 32}
SSD=/mnt/t4
case "$DATASET" in
single)
    REMOTE=gs://t4-bench-us-east4/clickbench/hits.parquet
    LOCAL=$SSD/data/hits.parquet
    ;;
partitioned)
    REMOTE=gs://t4-bench-us-east4/clickbench/partitioned/
    LOCAL=$SSD/data/partitioned/
    ;;
*)
    echo "DATASET is single or partitioned, not $DATASET" >&2
    exit 1
    ;;
esac
# Each tool's working memory beyond its cache: replay_reads peaks at 2.4
# GiB holding the bytes of the calls in flight (measured 2026-10-22, both
# datasets), and t4 adds 2 GiB more, the pages it reads into before copying
# them out. DataFusion's own is unmeasured: set both for TOOL=queries.
case "$TOOL" in
queries)
    : "${LOCAL_OVERHEAD_MIB:?TOOL=queries needs LOCAL_OVERHEAD_MIB, the working memory of DataFusion}"
    : "${T4_OVERHEAD_MIB:?TOOL=queries needs T4_OVERHEAD_MIB, the working memory of DataFusion and t4}"
    ;;
replay)
    TRACE=${TRACE:?TOOL=replay needs TRACE, a trace of $DATASET}
    LOCAL_OVERHEAD_MIB=${LOCAL_OVERHEAD_MIB:-2560}
    T4_OVERHEAD_MIB=${T4_OVERHEAD_MIB:-4608}
    ;;
*)
    echo "TOOL is queries or replay, not $TOOL" >&2
    exit 1
    ;;
esac
CACHE=$SSD/t4.cache
OUT=${OUT:-$HOME/t4-results/$TOOL-$DATASET-$(date +%Y%m%d-%H%M%S)}

cargo build --release -p t4-bench --bin run_queries --bin replay_reads
mkdir -p "$OUT"
[ -e "$LOCAL" ] || gcloud storage cp -r "$REMOTE" "$(dirname "$LOCAL")/"

# Reads the table at $3 with the tool, in a cgroup of $2 MiB, writing
# $OUT/$1.csv.
run() {
    local name=$1 memory_mib=$2 location=$3
    shift 3
    echo "== $name, cgroup memory $memory_mib MiB"
    rm -f "$CACHE"
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null
    local tool
    case "$TOOL" in
    queries) tool=(target/release/run_queries --queries bench/clickbench/queries.sql --table "hits=$location") ;;
    replay) tool=(target/release/replay_reads --trace "$TRACE" --location "$location") ;;
    esac
    sudo systemd-run --scope --quiet --same-dir --uid="$(id -u)" --gid="$(id -g)" \
        -p MemoryMax="${memory_mib}M" -p MemorySwapMax=0 -- \
        "${tool[@]}" --drive-dir "$SSD" --out "$OUT/$name.csv" "$@"
    rm -f "$CACHE"
}
# The median page cache, in MiB, after the warm (pass 1) queries of the run
# whose CSV is $1.
median_page_cache_mib() {
    awk -F, 'NR == 1 { for (i = 1; i <= NF; i++) if ($i == "page_cache_bytes") col = i; next }
        $1 == 1 { print int($col / 1048576) }' "$1" | sort -n |
        awk '{ v[NR] = $1 } END { print v[int((NR + 1) / 2)] }'
}
t4=(--cache-path "$CACHE" --cache-disk-gib 32)

run gcs $((32 * 1024 + LOCAL_OVERHEAD_MIB)) "$REMOTE" "$@"
for budget in $BUDGETS; do
    run "local-${budget}g" $((budget * 1024 + LOCAL_OVERHEAD_MIB)) "$LOCAL" --passes 2 "$@"
    memory_mib=$(median_page_cache_mib "$OUT/local-${budget}g.csv")
    echo "t4's memory tier: $memory_mib MiB, the page cache's median in local-${budget}g"
    run "t4-${budget}g" $((memory_mib + T4_OVERHEAD_MIB)) "$REMOTE" --passes 2 \
        --cache-policy clock --cache-memory-mib "$memory_mib" "${t4[@]}" "$@"
done
run t4-pinned-disk $((1024 + T4_OVERHEAD_MIB)) "$REMOTE" --passes 2 \
    --cache-policy disk --cache-memory-mib 1024 "${t4[@]}" "$@"
run t4-pinned-memory $((32 * 1024 + T4_OVERHEAD_MIB)) "$REMOTE" --passes 2 \
    --cache-policy memory --cache-memory-mib $((32 * 1024)) "${t4[@]}" "$@"

echo "results in $OUT"
