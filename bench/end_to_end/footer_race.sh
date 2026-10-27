#!/usr/bin/env bash
# What racing a footer buys (doc/06-read-modes.md, decision 10): ClickBench's
# query 24 through t4, each run a session of its own that reads the footers
# afresh, with 1, 2 and 4 requests racing for each file's last 2 MiB, the
# runs of each interleaved; for each format, from each bucket. One run.sh a
# bucket and format, each to $OUT/<scheme>-<format>/t4-memory.csv, a first
# round of one run each warming the process up (run.sh's FOOTER_RACES); the
# scheme is s3 or gcs.
#
#   BUCKETS  where the datasets are (default the S3 bucket and the GCS one)
#   FORMATS  default "parquet vortex"
#   RUNS     runs of each footer race (default 10)
#   QUERY    default 24, ClickBench's numbered from 0
#
# S3's credentials come from AWS_*, GCS's from GOOGLE_* or GCS_ACCESS_TOKEN,
# as run.sh says.
# Arguments go to every run_queries.
set -euo pipefail

cd "$(dirname "$0")"
BUCKETS=${BUCKETS:-s3://t4-bench-xiangpeng-use1 gs://t4-bench-us-east4}
FORMATS=${FORMATS:-parquet vortex}
RUNS=${RUNS:-10}
QUERY=${QUERY:-24}
OUT=${OUT:-$HOME/t4-results/footer-race-$(date +%Y%m%d-%H%M%S)}

for bucket in $BUCKETS; do
    scheme=${bucket%%://*}
    [ "$scheme" != gs ] || scheme=gcs
    for format in $FORMATS; do
        BUCKET=$bucket FORMAT=$format STORES=t4-memory MEMORY_GIB=16 \
            QUERIES=$QUERY RUNS=$RUNS FOOTER_RACES="1 2 4" \
            OUT="$OUT/$scheme-$format" ./run.sh "$@"
    done
done
echo "results in $OUT"
