#!/usr/bin/env bash
# Makes run.sh's datasets on the dev box (bench/devbox/) and puts them in
# S3: ClickBench in Parquet, TPC-H in Parquet and in Vortex (to_vortex, a
# file for a file, its rows in order):
#
#   $BUCKET/clickbench/partitioned/            ClickBench's 100 files
#   $BUCKET/tpch/sf$SF/parquet/<table>/        TPC-H at scale factor SF,
#   $BUCKET/tpch/sf$SF/vortex/<table>/         PARTS files a table at most
#
# Vortex is benched only with its dictionaries capped at 2 MiB of values.
# Vortex 0.90 caps them at 1 MiB, and its writer can't be told otherwise
# until vortex-data/vortex#10766 is released. So ClickBench's Vortex set,
# $BUCKET/clickbench/partitioned-vortex-dict2m/, was written by to_vortex
# built against a vortex-layout whose DictLayoutConstraints::default() is
# 2 MiB, and this script doesn't make it. TPC-H's Vortex tables still have
# the 1 MiB cap.
#
# ClickBench's files come from ClickHouse's public copy, TPC-H's from
# tpchgen-cli, installed under target/ if missing. Both are made on the
# first SSD, /mnt/t4/0/data, and a step whose files are there is skipped.
# The upload is `aws s3 sync`, through Nix where `aws` isn't installed, and
# takes S3's credentials from the environment. Run it in the dev shell
# (`nix develop -c`), for cargo.
#
# DATASETS picks which to make (default "clickbench tpch"); SF the scale
# factor (default 100); PARTS the files a TPC-H table splits into (default
# 100).
set -euo pipefail

cd "$(dirname "$0")/../.."
BUCKET=${BUCKET:-s3://t4-bench-xiangpeng-use1}
DATASETS=${DATASETS:-clickbench tpch}
SF=${SF:-100}
PARTS=${PARTS:-100}
DATA=/mnt/t4/0/data
TPCH_TABLES=(region nation supplier customer part partsupp orders lineitem)

cargo build --release -p t4-bench --bin to_vortex
to_vortex=target/release/to_vortex
tpchgen=target/tools/bin/tpchgen-cli
command -v aws >/dev/null || aws() { nix run nixpkgs#awscli2 -- "$@"; }

# Writes the Parquet table at $1, a directory, as Vortex at $2, unless
# there already.
vortex_copy() {
    if [ ! -d "$2" ]; then
        rm -rf "$2.part"
        "$to_vortex" --table "$1" --out "$2.part"
        mv "$2.part" "$2"
    fi
}

for dataset in $DATASETS; do
    case "$dataset" in
    clickbench)
        parquet=$DATA/clickbench/partitioned
        mkdir -p "$parquet"
        for n in $(seq 0 99); do
            file=$parquet/hits_$n.parquet
            if [ ! -e "$file" ]; then
                curl -sSf -o "$file.part" \
                    "https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_$n.parquet"
                mv "$file.part" "$file"
            fi
        done
        aws s3 sync "$parquet" "$BUCKET/clickbench/partitioned/"
        ;;
    tpch)
        tpch=$DATA/tpch/sf$SF
        if [ ! -d "$tpch/parquet" ]; then
            [ -x "$tpchgen" ] || cargo install tpchgen-cli --locked --root target/tools
            rm -rf "$tpch/parquet.part"
            "$tpchgen" parquet -s "$SF" --parts "$PARTS" --output-dir "$tpch/parquet.part"
            mv "$tpch/parquet.part" "$tpch/parquet"
        fi
        for table in "${TPCH_TABLES[@]}"; do
            vortex_copy "$tpch/parquet/$table" "$tpch/vortex/$table"
        done
        aws s3 sync "$tpch/parquet" "$BUCKET/tpch/sf$SF/parquet/"
        aws s3 sync "$tpch/vortex" "$BUCKET/tpch/sf$SF/vortex/"
        ;;
    *)
        echo "DATASETS holds clickbench and tpch, not $dataset" >&2
        exit 1
        ;;
    esac
done
