#!/usr/bin/env bash
# Runs on the dev box (bench/devbox/create.sh): times t4's restart, the cache dropped and its
# log replayed and rebased at build (disk_restart, doc/13-persistence.md), on the box's
# local SSDs at /mnt/t4/<n>, OS defaults throughout.
#
#   nix develop -c bench/disk_restart/run.sh <results dir>
#
#   desktop              20 GiB in 64 MiB files, one cache file
#   desktop-small-files  20 GiB in 1 MiB files: a path and e_tag a block, the log's worst
#   server               256 GiB in 64 MiB files, a cache file on each of three SSDs
#
# Each again under strace, timing the syscalls that sync or replace files: none should sync,
# so a sync there is a regression. Writes <run>.csv, <run>.log, drives.txt,
# build.log, and done at the end, or failed if a step fails.
set -euo pipefail

OUT=${1:?results dir}
mkdir -p "$OUT"
rm -f "$OUT/done" "$OUT/failed"
trap 'echo "line $LINENO" > "$OUT/failed"' ERR
BIN=target/release/disk_restart

{
    uname -r
    grep -m1 'model name' /proc/cpuinfo
    nproc
    for mount in /mnt/t4/0 /mnt/t4/1 /mnt/t4/2; do
        echo "$mount $(findmnt -no SOURCE,FSTYPE,OPTIONS "$mount")"
    done
    for queue in /sys/block/nvme*/queue; do
        echo "$queue write_cache=$(cat "$queue/write_cache") fua=$(cat "$queue/fua" 2>/dev/null)"
    done
} > "$OUT/drives.txt"

cargo build --release -p t4-bench --bin disk_restart 2>&1 | tee "$OUT/build.log"

run() {
    local name=$1
    shift
    timeout -k 10 300 "$BIN" --label "$name" --out "$OUT/$name.csv" "$@" 2> "$OUT/$name.log"
}

traced() {
    local name=$1
    shift
    timeout -k 10 300 strace -f --seccomp-bpf -T -tt -o "$OUT/$name.strace" \
        -e trace=fsync,fdatasync,rename,renameat,renameat2,unlink,unlinkat \
        "$BIN" --label "$name" --out "$OUT/$name.csv" --restarts 2 "$@" 2> "$OUT/$name.log"
}

DESKTOP=(--dir /mnt/t4/0 --blocks 20480)
SERVER=(--dir /mnt/t4/0 --dir /mnt/t4/1 --dir /mnt/t4/2 --blocks 262144)

run desktop "${DESKTOP[@]}" --file-blocks 64
run desktop-small-files "${DESKTOP[@]}" --file-blocks 1
run server "${SERVER[@]}" --file-blocks 64
traced desktop-traced "${DESKTOP[@]}" --file-blocks 64
traced server-traced "${SERVER[@]}" --file-blocks 64

touch "$OUT/done"
