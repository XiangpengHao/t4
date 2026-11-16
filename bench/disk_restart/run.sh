#!/usr/bin/env bash
# Runs on the dev box (bench/devbox/create.sh): times t4's restart through its log, and counts
# the blocks a restart keeps (disk_restart, doc/13-persistence.md), on the box's local SSDs at
# /mnt/t4/<n>, OS defaults throughout.
#
#   nix develop -c bench/disk_restart/run.sh <results dir>
#
#   desktop-instant       20 GiB in 64 MiB files from a store that answers at once: fills outrun
#                         the drive, the worst case
#   desktop               the same from a 1,000 MB/s link
#   desktop-small-files   20 GiB in 1 MiB files from that link: a path a block, the log's worst
#   server                128 GiB in 64 MiB files from a 6,000 MB/s link, a cache file on each
#                         of three SSDs
#   desktop-kill,         a fill aborted halfway (SIGABRT, no destructor: a kill under load),
#   server-kill           then the next build on its files
#   desktop-traced        desktop under strace, timing only the syscalls that make the log
#                         durable, so the time splits into waiting on the drives and the rest
#
# Each clean run reads every block back after its first restart: its GETs are the blocks the
# log didn't keep. Writes <run>.csv, <run>.log, drives.txt, build.log, and done at the end, or
# failed if a step fails.
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

# The first run aborts itself; anything but a timeout (124) or a clean exit is that abort.
kill_run() {
    local name=$1
    shift
    local status=0
    timeout -k 10 300 "$BIN" --label "$name" --out "$OUT/$name-killed.csv" "$@" \
        2> "$OUT/$name-killed.log" || status=$?
    if [ "$status" -eq 0 ] || [ "$status" -eq 124 ]; then
        echo "$name: the run meant to abort exited $status" >&2
        return 1
    fi
    run "$name" "$@" --after-kill
}

traced() {
    local name=$1
    shift
    timeout -k 10 300 strace -f --seccomp-bpf -T -tt -o "$OUT/$name.strace" \
        -e trace=fsync,fdatasync,rename,renameat,renameat2,unlink,unlinkat \
        "$BIN" --label "$name" --out "$OUT/$name.csv" --restarts 2 "$@" 2> "$OUT/$name.log"
}

DESKTOP=(--dir /mnt/t4/0 --blocks 20480 --file-blocks 64)
SERVER=(--dir /mnt/t4/0 --dir /mnt/t4/1 --dir /mnt/t4/2 --blocks 131072 --file-blocks 64)

run desktop-instant "${DESKTOP[@]}" --read-all
run desktop "${DESKTOP[@]}" --read-all --link-mb-per-s 1000
run desktop-small-files --dir /mnt/t4/0 --blocks 20480 --file-blocks 1 --read-all --link-mb-per-s 1000
run server "${SERVER[@]}" --read-all --link-mb-per-s 6000
kill_run desktop-kill "${DESKTOP[@]}" --link-mb-per-s 1000 --kill-at 10240
kill_run server-kill "${SERVER[@]}" --link-mb-per-s 6000 --kill-at 65536
traced desktop-traced "${DESKTOP[@]}" --link-mb-per-s 1000

touch "$OUT/done"
