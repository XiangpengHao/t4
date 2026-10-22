#!/usr/bin/env bash
# Runs as root on every boot of the dev box (create.sh). A stop wipes the
# local SSDs, so this formats each again when its filesystem is gone, and
# mounts local SSD n at /mnt/t4/<n>: a cache file on each, the datasets on
# the first.
set -euo pipefail

MOUNTS=/mnt/t4

shopt -s nullglob
ssds=(/dev/disk/by-id/google-local-nvme-ssd-*)
if [ "${#ssds[@]}" -eq 0 ]; then
    echo "t4 devbox: no local SSDs" >&2
    exit 1
fi
if ! command -v mkfs.xfs >/dev/null; then
    apt-get update
    apt-get install -y xfsprogs
fi
for ssd in "${ssds[@]}"; do
    mount=$MOUNTS/${ssd##*-}
    mountpoint -q "$mount" && continue
    # After a reboot an SSD keeps its filesystem; after a stop it is blank.
    blkid "$ssd" >/dev/null || mkfs.xfs -f "$ssd"
    mkdir -p "$mount"
    mount -o noatime "$ssd" "$mount"
    chmod 1777 "$mount"
done

# Packages, once: they live on the boot disk.
if [ ! -e /var/lib/t4-devbox-packages ]; then
    apt-get update
    apt-get install -y build-essential git pkg-config fio nvme-cli \
        linux-tools-common "linux-tools-$(uname -r)" nix-setup-systemd
    echo "experimental-features = nix-command flakes" >> /etc/nix/nix.conf
    systemctl restart nix-daemon
    touch /var/lib/t4-devbox-packages
fi
