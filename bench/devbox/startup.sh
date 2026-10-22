#!/usr/bin/env bash
# Runs as root on every boot of the dev box (create.sh). A stop or a Spot
# preemption wipes the local SSDs, so this formats the first one again
# when its filesystem is gone, and mounts it at /mnt/t4, where t4's cache
# file goes. Any other SSDs stay raw until t4 drives several devices
# itself.
set -euo pipefail

MOUNT=/mnt/t4
SSD=/dev/disk/by-id/google-local-nvme-ssd-0

if ! mountpoint -q "$MOUNT"; then
    if [ ! -e "$SSD" ]; then
        echo "t4 devbox: no local SSD at $SSD" >&2
        exit 1
    fi
    if ! command -v mkfs.xfs >/dev/null; then
        apt-get update
        apt-get install -y xfsprogs
    fi
    # After a reboot the SSD keeps its filesystem; after a stop it is blank.
    blkid "$SSD" >/dev/null || mkfs.xfs -f "$SSD"
    mkdir -p "$MOUNT"
    mount -o noatime "$SSD" "$MOUNT"
    chmod 1777 "$MOUNT"
fi

# Packages, once: they live on the boot disk.
if [ ! -e /var/lib/t4-devbox-packages ]; then
    apt-get update
    apt-get install -y build-essential git pkg-config fio nvme-cli \
        linux-tools-common "linux-tools-$(uname -r)" nix-setup-systemd
    echo "experimental-features = nix-command flakes" >> /etc/nix/nix.conf
    systemctl restart nix-daemon
    touch /var/lib/t4-devbox-packages
fi
