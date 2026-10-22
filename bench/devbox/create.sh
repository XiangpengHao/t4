#!/usr/bin/env bash
# Creates t4's dev box on GCP: a z3-highmem-22-highlssd in us-east4, 22
# vCPUs, 176 GB, and three 3 TB local SSDs at 3,000 MiB/s read each, for
# the disk tier striped over several drives (doc/04-multi-device.md).
#
# On-demand, not Spot: a Z3 with up to 18 TB of local SSD must live-migrate
# through host maintenance, which Spot VMs can't. Stop it when idle.
#
# A stop wipes the local SSDs but keeps the boot disk. So the repo, the
# toolchain and target/ live on the boot disk, and startup.sh formats each
# SSD again, at /mnt/t4/<n>, on every boot. BOOT_DISK names a boot disk to
# keep, an earlier box's, in place of a fresh one.
#
# It lives in spiraldb-robotics, whose network admits SSH only through
# IAP, with OS Login. Its labels set it apart in that project's bill.
#
#   bench/devbox/create.sh
#   gcloud compute ssh t4-devbox --project=spiraldb-robotics \
#       --zone=us-east4-b --tunnel-through-iap
#
# On a fresh boot disk, once logged in: `sudo usermod -aG nix-users $USER`,
# log in again, then `nix develop` in the repo gives the toolchain from
# flake.nix.
set -euo pipefail

PROJECT="${PROJECT:-spiraldb-robotics}"
NAME="${NAME:-t4-devbox}"
ZONE="${ZONE:-us-east4-b}"
MACHINE_TYPE="${MACHINE_TYPE:-z3-highmem-22-highlssd}"

if [ -n "${BOOT_DISK:-}" ]; then
    boot=(--disk="name=$BOOT_DISK,boot=yes,auto-delete=yes")
else
    boot=(--image-family=ubuntu-2604-lts-amd64 --image-project=ubuntu-os-cloud
        --boot-disk-type=hyperdisk-balanced --boot-disk-size=300GB)
fi

gcloud compute instances create "$NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    --maintenance-policy=MIGRATE \
    --network-interface=nic-type=GVNIC,network=spiral-robotics,subnet=spiral-robotics-us-east4 \
    "${boot[@]}" \
    --scopes=cloud-platform \
    --labels=purpose=t4-bench,owner=xiangpeng \
    --metadata=enable-oslogin=TRUE \
    --metadata-from-file=startup-script="$(dirname "$0")/startup.sh"
