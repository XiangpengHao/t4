#!/usr/bin/env bash
# Creates t4's dev box on GCP: a z3-highmem-14-standardlssd in us-east4,
# 14 vCPUs, 112 GB, and one 3 TB local SSD at 3,000 MiB/s read.
#
# On-demand, not Spot: a Z3 with up to 18 TB of local SSD must live-migrate
# through host maintenance, which Spot VMs can't. Stop it when idle.
#
# A stop wipes the local SSD but keeps the boot disk. So the repo, the
# toolchain and target/ live on the boot disk, and startup.sh formats the
# SSD again, /mnt/t4, on every boot. The boot disk can move to a Z3 with
# more SSDs once t4 drives several devices itself.
#
# It lives in spiraldb-robotics, whose network admits SSH only through
# IAP, with OS Login. Its labels set it apart in that project's bill.
#
#   bench/devbox/create.sh
#   gcloud compute ssh t4-devbox --project=spiraldb-robotics \
#       --zone=us-east4-b --tunnel-through-iap
#
# Once logged in: `sudo usermod -aG nix-users $USER`, log in again, then
# `nix develop` in the repo gives the toolchain from flake.nix.
set -euo pipefail

PROJECT="${PROJECT:-spiraldb-robotics}"
NAME="${NAME:-t4-devbox}"
ZONE="${ZONE:-us-east4-b}"

gcloud compute instances create "$NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type=z3-highmem-14-standardlssd \
    --maintenance-policy=MIGRATE \
    --network-interface=nic-type=GVNIC,network=spiral-robotics,subnet=spiral-robotics-us-east4 \
    --image-family=ubuntu-2604-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-type=hyperdisk-balanced \
    --boot-disk-size=300GB \
    --scopes=cloud-platform \
    --labels=purpose=t4-bench,owner=xiangpeng \
    --metadata=enable-oslogin=TRUE \
    --metadata-from-file=startup-script="$(dirname "$0")/startup.sh"
