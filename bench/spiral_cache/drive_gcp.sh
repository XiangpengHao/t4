#!/usr/bin/env bash
# Runs bench/spiral_cache/project.sh on a fresh Spot box in GCP, beside a Spiral project's
# storage, from this machine, and keeps nothing running after: GCP deletes the box at a hard
# cap (its maximum run duration), and this script deletes it on every way it exits. Results are
# copied back as they land, so a reclaimed box keeps what had finished.
#
#   SPIRALDB=<spiraldb checkout> SPIRAL_ENV=<sign-in file> PROJECT=<Spiral project> \
#       bench/spiral_cache/drive_gcp.sh <local results dir>
#
# The box gets this t4 tree (the scripts) and SPIRALDB's (engine-bench, built there), neither's
# target/ nor .git/, and SPIRAL_ENV, `KEY=value` lines that sign in to the deployment (such as a
# workload's SPIRAL_CLIENT_ID and SPIRAL_CLIENT_SECRET), in a file only the box's user reads.
# It is reached through IAP as the gcloud account's OS Login user. project.sh's settings
# (BENCHMARKS, SCALE, CACHES, CAPACITY, DISK_PASSES, WARM_PASSES, WRITE) pass through.
set -euo pipefail

LOCAL=${1:?local results dir}
SPIRALDB=${SPIRALDB:?the spiraldb checkout to build}
SPIRAL_ENV=${SPIRAL_ENV:?the file that signs the box in to Spiral}
PROJECT=${PROJECT:?the Spiral project}
GCP_PROJECT=${GCP_PROJECT:-spiraldb-robotics}
ZONE=${ZONE:-us-east4-b}
MACHINE_TYPE=${MACHINE_TYPE:-c3-standard-88-lssd}
CAP_HOURS=${CAP_HOURS:-4}
NAME=t4-spiral-cache-$(date +%s)
REPO=$(cd "$(dirname "$0")/../.." && pwd)
REMOTE_OUT=results/spiral_cache
mkdir -p "$LOCAL"

# macOS has no `timeout`: every local command that talks to the box or GCP gets one this way.
limit() { local seconds=$1; shift; perl -e 'alarm shift; exec @ARGV' "$seconds" "$@"; }
gcloud_() { limit 600 gcloud --project="$GCP_PROJECT" --quiet "$@"; }

# The hard cap first: the box is deleted at it, whatever becomes of this script.
gcloud_ compute instances create "$NAME" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    --provisioning-model=SPOT \
    --instance-termination-action=DELETE \
    --max-run-duration="${CAP_HOURS}h" \
    --network-interface=nic-type=GVNIC,network=spiral-robotics,subnet=spiral-robotics-us-east4 \
    --image-family=ubuntu-2604-lts-amd64 --image-project=ubuntu-os-cloud \
    --boot-disk-type=hyperdisk-balanced --boot-disk-size=300GB \
    --scopes=cloud-platform \
    --labels=purpose=t4-bench,owner=xiangpeng \
    --metadata=enable-oslogin=TRUE \
    --metadata-from-file=startup-script="$REPO/bench/devbox/startup.sh"
echo "box $NAME in $ZONE" | tee -a "$LOCAL/box.txt"
delete() { gcloud_ compute instances delete "$NAME" --zone="$ZONE" >/dev/null || true; echo "deleted $NAME"; }
trap delete EXIT

# ssh and rsync reach the box as any host, through an IAP tunnel, as the OS Login user whose
# key gcloud registers on its first ssh.
for _ in $(seq 30); do
    limit 120 gcloud --project="$GCP_PROJECT" --quiet compute ssh "$NAME" --zone="$ZONE" \
        --tunnel-through-iap --command=true 2>/dev/null && break
    sleep 10
done
CONFIG=$(mktemp -d)
user=$(gcloud_ compute os-login describe-profile --format='value(posixAccounts[0].username)')
cat > "$CONFIG/ssh_config" <<EOF
Host box
    HostName $NAME
    User $user
    IdentityFile ~/.ssh/google_compute_engine
    StrictHostKeyChecking accept-new
    UserKnownHostsFile $CONFIG/known_hosts
    ConnectTimeout 30
    ProxyCommand gcloud compute start-iap-tunnel $NAME 22 --listen-on-stdin --zone=$ZONE --project=$GCP_PROJECT --verbosity=warning
EOF
# ssh_ <seconds> <command>: a command on the box, killed past its limit.
ssh_() { local seconds=$1; shift; limit "$seconds" ssh -F "$CONFIG/ssh_config" box "$@"; }
rsync_() { local seconds=$1; shift; limit "$seconds" rsync -az -e "ssh -F $CONFIG/ssh_config" "$@"; }

ssh_ 1200 'cloud-init status --wait >/dev/null'
rsync_ 900 --delete --exclude target/ --exclude .git/ --exclude 'results/' "$REPO/" box:t4/
rsync_ 900 --delete --exclude target/ --exclude .git/ --exclude vortex-bench/ "$SPIRALDB/" box:spiraldb/
ssh_ 60 'umask 077 && cat > ~/spiral.env' < "$SPIRAL_ENV"
# A fresh session, after boot setup. setsid -f forks the run off with its own output, so the
# session ends at once.
ssh_ 60 "cd t4 && mkdir -p $REMOTE_OUT && PROJECT='$PROJECT' \
    BENCHMARKS='${BENCHMARKS:-}' SCALE='${SCALE:-}' CACHES='${CACHES:-}' CAPACITY='${CAPACITY:-}' WRITE='${WRITE:-}' \
    DISK_PASSES='${DISK_PASSES:-}' WARM_PASSES='${WARM_PASSES:-}' \
    setsid -f bash bench/spiral_cache/project.sh $REMOTE_OUT > $REMOTE_OUT/run.log 2>&1 < /dev/null"

while true; do
    sleep 60
    rsync_ 300 "box:t4/$REMOTE_OUT/" "$LOCAL/" || true
    [ -e "$LOCAL/done" ] && break
    if ! ssh_ 60 true 2>/dev/null; then echo "box unreachable; keeping what was copied" >&2; break; fi
done
echo "results in $LOCAL"
