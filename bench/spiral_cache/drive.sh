#!/usr/bin/env bash
# Runs bench/spiral_cache/run.sh on a fresh Spot box from this machine, and
# keeps nothing running after: the box shuts itself down at a hard cap, and
# this script terminates it on every way it exits. Results are copied back as
# they land, so a reclaimed box keeps what had finished.
#
#   AWS_PROFILE=spiral SPIRALDB=<spiraldb checkout> bench/spiral_cache/drive.sh <local results dir>
#
# The box gets this t4 tree (the scripts) and SPIRALDB's (engine-bench, built
# there), neither's target/ nor .git/, and S3 credentials exported from
# AWS_PROFILE's session, in a file only the box's user reads, written again
# each minute so that they outlast the run while the session lasts.
set -euo pipefail

LOCAL=${1:?local results dir}
SPIRALDB=${SPIRALDB:?the spiraldb checkout to build}
REGION=${REGION:-us-east-1}
export INSTANCE_TYPE=${INSTANCE_TYPE:-i7i.24xlarge}
CAP_MINUTES=${CAP_MINUTES:-150}
KEY=~/.ssh/t4-bench
REPO=$(cd "$(dirname "$0")/../.." && pwd)
REMOTE_OUT=results/spiral_cache
mkdir -p "$LOCAL"

# macOS has no `timeout`: every local command that talks to the box or AWS
# gets one this way.
limit() { local seconds=$1; shift; perl -e 'alarm shift; exec @ARGV' "$seconds" "$@"; }
aws_() { limit 60 aws --region "$REGION" "$@"; }

# The session's S3 credentials, as `KEY=value` lines.
s3_env() {
    limit 60 aws configure export-credentials --format env | sed 's/^export //'
    echo "AWS_REGION=$REGION"
}
s3_env > /dev/null || { echo "no S3 credentials: aws sso login first" >&2; exit 1; }

read -r instance address < <(limit 600 "$REPO/bench/devbox/aws-create.sh" | tail -1)
echo "box $instance at $address" | tee -a "$LOCAL/box.txt"
terminate() { aws_ ec2 terminate-instances --instance-ids "$instance" >/dev/null || true; echo "terminated $instance"; }
trap terminate EXIT

# ssh_ <seconds> <command>: a command on the box, killed past its limit.
ssh_() { local seconds=$1; shift; limit "$seconds" ssh -i "$KEY" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 "ubuntu@$address" "$@"; }

for _ in $(seq 60); do ssh_ 30 true 2>/dev/null && break; sleep 10; done
# The hard cap first, then the boot's setup.
ssh_ 60 "sudo shutdown -h +$CAP_MINUTES; sudo systemd-run --on-active=${CAP_MINUTES}m /bin/systemctl poweroff -i"
ssh_ 1200 'cloud-init status --wait >/dev/null; test -e /run/systemd/shutdown/scheduled' ||
    { echo "the shutdown cap did not take" >&2; exit 1; }

limit 600 rsync -az --delete --exclude target/ --exclude .git/ \
    -e "ssh -i $KEY" "$REPO/" "ubuntu@$address:t4/"
limit 600 rsync -az --delete --exclude target/ --exclude .git/ \
    -e "ssh -i $KEY" "$SPIRALDB/" "ubuntu@$address:spiraldb/"
push_credentials() { s3_env | ssh_ 60 'umask 077 && cat > ~/s3.env.new && mv ~/s3.env.new ~/s3.env'; }
push_credentials
# A fresh session, after boot setup. setsid -f forks the run off with its
# own output, so the session ends at once.
ssh_ 60 "cd t4 && mkdir -p $REMOTE_OUT && \
    DISK_PASSES='${DISK_PASSES:-}' WARM_PASSES='${WARM_PASSES:-}' CACHES='${CACHES:-}' PERF='${PERF:-}' \
    setsid -f bash bench/spiral_cache/run.sh $REMOTE_OUT > $REMOTE_OUT/run.log 2>&1 < /dev/null"

while true; do
    sleep 60
    push_credentials || echo "credentials not renewed $(date)" >&2
    limit 300 rsync -az -e "ssh -i $KEY" "ubuntu@$address:t4/$REMOTE_OUT/" "$LOCAL/" || true
    [ -e "$LOCAL/done" ] && break
    if ! ssh_ 30 true 2>/dev/null; then echo "box unreachable; keeping what was copied" >&2; break; fi
done
echo "results in $LOCAL"
