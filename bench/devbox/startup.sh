#!/usr/bin/env bash
# Runs as root on every boot of the dev box, on GCP (create.sh) or AWS
# (aws-create.sh). A stop wipes the local SSDs, so this formats each again
# when its filesystem is gone, and mounts local SSD n at /mnt/t4/<n>: a
# cache file on each, the datasets on the first.
set -euo pipefail

MOUNTS=/mnt/t4

# GCP names local SSD n google-local-nvme-ssd-<n>; on AWS an instance-store
# volume is known by its model.
shopt -s nullglob
gcp=(/dev/disk/by-id/google-local-nvme-ssd-*)
if [ "${#gcp[@]}" -gt 0 ]; then
    mapfile -t ssds < <(printf '%s\n' "${gcp[@]}" | sort -V)
else
    mapfile -t ssds < <(lsblk -dpno NAME,MODEL | awk '/Amazon EC2 NVMe Instance Storage/ {print $1}')
fi
# A box without them, a network-optimized one, still gets its idle stop and
# packages: /mnt/t4/0 is then a directory on the boot disk.
if [ "${#ssds[@]}" -eq 0 ]; then
    echo "t4 devbox: no local SSDs, /mnt/t4/0 on the boot disk" >&2
    mkdir -p "$MOUNTS/0"
    chmod 1777 "$MOUNTS/0"
fi
if ! command -v mkfs.xfs >/dev/null; then
    apt-get update
    apt-get install -y xfsprogs
fi
for n in "${!ssds[@]}"; do
    ssd=${ssds[$n]}
    mount=$MOUNTS/$n
    mountpoint -q "$mount" && continue
    # After a reboot an SSD keeps its filesystem; after a stop it is blank.
    blkid "$ssd" >/dev/null || mkfs.xfs -f -K "$ssd"
    mkdir -p "$mount"
    mount -o noatime "$ssd" "$mount"
    chmod 1777 "$mount"
done

# Stops the box once it has sat idle for 30 minutes: nobody logged in, and
# the 5-minute load under 0.5. A check every 5 minutes; six idle in a row
# power it off. The count lives in /run, so each boot starts it afresh.
cat > /usr/local/sbin/t4-idle-stop <<'EOF'
#!/usr/bin/env bash
count=/run/t4-idle-checks
if [ -z "$(who)" ] && awk '{ exit !($2 < 0.5) }' /proc/loadavg; then
    idle=$(($(cat "$count" 2>/dev/null || echo 0) + 1))
else
    idle=0
fi
echo "$idle" > "$count"
if [ "$idle" -ge 6 ]; then
    systemctl poweroff
fi
EOF
chmod +x /usr/local/sbin/t4-idle-stop
cat > /etc/systemd/system/t4-idle-stop.service <<'EOF'
[Unit]
Description=Stop the t4 dev box after 30 idle minutes
[Service]
Type=oneshot
ExecStart=/usr/local/sbin/t4-idle-stop
EOF
cat > /etc/systemd/system/t4-idle-stop.timer <<'EOF'
[Unit]
Description=Check every 5 minutes whether the t4 dev box is idle
[Timer]
OnBootSec=5min
OnUnitActiveSec=5min
[Install]
WantedBy=timers.target
EOF
systemctl daemon-reload
systemctl enable --now t4-idle-stop.timer

# Packages, once: they live on the boot disk.
if [ ! -e /var/lib/t4-devbox-packages ]; then
    apt-get update
    apt-get install -y build-essential git pkg-config fio nvme-cli \
        linux-tools-common "linux-tools-$(uname -r)" nix-setup-systemd
    echo "experimental-features = nix-command flakes" >> /etc/nix/nix.conf
    systemctl restart nix-daemon
    touch /var/lib/t4-devbox-packages
fi
# The daemon admits the nix-users group alone: AWS's login, ubuntu, joins it.
if id ubuntu >/dev/null 2>&1; then
    usermod -aG nix-users ubuntu
fi
