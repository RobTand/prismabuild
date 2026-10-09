#!/bin/bash
# One-time root enrollment of a host in automatic movement publication (#1659).
#
# After this runs, the host copies each live runtime generation into the
# root-owned /opt/prismabuild/movement-generations by itself, once a minute,
# with no person and no root step per generation. A gang's reservation
# (#1579) trusts a movement tool only from such a copy; until the copy of the
# generation a process runs exists, the host keeps the behaviour it had before
# the reservation, and nothing is refused.
#
# Like the client upgrader (docs/client_upgrade.md), this delegates one act to
# the publisher of the generation store: root copies what the store's live
# pointer names, after checking every member against the receipt. The receipt
# proves copy consistency, not publisher authenticity, so access to publish
# generations must stay with the principals that administer these hosts.
#
# Stage this file and runtime_publication.py on local storage as the
# publishing user (NFS root squash stays on), then run it as root:
#   dir=$(mktemp -d /tmp/pb-movement-enrollment.XXXXXX)
#   cp /mnt/shared/prismabuild-fleet/repo/tools/fleet/install_movement_publisher.sh \
#      /mnt/shared/prismabuild-fleet/repo/src/prismabuild/runtime_publication.py "$dir/"
#   sudo bash "$dir/install_movement_publisher.sh"
set -euo pipefail
umask 022
if [ "$(id -u)" -ne 0 ]; then
    echo 'install_movement_publisher.sh must run as root' >&2
    exit 1
fi
source_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
module="$source_dir/runtime_publication.py"
if [ ! -f "$module" ]; then
    echo 'runtime_publication.py must sit beside this installer' >&2
    exit 1
fi
# Root custody is the point: refuse an install under an ancestor that anyone
# else can write or that is a link.
for ancestor in /opt /etc /var/lib; do
    if [ -L "$ancestor" ] || [ "$(stat -c %u "$ancestor")" -ne 0 ] \
            || [ $(( 0$(stat -c %a "$ancestor") & 0022 )) -ne 0 ]; then
        echo "$ancestor has no root custody; refusing" >&2
        exit 1
    fi
done
install -d -o root -g root -m 0755 /opt/prismabuild
install -d -o root -g root -m 0755 /opt/prismabuild/movement-generations
install -d -o root -g root -m 0755 /etc/prismabuild
install -d -o root -g root -m 0755 /var/lib/prismabuild-movement-publish
install -o root -g root -m 0644 "$module" /opt/prismabuild/runtime_publication.py
config=/etc/prismabuild/movement-publish.json
if [ ! -e "$config" ]; then
    cat > "$config" <<'CONFIG'
{
  "runtime": "/mnt/shared/prismabuild-fleet/repo",
  "generation_store": "/mnt/shared/prismabuild-fleet/runtime-generations",
  "status": "/var/lib/prismabuild-movement-publish/status.json"
}
CONFIG
    chmod 0644 "$config"
fi
cat > /etc/systemd/system/prismabuild-movement-publish.service <<'UNIT'
[Unit]
Description=Publish the live PrismaBuild runtime generation's protected movement copy
After=network-online.target
Wants=network-online.target
RequiresMountsFor=/mnt/shared/prismabuild-fleet

[Service]
Type=oneshot
ExecStart=/usr/bin/python3 -I /opt/prismabuild/runtime_publication.py
UMask=0022
TimeoutStartSec=240
MemoryMax=256M
TasksMax=16
NoNewPrivileges=yes
PrivateTmp=yes
UNIT
cat > /etc/systemd/system/prismabuild-movement-publish.timer <<'UNIT'
[Unit]
Description=Converge the protected movement copy on the live PrismaBuild runtime every minute

[Timer]
OnBootSec=45s
OnUnitInactiveSec=60s
RandomizedDelaySec=10s
Unit=prismabuild-movement-publish.service

[Install]
WantedBy=timers.target
UNIT
chmod 0644 /etc/systemd/system/prismabuild-movement-publish.{service,timer}
systemctl daemon-reload
systemctl enable --now prismabuild-movement-publish.timer
systemctl start prismabuild-movement-publish.service || true
echo 'enrolled: see /var/lib/prismabuild-movement-publish/status.json'
