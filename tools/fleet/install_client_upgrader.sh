#!/bin/bash
# One-time root enrollment delegates privileged client publication to this store.
set -euo pipefail
if [ "$(id -u)" -ne 0 ]; then
    echo 'install_client_upgrader.sh must run as root' >&2
    exit 1
fi
source_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
install -d -o root -g root -m 0755 /opt/prismabuild-resource-broker /etc/prismabuild /var/lib/prismabuild-client-upgrade
install -o root -g root -m 0644 "$source_dir/upgrade_client.py" /opt/prismabuild-resource-broker/upgrade_client.py
config=/etc/prismabuild/client-upgrade.json
if [ ! -e "$config" ]; then
    cat > "$config" <<'CONFIG'
{
  "reader_uid": 1000,
  "runtime": "/mnt/shared/prismabuild-fleet/repo",
  "generation_store": "/mnt/shared/prismabuild-fleet/runtime-generations",
  "install_dir": "/opt/prismabuild-resource-broker",
  "state_dir": "/var/lib/prismabuild-client-upgrade",
  "socket": "/run/prismabuild/resources.sock"
}
CONFIG
    chmod 0644 "$config"
fi
cat > /etc/systemd/system/prismabuild-client-upgrade.service <<'UNIT'
[Unit]
Description=Converge PrismaBuild worker clients to the published runtime
After=network-online.target prismabuild-resource-broker.service
Wants=network-online.target
RequiresMountsFor=/mnt/shared/prismabuild-fleet

[Service]
Type=oneshot
ExecStart=/usr/bin/python3 -I /opt/prismabuild-resource-broker/upgrade_client.py
UMask=0022
TimeoutStartSec=240
MemoryMax=128M
TasksMax=16
UNIT
cat > /etc/systemd/system/prismabuild-client-upgrade.timer <<'UNIT'
[Unit]
Description=Check PrismaBuild client versions every minute

[Timer]
OnBootSec=45s
OnUnitInactiveSec=60s
RandomizedDelaySec=10s
Unit=prismabuild-client-upgrade.service

[Install]
WantedBy=timers.target
UNIT
chmod 0644 /etc/systemd/system/prismabuild-client-upgrade.{service,timer}
systemctl daemon-reload
systemctl enable --now prismabuild-client-upgrade.timer
systemctl start prismabuild-client-upgrade.service
