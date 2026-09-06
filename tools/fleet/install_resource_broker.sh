#!/bin/bash
# Install as root. Keep privileged Python and every ancestor root-owned.
set -euo pipefail
if [ "$(id -u)" -ne 0 ]; then
    echo 'install_resource_broker.sh must run as root' >&2
    exit 1
fi
# Never replace one half of a running broker/helper pair. For upgrades, drain
# PB on this host, verify its job groups are empty, then stop this service.
if systemctl is-active --quiet prismabuild-resource-broker.service; then
    echo 'broker already active: drain host work and stop the service before upgrading' >&2
    exit 1
fi
source_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
gpu_source=
for candidate in "$source_dir/gpu_memory.py" \
    "$source_dir/../../src/prismabuild/gpu_memory.py" \
    "$source_dir/../src/prismabuild/gpu_memory.py"; do
    if [ -f "$candidate" ]; then
        gpu_source=$candidate
        break
    fi
done
if [ -z "$gpu_source" ]; then
    echo 'missing resource monitor module; refusing an incomplete installation' >&2
    exit 1
fi
install -d -o root -g root -m 0755 /opt/prismabuild-resource-broker
for source_file in resource_broker.py resource_payload.py; do
    install -o root -g root -m 0644 "$source_dir/$source_file" "/opt/prismabuild-resource-broker/$source_file"
done
install -o root -g root -m 0644 "$gpu_source" /opt/prismabuild-resource-broker/gpu_memory.py
unit=/etc/systemd/system/prismabuild-resource-broker.service
if [ -e "$unit" ]; then
    cp -a "$unit" "$unit.backup-$(date +%s)"
fi
cat > "$unit" <<'UNIT'
[Unit]
Description=PrismaBuild per-job resource authority
After=local-fs.target

[Service]
Type=simple
ExecStart=/usr/bin/python3 -I /opt/prismabuild-resource-broker/resource_broker.py --uid 1000
Restart=on-failure
RestartSec=1
RuntimeDirectory=prismabuild
RuntimeDirectoryMode=0755
RuntimeDirectoryPreserve=yes
UMask=0077
NoNewPrivileges=yes
MemoryMax=256M
TasksMax=1024
OOMScoreAdjust=-900

[Install]
WantedBy=multi-user.target
UNIT
chmod 0644 "$unit"
systemctl daemon-reload
systemctl enable --now prismabuild-resource-broker.service
systemctl is-active prismabuild-resource-broker.service
