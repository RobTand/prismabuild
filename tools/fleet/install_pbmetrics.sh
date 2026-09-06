#!/bin/bash
# Install as root. Runs the read-only queue exporter and lets Netdata retain it.
#
# The exporter answers "what has been happening", which is the question every
# expensive queue investigation turns out to be. Answering it needs a series,
# and a series needs two things this fleet did not have: something running the
# exporter, and something keeping what it says. Neither existed -- `pbmetrics`
# was written, documented and never started, so 9469 was not listening and no
# scraper knew about it.
#
# Netdata is the store rather than a file appended under the queue, because the
# queue is the thing being observed: writing history onto the shared mount adds
# load to the resource whose contention is the most common thing you are trying
# to see, and loses the history exactly when the mount is the problem. Netdata
# is already on these boxes, already retains, and already runs anomaly detection
# on every series it holds.
set -euo pipefail
if [ "$(id -u)" -ne 0 ]; then
    echo 'install_pbmetrics.sh must run as root' >&2
    exit 1
fi

runtime=${PBMETRICS_RUNTIME:-/mnt/shared/prismabuild-fleet/repo}
exporter=$runtime/tools/fleet/pbmetrics.py
queue=${PBMETRICS_QUEUE:-/mnt/shared/prismabuild-fleet/pb-queue}
port=${PBMETRICS_PORT:-9469}
# The queue is read as its owner; the exporter never writes to it, but it must
# be able to read every worker's reservations subtree.
user=${PBMETRICS_USER:-rob}
python=${PBMETRICS_PYTHON:-/usr/bin/python3}

for path in "$exporter" "$queue" "$python"; do
    if [ ! -e "$path" ]; then
        echo "missing $path; refusing an incomplete installation" >&2
        exit 1
    fi
done
if ! id "$user" >/dev/null 2>&1; then
    echo "no such user $user; refusing an incomplete installation" >&2
    exit 1
fi
# Prove the exporter runs against this queue before installing a unit that
# would otherwise restart-loop in the background.
if ! sudo -u "$user" "$python" "$exporter" --once --queue-root "$queue" >/dev/null; then
    echo 'exporter failed its own snapshot; not installing' >&2
    exit 1
fi

unit=/etc/systemd/system/prismabuild-metrics.service
if [ -e "$unit" ]; then
    cp -a "$unit" "$unit.backup-$(date +%s)"
fi
cat > "$unit" <<UNIT
[Unit]
Description=PrismaBuild pull-queue metrics exporter
After=local-fs.target remote-fs.target

[Service]
Type=simple
User=$user
# Bound to loopback: the only intended reader is this box's Netdata. A remote
# scraper is a deliberate change, not a default, because the exporter reports
# the whole fleet's queue and every box can run its own.
ExecStart=$python $exporter --listen 127.0.0.1 --port $port --queue-root $queue
Restart=on-failure
RestartSec=5
# Read-only by construction, and confined to match. The exporter walks the
# queue and writes nothing anywhere; a unit that could write would be a
# standing invitation for it to grow a cache on the mount it observes.
ProtectSystem=strict
ProtectHome=read-only
PrivateTmp=yes
NoNewPrivileges=yes
MemoryMax=256M
TasksMax=64

[Install]
WantedBy=multi-user.target
UNIT
chmod 0644 "$unit"
systemctl daemon-reload
systemctl enable --now prismabuild-metrics.service

# Netdata retains the series. Its own directory is left alone apart from this
# one job file, and an existing one is kept beside the new copy.
netdata_job=/etc/netdata/go.d/prometheus.conf
if [ -d /etc/netdata/go.d ]; then
    if [ -e "$netdata_job" ]; then
        cp -a "$netdata_job" "$netdata_job.pb-before-$(date +%Y%m%dT%H%M)"
    fi
    if ! grep -q 'name: prismabuild' "$netdata_job" 2>/dev/null; then
        cat >> "$netdata_job" <<JOB
jobs:
  - name: prismabuild
    url: 'http://127.0.0.1:$port/metrics'
    # The exporter caches its queue read for ten seconds, so a faster scrape
    # buys repetition rather than resolution.
    update_every: 10
JOB
    fi
    systemctl restart netdata 2>/dev/null || true
else
    echo 'no /etc/netdata/go.d: exporter installed, nothing is retaining it' >&2
fi

systemctl is-active prismabuild-metrics.service
