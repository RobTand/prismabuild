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

# Prepare and validate the collector file before changing either service. YAML
# is an installer-only dependency; the exporter and fleet remain stdlib-only.
netdata_job=/etc/netdata/go.d/prometheus.conf
candidate=
trap 'if [ -n "$candidate" ]; then rm -f -- "$candidate"; fi' EXIT
if [ -d /etc/netdata/go.d ]; then
    candidate=$(mktemp "${netdata_job}.pb-candidate.XXXXXX")
    "$python" - "$netdata_job" "$candidate" "$port" <<'PY'
import pathlib
import re
import sys

try:
    import yaml
except ImportError:
    sys.exit("Netdata configuration needs PyYAML in PBMETRICS_PYTHON (Debian/Ubuntu: python3-yaml); not installing")


class StrictLoader(yaml.SafeLoader):
    def construct_object(self, node, deep=False):
        kind = node.tag.removeprefix("tag:yaml.org,2002:")
        if kind not in {"map", "seq", "str", "int", "float", "bool", "null"}:
            raise ValueError(f"unsupported YAML tag: {node.tag}")
        # PyYAML uses YAML 1.1 scalar resolution. Refuse spellings that could
        # change meaning when Netdata reads the rewritten file as YAML 1.2.
        if kind == "bool" and node.value.lower() not in {"true", "false"}:
            raise ValueError("ambiguous YAML boolean; quote it or use true/false")
        if kind in {"int", "float"} and ":" in node.value:
            raise ValueError("ambiguous YAML sexagesimal number")
        if kind == "int" and not re.fullmatch(r"[+-]?(?:0|[1-9][0-9]*)", node.value):
            raise ValueError("integers must use decimal digits without leading zeros or separators")
        if kind == "float" and "_" in node.value:
            raise ValueError("numeric separators are unsupported")
        return super().construct_object(node, deep=deep)

    def compose_node(self, parent, index):
        if self.check_event(yaml.AliasEvent):
            raise ValueError("YAML aliases are ambiguous for this installer")
        return super().compose_node(parent, index)

    def construct_mapping(self, node, deep=False):
        seen = set()
        for key_node, _ in node.value:
            if key_node.tag != "tag:yaml.org,2002:str":
                raise ValueError("mapping keys must be strings; YAML merges are unsupported")
            key = self.construct_object(key_node, deep=deep)
            if key in seen:
                raise ValueError(f"duplicate YAML mapping key: {key}")
            seen.add(key)
        return super().construct_mapping(node, deep=deep)


try:
    source, target = map(pathlib.Path, sys.argv[1:3])
    port = int(sys.argv[3])
    if not 1 <= port <= 65535:
        raise ValueError("PBMETRICS_PORT must be between 1 and 65535")
    if source.is_symlink():
        raise ValueError("refusing a symlink collector configuration")
    original = source.read_text() if source.exists() else ""
    config = yaml.load(original, Loader=StrictLoader)
    if config is None:
        # An empty/comment-only file is safe; an explicit null document is not
        # a collector mapping and should not be silently replaced.
        if any(line.strip() and not line.lstrip().startswith("#") for line in original.splitlines()):
            raise ValueError("collector configuration must be a mapping")
        config = {}
    if not isinstance(config, dict):
        raise ValueError("collector configuration must be a mapping")
    jobs = config.setdefault("jobs", [])
    if not isinstance(jobs, list) or any(not isinstance(job, dict) for job in jobs):
        raise ValueError("jobs must be a sequence of mappings")
    existing = [job for job in jobs if job.get("name") == "prismabuild"]
    if len(existing) > 1:
        raise ValueError("multiple prismabuild jobs require manual reconciliation")
    if existing:
        output = original
    else:
        jobs.append({"name": "prismabuild", "url": f"http://127.0.0.1:{port}/metrics",
                     # The exporter caches its queue read for ten seconds.
                     "update_every": 10})
        output = yaml.safe_dump(config, sort_keys=False)
    if yaml.load(output, Loader=StrictLoader) != config:
        raise ValueError("collector YAML did not round-trip")
    target.write_text(output)
except (OSError, ValueError, yaml.YAMLError) as exc:
    sys.exit(f"invalid Netdata configuration: {exc}; not installing")
PY
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

# Replace only after strict parsing and preserve a unique recovery copy. A
# repeat install leaves the bytes and metadata alone when the job exists.
if [ -n "$candidate" ]; then
    if ! cmp -s "$netdata_job" "$candidate"; then
        if [ -e "$netdata_job" ]; then
            backup=$(mktemp "${netdata_job}.pb-before.XXXXXX")
            cp -a -- "$netdata_job" "$backup"
            chmod --reference="$netdata_job" "$candidate"
            chown --reference="$netdata_job" "$candidate"
        else
            chmod 0644 "$candidate"
        fi
        mv -- "$candidate" "$netdata_job"
        candidate=
    fi
    if ! systemctl restart netdata || ! systemctl is-active netdata; then
        echo "Netdata failed after installation; inspect its logs and $netdata_job (recovery copies: $netdata_job.pb-before.*)" >&2
        exit 1
    fi
else
    echo 'no /etc/netdata/go.d: exporter installed, nothing is retaining it' >&2
fi

systemctl is-active prismabuild-metrics.service
