#!/bin/bash
# Install the per-box PrismaBuild supervisor as a systemd user unit.
#
# Why a unit and not only the crontab: the crontab's `--ensure` line is a
# five-minute poll, so a reboot leaves the box out of the pool for up to five
# minutes and nothing reports it. On 2026-09-06 all three boxes booted at
# 10:59 and rejoined at 11:05. A unit under linger starts the supervisor at
# boot and restarts it within RestartSec if it exits, which turns that window
# into seconds. The crontab line stays as a backstop: `--ensure` exits 0
# quietly whenever a supervisor already owns the box, so the two cannot
# double up -- the supervisor's box-local flock is what enforces that.
#
# KillMode=process is load-bearing. Worker loops are spawned by the supervisor
# and land in its cgroup, but they are not children to recycle: a loop
# finishes its action under the generation that claimed it, and a replacement
# supervisor adopts the census instead of respawning. The default
# KillMode=control-group would SIGTERM every loop mid-action on any restart.
set -euo pipefail
if [ "$(id -u)" -eq 0 ]; then
    echo 'install_supervisor_unit.sh runs as rob, not root' >&2
    exit 1
fi
unit_dir=$HOME/.config/systemd/user
unit=$unit_dir/prismabuild-supervisor.service
mkdir -p "$unit_dir" "$HOME/tmp"
if [ -e "$unit" ]; then
    cp -a "$unit" "$unit.backup-$(date +%s)"
fi
cat > "$unit" <<'UNIT'
[Unit]
Description=PrismaBuild per-box worker supervisor
Documentation=file:///mnt/shared/prismabuild-fleet/repo/tools/supervise.py
After=network-online.target remote-fs.target
RequiresMountsFor=/mnt/shared
# The supervisor is expected to exit 0 in the --ensure no-op case and to exit
# when it re-execs onto a newly published runtime generation, so no restart
# budget may ever retire this unit. This directive is only honoured in [Unit];
# in [Service] it is silently ignored and the 10s/5 default stays in force.
StartLimitIntervalSec=0

[Service]
Type=simple
# --ensure is the idempotent form: it becomes this box's supervisor, or exits
# 0 when one already owns the box-local claim. Restart=always then makes it a
# thirty-second watchdog rather than the crontab's five-minute one. Never run
# the bare form here: without --ensure a losing invocation exits non-zero and
# the unit flaps.
ExecStart=/usr/bin/python3 /mnt/shared/prismabuild-fleet/repo/tools/supervise.py --ensure
Restart=always
RestartSec=30
# Signal only the supervisor. Worker loops in the cgroup keep their claims.
KillMode=process
# One log file, the path the runbook already names.
StandardOutput=append:/home/rob/tmp/pb-supervisor.log
StandardError=append:/home/rob/tmp/pb-supervisor.log

[Install]
WantedBy=default.target
UNIT
systemctl --user daemon-reload
systemctl --user enable prismabuild-supervisor.service
echo "installed $unit"
