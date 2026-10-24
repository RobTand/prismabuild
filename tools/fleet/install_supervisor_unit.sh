#!/bin/bash
# Install the per-box PrismaBuild supervisor as a systemd user unit.
#
# Why a unit and not only the crontab: the crontab's `--ensure` line is a
# five-minute poll, so a reboot leaves the box out of the pool for up to five
# minutes and nothing reports it. On 2026-09-06 all three boxes booted at
# 10:59 and rejoined at 11:05. A unit under linger starts the supervisor at
# boot and restarts it within RestartSec if it exits. Once this unit is
# installed, cron's --ensure defers even while the unit is stopped. The
# --systemd invocation below makes startup ownership explicit.
#
# KillMode=process is load-bearing. Worker loops are spawned by the supervisor
# and land in its cgroup. The supervisor forwards TERM to their exact PIDs,
# then waits for each current action and cleanup. Never signal payload groups
# or impose a shutdown deadline that can strand a claim.
set -euo pipefail
system_mode=false
print_only=false
cap_slice=
cap_cpus=
cap_memory=
while [ "$#" -gt 0 ]; do
    case "$1" in
        --system) system_mode=true; shift ;;
        --slice|--cpus|--memory-gb)
            if [ "$#" -lt 2 ]; then echo "missing value for $1" >&2; exit 2; fi
            case "$1" in
                --slice) cap_slice=$2 ;;
                --cpus) cap_cpus=$2 ;;
                --memory-gb) cap_memory=$2 ;;
            esac
            shift 2 ;;
        --print) print_only=true; shift ;;
        --help) echo 'Usage: install_supervisor_unit.sh [--system --slice NAME.slice --cpus CPU-LIST --memory-gb N] [--print]'; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done
if $system_mode; then
    if [[ ! $cap_slice =~ ^[A-Za-z0-9_]+\.slice$ || ! $cap_cpus =~ ^[0-9]+(-[0-9]+)?(,[0-9]+(-[0-9]+)?)*$ || ! $cap_memory =~ ^[1-9][0-9]*$ ]]; then
        echo 'system mode requires a top-level --slice, --cpus and --memory-gb' >&2
        exit 2
    fi
    system_units() {
        cat <<UNIT
# $cap_slice
[Unit]
Description=PrismaBuild aggregate host cap
[Slice]
AllowedCPUs=$cap_cpus
MemoryMax=${cap_memory}G
MemorySwapMax=0
CPUAccounting=yes
MemoryAccounting=yes
UNIT
    }
    service_dropin() {
        cat <<UNIT
[Service]
Slice=$cap_slice
AllowedCPUs=$cap_cpus
UNIT
    }
    broker_dropin() {
        cat <<UNIT
[Service]
Slice=$cap_slice
AllowedCPUs=$cap_cpus
Environment=PB_SCOPE_PARENT=$cap_slice
UNIT
    }
    supervisor_unit() {
        cat <<UNIT
[Unit]
Description=PrismaBuild per-box worker supervisor
After=network-online.target remote-fs.target prismabuild-resource-broker.service
RequiresMountsFor=/mnt/shared
StartLimitIntervalSec=0
[Service]
Type=simple
User=rob
Slice=$cap_slice
AllowedCPUs=$cap_cpus
ExecStart=/usr/bin/python3 /mnt/shared/prismabuild-fleet/repo/tools/supervise.py --ensure --systemd
Restart=always
RestartSec=30
KillMode=process
TimeoutStopSec=infinity
SendSIGKILL=no
StandardOutput=append:/home/rob/tmp/pb-supervisor.log
StandardError=append:/home/rob/tmp/pb-supervisor.log
[Install]
WantedBy=multi-user.target
UNIT
    }
    if $print_only; then
        system_units
        broker_dropin
        service_dropin
        service_dropin
        supervisor_unit
        exit 0
    fi
    if [ "$(id -u)" -ne 0 ]; then echo 'system mode must run as root' >&2; exit 1; fi
    for service in supervisor resource-broker client-upgrade movement-publish; do
        if systemctl is-active --quiet "prismabuild-$service.service"; then
            echo "drain this host and stop prismabuild-$service.service before installing the cap" >&2
            exit 1
        fi
    done
    if [ -e /home/rob/.config/systemd/user/prismabuild-supervisor.service ]; then
        echo 'disable and remove the old user supervisor unit before switching to system mode' >&2
        exit 1
    fi
    install -d /etc/prismabuild
    for service in resource-broker client-upgrade movement-publish; do
        install -d "/etc/systemd/system/prismabuild-$service.service.d"
    done
    install -d -o rob -g rob /home/rob/tmp
    for unit_file in "$cap_slice" prismabuild-supervisor.service prismabuild-resource-broker.service.d/10-host-cap.conf prismabuild-client-upgrade.service.d/10-host-cap.conf prismabuild-movement-publish.service.d/10-host-cap.conf; do
        if [ -e "/etc/systemd/system/$unit_file" ]; then cp -a "/etc/systemd/system/$unit_file" "/etc/systemd/system/$unit_file.backup-$(date +%s)"; fi
    done
    system_units > "/etc/systemd/system/$cap_slice"
    service_dropin > /etc/prismabuild/host-cap.conf
    for service in client-upgrade movement-publish; do
        install -m 0644 /etc/prismabuild/host-cap.conf "/etc/systemd/system/prismabuild-$service.service.d/10-host-cap.conf"
    done
    broker_dropin > /etc/systemd/system/prismabuild-resource-broker.service.d/10-host-cap.conf
    supervisor_unit > /etc/systemd/system/prismabuild-supervisor.service
    systemctl daemon-reload
    systemctl enable --now "$cap_slice" prismabuild-resource-broker.service prismabuild-supervisor.service
    exit 0
fi
if $print_only || [ -n "$cap_slice$cap_cpus$cap_memory" ]; then
    echo '--print and cap arguments require --system' >&2
    exit 2
fi
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
# --systemd distinguishes the service owner from cron's compatibility entry.
# --ensure still serializes a handover from an existing supervisor.
ExecStart=/usr/bin/python3 /mnt/shared/prismabuild-fleet/repo/tools/supervise.py --ensure --systemd
Restart=always
RestartSec=30
# The supervisor drains workers without signalling their action children.
KillMode=process
TimeoutStopSec=infinity
SendSIGKILL=no
# One log file, the path the runbook already names.
StandardOutput=append:/home/rob/tmp/pb-supervisor.log
StandardError=append:/home/rob/tmp/pb-supervisor.log

[Install]
WantedBy=default.target
UNIT
systemctl --user daemon-reload
systemctl --user enable --now prismabuild-supervisor.service
echo "installed $unit"
