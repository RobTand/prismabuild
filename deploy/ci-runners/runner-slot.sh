#!/bin/bash
set +x
set -euo pipefail
umask 077
unset GH_DEBUG ACTIONS_RUNNER_INPUT_JITCONFIG

slot=${1:?Usage: runner-slot.sh SLOT}
[[ "$slot" =~ ^[12]$ ]] || exit 1
[[ ${PB_CI_IMAGE:-} =~ ^sha256:[a-f0-9]{64}$ ]] || {
    echo 'PB_CI_IMAGE must be the installed immutable image ID' >&2
    exit 1
}
host=$(hostname -s)
architecture=$(uname -m)
options=()
case "$host:$architecture" in
    dl380g10:x86_64)
        role=pb-suite
        label=x64
        memory=6g
        options+=(--cpus=4 --cpuset-cpus 40-79)
        ;;
    sparky:aarch64|sparklina:aarch64)
        role=pb-gate
        label=arm64
        memory=8g
        # No cpuset controller is delegated here. systemd sets CPUAffinity.
        ;;
    *) echo 'Unsupported PB runner host or architecture' >&2; exit 1 ;;
esac
runner="pb-$host-slot-$slot-$(cat /proc/sys/kernel/random/uuid)"
registration=(gh api --method POST
    repos/RobTand/prismabuild/actions/runners/generate-jitconfig
    -f "name=$runner" -F runner_group_id=1 -f work_folder=_work
    -f 'labels[]=self-hosted' -f 'labels[]=linux' -f "labels[]=$label"
    -f 'labels[]=pb-fleet' -f "labels[]=$role" --jq .encoded_jit_config)
if [[ "$role" == pb-gate ]]; then
    printf -v remote_command '%q ' "${registration[@]}"
    registration=(ssh -o BatchMode=yes dl380g10 "$remote_command")
fi
# Neither the CLI's response nor its diagnostic body may reach the journal.
jit_config=$("${registration[@]}" 2>/dev/null) || {
    status=$?
    echo "JIT registration failed for slot $slot (exit $status); systemd will back off" >&2
    exit 1
}
[[ -n "$jit_config" && "$jit_config" != null && "$jit_config" =~ ^[A-Za-z0-9+/=_-]+$ ]] || {
    echo 'JIT registration returned no valid encoded config' >&2
    exit 1
}
# OCI runtimes can widen inherited task affinity when creating the container.
# Carry the service's mask into its entrypoint, without a host mount or socket.
affinity=
while read -r key value; do
    if [[ "$key" == Cpus_allowed_list: ]]; then affinity=$value; break; fi
done < /proc/self/status
[[ "$affinity" =~ ^[0-9,-]+$ ]] || exit 1
systemd-notify RESTART_RESET=1
echo "Starting $runner"
printf '%s\n' "$jit_config" | podman run --rm --interactive \
    --name "pb-ci-runner-$slot" --pull=never \
    --memory="$memory" --memory-swap="$memory" --pids-limit=1024 \
    --network=private --pid=private --ipc=private \
    --http-proxy=false --sdnotify=ignore --cap-drop=all \
    --security-opt=no-new-privileges --label "io.prismabuild.ci-slot=$slot" \
    "${options[@]}" "$PB_CI_IMAGE" "$affinity"
