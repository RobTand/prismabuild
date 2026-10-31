#!/bin/bash
set +x
set -euo pipefail
# Restore the systemd service's affinity after the OCI runtime initializes.
if [[ -n ${1:-} ]]; then
    exec taskset --cpu-list "$1" /bin/bash "${BASH_SOURCE[0]}"
fi
IFS= read -r jit_config
[[ -n "$jit_config" ]] || exit 1
export ACTIONS_RUNNER_INPUT_JITCONFIG="$jit_config"
unset jit_config
exec /home/runner/bin/Runner.Listener run
