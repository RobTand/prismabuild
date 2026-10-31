#!/bin/bash
set +x
set -euo pipefail
IFS= read -r jit_config
[[ -n "$jit_config" ]] || exit 1
export ACTIONS_RUNNER_INPUT_JITCONFIG="$jit_config"
unset jit_config
exec /home/runner/bin/Runner.Listener run
