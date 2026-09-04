#!/bin/bash
# A `docker` that runs no containers and writes down what it was asked to do.
#
# The Epilog removes a killed job's containers by the ownership label the
# fleet's Docker shim stamps.  What the smoke has to establish is that the
# Epilog *ran* and *matched on that label*, not that Docker works -- so this
# stands in for it: every invocation is appended to the log named by
# PB_SMOKE_DOCKER_LOG, and a `ps` filtered by a label answers with one
# plausible container id so that the `rm -f` branch is reached too.
set -u

# The default, not the environment variable, is what matters: SLURM builds the
# Epilog's environment from its own SLURM_* variables, so nothing this container
# exports to slurmd is visible when the Epilog calls this.
log="${PB_SMOKE_DOCKER_LOG:-/mnt/shared/docker.log}"
mkdir -p "$(dirname "$log")" 2>/dev/null
printf '%s\n' "$*" >>"$log" 2>/dev/null
chmod 0666 "$log" 2>/dev/null

for arg in "$@"; do
    case "$arg" in
        label=prismabuild.action=*)
            # One container, so the Epilog's `rm -f` branch is exercised.
            echo "cafebabe0001"
            exit 0
            ;;
    esac
done
exit 0
