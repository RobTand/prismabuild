#!/bin/bash
# Node-side cleanup after one PrismaBuild job.  Installed at /etc/slurm/epilog.sh
# and named by Epilog= in slurm.conf.  Runs as root on the compute node, with
# SLURM_JOB_ID set, after the job's processes are gone.
#
# It exists because two things outlive a job that was killed rather than ended.
#
# A Docker container started by an action is reparented to containerd-shim and
# runs under dockerd's cgroup: it survives a kill of every process group below
# the job, and no cgroup limit ever charged it.  The one thing that connects it
# back to the job is the ownership label the fleet's Docker shim stamps on
# creation (prismabuild.action=<owner>), which is why the job writes that owner
# down before it starts work.
#
# A materialized checkout is removed by the job itself on the way out -- unless
# the job did not get a way out, which is exactly what a time limit is.
#
# THIS SCRIPT ALWAYS EXITS 0.  A non-zero Epilog drains the node, and a cleanup
# that could not find a container must not take a box out of the fleet.  Every
# failure is reported to the slurmd log and swallowed.

set -u

LANE_ROOT="${PRISMABUILD_SLURM_LANE_ROOT:-/mnt/shared/prismabuild-fleet/slurm}"
DOCKER="${PRISMABUILD_EPILOG_DOCKER:-docker}"
LABEL="prismabuild.action"

log() { echo "prismabuild-epilog[${SLURM_JOB_ID:-?}]: $*" >&2; }

job_id="${SLURM_JOB_ID:-}"
if [ -z "$job_id" ]; then
    log "no SLURM_JOB_ID; nothing to clean up"
    exit 0
fi

state_file="${LANE_ROOT}/jobs/${job_id}.job"
if [ ! -f "$state_file" ]; then
    # The normal ending.  A job that finished on its own removed this file
    # itself, having already done both cleanups.
    exit 0
fi

field() { sed -n "s/^$1=//p" "$state_file" | head -n 1; }

owner="$(field container_owner)"
checkout_dir="$(field checkout_dir)"
local_root="$(field local_checkout_root)"

# -- containers --------------------------------------------------------------
# Matched by label, never by name or by image: the label is the action's
# complete identity and the only thing that distinguishes this job's container
# from an identical one somebody else is using right now.
case "$owner" in
    "")
        ;;
    *[!0-9a-f]* | ?)
        log "container owner is not a 64-hex digest; refusing to match on it"
        ;;
    *)
        if [ "${#owner}" -eq 64 ]; then
            containers="$("$DOCKER" ps -aq --filter "label=${LABEL}=${owner}" 2>/dev/null)"
            if [ -n "$containers" ]; then
                # shellcheck disable=SC2086
                if "$DOCKER" rm -f $containers >/dev/null 2>&1; then
                    log "removed containers for ${owner:0:12}: $(echo "$containers" | tr '\n' ' ')"
                else
                    log "could not remove containers for ${owner:0:12}"
                fi
            fi
        else
            log "container owner is not 64 characters; refusing to match on it"
        fi
        ;;
esac

# -- materialized checkout ---------------------------------------------------
# Bounded three ways before anything is removed: absolute, strictly below the
# recorded local checkout root, and not that root itself.  A cleanup that can be
# talked into an unbounded path is worse than a leaked directory.
if [ -n "$checkout_dir" ] && [ -n "$local_root" ]; then
    case "$checkout_dir" in
        "$local_root"/?*)
            if [ "$checkout_dir" != "$local_root" ] && [ -d "$checkout_dir" ]; then
                if rm -rf -- "$checkout_dir"; then
                    log "removed materialized checkout $checkout_dir"
                else
                    log "could not remove materialized checkout $checkout_dir"
                fi
            fi
            ;;
        *)
            log "recorded checkout $checkout_dir is not below $local_root; left alone"
            ;;
    esac
fi

rm -f -- "$state_file" 2>/dev/null || true
exit 0
