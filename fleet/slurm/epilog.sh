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

# Both variables below are overridable for the tests, and for nothing else.
# SLURM builds this script's environment out of its own SLURM_* variables, so
# nothing exported to slurmd -- or to the job -- is visible here.  Measured in
# the container smoke on 2026-09-04: an Epilog run with
# PRISMABUILD_EPILOG_DOCKER exported to slurmd still ran plain `docker`.  On a
# node the defaults are therefore what run, which is why they are the fleet's
# real job-state root and the real command name rather than placeholders.
# Where the job left its state file.  This is a NODE-side path, and it is the
# one thing this script and `slurm_job.py` have to agree on: the variable name
# and the default below are `slurm_lane.JOB_STATE_ROOT_ENV` and
# `slurm_lane.DEFAULT_JOB_STATE_ROOT`, spelled again here because a shell
# script cannot import that module.  It is deliberately NOT the submitter's
# lane root: a submitter that exported PRISMABUILD_SLURM_LANE_ROOT used to bake
# its own answer into the batch script while this script kept reading its
# default, the two disagreed, and node-side cleanup silently stopped happening.
JOB_STATE_ROOT="${PRISMABUILD_SLURM_JOB_STATE_ROOT:-/mnt/shared/prismabuild-fleet/slurm/jobs}"
DOCKER="${PRISMABUILD_EPILOG_DOCKER:-docker}"
LABEL="prismabuild.action"
# Who owns the files under the job-state root.  The job wrote them; this script runs
# as root; and dl380g10 exports that dataset without no_root_squash --
# measured 2026-09-04:
#
#     /storage_pool/shared 192.168.1.180(rw,async,no_subtree_check) \
#                          192.168.1.110(rw,async,no_subtree_check)
#
# so this root is `nobody` over NFS and its unlink fails.  Silently, because
# this script swallows every failure by design.  So every delete below the
# job-state root is performed as the job's own user instead.
JOB_USER="${SLURM_JOB_USER:-}"

log() { echo "prismabuild-epilog[${SLURM_JOB_ID:-?}]: $*" >&2; }

job_id="${SLURM_JOB_ID:-}"
if [ -z "$job_id" ]; then
    log "no SLURM_JOB_ID; nothing to clean up"
    exit 0
fi

state_file="${JOB_STATE_ROOT}/${job_id}.job"
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

# -- the state file ----------------------------------------------------------
# On the shared mount, written by the job's user, deleted here.  As the job's
# user, for the reason above: root's unlink is squashed to `nobody` and fails,
# and the symptom of that is not an error but a `jobs/` directory that fills up
# for months.  Falling back to a plain unlink keeps a job-state root that is
# NOT on NFS -- a single-box deployment, the container smoke -- working
# unchanged.
lane_delete() {
    if [ -n "$JOB_USER" ] && command -v runuser >/dev/null 2>&1; then
        if runuser -u "$JOB_USER" -- rm -f -- "$1" 2>/dev/null; then
            # Said out loud because the fallback below is silent and correct
            # on a non-NFS lane root: without this line a run where
            # SLURM_JOB_USER was never set looks exactly like a run where the
            # squash-safe path worked, and the smoke could not tell them
            # apart.
            log "removed state file $1 as $JOB_USER"
            return 0
        fi
        log "could not remove $1 as $JOB_USER; trying as $(id -un)"
    fi
    rm -f -- "$1" 2>/dev/null || true
}

lane_delete "$state_file"
if [ -e "$state_file" ]; then
    # Say it rather than exit non-zero: a non-zero Epilog drains the node, and
    # a state file nobody could delete is not a reason to take a box out of the
    # fleet.  It IS a reason for somebody to read this line.
    log "state file $state_file survived cleanup; check the NFS export"
fi
exit 0
