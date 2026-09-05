#!/bin/bash
# Node-side cleanup after one PrismaBuild job.  Installed at /etc/slurm/epilog.sh
# and named by Epilog= in slurm.conf.  Runs as root on the compute node, with
# SLURM_JOB_ID set, after the job's processes are gone.
#
# It exists because things outlive the job, and this is the only thing that
# runs after every ending, killed or not.  It is therefore the single owner of
# node-side cleanup: the job runner leaves its state file in place and this
# script does both cleanups and then deletes the file.
#
# A Docker container started by an action is reparented to containerd-shim and
# runs under dockerd's cgroup: it survives a kill of every process group below
# the job, and no cgroup limit ever charged it -- and it survives a NORMAL
# ending just as completely, which is why this runs on both.  The one thing
# that connects it back to the job is the ownership label the fleet's Docker
# shim stamps on creation (prismabuild.action=<owner>), which is why the job
# writes that owner down before it starts work.
#
# A materialized checkout is removed by the job itself on the way out -- unless
# the job did not get a way out, which is exactly what a time limit is.  Both
# cases arrive here; the removal below is guarded on the tree still existing,
# so the job having done it already is not an error.
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
# The second label the shim stamps, naming the SLURM job the container was
# created inside.  The owner label is the ACTION's identity, and under the pull
# queue that was also one execution; SLURM has no claim, so two jobs of one
# action can run on one node and share the owner label.  Removing on the owner
# label alone therefore removed a sibling job's containers.  The shim reads the
# job id from its own cgroup, which is where proctrack/cgroup puts it and which
# nothing in the job can move itself out of.
JOB_LABEL="prismabuild.job"
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
    # Not a PrismaBuild job, or one whose launcher never got as far as writing
    # its state file.  Either way there is nothing recorded to clean up.
    exit 0
fi

field() { sed -n "s/^$1=//p" "$state_file" | head -n 1; }

owner="$(field container_owner)"
marker="$(field container_marker)"
container_job="$(field container_job)"
checkout_dir="$(field checkout_dir)"
local_root="$(field local_checkout_root)"

# Whether the ownership label has no containers left behind it.  Starts true:
# an action that started none is as clean as one whose containers were removed,
# and both are allowed to retire the marker below.
owner_settled=1

# -- containers --------------------------------------------------------------
# Matched by label, never by name or by image: the labels are the action's
# identity and this job's, and together they are the only thing that
# distinguishes this job's container from an identical one somebody else --
# including another job of the same action -- is using right now.
case "$owner" in
    "")
        ;;
    *[!0-9a-f]* | ?)
        log "container owner is not a 64-hex digest; refusing to match on it"
        owner_settled=0
        ;;
    *)
        if [ "${#owner}" -ne 64 ]; then
            log "container owner is not 64 characters; refusing to match on it"
            owner_settled=0
        elif [ -n "$container_job" ] && [ "$container_job" != "$job_id" ]; then
            # slurm_job writes its own SLURM_JOB_ID here, and the value goes
            # into a docker filter unquoted from a file in a directory every
            # job's user can write.  Anything but this job's id is refused
            # rather than passed to the daemon.
            log "state file names container job '$container_job' but this is job $job_id; refusing to match on it"
            owner_settled=0
        else
            # Both labels, ANDed by the daemon.  A state file written before
            # the job label existed -- a job that was already running when the
            # runtime generation rolled -- records no job id, and matching on
            # the owner alone is what this did for all of them; say so, because
            # in that window a sibling job's container can still be caught.
            if [ -n "$container_job" ]; then
                filters="--filter label=${LABEL}=${owner} --filter label=${JOB_LABEL}=${container_job}"
            else
                filters="--filter label=${LABEL}=${owner}"
                log "no job id recorded for ${owner:0:12}; matching on the owner label alone"
            fi
            # shellcheck disable=SC2086
            containers="$("$DOCKER" ps -aq $filters 2>/dev/null)"
            if [ -n "$containers" ]; then
                # shellcheck disable=SC2086
                if "$DOCKER" rm -f $containers >/dev/null 2>&1; then
                    log "removed containers for ${owner:0:12}: $(echo "$containers" | tr '\n' ' ')"
                else
                    log "could not remove containers for ${owner:0:12}"
                fi
                # Asked again rather than inferred from the exit status, the
                # way cleanup_action_containers asks: the marker below may be
                # retired only when the label has nothing left behind it.
            fi
            # Asked on the OWNER label alone, and asked whether or not this job
            # started anything: the marker below is the ACTION's, so a sibling
            # job's container has to keep it alive, and this job may have
            # started none at all while that sibling did.
            remaining="$("$DOCKER" ps -aq --filter "label=${LABEL}=${owner}" 2>/dev/null)"
            if [ -n "$remaining" ]; then
                owner_settled=0
                log "containers for ${owner:0:12} remain: $(echo "$remaining" | tr '\n' ' ')"
            fi
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
# $1 is the path; $2 names it for the log, because the smoke and the runbook
# read those lines back and "removed state file" has to keep meaning that one.
lane_delete() {
    if [ -n "$JOB_USER" ] && command -v runuser >/dev/null 2>&1; then
        if runuser -u "$JOB_USER" -- rm -f -- "$1" 2>/dev/null; then
            # Said out loud because the fallback below is silent and correct
            # on a non-NFS lane root: without this line a run where
            # SLURM_JOB_USER was never set looks exactly like a run where the
            # squash-safe path worked, and the smoke could not tell them
            # apart.
            log "removed $2 $1 as $JOB_USER"
            return 0
        fi
        log "could not remove $1 as $JOB_USER; trying as $(id -un)"
    fi
    rm -f -- "$1" 2>/dev/null || true
}

# -- the container-ownership marker ------------------------------------------
# The shim writes it on first container creation and the pull queue's
# `finish` unlinks it once the containers are gone.  Under SLURM nothing did,
# so `container-owners/` grew a file per containerized action and never shrank.
# Deleted here, in the same step and as the same user as the state file, and
# only once the label has no containers left -- a marker removed while a
# container still carries its label would tell the next reader the action never
# used Docker.
#
# Bounded the way the checkout removal is: an absolute path whose last
# component is exactly this action's own `<owner>.used`.  A cleanup that can be
# talked into deleting an arbitrary path is worse than a leaked file.
if [ "$owner_settled" -eq 1 ] && [ -n "$marker" ] && [ "${#owner}" -eq 64 ]; then
    case "$marker" in
        /*)
            if [ "${marker##*/}" = "${owner}.used" ]; then
                [ -e "$marker" ] && lane_delete "$marker" \
                    "container-ownership marker"
            else
                log "recorded marker $marker does not name ${owner:0:12}; left alone"
            fi
            ;;
        *)
            log "recorded marker $marker is not an absolute path; left alone"
            ;;
    esac
fi

lane_delete "$state_file" "state file"
if [ -e "$state_file" ]; then
    # Say it rather than exit non-zero: a non-zero Epilog drains the node, and
    # a state file nobody could delete is not a reason to take a box out of the
    # fleet.  It IS a reason for somebody to read this line.
    log "state file $state_file survived cleanup; check the NFS export"
fi
exit 0
