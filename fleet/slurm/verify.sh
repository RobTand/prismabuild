#!/bin/bash
# Step 8 of docs/slurm_runbook_2026-09-04.md, as an executable.
#
# Run it as rob, from any of the three boxes, after fleet/slurm/install.sh has
# been run on all three:
#
#     fleet/slurm/verify.sh
#
# No sudo.  Nothing here writes outside the SLURM lane's own directory and
# rob's home.
#
# The rows are ordered so the first failure is the most informative one: a node
# that never registered fails row 1 rather than making every later row time out
# against a fleet that is one box short.  The script stops at the first
# failure, because the rows after a failure are being run against a fleet in a
# state nobody described.
#
# Two rows are worth reading before you run them.
#
# Row 4 is the claim no container could test.  `ConstrainDevices=yes` plus
# cgroup_allowed_devices_file.conf are supposed to mean that a job which
# reserved no GRES cannot reach the GPU, and the smoke had neither devices nor
# a GPU to prove it with.  So row 4 is written to discriminate rather than to
# assert: it checks that the job *ran*, and that it ran *on a Spark*, before it
# reads anything into a GPU it could not see.  A job that failed to launch also
# sees no GPU, and that is not device containment, it is a broken lane.  It
# asks the question twice, once through `nvidia-smi -L` and once by opening
# /dev/nvidia0 directly, because those can disagree: nvidia-smi may enumerate
# through /dev/nvidiactl, which stays allowed by design.
#
# Row 8 reads the lane's `jobs/` directory back out.  A `<job id>.job` file
# left behind for a job that has finished means the Epilog could not delete it,
# which on this fleet means root_squash on the NFS export.  Files belonging to
# jobs that are still in the queue are not leftovers and are skipped.
#
# On success it writes a marker at ~/.prismabuild/slurm-verify-passed.json,
# which fleet/slurm/cutover.sh looks for.  The marker is box-local -- it says
# "verification passed and this is where it was run from" -- which is why
# cutover also accepts --verified for the case where you verified elsewhere.

set -uo pipefail

usage() {
    cat <<'USAGE'
usage: verify.sh [--keep-going]

Runs the fleet's SLURM installation through the runbook's step-8 checks and
prints PASS or FAIL per row.  Exits non-zero on the first failure.

  --keep-going   run every row even after one fails; still exits non-zero

Environment, for the tests and for nothing else:
  PRISMABUILD_SLURM_LANE_ROOT  the lane root (default /mnt/shared/prismabuild-fleet/slurm)
  PB_VERIFY_TIMEOUT_S          wall-clock bound per job-running row (default 900)

On success it writes ~/.prismabuild/slurm-verify-passed.json.  That marker is
box-local; fleet/slurm/cutover.sh accepts --verified instead when you verified
from another box.
USAGE
}

KEEP_GOING=0
while [ $# -gt 0 ]; do
    case "$1" in
        --keep-going) KEEP_GOING=1 ;;
        -h|--help) usage; exit 0 ;;
        *) printf 'verify.sh: unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
    esac
    shift
done

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CONF="$REPO/fleet/slurm/slurm.conf"
LANE_ROOT="${PRISMABUILD_SLURM_LANE_ROOT:-/mnt/shared/prismabuild-fleet/slurm}"
NODES="dl380g10 sparky gx10-6b77"
SPARKS="sparky gx10-6b77"
#: A bound on one verification step, so a fleet with a down node fails a row
#: instead of hanging.  It is not a bound on any fleet *work*: nothing this
#: script submits does any.
LIMIT="${PB_VERIFY_TIMEOUT_S:-900}"
#: Every job runs from a directory all three boxes have.  A job whose --chdir
#: exists on the submitter only dies on the node with a message about the
#: directory, which reads like a lane defect and is not one.
ANYWHERE=/home/rob
MARKER_DIR="$HOME/.prismabuild"
MARKER="$MARKER_DIR/slurm-verify-passed.json"

PASSED=0
FAILED=0
RESULTS=()

verdict() {
    local mark="$1" id="$2" claim="$3" detail="$4"
    printf '[%s] %-4s %s\n' "$mark" "$id" "$claim"
    if [ -n "$detail" ]; then
        printf '%s\n' "$detail" | sed 's/^/          /'
    fi
    RESULTS+=("$mark $id $claim")
}

pass() { PASSED=$((PASSED + 1)); verdict PASS "$1" "$2" "${3:-}"; }

fail() {
    FAILED=$((FAILED + 1))
    verdict FAIL "$1" "$2" "${3:-}"
    [ "$KEEP_GOING" = 1 ] || finish
}

finish() {
    printf '\n%s\n' "$(printf '=%.0s' $(seq 1 72))"
    printf 'prismabuild SLURM verification: %d passed, %d failed\n' "$PASSED" "$FAILED"
    printf '%s\n' "$(printf '=%.0s' $(seq 1 72))"
    for line in "${RESULTS[@]}"; do printf '%s\n' "$line"; done
    if [ "$FAILED" -gt 0 ]; then
        printf '\nverification did not pass; no marker written\n'
        exit 1
    fi
    mkdir -p "$MARKER_DIR"
    local commit
    commit="$(git -C "$REPO" rev-parse --verify HEAD 2>/dev/null || echo unknown)"
    cat > "$MARKER" <<EOF
{
 "schema": "prismaquant.prismabuild.slurm_verify.v1",
 "host": "$(hostname -s)",
 "verified_unix": $(date +%s),
 "checkout": "$REPO",
 "commit": "$commit",
 "slurm_conf_sha256": "$(sha256sum "$CONF" | cut -d' ' -f1)",
 "rows": $PASSED
}
EOF
    printf '\nwrote %s\n' "$MARKER"
    printf 'the fleet is ready for fleet/slurm/cutover.sh\n'
    exit 0
}

# -- reading the configuration this fleet is supposed to have ----------------

node_stanza() {
    awk '
        /\\$/ { sub(/\\$/, "", $0); buf = buf $0 " "; next }
        { print buf $0; buf = "" }
    ' "$CONF" \
        | sed 's/^[[:space:]]*//' \
        | grep "^NodeName=$1[[:space:]]" \
        | head -n 1
}

stanza_field() {
    printf '%s\n' "$1" | tr ' ' '\n' | sed -n "s/^$2=//p" | head -n 1
}

#: A comma-separated list, sorted, so a reordering is not a failure.
sorted_list() {
    printf '%s\n' "$1" | tr ',' '\n' | sed '/^$/d' | LC_ALL=C sort | paste -sd, -
}

#: scontrol's own value for one field of one node.
node_field() {
    scontrol show node "$1" --oneliner 2>/dev/null \
        | tr ' ' '\n' | sed -n "s/^$2=//p" | head -n 1
}

srun_here() {
    timeout "$LIMIT" srun --chdir="$ANYWHERE" --time=00:05:00 "$@" 2>&1
}

printf 'prismabuild SLURM verification\n'
printf 'run from : %s as %s\n' "$(hostname -s)" "$(id -un)"
printf 'checkout : %s\n' "$REPO"
printf 'lane root: %s\n\n' "$LANE_ROOT"

if ! command -v sinfo >/dev/null 2>&1; then
    printf 'verify.sh: sinfo is not on PATH; run fleet/slurm/install.sh on this box first\n' >&2
    exit 2
fi

# -- row 0: this box runs the configuration in the checkout ------------------

if [ -r /etc/slurm/slurm.conf ]; then
    if [ "$(sha256sum < /etc/slurm/slurm.conf)" = "$(sha256sum < "$CONF")" ]; then
        pass 0 "this box's /etc/slurm/slurm.conf is the one in the checkout"
    else
        fail 0 "this box's /etc/slurm/slurm.conf is the one in the checkout" \
            "installed and checked-in copies differ; re-run install.sh here and on every box, then restart slurmd"
    fi
else
    fail 0 "this box's /etc/slurm/slurm.conf is the one in the checkout" \
        "/etc/slurm/slurm.conf is not readable"
fi

# -- row 1: three nodes, registered, idle, offering what they declare --------

down=""
for node in $NODES; do
    state="$(node_field "$node" State)"
    case "$state" in
        IDLE|MIXED|ALLOCATED) ;;
        "") down="$down $node(unregistered)" ;;
        *) down="$down $node($state)" ;;
    esac
done
if [ -n "$down" ]; then
    fail 1a "all three nodes are registered and idle" \
        "not usable:$down
sinfo -N -l says:
$(sinfo -N -l 2>&1 | sed -n '1,8p')
read /var/log/slurm/slurmd.log on the box that is down"
else
    pass 1a "all three nodes are registered and idle" "$(sinfo -h -N -o '%N %T %G' | tr '\n' '; ')"
fi

for node in $NODES; do
    stanza="$(node_stanza "$node")"
    want="$(stanza_field "$stanza" Gres)"
    got="$(node_field "$node" Gres)"
    [ "$got" = "(null)" ] && got=""
    if [ "$(sorted_list "$want")" = "$(sorted_list "$got")" ]; then
        pass "1b" "$node offers exactly the Gres slurm.conf declares" \
            "${want:-none}"
    else
        fail "1b" "$node offers exactly the Gres slurm.conf declares" \
            "slurm.conf declares '${want:-none}', the controller reports '${got:-none}'"
    fi
done

for node in $NODES; do
    stanza="$(node_stanza "$node")"
    want="$(sorted_list "$(stanza_field "$stanza" Feature)")"
    got="$(sorted_list "$(node_field "$node" AvailableFeatures)")"
    if [ "$want" != "$got" ]; then
        fail "1c" "$node advertises exactly the Features slurm.conf declares" \
            "slurm.conf declares '$want', the controller reports '$got'"
    elif ! printf '%s\n' "$got" | tr ',' '\n' | grep -qx "$node"; then
        fail "1c" "$node advertises exactly the Features slurm.conf declares" \
            "'$node' is not among its own Features; every action pinned to this box would be unschedulable"
    else
        pass "1c" "$node advertises exactly the Features slurm.conf declares" "$got"
    fi
done

# -- row 2: a job runs on each hardware partition ----------------------------
#
# --output=/dev/null on purpose: sbatch writes the job's output on the node
# that runs it, so a path under the submitter's home would be a second thing
# that can fail and would fail on the wrong box.  `sbatch --wait` exits with
# the job's own exit code, which is the claim.

for partition in cpu gpu; do
    # SLURM_JOB_PARTITION is the node's variable, not this shell's: the single
    # quotes are the point, and so is the disable below.
    # shellcheck disable=SC2016
    if out="$(timeout "$LIMIT" sbatch --wait --partition="$partition" \
        --chdir="$ANYWHERE" --output=/dev/null --error=/dev/null \
        --time=00:05:00 --wrap='hostname; echo $SLURM_JOB_PARTITION' 2>&1)"; then
        pass "2-$partition" "sbatch --wait runs a job on partition $partition" "$out"
    else
        fail "2-$partition" "sbatch --wait runs a job on partition $partition" \
            "$out
re-run it as srun to see the job's own output:
  srun --partition=$partition --chdir=$ANYWHERE hostname"
    fi
done

# -- row 3: a shard allocation reaches the GPU -------------------------------

out="$(srun_here --partition=gpu --gres=shard:1 nvidia-smi -L)"
if printf '%s\n' "$out" | grep -q '^GPU 0:'; then
    pass 3 "a --gres=shard:1 job sees the GPU" "$out"
else
    fail 3 "a --gres=shard:1 job sees the GPU" \
        "$out
if this fails at CUDA init rather than at scheduling, the first place to look
is /etc/slurm/cgroup_allowed_devices_file.conf: the NVIDIA control interfaces
listed there are what CUDA needs even for a job that WAS granted the GPU"
fi

# -- row 4: a job that reserved no GRES cannot reach it ----------------------
#
# The discriminating part is the first two assertions.  A job that never ran
# also fails to see a GPU.

# Every expansion below happens on the compute node, which is the whole
# question this row asks; nothing here may expand in this shell.
# shellcheck disable=SC2016
probe='echo "node=$(hostname -s)"
nvidia-smi -L 2>&1 | sed "s/^/smi: /"
echo "smi-rc=$?"
if : < /dev/nvidia0 2>/dev/null; then echo "open=/dev/nvidia0 OPENED"; else echo "open=/dev/nvidia0 denied"; fi
exit 0'
out="$(srun_here --partition=gpu bash -c "$probe")"
node="$(printf '%s\n' "$out" | sed -n 's/^node=//p' | head -n 1)"
opened="$(printf '%s\n' "$out" | sed -n 's/^open=//p' | head -n 1)"
saw_gpu=no
printf '%s\n' "$out" | grep -q '^smi: GPU 0:' && saw_gpu=yes
if [ -z "$node" ]; then
    fail 4 "a job with no GRES runs on a Spark and cannot reach the GPU" \
        "the job did not run, so this says nothing about device containment:
$out"
elif ! printf '%s\n' "$SPARKS" | tr ' ' '\n' | grep -qx "$node"; then
    fail 4 "a job with no GRES runs on a Spark and cannot reach the GPU" \
        "the job landed on '$node', which has no GPU to be denied; it says nothing about containment"
elif [ "$saw_gpu" = yes ] || [ "$opened" = "/dev/nvidia0 OPENED" ]; then
    fail 4 "a job with no GRES runs on a Spark and cannot reach the GPU" \
        "the job ran on $node and reached the GPU anyway:
$out
ConstrainDevices=yes in /etc/slurm/cgroup.conf is what is supposed to stop
this.  Check that cgroup.conf is installed on $node and that slurmd was
restarted after it was."
else
    pass 4 "a job with no GRES runs on a Spark and cannot reach the GPU" \
        "ran on $node; nvidia-smi listed no GPU; $opened"
fi

# -- row 5: the cgroup the worker attests against ----------------------------

# SLURM_JOB_ID is the job's, read inside the job.
# shellcheck disable=SC2016
out="$(srun_here --partition=gpu --gres=shard:1 bash -c 'grep -c "job_$SLURM_JOB_ID" /proc/self/cgroup')"
if [ "$(printf '%s\n' "$out" | tail -n 1)" = "1" ]; then
    pass 5 "a job's processes are in a job_<id> cgroup" "$out"
else
    fail 5 "a job's processes are in a job_<id> cgroup" \
        "expected 1, got: $out
core._collect_worker_evidence attests SLURM_JOB_ID against this membership and
refuses every action without it.  ProctrackType in /etc/slurm/slurm.conf is
what to look at."
fi

# -- row 6: placement -------------------------------------------------------

out="$(srun_here --gres=shard:1 hostname -s)"
node="$(printf '%s\n' "$out" | tail -n 1)"
if printf '%s\n' "$SPARKS" | tr ' ' '\n' | grep -qx "$node"; then
    pass 6a "a shard job in the default partition lands on a Spark" "$node"
else
    fail 6a "a shard job in the default partition lands on a Spark" "landed on '$node': $out"
fi

out="$(timeout "$LIMIT" srun --chdir="$ANYWHERE" --time=00:05:00 \
    --partition=cpu hostname -s 2>&1)"
node="$(printf '%s\n' "$out" | tail -n 1)"
if [ "$node" = dl380g10 ]; then
    pass 6b "a CPU job with no constraint lands on dl380g10" "$node"
else
    fail 6b "a CPU job with no constraint lands on dl380g10" "landed on '$node': $out"
fi

# The lane never names a partition: pbrun turns tags into --constraint, and
# this is that path across boxes rather than within one partition.
out="$(srun_here --constraint=x86 hostname -s)"
node="$(printf '%s\n' "$out" | tail -n 1)"
if [ "$node" = dl380g10 ]; then
    pass 6c "--constraint=x86 in the default partition lands on dl380g10" "$node"
else
    fail 6c "--constraint=x86 in the default partition lands on dl380g10" "landed on '$node': $out"
fi

# -- row 7: one real action through the lane ---------------------------------

out="$(cd "$REPO" && timeout "$LIMIT" tools/fleet/pbrun.py --transport slurm \
    --here --timeout-s 600 -- /bin/echo hello from slurm 2>&1)"
status=$?
if [ $status -eq 0 ] && printf '%s\n' "$out" | grep -q 'executed via slurm job'; then
    pass 7 "pbrun --transport slurm --here runs an action end to end" \
        "$(printf '%s\n' "$out" | tail -n 3)"
else
    fail 7 "pbrun --transport slurm --here runs an action end to end" \
        "exit $status
$out
--here is load-bearing: the job execs this checkout's slurm_job.py, which
exists on this box only.  A cross-box submission is meaningful after the
runtime generation is published, from the published path."
fi

# -- row 8: the Epilog left nothing behind -----------------------------------

leftovers=""
if [ -d "$LANE_ROOT/jobs" ]; then
    for state in "$LANE_ROOT/jobs"/*.job; do
        [ -e "$state" ] || continue
        id="$(basename "$state" .job)"
        # A job still in the queue owns its state file; it is not a leftover.
        if squeue -h -j "$id" >/dev/null 2>&1 && [ -n "$(squeue -h -j "$id" 2>/dev/null)" ]; then
            continue
        fi
        leftovers="$leftovers $id"
    done
fi
if [ -n "$leftovers" ]; then
    fail 8 "the lane's jobs/ directory holds no orphaned state file" \
        "left behind for finished jobs:$leftovers
The Epilog could not delete these.  On this fleet that means root_squash on
dl380g10's NFS export: epilog.sh deletes as \$SLURM_JOB_USER for exactly this
reason, and 'state file ... survived cleanup' in /var/log/slurm/slurmd.log on
the node says it tried."
elif [ -d "$LANE_ROOT/jobs" ]; then
    pass 8 "the lane's jobs/ directory holds no orphaned state file"
else
    fail 8 "the lane's jobs/ directory holds no orphaned state file" \
        "$LANE_ROOT/jobs does not exist; install.sh step 7 creates it"
fi

finish
