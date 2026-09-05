#!/bin/bash
# Phase 2 of docs/scheduler_decision_2026-09-04.md: stop the pull queue's
# execution plane and make SLURM the fleet's default transport.
#
# Run it as rob, from a checkout, with no sudo:
#
#     fleet/slurm/cutover.sh --dry-run
#     fleet/slurm/cutover.sh --yes
#
# Nothing here needs root.  The loops are rob's processes, the crontab is
# rob's, pqwork.service is rob's user unit, and the runtime generation is
# rob's to publish.
#
# It refuses unless all six of these hold, because each one is a way for the
# cutover to lose work rather than move it:
#
#   * fleet/slurm/verify.sh passed -- its marker, read rather than counted
#     (the slurm.conf it verified must be this checkout's), and no later run
#     of verify.sh recorded a failure, or --verified
#   * pb-queue/ready and pb-queue/claimed are both empty, asked in that
#     order: an item claimed between the two listings has to be seen by one
#     of them, and only ready-first guarantees that
#   * publish_runtime.py --dry-run accepts this checkout, asked here rather
#     than at step 5, which runs after every loop is already dead
#   * no pbrun is waiting on a pull-queue action anywhere in the fleet
#   * the controller reports every box idle, mixed or allocated with no state
#     flag, asked last because it is the only one of these whose answer
#     expires -- and not skipped by --verified, which is about an earlier
#     verification, not now
#   * --yes
#
# The order of what it then does is not arrangeable.  The supervise loops are
# kept alive by a per-user crontab entry on all three boxes:
#
#     */5 * * * * /usr/bin/python3 \
#         /mnt/shared/prismabuild-fleet/repo/tools/supervise.py --ensure \
#         >> /home/rob/tmp/pb-supervisor.log 2>&1
#
# so killing a supervisor without removing that line buys five minutes, and
# killing worker loops while a supervisor is alive buys thirty seconds -- it
# respawns them to the count fleet_boxes.json declares.  Crontab first, then
# supervisors, then loops.
#
# The runbook's `pkill -f tools/fleet/supervise.py` matches none of the live
# processes: they run the published path, `.../repo/tools/supervise.py`, and on
# dl380g10 the relative `repo/tools/supervise.py`.  A pattern that broad would
# also match the ssh command carrying it.  So processes are confirmed the way
# supervise._live_loops confirms them -- argv[0] is an interpreter and argv[1]
# is the script -- and killed by pid.
#
# Once the queue is confirmed empty it is FENCED, and the fence is a
# filesystem fact rather than a flag.  During steps 1 to 4 every producer and
# loop on the fleet is still running the currently published generation, which
# predates the scheduler decision and reads no marker, so a marker checked
# only by new `publish` code protects nothing.  What both old and new bytes
# obey is `chmod a-w` on pb-queue/ready: the rename that publishes an item
# into ready fails with EACCES and says so, instead of the item sitting in a
# queue whose workers are being retired.  An old loop's `claim` -- a rename
# out of ready -- and `reap_stale`'s requeue fail the same way, which is
# acceptable only because the scan above already required the queue empty.
#
# The marker written beside it is the explanation, not the mechanism.  New
# `PoolQueue.publish` reads it to turn the EACCES into a refusal that names
# the cutover, and it records the mode ready had before the fence so rollback
# restores that mode rather than a guessed one.
#
# The queue is scanned again after the fence and again once every loop is
# stopped, because a submission that landed in the window between the first
# scan and the fence is exactly the thing being fenced against.
#
# Rollback is fleet/slurm/rollback.sh, which reads the state file this writes,
# and lifting the fence is part of what it does.

set -uo pipefail

DRY_RUN=0
YES=0
VERIFIED=0

usage() {
    cat <<'USAGE'
usage: cutover.sh [--dry-run] [--verified] --yes

Stops the pull queue's execution plane on all three boxes and publishes a
runtime generation whose default transport is slurm.

  --yes        required; this changes what the whole fleet executes
  --verified   accept that fleet/slurm/verify.sh passed on another box
  --dry-run    print every command, run none of them, refuse nothing

Environment, for the tests and for nothing else:
  PB_QUEUE_ROOT   the pull queue (default /mnt/shared/prismabuild-fleet/pb-queue)
  PB_RUNTIME_DIR  the fleet runtime directory (default /mnt/shared/prismabuild-fleet)
  PB_BOXES        ssh names, in order (default "dl380g10 sparky sparklina")
  PB_SPARKS       the boxes with a pqwork user unit (default "sparky sparklina")
  PB_SSH          the ssh command (default "ssh -o BatchMode=yes")
  PB_STATE_DIR    where the state file goes (default $HOME/.prismabuild)
  PB_PUBLISH      the publish_runtime.py invocation

The pull queue's ready directory is left write-protected on success; that is
the admission fence, and fleet/slurm/rollback.sh lifts it.
USAGE
}

while [ $# -gt 0 ]; do
    case "$1" in
        --dry-run) DRY_RUN=1 ;;
        --yes) YES=1 ;;
        --verified) VERIFIED=1 ;;
        -h|--help) usage; exit 0 ;;
        *) printf 'cutover.sh: unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
    esac
    shift
done

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# Through the interpreter on purpose: publish_runtime.py is checked in
# mode 644, so running it as a command is a "Permission denied" at the
# one step that happens after every loop is already stopped.
PUBLISH="${PB_PUBLISH:-python3 $REPO/tools/fleet/publish_runtime.py}"
# The configuration this checkout would have the fleet run.  Same spelling as
# verify.sh's CONF, because the marker below records a hash of this file and
# the two have to be talking about the same one.
CONF="$REPO/fleet/slurm/slurm.conf"
QUEUE_ROOT="${PB_QUEUE_ROOT:-/mnt/shared/prismabuild-fleet/pb-queue}"
RUNTIME_DIR="${PB_RUNTIME_DIR:-/mnt/shared/prismabuild-fleet}"
BOXES="${PB_BOXES:-dl380g10 sparky sparklina}"
# `-` rather than `:-`: an empty PB_SPARKS means no box has a pqwork unit,
# which is what the tests set and not the same as leaving it unset.
SPARKS="${PB_SPARKS-sparky sparklina}"
SSH="${PB_SSH:-ssh -o BatchMode=yes}"
STATE_DIR="${PB_STATE_DIR:-$HOME/.prismabuild}"
READY_DIR="$QUEUE_ROOT/ready"
#: The fence's explanation, in the queue root so anything holding a PoolQueue
#: can read it without being told where to look.  Same spelling as
#: `pool.PoolQueue.FENCE_NAME`; a shell script cannot import that module.
FENCE_MARKER="$QUEUE_ROOT/cutover-fence.json"
#: Whether this run has fenced the queue, whether an exit before the loops are
#: stopped should lift it again, and the mode ready had before the fence.
FENCED=0
LIFT_ON_EXIT=1
PRIOR_READY_MODE=""
MARKER="$STATE_DIR/slurm-verify-passed.json"
#: What verify.sh writes when it does not pass, removing MARKER as it does.
#: The two never coexist: whichever verify.sh writes, it removes the other.
#: So this file existing means the last verification run on this box failed,
#: whatever an older success said about the same slurm.conf.
FAILURE_MARKER="$STATE_DIR/slurm-verify-failed.json"
CRONTAB_BACKUP="$STATE_DIR/crontab.pre-cutover"
STAMP="$(date +%s)"
STATE="$STATE_DIR/cutover-$STAMP.json"

exec 3>&1
say() { printf '%s\n' "$*" >&3; }
die() { printf 'cutover.sh: REFUSED: %s\n' "$*" >&2; exit 1; }

#: Which box this is, under its ssh name.
this_box="$(hostname -s)"
ssh_name_of_this_box() {
    case "$this_box" in
        gx10-6b77) printf 'sparklina' ;;
        *) printf '%s' "$this_box" ;;
    esac
}
LOCAL="$(ssh_name_of_this_box)"

#: The name SLURM knows one entry of $BOXES by.  fleet/slurm/slurm.conf spells
#: every node as its `hostname -s`, so sparklina is `NodeName=gx10-6b77` there
#: and every other box is the name ssh already uses.  This is
#: ssh_name_of_this_box read the other way round.
slurm_node_of_box() {
    case "$1" in
        sparklina) printf 'gx10-6b77' ;;
        *) printf '%s' "$1" ;;
    esac
}

#: One top-level key out of the verify marker, quotes and any trailing comma
#: removed.  verify.sh's `finish` writes it as a fixed heredoc, one key per
#: line, so sed reads it without needing a JSON parser this script does not
#: otherwise depend on.  An empty answer means the key is not there.
marker_field() {
    sed -n "s/^[[:space:]]*\"$1\"[[:space:]]*:[[:space:]]*\(.*\)\$/\1/p" "${2:-$MARKER}" \
        | head -n 1 \
        | sed 's/,[[:space:]]*$//; s/^"//; s/"$//'
}

#: A count of seconds as the largest two units that are not zero.  Reported,
#: never judged: how old a verification may be is the operator's call.
human_age() {
    local seconds="$1" days hours minutes
    days=$((seconds / 86400))
    hours=$((seconds % 86400 / 3600))
    minutes=$((seconds % 3600 / 60))
    if [ "$days" -gt 0 ]; then
        printf '%dd %dh' "$days" "$hours"
    elif [ "$hours" -gt 0 ]; then
        printf '%dh %dm' "$hours" "$minutes"
    else
        printf '%dm' "$minutes"
    fi
}

#: The characters `sinfo` appends to a node state as flags, and what each one
#: says about the node.  Single-quoted and read back through a variable
#: because `$` is live inside a bracket expression.  The table is sinfo(1)'s
#: NODE STATE CODES.
STATE_FLAGS='*~#!%@$^-'
flag_meaning() {
    case "$1" in
        '*') printf 'the controller is getting no response from it' ;;
        '~') printf 'it is powered off' ;;
        '#') printf 'it is powering up or being configured' ;;
        '!') printf 'a power-down is pending' ;;
        '%') printf 'it is powering down' ;;
        '$') printf 'it is in a reservation with the maintenance flag' ;;
        '@') printf 'a reboot is pending' ;;
        '^') printf 'a reboot has been issued' ;;
        '-') printf 'the backfill scheduler has planned it for another job' ;;
        *) printf 'the controller has flagged it' ;;
    esac
}

#: Every flag on one state, read out in order.
flag_reasons() {
    local rest="$1" first out=""
    while [ -n "$rest" ]; do
        first="${rest%"${rest#?}"}"
        rest="${rest#?}"
        [ -z "$out" ] || out="$out, "
        out="$out$(flag_meaning "$first")"
    done
    printf '%s' "$out"
}

#: Which boxes the controller says are not usable right now, one per line, or
#: nothing when every one of them is.  Exit 2 means sinfo could not be asked
#: at all, which is a different failure and reads differently.
#:
#: `sinfo -N` prints one line per node per partition, so a node in `all` and
#: in `gpu` appears twice; every line is read and the node is reported once.
#: A state carries flags, and the flag is classified rather than removed: a
#: node reported `idle*` is one the controller is getting no response from and
#: `idle~` one that is powered off, and both used to reach the accepting
#: branch because the flag was stripped before the word was read.  Any flag
#: refuses, because a flag is the controller saying something is happening to
#: that node and this gate wants the boxes nothing is happening to.  The
#: original state string is what gets reported, flag included.
node_liveness() {
    local table box node name state base flags seen offending bad
    command -v sinfo >/dev/null 2>&1 || return 2
    table="$(sinfo -h -N -o '%N %T' 2>&1)" || return 2
    bad=""
    for box in $BOXES; do
        node="$(slurm_node_of_box "$box")"
        seen=""
        offending=""
        while read -r name state; do
            [ "$name" = "$node" ] || continue
            seen=yes
            state="$(printf '%s' "$state" | tr '[:upper:]' '[:lower:]')"
            base="${state%%["$STATE_FLAGS"]*}"
            flags="${state#"$base"}"
            if [ -n "$flags" ]; then
                [ -n "$offending" ] \
                    || offending="$state, $(flag_reasons "$flags")"
                continue
            fi
            case "$base" in
                idle|mixed|allocated) ;;
                *) [ -n "$offending" ] || offending="$state" ;;
            esac
        done <<NODES
$table
NODES
        if [ -z "$seen" ]; then
            bad="$bad
  $node: not reported by sinfo -N at all"
        elif [ -n "$offending" ]; then
            bad="$bad
  $node: $offending"
        fi
    done
    printf '%s' "$bad"
}

#: What one queue directory is holding, up to twenty names, or nothing.  The
#: directory not existing and the directory being empty read the same, which
#: is what the callers want: neither is work in flight.
queue_listing() {
    find "$QUEUE_ROOT/$1" -maxdepth 1 -name '*.json' -printf '%f\n' 2>/dev/null \
        | head -n 20
}

#: Everything the queue is holding, ready first and claimed second, formatted
#: for a refusal.  Nothing when it is holding nothing.
#:
#: The order is the point.  Listing claimed and then ready missed an item that
#: was in ready when claimed was listed and in claimed when ready was -- a
#: worker claiming between the two listings hid it from both.  Ready first
#: cannot lose one: an item still in ready is seen there, and one that moves
#: to claimed after ready was listed is seen in claimed.
queue_holdings() {
    local name held out=""
    for name in ready claimed; do
        held="$(queue_listing "$name")"
        [ -n "$held" ] || continue
        out="$out
  pb-queue/$name:
$(printf '%s\n' "$held" | sed 's/^/    /')"
    done
    printf '%s' "$out"
}

#: Close the pull queue to new submissions, as a filesystem fact.
#:
#: `chmod a-w` on ready is what old bytes obey.  Every box writes as rob and
#: the directory is rob's, so removing the write bit for everybody stops the
#: rename that publishes an item -- loudly, with EACCES, in the producer that
#: is still holding the action -- rather than letting it land in a queue whose
#: workers are being retired.  The marker is written first and is the
#: explanation a reader gets; it records the mode to go back to.
#:
#: A marker that is already there belongs to an earlier run of this cutover.
#: Its recorded mode is the pre-cutover one, and re-reading the directory now
#: would record the fenced mode as the mode to restore.
fence_the_queue() {
    local mode
    if [ -f "$FENCE_MARKER" ]; then
        PRIOR_READY_MODE="$(marker_field prior_ready_mode "$FENCE_MARKER")"
        FENCED=1
        say "# $READY_DIR is already fenced; the mode to go back to is ${PRIOR_READY_MODE:-unrecorded}"
        return 0
    fi
    mode="$(stat -c %a "$READY_DIR" 2>/dev/null)"
    if [ -z "$mode" ]; then
        die "could not read the mode of $READY_DIR, so the fence could not
record what to restore.  The fence is what stops a producer submitting into a
queue whose workers this cutover is about to retire."
    fi
    cat > "$FENCE_MARKER" <<EOF
{
 "schema": "prismaquant.prismabuild.slurm_cutover_fence.v1",
 "fenced_unix": "$STAMP",
 "fenced_by": "$this_box",
 "checkout": "$REPO",
 "prior_ready_mode": "$mode",
 "reason": "fleet/slurm/cutover.sh is retiring the pull queue's execution plane; submit through SLURM, or run fleet/slurm/rollback.sh"
}
EOF
    if ! chmod a-w "$READY_DIR"; then
        rm -f "$FENCE_MARKER"
        die "could not remove the write bit on $READY_DIR.  Without the fence a
producer can submit into the queue while its workers are being stopped, and
that submission is executed by nothing."
    fi
    PRIOR_READY_MODE="$mode"
    FENCED=1
    say "# fenced $READY_DIR: mode $mode is now $(stat -c %a "$READY_DIR")"
    say "#   and wrote $FENCE_MARKER, which says why to anybody who is refused"
}

#: Put the queue back the way it was.  Idempotent, and a no-op when this run
#: never fenced.
unfence_the_queue() {
    [ "$FENCED" = 1 ] || return 0
    if [ -n "$PRIOR_READY_MODE" ]; then
        if chmod "$PRIOR_READY_MODE" "$READY_DIR" 2>/dev/null; then
            say "# lifted the fence: $READY_DIR is back to mode $PRIOR_READY_MODE"
        else
            printf 'cutover.sh: could not restore %s to mode %s; run: chmod %s %s\n' \
                "$READY_DIR" "$PRIOR_READY_MODE" "$PRIOR_READY_MODE" "$READY_DIR" >&2
        fi
    else
        printf 'cutover.sh: no recorded mode for %s; the fence is still up\n' \
            "$READY_DIR" >&2
    fi
    rm -f "$FENCE_MARKER"
    FENCED=0
}

#: An exit before the loops are stopped lifts the fence, because the pull
#: queue is still the fleet's execution plane and a fenced queue with live
#: workers refuses submissions for no reason.  After step 3 the opposite is
#: true: lifting it would reopen a queue with nothing left to drain it, which
#: is the stranding this fence exists to prevent, so those exits leave it up
#: and point at rollback.sh.  COMPLETED keeps the fence up on success.
COMPLETED=0
on_exit() {
    [ "$COMPLETED" = 0 ] || return 0
    [ "$LIFT_ON_EXIT" = 1 ] || return 0
    unfence_the_queue
}
trap on_exit EXIT
# So that an interrupt reaches the EXIT trap rather than leaving the queue
# fenced with no cutover running.
trap 'exit 130' INT
trap 'exit 143' TERM

#: Run a shell snippet on one box, locally when it is this one.  Prints the
#: command verbatim; in a dry run that is all it does.
on_box() {
    local box="$1" snippet="$2"
    if [ "$box" = "$LOCAL" ]; then
        printf '# on %s (this box)\n%s\n' "$box" "$snippet" >&3
        [ "$DRY_RUN" = 0 ] || return 0
        bash -c "$snippet"
    else
        printf '# on %s\n%s %s <<%s\n%s\n%s\n' \
            "$box" "$SSH" "$box" "'PBEOF'" "$snippet" "PBEOF" >&3
        [ "$DRY_RUN" = 0 ] || return 0
        printf '%s\n' "$snippet" | $SSH "$box" bash -s
    fi
}

# -- the one thing worth getting exactly right -------------------------------
#
# Sent to every box.  `pgrep -f <script>` matches anything whose command line
# mentions the script -- the ssh invocation carrying this very snippet, an
# editor, an agent grepping for loops -- and over-killing is the dangerous
# direction here: it would kill the command doing the cutover.  So a candidate
# is confirmed by reading its own argv, exactly as supervise._live_loops does.

read -r -d '' STOP_FUNCTIONS <<'SNIPPET'
pb_pids() {
    script="$1"
    for pid in $(pgrep -f "$script" 2>/dev/null); do
        [ "$pid" = "$$" ] && continue
        # 2>/dev/null goes before the input redirect, not after it: bash
        # applies redirections left to right, so a pid that exited between
        # pgrep and here fails `< /proc/N/cmdline` while stderr is still the
        # terminal, and prints "No such file or directory" during the one
        # operation nobody wants to see an error during.  A vanished pid is
        # not running the loop, which is all this is asking.
        cmdline=$(tr '\0' '\n' 2>/dev/null < "/proc/$pid/cmdline") || continue
        interp=$(printf '%s\n' "$cmdline" | sed -n 1p)
        target=$(printf '%s\n' "$cmdline" | sed -n 2p)
        case "$interp" in *python*) ;; *) continue ;; esac
        case "$target" in *"/$script"|"$script") ;; *) continue ;; esac
        printf '%s\n' "$pid"
    done
}
pb_stop() {
    script="$1"
    pids=$(pb_pids "$script")
    if [ -z "$pids" ]; then
        echo "$(hostname -s): no $script running"
        return 0
    fi
    echo "$(hostname -s): stopping $script: $(echo "$pids" | tr '\n' ' ')"
    # shellcheck disable=SC2086
    kill $pids 2>/dev/null
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        sleep 1
        left=$(pb_pids "$script")
        [ -z "$left" ] && break
    done
    left=$(pb_pids "$script")
    if [ -n "$left" ]; then
        echo "$(hostname -s): $script survived SIGTERM, sending SIGKILL: $(echo "$left" | tr '\n' ' ')"
        # shellcheck disable=SC2086
        kill -9 $left 2>/dev/null
        sleep 1
    fi
    left=$(pb_pids "$script")
    if [ -n "$left" ]; then
        echo "$(hostname -s): FAILED to stop $script: $(echo "$left" | tr '\n' ' ')"
        return 1
    fi
    echo "$(hostname -s): $script stopped"
}
SNIPPET

say "prismabuild SLURM cutover"
say "from     : $this_box (ssh name $LOCAL)"
say "checkout : $REPO"
say "boxes    : $BOXES"
say "mode     : $([ "$DRY_RUN" = 1 ] && echo 'dry run, nothing is executed and nothing is refused' || echo 'live')"
say ""

# -- refusals ----------------------------------------------------------------

if [ "$DRY_RUN" = 1 ]; then
    # A dry run refuses nothing, so say what it would have checked.  These are
    # read-only questions and they are the six ways this can lose work.
    say "# a live run refuses unless all six of these hold:"
    say "#   --yes was given"
    say "#   $MARKER records this checkout's slurm.conf and $FAILURE_MARKER is absent, or --verified"
    say "#   $QUEUE_ROOT/ready and .../claimed are empty"
    say "#   $PUBLISH --dry-run --default-transport slurm succeeds"
    say "#   no confirmed pbrun.py process on any of: $BOXES"
    say "#   sinfo reports every one of $BOXES idle, mixed or allocated, with no state flag"
    say "# and a live run then fences the queue rather than trusting that scan:"
    say "#   chmod a-w $READY_DIR, so a rename into ready fails with EACCES in"
    say "#   the producer that is still holding the action instead of landing"
    say "#   in a queue whose workers are being retired, plus"
    say "#   $FENCE_MARKER saying why.  rollback.sh lifts it."
    # The last one is the only refusal a dry run can answer rather than name:
    # sinfo reads and changes nothing, and the answer is about now, so it is
    # worth having before the window is chosen.  It still refuses nothing.
    unusable="$(node_liveness)"
    liveness_status=$?
    if [ "$liveness_status" != 0 ]; then
        say "# sinfo cannot be asked here, so a live run would refuse: SLURM is"
        say "#   not installed on this box, or slurmctld is unreachable"
    elif [ -n "$unusable" ]; then
        say "# a live run would refuse; these are not usable right now:$unusable"
    else
        say "# every one of $BOXES is idle, mixed or allocated, with no state flag"
    fi
fi

if [ "$DRY_RUN" = 0 ]; then
    [ "$YES" = 1 ] || die "this changes what the whole fleet executes; pass --yes"

    # The marker is read, not counted.  Existence alone said only that
    # verify.sh once passed somewhere -- against a slurm.conf this checkout
    # may no longer contain, which is a verification of a different fleet than
    # the one about to be cut over to.  The hash is the refusal; the age and
    # the commit are reported, because how old is too old is the operator's
    # call and not this script's.
    if [ "$VERIFIED" = 1 ]; then
        say "# --verified: taking it that fleet/slurm/verify.sh passed elsewhere"
    elif [ -f "$FAILURE_MARKER" ]; then
        # Asked before the success marker, because the two answer different
        # questions and this one is newer by construction.  A success marker
        # says a fleet passed once, against a slurm.conf that may still be
        # this checkout's and nodes that may still register; it cannot say
        # that the run the operator just watched failed.
        die "$FAILURE_MARKER records a verification that did not pass:
$(sed 's/^/  /' "$FAILURE_MARKER")
An earlier pass says a fleet worked once.  This says the last verification
run on this box did not, so end-to-end execution is not established for the
fleet this cutover would produce.  Re-run fleet/slurm/verify.sh -- a pass
removes this file -- or pass --verified if you have verified the fleet from
another box."
    elif [ -f "$MARKER" ]; then
        marker_sha="$(marker_field slurm_conf_sha256)"
        marker_when="$(marker_field verified_unix)"
        marker_host="$(marker_field host)"
        marker_commit="$(marker_field commit)"
        conf_sha="$(sha256sum "$CONF" | cut -d' ' -f1)"

        if ! printf '%s' "$marker_sha" | grep -Eqx '[0-9a-f]{64}'; then
            die "$MARKER does not say which slurm.conf was verified: no readable
slurm_conf_sha256 field.  A marker that cannot be matched against this
checkout is not evidence about this fleet.  Re-run fleet/slurm/verify.sh."
        fi
        if ! printf '%s' "$marker_when" | grep -Eqx '[0-9]+'; then
            die "$MARKER does not say when it was written: no readable
verified_unix field.  A marker whose age cannot be read is not evidence about
this fleet.  Re-run fleet/slurm/verify.sh."
        fi
        if [ "$marker_sha" != "$conf_sha" ]; then
            die "$MARKER verified a different slurm.conf:
  the marker's:  $marker_sha
  this checkout: $conf_sha
                 ($CONF)
verify.sh checked a fleet running a configuration this checkout no longer
contains, so it says nothing about the fleet this cutover would produce.
Re-run fleet/slurm/verify.sh, or check out the commit it verified."
        fi

        say "# verified: $MARKER"
        say "$(sed 's/^/#   /' "$MARKER")"
        marker_age=$((STAMP - marker_when))
        if [ "$marker_age" -lt 0 ]; then
            say "# verified at $marker_when, which is in this box's future, on ${marker_host:-an unnamed box}, commit ${marker_commit:-unknown}"
        else
            say "# verified $(human_age "$marker_age") ago on ${marker_host:-an unnamed box}, commit ${marker_commit:-unknown}"
        fi
        say "#   the age is reported, not judged: how old is too old is your call"
        say "# the marker's slurm.conf is this checkout's ($conf_sha)"
        head_commit="$(git -C "$REPO" rev-parse --verify HEAD 2>/dev/null || echo unknown)"
        if [ "$marker_commit" = "$head_commit" ]; then
            say "# and it was verified at this checkout's HEAD"
        else
            say "# but this checkout's HEAD is $head_commit, not $marker_commit (informational)"
        fi
    else
        die "no $MARKER. Run fleet/slurm/verify.sh first, or pass --verified if you ran it on another box (the marker is box-local)"
    fi

    # Ready first, then claimed.  An item that a worker claims between the
    # two listings has to be seen by one of them, and only this order
    # guarantees that: claimed-first missed an item that was in ready when
    # claimed was listed and in claimed when ready was.
    for name in ready claimed; do
        held="$(queue_listing "$name")"
        if [ -n "$held" ]; then
            die "pb-queue/$name is not empty:
$(printf '%s\n' "$held" | sed 's/^/  /')
A stopped loop leaves its claim behind for a reaper that will not run again,
and an item in ready is an action no SLURM job will ever pick up.  Wait for
them, or withdraw them with: pbrun --withdraw <key prefix>"
        fi
    done
    say "# pb-queue/ready and pb-queue/claimed are both empty"

    # Fenced here, before anything else is asked, because everything asked
    # after this takes time and a scan is only true for the instant it ran.
    # A detached producer that publishes between that instant and the moment
    # the loops stop had its action accepted and executed by nothing.
    fence_the_queue
    arrived="$(queue_holdings)"
    if [ -n "$arrived" ]; then
        die "work arrived in the pull queue between the scan and the fence:$arrived
That item was accepted by a transport this cutover is about to retire, so it
is not something to cut over on top of.  Nothing else has been changed and the
fence is lifted, so the loops that are still running will pick it up.  Let it
finish, then re-run."
    fi
    say "# and still empty with the fence up, so nothing landed in that window"

    # Step 5 is the only step that cannot simply be re-run: by the time it
    # fires, cron is edited and every loop on every box is dead.  So ask
    # publish_runtime the same questions now, while nothing has been stopped.
    # --dry-run establishes the commit identity and the dirty check before it
    # returns, so a dirty tree -- the usual cause, and this checkout is a
    # worktree that collects untracked files -- is refused here instead of
    # there.  (--activate-generation returns before those checks, which is why
    # rollback.sh needs no clean tree to undo this.)
    if ! preflight="$($PUBLISH --dry-run --default-transport slurm 2>&1)"; then
        die "publish_runtime.py refuses this checkout, and step 5 would hit the
same refusal with the crontab already edited and every loop already dead:
$(printf '%s\n' "$preflight" | sed 's/^/  /')
Fix it in $REPO first; commit or stash, then re-run."
    fi
    say "# publish_runtime.py --dry-run accepts $REPO"

    waiters=""
    for box in $BOXES; do
        found="$(on_box "$box" "$STOP_FUNCTIONS
pb_pids pbrun.py | sed \"s|^|\$(hostname -s) pid |\"")" || true
        [ -n "$found" ] && waiters="$waiters
$found"
    done
    if [ -n "$(printf '%s' "$waiters" | tr -d '[:space:]')" ]; then
        die "a pbrun is still waiting somewhere:$waiters
Each one is somebody watching for a result that the loops are about to stop
producing.  Let them finish."
    fi
    say "# no pbrun is waiting on any box"

    # Last, because it is the only question whose answer expires: the marker
    # says a fleet passed once, and this asks the controller whether that
    # fleet is up now.  --verified does not skip it for the same reason.  It
    # is also the last read before anything is written -- the state file
    # below and step 1's crontab edit are the first irreversible acts -- so a
    # fleet that is down is refused with nothing yet changed.
    unusable="$(node_liveness)"
    liveness_status=$?
    if [ "$liveness_status" != 0 ]; then
        die "sinfo could not be asked which nodes are up: SLURM is not installed
here, or slurmctld is unreachable.  This cutover makes SLURM the fleet's
transport, so a controller that cannot answer now is a fleet with no execution
plane the moment step 1 runs.  Run fleet/slurm/verify.sh."
    fi
    if [ -n "$unusable" ]; then
        die "the controller does not report every box as usable right now:$unusable
A node that is down, drained or unregistered runs nothing after the pull
queue's loops are stopped, and stopping them is step 3.  A state flag refuses
for the same reason: the flag is the controller saying something is happening
to that node, and this gate wants the boxes nothing is happening to.  Bring it
back -- and a node the controller merely drained comes back with:
  scontrol update NodeName=<node> State=RESUME"
    fi
    say "# every one of $BOXES is idle, mixed or allocated, with no state flag"
fi

# -- record what is being replaced, before replacing it ----------------------

previous_generation=""
if [ -L "$RUNTIME_DIR/repo" ]; then
    previous_generation="$(basename "$(readlink "$RUNTIME_DIR/repo")")"
fi
say ""
say "# the generation being replaced: ${previous_generation:-none (repo is not a symlink)}"
if [ -z "$previous_generation" ] && [ "$DRY_RUN" = 0 ]; then
    die "$RUNTIME_DIR/repo is not a symlink to a generation, so rollback would have nothing to point back at. Publish once with publish_runtime.py --migrate-directory first."
fi

mkdir -p "$STATE_DIR"

# -- the state file rollback reads, before anything is changed ---------------
#
# Everything rollback.sh needs is known now: the generation being replaced,
# the boxes, and where the crontab backup goes.  Written here rather than at
# the end, because the failure that points the operator at rollback.sh is
# step 5's, and a rollback that refuses for want of a state file leaves the
# crontab edited and every loop dead.  new_generation is filled in once step
# 5 has produced it.

write_state() {
    cat > "$STATE" <<EOF
{
 "schema": "prismaquant.prismabuild.slurm_cutover.v1",
 "cutover_unix": $STAMP,
 "run_from": "$this_box",
 "checkout": "$REPO",
 "boxes": "$BOXES",
 "sparks": "$SPARKS",
 "previous_generation": "$previous_generation",
 "new_generation": "$1",
 "crontab_backup": "$CRONTAB_BACKUP",
 "queue_root": "$QUEUE_ROOT",
 "ready_prior_mode": "$PRIOR_READY_MODE"
}
EOF
}

if [ "$DRY_RUN" = 0 ]; then
    write_state "" || die "could not write $STATE"
    say "# wrote $STATE (new_generation is filled in after step 5)"
fi

# -- 1. the crontab, first, on every box -------------------------------------
#
# Before any process is killed: cron re-runs `supervise.py --ensure` every five
# minutes, and a supervisor tops the loops back up thirty seconds later.  The
# whole crontab is backed up verbatim rather than the one line, so rollback
# restores what was there instead of reconstructing it.
#
# The backup is taken only while the crontab still has the line.  A re-run
# after a partial cutover -- the thing step 5's failure message suggests --
# sees a crontab the first run already edited, and saving that over the
# backup would give rollback a crontab with no supervise line to restore.

say ""
say "# step 1: take the supervise line out of each box's crontab"
for box in $BOXES; do
    on_box "$box" "set -e
mkdir -p '$STATE_DIR'
if crontab -l 2>/dev/null | grep -q supervise.py; then
    crontab -l > '$CRONTAB_BACKUP'
    grep -v supervise.py '$CRONTAB_BACKUP' | crontab -
    echo \"\$(hostname -s): supervise line removed; whole crontab saved to $CRONTAB_BACKUP\"
elif [ -f '$CRONTAB_BACKUP' ]; then
    echo \"\$(hostname -s): no supervise line in the crontab; keeping the backup an earlier run saved to $CRONTAB_BACKUP\"
else
    crontab -l > '$CRONTAB_BACKUP' 2>/dev/null || : > '$CRONTAB_BACKUP'
    echo \"\$(hostname -s): no supervise line in the crontab\"
fi" || die "could not edit the crontab on $box"
done

# -- 2. the supervisors ------------------------------------------------------

say ""
say "# step 2: stop the supervisors"
for box in $BOXES; do
    on_box "$box" "$STOP_FUNCTIONS
pb_stop supervise.py" || die "a supervisor survived on $box"
done

# -- 3. the worker loops -----------------------------------------------------
#
# SIGTERM first and a bounded wait: worker_loop handles it and unwinds a claim
# it is holding.  With claimed empty and no supervisor left, nothing respawns.

say ""
say "# step 3: stop the worker loops"
for box in $BOXES; do
    on_box "$box" "$STOP_FUNCTIONS
pb_stop worker_loop.py" || die "a worker loop survived on $box"
done

# From here on an exit leaves the fence up.  Before this line the pull queue
# was still the fleet's execution plane, so a fenced queue with live workers
# refuses submissions for no reason and the trap lifts it.  After it there is
# nothing left to drain the queue, and reopening it would let a producer put
# an action somewhere no transport is reading -- which is the whole thing this
# fence exists to prevent.  rollback.sh restores the loops and the mode
# together.
LIFT_ON_EXIT=0

# -- 4. the legacy pqwork unit on the Sparks ---------------------------------
#
# A user unit, so no sudo: `systemctl --user`.  It is stopped, not disabled --
# stopping is what the decision record asks for and disabling is a decision
# about what this box does at boot.  It is Restart=always and
# WantedBy=default.target, so it comes back on the next reboot; rollback.sh
# starts it again, and the line below says so out loud so a reboot during a
# SLURM week is not a surprise.

say ""
say "# step 4: stop the legacy pqwork user unit on the Sparks"
#
# `set -e` and a state check, not an echo.  The snippet used to end in an
# `echo` whose own status is what `on_box` returned, so a `stop` that failed
# was reported as "pqwork.service active" and step 5 published the SLURM
# generation with the legacy executor still draining the pull queue.  The
# outer script's `set -uo pipefail` does not reach inside a snippet run by
# another bash, and it carries no errexit to reach with.
#
# `is-active` prints `inactive` or `failed` for a unit that is not running,
# and both of those are stopped.  What refuses is the unit still being up:
# `active`, or on its way there.  The `|| true` is load-bearing -- `is-active`
# exits nonzero for an inactive unit, which under `set -e` would end the
# snippet at the assignment with nothing said.
for box in $SPARKS; do
    on_box "$box" "set -e
if systemctl --user list-unit-files pqwork.service >/dev/null 2>&1; then
    systemctl --user stop pqwork.service
    state=\"\$(systemctl --user is-active pqwork.service 2>&1 || true)\"
    echo \"\$(hostname -s): pqwork.service \$state\"
    case \"\$state\" in
        active|activating|reloading)
            echo \"\$(hostname -s): pqwork.service is still \$state after stop\" >&2
            exit 1
            ;;
    esac
else
    echo \"\$(hostname -s): no pqwork.service\"
fi" || die "step 4 could not stop pqwork.service on $box, so the legacy
executor is still draining the pull queue there.  Nothing has been published:
the fleet is still on $previous_generation.  Stop the unit by hand and re-run,
or run fleet/slurm/rollback.sh."
done
say "# note: pqwork.service is left ENABLED, so a reboot starts it again."
say "#       Stop it again after a reboot, or disable it deliberately."

# -- the queue, once more, with nothing left that could drain it -------------
#
# The last read before the irreversible act.  The fence has been up since
# before the publisher preflight, so nothing should have arrived; what this
# catches is the other direction -- a worker that claimed an item just before
# its loop died and did not unwind the claim on the way out.  Its own item is
# then in claimed, with no reaper that will ever run again.
#
# The fence stays up on this refusal.  Reopening the queue here would give a
# producer a directory that nothing reads.

if [ "$DRY_RUN" = 0 ]; then
    say ""
    remaining="$(queue_holdings)"
    if [ -n "$remaining" ]; then
        die "the pull queue is not empty now that every loop is stopped:$remaining
An item in claimed is a worker's, left behind when its loop was stopped, and
no reaper will run again to unwind it; an item in ready got past the fence.
Nothing has been published, so the fleet is still on $previous_generation and
the queue is still fenced.  Run fleet/slurm/rollback.sh: it puts the loops,
the crontab and the fence back, and these items are then executed by the
transport that accepted them."
    fi
    say "# pb-queue/ready and pb-queue/claimed are still empty"
fi

# -- 5. publish the generation that makes SLURM the default ------------------

say ""
say "# step 5: publish a runtime generation whose default transport is slurm"
if [ "$DRY_RUN" = 1 ]; then
    say "$PUBLISH --default-transport slurm"
else
    $PUBLISH --default-transport slurm \
        || die "publication failed; the loops are stopped and the fleet is still on the previous generation. Fix the publication and re-run, or run fleet/slurm/rollback.sh"
fi

# -- the state file, completed ------------------------------------------------

new_generation=""
[ -L "$RUNTIME_DIR/repo" ] && new_generation="$(basename "$(readlink "$RUNTIME_DIR/repo")")"

if [ "$DRY_RUN" = 0 ]; then
    write_state "$new_generation" || die "could not rewrite $STATE"
    say ""
    say "# wrote $STATE"
fi

COMPLETED=1

say ""
say "# cutover complete."
say "# The fleet's default transport is now slurm, carried by the published"
say "# generation rather than by anybody's environment."
if [ "$DRY_RUN" = 0 ]; then
    say "# $READY_DIR stays write-protected, which is the admission fence: a"
    say "# producer still running old bytes is refused with EACCES rather than"
    say "# stranding an action in a queue nothing drains."
fi
say "# Roll back with: fleet/slurm/rollback.sh, which lifts the fence too."
