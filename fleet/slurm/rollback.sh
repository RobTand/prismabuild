#!/bin/bash
# Undo fleet/slurm/cutover.sh: put the pull queue back in charge.
#
# Run it as rob, from a checkout, with no sudo:
#
#     fleet/slurm/rollback.sh --dry-run
#     fleet/slurm/rollback.sh
#
# It reads the state file cutover.sh wrote -- the newest one in the state
# directory unless --state names another -- and reverses it in reverse order:
#
#   1. point the live runtime back at the generation cutover replaced, which
#      restores the previous default transport in the same atomic namespace
#      operation that changed it.  Nothing is re-published: that generation's
#      bytes and receipt were proved when it was published, and rebuilding
#      them from a checkout that has since moved would not be the same thing.
#      Then lift the cutover's admission fence: put pb-queue/ready back to the
#      mode the cutover recorded and remove the fence marker, so the pull
#      queue accepts submissions again.  It goes here, right after the
#      runtime, because a producer reading the restored generation is told to
#      use the pull queue and must not then be refused by the fence.
#   2. restore each box's crontab from the verbatim backup cutover took, so
#      cron resumes keeping a supervisor alive.
#   3. start pqwork.service again on the two Sparks.
#   4. start one supervisor per box now, rather than waiting up to five
#      minutes for cron to notice.
#
# The runtime first, deliberately.  Between step 1 and step 4 the fleet has no
# workers and the default transport is the pull queue, so new submissions
# queue and wait -- which is a fleet that is idle.  The other order gives a
# window where workers are draining the queue while producers are still being
# told to use SLURM.
#
# SLURM jobs already running keep running; the decision record says so and this
# does not change it.  Cancel the ones you do not want with
# `pbrun --transport slurm --withdraw <key prefix>`.  slurmctld and slurmd can
# stay up: with no submissions they do nothing.

set -uo pipefail

DRY_RUN=0
STATE_FILE=""

usage() {
    cat <<'USAGE'
usage: rollback.sh [--dry-run] [--state FILE]

Reverses fleet/slurm/cutover.sh: restores the previous runtime generation (and
with it the pull queue as the default transport), the supervise crontab line,
pqwork.service on the Sparks, and one supervisor per box.

  --state FILE  a cutover state file; default is the newest in the state dir
  --dry-run     print every command and run none of them

Set PB_ROLLOUT_REASON to the reviewed compatibility reason for this reverse
transition. Existing-generation activation does not infer it from the original
publication. Dry-run requires the reason too.

Environment, for the tests and for nothing else:
  PB_RUNTIME_DIR  the fleet runtime directory (default /mnt/shared/prismabuild-fleet)
  PB_BOXES        ssh names, in order (default: from the state file)
  PB_SSH          the ssh command (default "ssh -o BatchMode=yes")
  PB_STATE_DIR    where the state file lives (default $HOME/.prismabuild)
  PB_PUBLISH      the publish_runtime.py invocation
  PB_SUPERVISOR_LOG  where step 4 sends a supervisor's output
                  (default /home/rob/tmp/pb-supervisor.log)
USAGE
}

while [ $# -gt 0 ]; do
    case "$1" in
        --dry-run) DRY_RUN=1 ;;
        --state) shift; STATE_FILE="${1:-}" ;;
        -h|--help) usage; exit 0 ;;
        *) printf 'rollback.sh: unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
    esac
    shift
done

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# Mode 644 in the checkout, so it runs through the interpreter.
PUBLISH="${PB_PUBLISH:-python3 $REPO/tools/fleet/publish_runtime.py}"
ROLLOUT_REASON="${PB_ROLLOUT_REASON:-}"
RUNTIME_DIR="${PB_RUNTIME_DIR:-/mnt/shared/prismabuild-fleet}"
SSH="${PB_SSH:-ssh -o BatchMode=yes}"
STATE_DIR="${PB_STATE_DIR:-$HOME/.prismabuild}"
#: Where step 4's supervisor sends its output.  Overridable only so the tests
#: that exercise these snippets do not append to the fleet's real log: a
#: hardcoded absolute path was the one thing in this script that reached
#: outside the directories a test names.
SUPERVISOR_LOG="${PB_SUPERVISOR_LOG:-/home/rob/tmp/pb-supervisor.log}"

exec 3>&1
say() { printf '%s\n' "$*" >&3; }
die() { printf 'rollback.sh: %s\n' "$*" >&2; exit 1; }

[[ "$ROLLOUT_REASON" =~ [^[:space:]] ]] \
    || die "set PB_ROLLOUT_REASON to the reviewed rolling-transition compatibility reason"

if [ -z "$STATE_FILE" ]; then
    STATE_FILE="$(find "$STATE_DIR" -maxdepth 1 -name 'cutover-*.json' 2>/dev/null \
        | LC_ALL=C sort | tail -n 1)"
fi
[ -n "$STATE_FILE" ] && [ -f "$STATE_FILE" ] \
    || die "no cutover state file in $STATE_DIR. Pass --state, or roll back by hand: the two things cutover changed are the live runtime symlink and one crontab line per box."

#: One string field out of the state file, without a JSON parser.  The file is
#: written by cutover.sh a few lines at a time and every value is a plain
#: string; a field this cannot read is a field cutover did not write.
state_field() {
    sed -n "s/^ \"$1\": \"\\(.*\\)\",\\{0,1\\}$/\\1/p" "${2:-$STATE_FILE}" | head -n 1
}

PREVIOUS="$(state_field previous_generation)"
BOXES="${PB_BOXES:-$(state_field boxes)}"
SPARKS="$(state_field sparks)"
CRONTAB_BACKUP="$(state_field crontab_backup)"
#: The cutover's admission fence.  A state file written before the fence
#: existed names no queue root, and there is then nothing to lift.
QUEUE_ROOT="$(state_field queue_root)"
READY_DIR=""
FENCE_MARKER=""
if [ -n "$QUEUE_ROOT" ]; then
    READY_DIR="$QUEUE_ROOT/ready"
    #: Same spelling as `pool.PoolQueue.FENCE_NAME` and cutover.sh's own.
    FENCE_MARKER="$QUEUE_ROOT/cutover-fence.json"
fi
[ -n "$PREVIOUS" ] || die "$STATE_FILE names no previous_generation"
[ -n "$BOXES" ] || die "$STATE_FILE names no boxes"

this_box="$(hostname -s)"
case "$this_box" in
    gx10-6b77) LOCAL=sparklina ;;
    *) LOCAL="$this_box" ;;
esac

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

say "prismabuild SLURM rollback"
say "state    : $STATE_FILE"
say "restoring: $PREVIOUS"
say "boxes    : $BOXES"
say "mode     : $([ "$DRY_RUN" = 1 ] && echo 'dry run, nothing is executed' || echo 'live')"

# -- 1. the runtime ----------------------------------------------------------

say ""
say "# step 1: point the live runtime back at $PREVIOUS"
if [ "$DRY_RUN" = 1 ]; then
    say "$PUBLISH --activate-generation $PREVIOUS --rollout rolling --rollout-reason $(printf '%q' "$ROLLOUT_REASON")"
else
    $PUBLISH --activate-generation "$PREVIOUS" --rollout rolling --rollout-reason "$ROLLOUT_REASON" \
        || die "could not activate $PREVIOUS. Check that it is still under $RUNTIME_DIR/runtime-generations; publication never deletes a generation, so it should be."
fi

# -- 1b. the admission fence -------------------------------------------------
#
# The mode is restored from what the cutover recorded rather than from a
# guess: this fleet's ready directory is 2775, and a rollback that assumed 775
# would drop the setgid bit that keeps new items in the queue's group.  The
# marker is the authority because it is written beside the directory it
# describes; the state file's copy is the fallback for a marker somebody has
# already removed by hand.

say ""
if [ -z "$READY_DIR" ]; then
    say "# step 1b: this state file names no queue root, so there is no fence to lift"
elif [ "$DRY_RUN" = 1 ]; then
    say "# step 1b: lift the fence on $READY_DIR"
    say "chmod <recorded mode> $READY_DIR"
    say "rm -f $FENCE_MARKER"
else
    say "# step 1b: lift the cutover's admission fence on $READY_DIR"
    MODE="$(state_field prior_ready_mode "$FENCE_MARKER" 2>/dev/null)"
    [ -n "$MODE" ] || MODE="$(state_field ready_prior_mode)"
    if [ -z "$MODE" ]; then
        die "step 1b: neither $FENCE_MARKER nor $STATE_FILE records the mode
$READY_DIR had before the cutover fenced it, and guessing it would be the
difference between 2775 and 775 -- the setgid bit that keeps new items in the
queue's group.  Read the mode off another box's queue and run:
  chmod <mode> $READY_DIR && rm -f $FENCE_MARKER
then re-run this rollback."
    fi
    if [ -d "$READY_DIR" ] && ! chmod "$MODE" "$READY_DIR"; then
        die "step 1b: could not restore $READY_DIR to mode $MODE.  Until it is
writable the pull queue accepts nothing, so the transport this rollback just
restored has no way in.  Run: chmod $MODE $READY_DIR"
    fi
    rm -f "$FENCE_MARKER"
    say "# $READY_DIR is back to mode $MODE and the fence marker is gone"
fi

# -- 2. the crontab ----------------------------------------------------------

say ""
say "# step 2: restore each box's crontab from the backup cutover took"
#
# `set -e`, and then the crontab is read back.  The snippet used to end in an
# `echo`, so a `crontab` that refused the file was reported as "crontab
# restored" and the rollback went on to say it was complete with nothing
# keeping a supervisor alive.  The read-back asks for the property step 2
# exists for rather than for byte equality: some crons prepend their own
# header to `crontab -l`, so a backup that carried the supervise line has to
# produce a crontab that carries it, and a backup that carried none is not
# evidence of anything to check.
for box in $BOXES; do
    on_box "$box" "set -e
if [ -f '$CRONTAB_BACKUP' ]; then
    crontab '$CRONTAB_BACKUP'
    if grep -q supervise.py '$CRONTAB_BACKUP' \\
        && ! crontab -l 2>/dev/null | grep -q supervise.py; then
        echo \"\$(hostname -s): the supervise line is not in the crontab after restoring $CRONTAB_BACKUP\" >&2
        exit 1
    fi
    echo \"\$(hostname -s): crontab restored from $CRONTAB_BACKUP\"
else
    echo \"\$(hostname -s): NO BACKUP at $CRONTAB_BACKUP; restore the supervise line by hand\"
    exit 1
fi" || die "step 2 could not restore the crontab on $box; the supervise line is what keeps a supervisor alive.  The runtime is already back on $PREVIOUS, so restore that line by hand and re-run, or add it with crontab -e"
done

# -- 3. pqwork on the Sparks -------------------------------------------------

say ""
say "# step 3: start pqwork.service again on the Sparks"
#
# The mirror image of the cutover's step 4, and it had the same defect: the
# trailing `echo` returned 0 whatever `start` did, so a unit that failed to
# start was reported by its own state line and the rollback still said it was
# complete.  A unit that is `active`, or on its way there, is started; an
# `inactive` or `failed` one is the executor the rollback exists to bring
# back, still absent.
for box in $SPARKS; do
    on_box "$box" "set -e
if systemctl --user list-unit-files pqwork.service >/dev/null 2>&1; then
    systemctl --user start pqwork.service
    state=\"\$(systemctl --user is-active pqwork.service 2>&1 || true)\"
    echo \"\$(hostname -s): pqwork.service \$state\"
    case \"\$state\" in
        active|activating|reloading) ;;
        *)
            echo \"\$(hostname -s): pqwork.service is \$state after start\" >&2
            exit 1
            ;;
    esac
else
    echo \"\$(hostname -s): no pqwork.service\"
fi" || die "step 3 could not start pqwork.service on $box.  The runtime is back on $PREVIOUS and the crontab is restored, so the pull queue's worker loops will come back; this box's legacy executor will not until the unit starts.  Start it by hand with: systemctl --user start pqwork.service"
done

# -- 4. a supervisor now, rather than in five minutes ------------------------
#
# --ensure is a no-op when a supervisor already owns the box, which is what
# makes running it here safe alongside the cron entry restored above.  Detached
# with setsid, because a supervisor is a long-lived process and this ssh is not.

say ""
say "# step 4: start one supervisor per box now"
for box in $BOXES; do
    on_box "$box" "setsid /usr/bin/python3 $RUNTIME_DIR/repo/tools/supervise.py --ensure \\
    >> $SUPERVISOR_LOG 2>&1 < /dev/null &
sleep 2
echo \"\$(hostname -s): supervisors now: \$(pgrep -fc 'python.*supervise.py' 2>/dev/null || echo 0)\"" \
        || die "could not start a supervisor on $box"
done

say ""
say "# rollback complete."
say "# The pull queue accepts submissions again and the fence marker is gone."
say "# The default transport is whatever $PREVIOUS was published with, which"
say "# for every generation before the cutover is the pull queue."
say "# SLURM jobs already running are unaffected; withdraw the ones you do not"
say "# want with: pbrun --transport slurm --withdraw <key prefix>"
