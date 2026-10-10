#!/bin/bash
# Publish a reviewed carry head as one runtime generation WITH the normal fleet canary. Author pb-integrator, 2026-10-07.
# The canary is publish_runtime's own (default ON): four legs, about 3 minutes. A failed canary marks the rollout record failed and exits
# nonzero; it never rolls back. The rollback is by hand and is printed on failure (CEO condition 3 of the 17:38Z decision).
#   REL=<release worktree> HEAD_WANT=<40 hex> GATE_KEY=<64 hex> REASON_FILE=<file> PARENT_WANT=<40 hex live commit> PUBLISH_AUTH="<who/when>" this-script
set -euo pipefail
: "${REL:?}"; : "${HEAD_WANT:?}"; : "${GATE_KEY:?}"; : "${REASON_FILE:?}"; : "${PARENT_WANT:?}"; : "${PUBLISH_AUTH:?}"
LOGDIR=/mnt/shared/fleet-ceo/pb-publish-20261007; mkdir -p "$LOGDIR"; STAMP=$(date -u +%Y%m%dT%H%M%SZ); LOG="$LOGDIR/publish-canary-$STAMP.log"
exec > >(tee -a "$LOG") 2>&1
[[ "$HEAD_WANT" =~ ^[0-9a-f]{40}$ ]] && [[ "$GATE_KEY" =~ ^[0-9a-f]{64}$ ]] || { echo "bad HEAD_WANT or GATE_KEY"; exit 2; }
[ -s "$REASON_FILE" ] || { echo "empty reason"; exit 2; }; [ -n "${PUBLISH_AUTH// /}" ] || { echo "blank PUBLISH_AUTH"; exit 2; }
echo "== $STAMP publish with canary; head $HEAD_WANT; gate $GATE_KEY; authorized by: $PUBLISH_AUTH"
[ "$(git -C "$REL" rev-parse HEAD)" = "$HEAD_WANT" ] || { echo "release worktree is not at the approved head"; exit 3; }
[ -z "$(git -C "$REL" status --short)" ] || { echo "release worktree is dirty"; exit 3; }
live_gen=$(readlink /mnt/shared/prismabuild-fleet/repo | sed 's#.*/##'); echo "live generation before: $live_gen"
live=$(python3 -c "import json;print(json.load(open('/mnt/shared/prismabuild-fleet/repo/RUNTIME_VERSION.json'))['commit'])")
[ "$live" = "$PARENT_WANT" ] || { echo "live commit is $live, not the reviewed parent $PARENT_WANT. STOP."; exit 3; }
git -C "$REL" merge-base --is-ancestor "$PARENT_WANT" "$HEAD_WANT" || { echo "live commit is not an ancestor of the head"; exit 3; }
"$(dirname "$0")/pb-emitter-reader-gate-20261005.sh" "$REL" "$HEAD_WANT"
echo "$live_gen" > "$LOGDIR/generation-before-$STAMP.txt"
cd "$REL"
echo "== publish (canary ON)"
set +e
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 tools/fleet/publish_runtime.py --rollout rolling --rollout-reason "$(cat "$REASON_FILE")" --shape-gate-action "$GATE_KEY" 2>&1 | tee "$LOGDIR/publish-out-$STAMP.log" | tail -30 | cut -c1-260
rc=${PIPESTATUS[0]}; set -e
GEN=$(readlink /mnt/shared/prismabuild-fleet/repo | sed 's#.*/##'); echo "live generation after: $GEN; publish exit $rc"; echo "$GEN" > "$LOGDIR/generation-$STAMP.txt"
CAN=/mnt/shared/prismabuild-fleet/runtime-generations/$GEN.canary.json; [ -f "$CAN" ] && cat "$CAN" || echo "no canary record at $CAN"
if [ "$rc" -ne 0 ]; then
  echo "CANARY OR PUBLISH FAILED. Rollback by hand, now:"
  echo "  python3 $REL/tools/fleet/publish_runtime.py --activate-generation $live_gen --rollout rolling --rollout-reason '<reverse transition: canary failed on $GEN>'"
fi
exit "$rc"
