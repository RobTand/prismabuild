#!/bin/bash
# Publish gen11: main c3c98642ad plus the revert of PR 1640 (one commit), as one runtime generation. Author pb-integrator, 2026-10-09.
# CEO decision dec-1009-013440-42a4 APPROVE option B. Adapted from pb-publish-gen8-20261008.sh. This is NOT a carry on the live commit: the head
# is a child of main, so the "live is an ancestor" check is replaced by two checks below (parent is main; PR 1640 patch + head tree = main tree).
# Replaces pb1526-publish-20261005.sh, which is pinned to one head and requires WINDOW4_DONE.
#
# Authorization: CEO message 14:30Z (P0, image eligibility blocks Window 4) lifts "no activation while Window 4 is pending" for this
# publication. PUBLISH_AUTH must say who authorized this exact publication; it is printed and recorded.
#
# usage (all variables required unless marked optional):
#   REL=/home/rob/tmp/<worktree> HEAD_WANT=<40 hex> GATE_KEY=<64 hex shape-gate action key of THIS head> \
#   REASON_FILE=<file holding the rollout reason> PUBLISH_AUTH="<who/when>" ~/fleet/inventory/pb-publish-stack-20261005.sh [--dry-run-only]
#   optional: PARENT_WANT (default: the live commit 04f6f00e0410...)
#
# Nothing is rolled back automatically. A running action is not touched by a rolling activation (supervise.py: "active work finishes under
# the generation that claimed it, and idle loops upgrade independently"). The canary is started in the BACKGROUND on sparky so GPU legs
# can queue behind a running arm without holding this script.
set -euo pipefail

: "${REL:?set REL to the release worktree}"; : "${HEAD_WANT:?set HEAD_WANT (40 hex)}"; : "${GATE_KEY:?set GATE_KEY (64 hex)}"
: "${MAIN_WANT:?set MAIN_WANT (40 hex, the main commit the head is built on)}"; : "${REASON_FILE:?set REASON_FILE}"; : "${PUBLISH_AUTH:?set PUBLISH_AUTH to who authorized this exact publication and when}"
PARENT_WANT=${PARENT_WANT:-cfe060ca720547273d2fdba125cdbffb1aaa8223}   # the live commit (gen10)
GPU_IMAGE='prismaquant-glm-derivative@sha256:c0e532d28a78b3bf425bbbc0d862e2840ba624249162aedfadd09748a6c68c37'
CONTROLLER=/home/rob/tmp/pb1514-1523-canary-controller-20261005   # on sparky; the checkout that drove the 10:44Z canary
LOGDIR=/mnt/shared/fleet-ceo/pb-publish-20261009; mkdir -p "$LOGDIR"
[[ "$HEAD_WANT" =~ ^[0-9a-f]{40}$ ]] || { echo "HEAD_WANT must be 40 hex"; exit 2; }
[[ "$GATE_KEY" =~ ^[0-9a-f]{64}$ ]] || { echo "GATE_KEY must be 64 hex"; exit 2; }
[ -s "$REASON_FILE" ] || { echo "REASON_FILE is empty or missing"; exit 2; }
REASON=$(cat "$REASON_FILE"); [ -n "${PUBLISH_AUTH// /}" ] || { echo "PUBLISH_AUTH is blank"; exit 2; }
STAMP=$(date -u +%Y%m%dT%H%M%SZ); LOG="$LOGDIR/publish-$STAMP.log"
exec > >(tee -a "$LOG") 2>&1
echo "== $STAMP publish stack; head $HEAD_WANT; gate $GATE_KEY; authorized by: $PUBLISH_AUTH"

echo "== preflight"
[ "$(git -C "$REL" rev-parse HEAD)" = "$HEAD_WANT" ] || { echo "release worktree is not at the approved head"; exit 3; }
[ -z "$(git -C "$REL" status --short)" ] || { echo "release worktree is dirty"; exit 3; }
live=$(python3 -c "import json;print(json.load(open('/mnt/shared/prismabuild-fleet/repo/RUNTIME_VERSION.json'))['commit'])")
[ "$live" = "$PARENT_WANT" ] || { echo "live generation is $live, not the reviewed parent $PARENT_WANT. STOP and ask the CEO."; exit 3; }
[ "$(git -C "$REL" rev-parse "$HEAD_WANT^")" = "$MAIN_WANT" ] && [ "$(git -C "$REL" rev-list --count "$MAIN_WANT..$HEAD_WANT")" = 1 ] || { echo "the head is not exactly one commit on $MAIN_WANT"; exit 3; }
PROOF=$(mktemp -d /tmp/gen11-proof.XXXXXX)
git -C "$REL" diff ba540d991bbd06c3cfc9c59d4b8c60052e6ad09b^1 ba540d991bbd06c3cfc9c59d4b8c60052e6ad09b > "$PROOF/pr1640.patch"
git -C "$REL" worktree add -q --detach "$PROOF/t" "$HEAD_WANT"
( cd "$PROOF/t" && git apply --3way "$PROOF/pr1640.patch" >/dev/null 2>&1 && git add -A && [ "$(git write-tree)" = "$(git rev-parse "$MAIN_WANT^{tree}")" ] ) \
  || { git -C "$REL" worktree remove --force "$PROOF/t"; echo "PROOF FAILED: PR 1640's patch on the head does not give main's tree"; exit 3; }
git -C "$REL" worktree remove --force "$PROOF/t"; rm -rf "$PROOF"
echo "live $live ok; head ok; clean; head = main $MAIN_WANT + one commit; PR 1640's patch on the head gives main's tree exactly"
echo "== emitter gate (CEO dec-1005-222738-0a37): refuse an emitter generation while any pbmcp reader predates the reader-first generation"
"$(dirname "$0")/pb-emitter-reader-gate-20261005.sh" "$REL" "$HEAD_WANT"; gate_rc=$?
[ "$gate_rc" -eq 0 ] || { echo "PUBLICATION REFUSED by the emitter reader gate (exit $gate_rc). Nothing was published." >&2; exit "$gate_rc"; }
echo "== what is running (read it; this publication does not touch running work)"
P=/home/rob/tmp/pb-submit-celestia-20261003/bin/python; R=/mnt/shared/prismabuild-fleet/repo/tools
timeout 100 $P $R/pbstatus.py 2>&1 | head -40 || true

cd "$REL"
echo "== dry run"
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src timeout 110 python3 tools/fleet/publish_runtime.py --dry-run --rollout rolling \
  --rollout-reason "$REASON" --shape-gate-action "$GATE_KEY" 2>&1 | tee "$LOGDIR/dry-run-$STAMP.log" | grep -E "^(rollout|shape gate)|refus|error|Error" | cut -c1-240
[ "${1:-}" = "--dry-run-only" ] && { echo "dry run only; stopping"; exit 0; }

echo "== publish (--no-canary: the canary is driven from sparky below, in the background)"
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 tools/fleet/publish_runtime.py --rollout rolling \
  --rollout-reason "$REASON" --shape-gate-action "$GATE_KEY" --no-canary 2>&1 | tee "$LOGDIR/publish-out-$STAMP.log" | tail -8
GEN=$(python3 -c "import json;print(json.load(open('/mnt/shared/prismabuild-fleet/repo/RUNTIME_VERSION.json'))['generation'])")
echo "live generation: $GEN"; echo "$GEN" > "$LOGDIR/generation-$STAMP.txt"
case "$GEN" in "${HEAD_WANT:0:12}"-*) ;; *) echo "live generation does not start with the approved head ${HEAD_WANT:0:12}"; exit 4;; esac
echo "== published. Canary is the separate step (the wrapper pb-gen11-publish-canary-20261009.sh)"
