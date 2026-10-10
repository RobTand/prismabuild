#!/bin/bash
# Publish gen11 (main c3c98642ad + the revert of PR 1640, one commit), then the four-leg canary. Author pb-integrator, 2026-10-09.
# CEO decision dec-1009-013440-42a4 APPROVE option B: independent Sol review of the diff first; dry run, gates, rolling rollout that drains each
# worker, four-leg canary, rollback to gen11 ready. Canary rule (proposed in rep-1009-013529-ccb3): ANYTHING other than driver exit 0 with verdict
# exit_code 0 -- a failed leg, a leg not verified, no result file -- rolls back to gen11 at once. No retries to chase green.
# Run only after the review approves and the CEO has said go. Required environment: GK (shape-gate action key of the head), REVIEW_OK (review id).
set -uo pipefail
HW=281006ef6374e4c058b84d0c306c2460a21771dd
MAIN=c3c98642ad5661280dce6e2d80770686b5f8a14e
: "${GK:?set GK to the shape-gate action key (64 hex) of the head}"; : "${REVIEW_OK:?set REVIEW_OK to the approving review id}"
PARENT=cfe060ca720547273d2fdba125cdbffb1aaa8223
PARENT_GEN=cfe060ca7205-1791554831-7b15cf417d57
REL=/home/rob/wt/pb-gen11-prep
W=/mnt/shared/fleet-ceo/pb-publish-20261009
LOG=/home/rob/fleet/inventory/pb-gen11-publish-canary-20261009.log
say() { echo "$(date -u +%H:%M:%SZ) $*" | tee -a "$LOG"; }
live() { python3 -c "import json;print(json.load(open('/mnt/shared/prismabuild-fleet/repo/RUNTIME_VERSION.json'))['generation'])"; }
[ "$(live)" = "$PARENT_GEN" ] || { say "ABORT: live generation is $(live), not $PARENT_GEN"; exit 3; }
say "publishing gen11 over $PARENT_GEN"
REL=$REL HEAD_WANT=$HW MAIN_WANT=$MAIN GATE_KEY=$GK REASON_FILE=/home/rob/fleet/inventory/pb-publish-gen11-reason-20261009.txt \
  PUBLISH_AUTH="CEO message 2026-10-09 ~20:2xZ: publish a PrismaBuild runtime generation from main now, holding the fixes for #1709 (PR 1712) and #1705; check that a pbtest shard uses the shorter path; report the generation id (main c3c98642ad plus the revert of PR 1640, one commit; independent review $REVIEW_OK; emitter and shape gates; full-suite gate with the candidate pbtest; rolling, draining each worker; four-leg canary; rollback to gen10 $PARENT_GEN), via pb-integrator" \
  PARENT_WANT=$PARENT /home/rob/fleet/inventory/pb-publish-gen11-20261009.sh > "$W/publish-gen11.out" 2>&1; prc=$?
say "publish rc=$prc; live generation now $(live)"
[ "$prc" -eq 0 ] || { say "PUBLISH FAILED: see $W/publish-gen11.out; nothing to roll back unless the live generation moved"; exit 4; }
G=$(live); case "$G" in "${HW:0:12}"-*) ;; *) say "live generation $G does not start with ${HW:0:12}: STOP"; exit 5;; esac
rm -rf "$W/carry-clone-gen11"; git clone -q "$REL" "$W/carry-clone-gen11" && git -C "$W/carry-clone-gen11" checkout -q "$HW" || { say "carry clone failed: STOP"; exit 5; }
sed -e "s#^G=.*#G=$G#" -e 's#carry-clone-gen7#carry-clone-gen11#' -e 's#g7; echo#g11; echo#' -e 's#generation 2399c0008ff7[^ ]*#generation '"$G"'#' -e 's#pb-publish-20261008#pb-publish-20261009#g' /home/rob/fleet/inventory/pb-canary-gen7-20261008.sh > /home/rob/fleet/inventory/pb-canary-gen11-20261009.sh
chmod +x /home/rob/fleet/inventory/pb-canary-gen11-20261009.sh
say "starting the four-leg canary against $G"
/home/rob/fleet/inventory/pb-canary-gen11-20261009.sh > "$W/canary-gen11.out" 2>&1 & cpid=$!
sleep 5; RID=$(cat $W/canary-id.txt); RES=/mnt/shared/prismabuild-fleet/pb-canary/$RID/canary-result.json
say "canary run $RID; result file $RES"
for i in $(seq 1 540); do [ -s "$RES" ] && break; kill -0 $cpid 2>/dev/null || break; sleep 10; done
wait $cpid 2>/dev/null; crc=$?
for i in $(seq 1 30); do [ -s "$RES" ] && break; sleep 2; done  # the driver can exit in the same second it seals the file
verdict=$(python3 -c "
import json
try:
    d=json.load(open('$RES')); v=d.get('verdict',{}); print(v.get('exit_code'), v.get('failed_leg'), v.get('failed_check'))
except Exception as x: print('none', x)")
say "canary driver rc=$crc; verdict (exit failed_leg failed_check): $verdict"
if [ "$crc" -eq 0 ] && [ "${verdict%% *}" = "0" ]; then say "CANARY PASSED"; exit 0; fi
if [ "${verdict%% *}" = "none" ]; then say "NO RESULT FILE after a wait: not a pass; under the proposed rule that rolls back"; fi
say "CANARY DID NOT PASS (anything but exit 0): rolling back to $PARENT_GEN"
cd "$REL" && PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 tools/fleet/publish_runtime.py --activate-generation "$PARENT_GEN" --rollout rolling \
  --rollout-reason "Rollback of ${HW:0:10}: its four-leg canary did not pass ($verdict), run $RID. CEO dec-1009-013440-42a4." 2>&1 | tail -4 | tee -a "$LOG"
say "rollback done; live generation now $(live)"; exit 7
