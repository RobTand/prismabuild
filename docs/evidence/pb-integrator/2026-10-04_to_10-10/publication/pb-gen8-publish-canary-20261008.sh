#!/bin/bash
# Publish gen8 (8f58b7f33e, the PR 1638 net on the live 54600c279f), then the four-leg canary, rolling back on a definitive failure.
# CEO decision dec-1008-161401-bcf1 APPROVE option A (carry only; emitter and shape gates first; roll out worker by worker, draining each
# worker so no running action stops; four-leg canary; rollback to 54600c279f74 kept ready and used at once on a canary failure; PACT keeps its
# priority and is never held for the rollout). Author pb-integrator, 2026-10-08. The CEO ordered the rollout held at ~16:35Z while four PACT rows
# wait: run this only on the CEO's word.
set -uo pipefail
HW=8f58b7f33ef627100eb2e5be25a65e4b13f5318e
GK=9ee6414689315b11a618ea5c1b64039d20b565faada6e99063f1ce2240f567b3
PARENT=54600c279f7446c72eef6a6e4d9061fac1a98242
PARENT_GEN=54600c279f74-1791454732-72ee56a0cf48
REL=/home/rob/wt/pb-carry-gen8
W=/mnt/shared/fleet-ceo/pb-publish-20261008
LOG=/home/rob/fleet/inventory/pb-gen8-publish-canary-20261008.log
say() { echo "$(date -u +%H:%M:%SZ) $*" | tee -a "$LOG"; }
live() { python3 -c "import json;print(json.load(open('/mnt/shared/prismabuild-fleet/repo/RUNTIME_VERSION.json'))['generation'])"; }
[ "$(live)" = "$PARENT_GEN" ] || { say "ABORT: live generation is $(live), not $PARENT_GEN"; exit 3; }
say "publishing gen8 over $PARENT_GEN"
REL=$REL HEAD_WANT=$HW GATE_KEY=$GK REASON_FILE=/home/rob/fleet/inventory/pb-publish-gen8-reason-20261008.txt \
  PUBLISH_AUTH="CEO decision dec-1008-161401-bcf1 APPROVE option A (carry-only generation of PR 1638 on 54600c279f74; emitter and shape gates; rolling, draining each worker; four-leg canary; rollback to 54600c279f74), 2026-10-08, via pb-integrator" \
  PARENT_WANT=$PARENT /home/rob/fleet/inventory/pb-publish-gen8-20261008.sh > "$W/publish-gen8.out" 2>&1; prc=$?
say "publish rc=$prc; live generation now $(live)"
[ "$prc" -eq 0 ] || { say "PUBLISH FAILED: see $W/publish-gen8.out; nothing to roll back unless the live generation moved"; exit 4; }
G=$(live); case "$G" in "${HW:0:12}"-*) ;; *) say "live generation $G does not start with ${HW:0:12}: STOP"; exit 5;; esac
rm -rf "$W/carry-clone-gen8"; git clone -q "$REL" "$W/carry-clone-gen8" && git -C "$W/carry-clone-gen8" checkout -q "$HW" || { say "carry clone failed: STOP"; exit 5; }
sed -e "s#^G=.*#G=$G#" -e 's#carry-clone-gen7#carry-clone-gen8#' -e 's#g7; echo#g8; echo#' -e 's#generation 2399c0008ff7[^ ]*#generation '"$G"'#' /home/rob/fleet/inventory/pb-canary-gen7-20261008.sh > /home/rob/fleet/inventory/pb-canary-gen8-20261008.sh
chmod +x /home/rob/fleet/inventory/pb-canary-gen8-20261008.sh
say "starting the four-leg canary against $G"
/home/rob/fleet/inventory/pb-canary-gen8-20261008.sh > "$W/canary-gen8.out" 2>&1 & cpid=$!
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
if [ "${verdict%% *}" = "none" ]; then say "NO RESULT FILE: not a pass and not proven a failure; NOT rolling back; waiting for a human"; exit 6; fi
say "CANARY FAILED: rolling back to $PARENT_GEN"
cd "$REL" && PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 tools/fleet/publish_runtime.py --activate-generation "$PARENT_GEN" --rollout rolling \
  --rollout-reason "Rollback of ${HW:0:10}: its four-leg canary failed ($verdict), run $RID. CEO dec-1008-161401-bcf1." 2>&1 | tail -4 | tee -a "$LOG"
say "rollback done; live generation now $(live)"; exit 7
