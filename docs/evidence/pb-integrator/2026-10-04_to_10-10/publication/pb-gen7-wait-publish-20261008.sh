#!/bin/bash
# Publish gen7 (54600c279f, PR 1633) when the GPU census rows end, or after one hour, then run the four-leg canary.
# CEO decision dec-1008-100930-eccf option B (2026-10-08). Author pb-integrator. A definitive canary failure rolls back to 2399c0008f.
set -uo pipefail
HW=54600c279f7446c72eef6a6e4d9061fac1a98242
GK=eb142b977097e1e7084d7c48d7f03db907414e665bcc27b92c646c9c17a21fe9
PARENT=2399c0008ff75f6d4eed2f9c666b7591275579e4
PARENT_GEN=2399c0008ff7-1791450990-6fa074e867f3
W=/mnt/shared/fleet-ceo/pb-publish-20261008
LOG=/home/rob/fleet/inventory/pb-gen7-wait-publish-20261008.log
P=/home/rob/tmp/pb-submit-celestia-20261003/bin/python; R=/mnt/shared/prismabuild-fleet/repo/tools
DEADLINE=$(( $(date +%s) + ${WAIT_S:-3500} ))
say() { echo "$(date -u +%H:%M:%SZ) $*" | tee -a "$LOG"; }
live() { python3 -c "import json;print(json.load(open('/mnt/shared/prismabuild-fleet/repo/RUNTIME_VERSION.json'))['generation'])"; }
gpu_jobs() { timeout 100 $P $R/pbstatus.py --transport pool --json 2>/dev/null | python3 -c "
import json,sys
d=json.load(sys.stdin)
print(len([j for j in d.get('jobs') or [] if (j.get('resources') or {}).get('gpu') and j.get('state') in ('READY','CLAIMED')]))" 2>/dev/null; }
quiet=0; reason=""
while true; do
  [ "$(live)" = "$PARENT_GEN" ] || { say "ABORT: live generation is $(live), not $PARENT_GEN"; exit 3; }
  n=$(gpu_jobs); n=${n:-?}
  if [ "$n" = "0" ]; then quiet=$((quiet+1)); else quiet=0; fi
  say "GPU jobs READY or CLAIMED: $n (quiet checks in a row: $quiet)"
  if [ "$quiet" -ge 2 ]; then reason="the GPU census rows ended (two quiet checks)"; break; fi
  if [ "$(date +%s)" -ge "$DEADLINE" ]; then reason="one hour passed with GPU work still running; the CEO said to publish anyway and let the canary queue"; break; fi
  sleep 30
done
say "publishing: $reason"
REL=/home/rob/wt/pb-carry-gen7 HEAD_WANT=$HW GATE_KEY=$GK REASON_FILE=/home/rob/fleet/inventory/pb-publish-gen7-reason-20261008.txt \
  PUBLISH_AUTH="CEO decision dec-1008-100930-eccf APPROVE option B (publish when the census ends, or after one hour, then the four-leg canary; roll back to 2399c0008f if it fails), 2026-10-08, via pb-integrator" \
  PARENT_WANT=$PARENT /home/rob/fleet/inventory/pb-publish-gen7-20261008.sh > "$W/publish-gen7.out" 2>&1; prc=$?
say "publish rc=$prc; live generation now $(live)"
[ "$prc" -eq 0 ] || { say "PUBLISH FAILED: see $W/publish-gen7.out; nothing to roll back unless the live generation moved"; exit 4; }
G=$(live); case "$G" in "${HW:0:12}"-*) ;; *) say "live generation $G does not start with ${HW:0:12}: STOP"; exit 5;; esac
sed -e "s#^G=.*#G=$G#" -e 's#carry-clone-gen6#carry-clone-gen7#' -e 's#g6; echo#g7; echo#' /home/rob/fleet/inventory/pb-canary-gen6-20261008.sh > /home/rob/fleet/inventory/pb-canary-gen7-20261008.sh
chmod +x /home/rob/fleet/inventory/pb-canary-gen7-20261008.sh
say "starting the four-leg canary against $G"
/home/rob/fleet/inventory/pb-canary-gen7-20261008.sh > "$W/canary-gen7.out" 2>&1 & cpid=$!
sleep 5; RID=$(cat $W/canary-id.txt); RES=/mnt/shared/prismabuild-fleet/pb-canary/$RID/canary-result.json
say "canary run $RID; result file $RES"
for i in $(seq 1 540); do [ -s "$RES" ] && break; kill -0 $cpid 2>/dev/null || break; sleep 10; done
wait $cpid 2>/dev/null; crc=$?
verdict=$(python3 -c "
import json
try:
    d=json.load(open('$RES')); v=d.get('verdict',{}); print(v.get('exit_code'), v.get('failed_leg'), v.get('failed_check'))
except Exception as x: print('none', x)")
say "canary driver rc=$crc; verdict (exit failed_leg failed_check): $verdict"
if [ "$crc" -eq 0 ] && [ "${verdict%% *}" = "0" ]; then say "CANARY PASSED"; exit 0; fi
if [ "${verdict%% *}" = "none" ]; then say "NO RESULT FILE: not a pass and not proven a failure; NOT rolling back; waiting for a human"; exit 6; fi
say "CANARY FAILED: rolling back to $PARENT_GEN"
cd /home/rob/wt/pb-carry-gen6 && PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 tools/fleet/publish_runtime.py --activate-generation "$PARENT_GEN" --rollout rolling \
  --rollout-reason "Rollback of 54600c279f: its four-leg canary failed ($verdict), run $RID. CEO dec-1008-100930-eccf." 2>&1 | tail -4 | tee -a "$LOG"
say "rollback done; live generation now $(live)"; exit 7
