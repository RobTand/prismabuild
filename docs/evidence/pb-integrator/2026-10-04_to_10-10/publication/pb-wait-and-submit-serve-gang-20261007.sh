#!/bin/bash
# Wait until neither Spark holds a GPU claim, then submit the serve gang once. Author pb-integrator, 2026-10-07.
# Authority: eng-serve-measure answered WAIT to msg-1007-045655-216a; CEO release dec-1007-034122-babe.
# Stops after MAX_WAIT_S without submitting. Submits at most once. Sends the result to eng-serve-measure.
set -uo pipefail
MANIFEST=/mnt/shared/tessera-measurements/eng-serve-measure-20261007/local-pair-run-9d3ed3f9-20261007T0343Z/native-gang-manifest.json
MAX_WAIT_S=${MAX_WAIT_S:-10800}; STEP_S=20
LOG=/mnt/shared/fleet-ceo/pb-publish-20261005/serve-gang-watch-$(date -u +%Y%m%dT%H%M%SZ).log
mkdir -p "$(dirname "$LOG")"; exec > >(tee -a "$LOG") 2>&1
send() { ~/fleet/ceo/bin/fleetctl send eng-serve-measure --text "$1" || ~/fleet/ceo/bin/fleetctl send LEAD --text "(for eng-serve-measure) $1"; }
gpu_claims() {  # prints the GPU claims on the two Sparks, one key per line
python3 - <<'PY'
import json,glob
for f in glob.glob('/mnt/shared/prismabuild-fleet/pb-queue/claimed/*.json'):
    try: d=json.load(open(f))
    except Exception: continue
    res=d.get('resources') or {}
    if d.get('claimed_host') in ('sparky','sparklina') and (d.get('needs_gpu') or int(res.get('gpu',0) or 0)>0):
        print(d.get('claimed_host'), d.get('action_key','')[:12])
PY
}
start=$(date +%s); clear=0
echo "== watching from $(date -u +%FT%TZ); max ${MAX_WAIT_S}s"
while :; do
  now=$(date +%s); [ $((now-start)) -ge "$MAX_WAIT_S" ] && { echo "max wait reached; not submitted"; send "pb-integrator: the watcher reached its time limit (${MAX_WAIT_S} s) and did not submit. A GPU claim was still present on a Spark. Tell me if I should watch again."; exit 5; }
  claims=$(gpu_claims)
  if [ -z "$claims" ]; then clear=$((clear+1)); else clear=0; echo "$(date -u +%T) held: $(echo $claims | tr '\n' ' ')"; fi
  [ "$clear" -ge 2 ] && break
  sleep "$STEP_S"
done
echo "== both Sparks clear twice; D1 and manifest check"
~/fleet/ceo/bin/fleet-diskcheck --need-gb 20 --hosts sparky,sparklina > /tmp/serve-d1.json 2>&1
python3 -c "import json;d=json.loads(open('/tmp/serve-d1.json').read().strip().splitlines()[-1]);import sys;sys.exit(0 if d['pass'] else 1)" || { send "pb-integrator: D1 disk check failed at submit time. I did not submit. Details are in $LOG."; exit 6; }
out=$(MANIFEST=$MANIFEST PROOF="eng-serve-measure WAIT answer; CEO dec-1007-034122-babe" ~/fleet/inventory/pb-submit-serve-gang-20261007.sh 2>&1); rc=$?
echo "$out" | tail -25
line=$(echo "$out" | grep -E '^\{.*"group"' | tail -1)
if [ $rc -ne 0 ] || [ -z "$line" ]; then send "pb-integrator: the gang submission failed (exit $rc). Nothing is claimed. Read the log: $LOG"; exit 7; fi
msg=$(python3 - "$line" <<'PY'
import json,sys
d=json.loads(sys.argv[1]); m=d.get('members',[])
print("pb-integrator: the corrected gang is SUBMITTED. Gang group id: %s. Priority %s. Member keys: %s. The gang claims when both Sparks pass every gate. The 1800 s bound starts at the claim. Register completion events for both keys." % (d.get('group'), d.get('priority'), "; ".join(str(x) for x in m)))
PY
)
echo "$msg"; send "$msg"; exit 0
