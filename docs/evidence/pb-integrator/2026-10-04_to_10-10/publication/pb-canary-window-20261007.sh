#!/bin/bash
# The 1598 carry canary window (dec-1007-120346-6a42, option B). Author pb-integrator, 2026-10-07. NOT RUN.
#
# Runs ONLY after D44 reports its last grid wave ended and holds new submissions. One step per call, so each step is read
# before the next. This script submits canary work and the target-evidence gang. It never publishes and never adopts:
# adoption is the separate, last step, by hand, only if EVERY step passed.
#
#   preflight   read-only. Aborts unless the live pointer is unchanged, the staged generation is sealed and not active,
#               the clone is at the approved head, and neither Spark holds or queues GPU work.
#   packet      priority 0, BEFORE any GPU row of mine is READY (a READY GPU row defers a CPU action on a Spark:
#               deferred_for_ready_gpu). Collects the evidence packet and checks the staged client accepts it.
#   canary      the four-leg driver from sparky, priority 0. Exit 0 is the only pass; a partial run cannot exit 0.
#   gang        the two-host target-evidence gang from celestia, priority 0, waited to terminal.
#
# env: none required. Everything is pinned below.
set -euo pipefail

LIVE=234357e7f55f-1791328304-34c26609fa28
G=a7bf5d3f66cf-1791369565-5c5f62ec90d2
HEAD_WANT=a7bf5d3f66cfff96b494b52e70998cd304fe2518
GP=/mnt/shared/prismabuild-fleet/runtime-generations/$G
D=/mnt/shared/fleet-ceo/pb-publish-20261007/carry-clone
W=/mnt/shared/fleet-ceo/pb-publish-20261007
PACKET=$W/packet-$(date -u +%Y%m%dT%H%M%SZ).json
PACKET_FILE=$W/packet-current.txt
GPU_IMAGE='prismaquant-glm-derivative@sha256:c0e532d28a78b3bf425bbbc0d862e2840ba624249162aedfadd09748a6c68c37'
P=/home/rob/tmp/pb-submit-celestia-20261003/bin/python
R=/mnt/shared/prismabuild-fleet/repo/tools

step=${1:?usage: $0 preflight|packet|canary|gang}

case "$step" in
preflight)
  live=$(python3 -c "import json;print(json.load(open('/mnt/shared/prismabuild-fleet/repo/RUNTIME_VERSION.json'))['generation'])")
  [ "$live" = "$LIVE" ] || { echo "ABORT: live generation is $live, not $LIVE"; exit 3; }
  python3 - <<E
import json
d=json.load(open("$GP/RUNTIME_VERSION.json"))
assert d["generation"]=="$G", d["generation"]
assert d["commit"]=="$HEAD_WANT", d["commit"]
assert d.get("dirty") is False
print("staged generation sealed:", d["generation"], d["commit"][:12], "rollout", d.get("rollout"))
E
  [ "$(git -C "$D" rev-parse HEAD)" = "$HEAD_WANT" ] || { echo "ABORT: clone is not at $HEAD_WANT"; exit 3; }
  [ -z "$(git -C "$D" status --short)" ] || { echo "ABORT: clone is dirty"; exit 3; }
  echo "== fleet: no GPU job may be claimed or READY on a Spark"
  $P $R/pbstatus.py --transport pool --json > /tmp/window-status.json
  python3 - <<'E'
import json,sys
d=json.load(open('/tmp/window-status.json'))
jobs=d.get('jobs') or []
busy=[j for j in jobs if (j.get('resources') or {}).get('gpu') and j.get('state') in ('READY','CLAIMED')]
for j in busy: print(' GPU job', j.get('action_key','')[:12], j.get('state'), j.get('node'), j.get('priority'), j.get('resources'))
if busy:
    print('ABORT: GPU work is queued or running; D44 has not finished'); sys.exit(4)
print('no GPU job is claimed or READY')
E
  echo "preflight ok: the window may open"
  ;;

packet)
  # Priority 0, from a snapshotted checkout (pbrun refuses a script outside the repository). Waits to terminal.
  echo "$PACKET" > "$PACKET_FILE"
  (cd "$D" && timeout 300 python3 "$GP/tools/pbrun.py" --cwd "$D" --tag gb10 --priority 0 --demand mem_gb=2 --cpus 1 \
      --timeout-s 240 -- /usr/bin/python3 tools/fleet/pbevidence.py --out "$PACKET")
  [ -s "$PACKET" ] || { echo "ABORT: no packet was written"; exit 5; }
  python3 - <<E
import sys,json
sys.path.insert(0,"$GP/tools"); sys.path.insert(0,"$GP/src")
import pbrun
pbrun.load_target_evidence("$PACKET", host_class="gb10")
raw=json.load(open("$PACKET"))
print("the STAGED pbrun accepts the packet; host", raw.get("host"), "driver", raw.get("nvidia_driver"), "cc", raw.get("cuda_compute_capability"))
E
  echo "packet: $PACKET"
  ;;

canary)
  RID=$(date -u +%Y%m%dT%H%M%SZ)w; echo "$RID" > $W/window-canary-id.txt
  # From sparky: leg 4 pins sparky and sparklina by tag, and the driver's bare python3 must resolve on a worker box.
  set +e
  ssh -o BatchMode=yes sparky "cd $D && PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 tools/fleet/pbcanary.py \
      --legs leg-1,leg-2,leg-3,leg-4 --priority 0 --run-id $RID --generation $G --published-root $GP --checkout $D \
      --gpu-image '$GPU_IMAGE'" 2>&1 | tee "$W/window-canary-$RID.log"
  rc=${PIPESTATUS[0]}
  set -e
  echo "canary driver exit: $rc  (0 is the only pass)  result: /mnt/shared/prismabuild-fleet/pb-canary/$RID/canary-result.json"
  exit "$rc"
  ;;

gang)
  PK=$(cat "$PACKET_FILE")
  [ -s "$PK" ] || { echo "ABORT: no packet recorded; run the packet step first"; exit 5; }
  M=$W/window-gang-manifest.json
  python3 - <<E
import json
member={"measurement":True,"host_class":"gb10","exclusive":True,"max_attempts":1,
        "demand":{"cpu":2,"gpu":1,"mem_gb":8},"argv":["/bin/true"]}
json.dump({"priority":0,"timeout_s":600,"members":[dict(member,tag="sparklina"),dict(member,tag="sparky")]},open("$M","w"),indent=1)
print("manifest written: $M")
E
  python3 "$GP/tools/pbgang.py" --manifest "$M" --cwd "$D" --target-evidence "$PK" 2>&1 | tee "$W/window-gang.log"
  echo "gang submitted; wait for BOTH members to reach terminal with pbwait, then read both receipts"
  ;;
*) echo "unknown step $step"; exit 2;;
esac
