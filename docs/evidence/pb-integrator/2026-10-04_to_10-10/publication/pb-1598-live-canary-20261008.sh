#!/bin/bash
# The real two-host target-evidence canary for #1598. Author pb-integrator, 2026-10-08.
# Authority: CEO message 2026-10-08 (relaying kernels): idle-Spark ship work now at priority 0 when PACT has no ready GPU work queued.
# Replaces the 10-07 window script (pb-canary-window-20261007.sh), which was pinned to a generation that was never adopted.
#
# One step per call, so each result is read before the next. This script never publishes a runtime and never waives a gate.
#
#   preflight   read-only. Aborts unless the live generation is the expected one, no GPU job is READY or CLAIMED, no priority>=10
#               job is READY or CLAIMED, and fleet-diskcheck passes on both Sparks.
#   packet      a CPU row tagged gb10 at priority 0 collects the target-evidence packet on a Spark; the LIVE client then validates it.
#   validate    re-run the live client's validation on the recorded packet (no new row)
#   gang        the two-host gang from celestia at priority 0, one member per Spark, measurement + gb10 + exclusive, argv /bin/true.
#   wait KEY..  waits for the member keys to reach terminal and prints each row.
#
# The gang runs /bin/true. It reads no model and runs no matrix. It holds both Sparks' GPUs exclusively for a few seconds.
set -uo pipefail

LIVE=${LIVE:-54600c279f74-1791454732-72ee56a0cf48}
GP=/mnt/shared/prismabuild-fleet/runtime-generations/$LIVE
D=/mnt/shared/fleet-ceo/pb-publish-20261008/carry-clone-gen7
W=/mnt/shared/fleet-ceo/pb-1598-canary-20261008
PACKET_FILE=$W/packet-current.txt
P=/home/rob/tmp/pb-submit-celestia-20261003/bin/python
R=/mnt/shared/prismabuild-fleet/repo/tools
mkdir -p "$W"
LOG=$W/canary.log
say() { echo "$(date -u +%H:%M:%SZ) $*" | tee -a "$LOG"; }

step=${1:?usage: $0 preflight|packet|gang|wait KEY...}
shift || true

case "$step" in
preflight)
  live=$(python3 -c "import json;print(json.load(open('/mnt/shared/prismabuild-fleet/repo/RUNTIME_VERSION.json'))['generation'])")
  [ "$live" = "$LIVE" ] || { say "ABORT: live generation is $live, not $LIVE"; exit 3; }
  [ -f "$GP/tools/pbgang.py" ] || { say "ABORT: no pbgang in the live generation"; exit 3; }
  timeout 100 $P $R/pbstatus.py --transport pool --json > $W/status-preflight.json 2>/dev/null
  python3 - <<'E' || exit 4
import json,sys
d=json.load(open('/mnt/shared/fleet-ceo/pb-1598-canary-20261008/status-preflight.json'))
jobs=d.get('jobs') or []
gpu=[j for j in jobs if (j.get('resources') or {}).get('gpu') and j.get('state') in ('READY','CLAIMED')]
p10=[j for j in jobs if (j.get('priority') or 0)>=10 and j.get('state') in ('READY','CLAIMED')]
for j in gpu+p10: print(' queued:',j.get('action_key','')[:12],j.get('state'),j.get('node'),'priority',j.get('priority'),j.get('resources'))
if gpu or p10:
    print('ABORT: GPU work or priority>=10 work is queued or running; the CEO allows this only when PACT has no ready GPU work'); sys.exit(1)
nodes={n['node']:n for n in d['nodes']}
for need in ('sparky','sparklina'):
    n=nodes.get(need)
    if not n or n.get('state')!='live': print('ABORT: %s is not live' % need); sys.exit(1)
print('no GPU job and no priority>=10 job is READY or CLAIMED; both Sparks are live')
E
  say "diskcheck, need 1 GiB"
  out=$(fleet-diskcheck --need-gb 1 --hosts sparky,sparklina 2>&1); echo "$out" > $W/diskcheck-preflight.json
  python3 - "$W/diskcheck-preflight.json" <<'E' || { say "ABORT: fleet-diskcheck did not pass"; exit 5; }
import json,sys
t=open(sys.argv[1]).read()
d=json.loads(t[t.index('{'):])
bad=[h for h,v in d['hosts'].items() if not v.get('pass')]
print('diskcheck hosts', {h:v.get('pass') for h,v in d['hosts'].items()})
sys.exit(1 if bad else 0)
E
  say "preflight ok: the window may open"
  ;;

packet)
  PACKET=$W/packet-$(date -u +%Y%m%dT%H%M%SZ).json
  echo "$PACKET" > "$PACKET_FILE"
  say "collecting the packet on a gb10 box, priority 0"
  (cd "$D" && timeout 300 python3 "$GP/tools/pbrun.py" --cwd "$D" --tag gb10 --priority 0 --demand mem_gb=2 --cpus 1 \
      --timeout-s 240 -- /usr/bin/python3 tools/fleet/pbevidence.py --out "$PACKET") 2>&1 | tail -4 | tee -a "$LOG"
  # The worker wrote the packet on another host; this box may not see it for a moment (NFS attribute cache).
  for i in $(seq 1 30); do [ -s "$PACKET" ] && break; sleep 1; done
  [ -s "$PACKET" ] || { say "ABORT: no packet was written within 30 s"; exit 5; }
  python3 - "$PACKET" <<E || exit 6
import sys,json
sys.path.insert(0,"$GP/tools"); sys.path.insert(0,"$GP/src")
import pbrun
pbrun.load_target_evidence(sys.argv[1], host_class="gb10")
raw=json.load(open(sys.argv[1]))
print("the LIVE pbrun accepts the packet; host", raw.get("host"), "driver", raw.get("nvidia_driver"), "cc", raw.get("cuda_compute_capability"))
E
  say "packet: $PACKET"
  ;;

validate)
  PK=$(cat "$PACKET_FILE" 2>/dev/null)
  [ -s "$PK" ] || { say "ABORT: no packet recorded or it is empty: $PK"; exit 5; }
  python3 - "$PK" <<E || exit 6
import sys,json
sys.path.insert(0,"$GP/tools"); sys.path.insert(0,"$GP/src")
import pbrun
pbrun.load_target_evidence(sys.argv[1], host_class="gb10")
raw=json.load(open(sys.argv[1]))
print("the LIVE pbrun accepts the packet; host", raw.get("host"), "driver", raw.get("nvidia_driver"), "cc", raw.get("cuda_compute_capability"))
print(json.dumps(raw)[:600])
E
  say "validated: $PK"
  ;;

gang)
  PK=$(cat "$PACKET_FILE" 2>/dev/null)
  [ -s "$PK" ] || { say "ABORT: no packet recorded; run the packet step first"; exit 5; }
  M=$W/gang-manifest.json
  python3 - <<E
import json
member={"measurement":True,"host_class":"gb10","exclusive":True,"max_attempts":1,
        "demand":{"cpu":2,"gpu":1,"mem_gb":8},"argv":["/bin/true"]}
json.dump({"priority":0,"timeout_s":600,"members":[dict(member,tag="sparklina"),dict(member,tag="sparky")]},open("$M","w"),indent=1)
print("manifest written: $M")
E
  say "submitting the two-host gang at priority 0"
  python3 "$GP/tools/pbgang.py" --manifest "$M" --cwd "$D" --target-evidence "$PK" 2>&1 | tee "$W/gang-submit.out" | tail -12 | tee -a "$LOG"
  say "gang submitted; wait for BOTH members with: $0 wait KEY KEY"
  ;;

wait)
  [ "$#" -ge 2 ] || { echo "usage: $0 wait KEY KEY"; exit 2; }
  python3 "$GP/tools/pbwait.py" --wait-s 900 --json "$@" 2>&1 | tee "$W/wait.out" | tail -30 | cut -c1-300 | tee -a "$LOG"
  ;;
*) echo "unknown step $step"; exit 2 ;;
esac
