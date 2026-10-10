#!/bin/bash
# The PB1627 live probe (CEO dec-1008-080711-64fe, step B). Author pb-integrator, 2026-10-08.
#
# One declared prelaunch CPU row at priority 0: nine model shards (43.86 GiB) in one resident_before_launch phase, read by
# nothing. The stage tier has about 35 GiB free and about 1,400 GiB held by ended consumers, so the row cannot launch until the
# tier loop makes room. The question: does window_pressure ask for that room, does the sweep take it, and if not, what does the
# new window-pressure-skipped event say?
#
#   preflight   read-only. Aborts unless the live generation is the instrumented one, no GPU job is READY or CLAIMED, and the
#               stage's free tokens are below the probe's need and its evictable bytes are above it.
#   submit      preflight, then submit with --detach, then watch for up to WATCH_S seconds. The watch withdraws the probe at once
#               if a GPU job becomes READY or CLAIMED (a PACT capture arrived), and at the end if it is still not terminal.
#   withdraw    withdraw the key in $PROBE_KEY_FILE now.
#
# The probe reads no model byte itself. Its movers copy the shards from the pool to the stage; the originals stay where they are.
set -uo pipefail

REPO=/home/rob/wt/lead-pb-integrator
P=/home/rob/tmp/pb-submit-celestia-20261003/bin/python
R=/mnt/shared/prismabuild-fleet/repo/tools
MANIFEST=/home/rob/fleet/inventory/pb-probe-manifest-20261008.json
LIVE_WANT=2399c0008ff7-1791450990-6fa074e867f3
TIER=prismabuild-stage:dl380g10
NEED_GIB=48
WATCH_S=${WATCH_S:-1200}
LOG=/home/rob/fleet/inventory/pb-live-probe-20261008.log
PROBE_KEY_FILE=/home/rob/fleet/inventory/pb-live-probe-20261008.key
step=${1:?usage: $0 preflight|submit|withdraw}

say() { echo "$(date -u +%H:%M:%SZ) $*" | tee -a "$LOG"; }

gpu_busy() {
  timeout 100 $P $R/pbstatus.py --transport pool --json 2>/dev/null | python3 -c "
import json,sys
d=json.load(sys.stdin)
for j in d.get('jobs') or []:
    if (j.get('resources') or {}).get('gpu') and j.get('state') in ('READY','CLAIMED'):
        print(j.get('action_key','')[:12], j.get('state'), j.get('node'))
"
}

tier_facts() {
  (cd $REPO && PYTHONDONTWRITEBYTECODE=1 python3 - <<E
import sys, json, pathlib
sys.path.insert(0,'tools/fleet'); sys.path.insert(0,'src')
from prismabuild import pool
import stage_release
q=pool.PoolQueue(pathlib.Path('/mnt/shared/prismabuild-fleet/pb-queue'))
led=q.tier_ledger('$TIER'); held=led.held_keys()
wanted,owners=stage_release.live_claims(q)
live=[h for h in held if h in wanted or h in owners]
print(json.dumps({'capacity':led.capacity().get('stage_gib'),'held':led.held().get('stage_gib'),'free':led.available().get('stage_gib'),'holders':len(held),'live_holders':len(live)}))
E
  )
}

preflight() {
  local live; live=$(python3 -c "import json;print(json.load(open('/mnt/shared/prismabuild-fleet/repo/RUNTIME_VERSION.json'))['generation'])")
  [ "$live" = "$LIVE_WANT" ] || { say "ABORT: live generation is $live, not $LIVE_WANT"; return 3; }
  local busy; busy=$(gpu_busy)
  [ -z "$busy" ] || { say "ABORT: GPU work is READY or CLAIMED: $busy"; return 4; }
  local facts; facts=$(tier_facts); say "tier: $facts"
  python3 - "$facts" "$NEED_GIB" <<'E' || return 5
import json,sys
f=json.loads(sys.argv[1]); need=int(sys.argv[2])
free=int(f['free']); evict=int(f['held'])-0
if free >= need: print('ABORT: free %d >= need %d: the probe would not need room' % (free,need)); sys.exit(1)
if int(f['held']) - need < 0: print('ABORT: held below need'); sys.exit(1)
print('probe need %d GiB > free %d GiB; held %d GiB' % (need,free,int(f['held'])))
E
  say "preflight ok"
}

watch() {
  local key=$1 start now terminal
  start=$(date +%s)
  while true; do
    now=$(date +%s)
    local busy; busy=$(gpu_busy)
    if [ -n "$busy" ]; then say "GPU WORK ARRIVED ($busy): withdrawing the probe at once"; withdraw_now "$key"; return 6; fi
    local snap; snap=$(cd $REPO && PYTHONDONTWRITEBYTECODE=1 python3 - <<E
import sys, json, pathlib
sys.path.insert(0,'tools/fleet'); sys.path.insert(0,'src')
from prismabuild import pool, residency_plan
q=pool.PoolQueue(pathlib.Path('/mnt/shared/prismabuild-fleet/pb-queue'))
key='$key'
state='absent'
for st in (pool.READY,pool.CLAIMED,pool.DONE,pool.FAILED,'withdrawn'):
    if q.item_path(st,key).exists(): state=st; break
led=q.tier_ledger('$TIER')
plan=residency_plan.read(q,key)
leads=residency_plan.leads_for(plan) if plan else []
held_leads=sum(1 for l in leads if led.holder_tokens(l))
ev=[e for e in q.consumer_events(key)]
names={}
for e in ev: names[e.get('event')]=names.get(e.get('event'),0)+1
skips=[{k:e.get(k) for k in ('scope','reason','tier_id','evictable_gib','free_gib','held_gib','shortfall_gib','cur_min_gib','receiptless_holders','live_holders','prelaunch_holders','holders','waiters')} for e in ev if e.get('event')=='window-pressure-skipped']
print(json.dumps({'state':state,'free':led.available().get('stage_gib'),'held':led.held().get('stage_gib'),'leads':len(leads),'leads_holding':held_leads,'events':names,'skips':skips[-3:]}))
E
)
    say "watch: $snap"
    terminal=$(python3 -c "import json,sys;print(json.loads(sys.argv[1])['state'] in ('done','failed','withdrawn'))" "$snap")
    if [ "$terminal" = "True" ]; then say "probe reached a terminal state"; return 0; fi
    if [ $((now-start)) -ge "$WATCH_S" ]; then say "watch time (${WATCH_S}s) is over and the probe is not terminal: withdrawing it"; withdraw_now "$key"; return 7; fi
    sleep 10
  done
}

withdraw_now() {
  local key=$1
  $P $R/pbrun.py --withdraw "$key" --reason "PB1627 live probe ended by its own guard; see $LOG" 2>&1 | tail -2 | tee -a "$LOG"
}

case "$step" in
preflight) preflight ;;
submit)
  preflight || exit $?
  say "submitting the probe (detached)"
  out=$(cd $REPO && $P $R/pbrun.py --detach --cwd "$REPO" --tag x86 --priority 0 \
        --priority-reason "PB1627 live probe, CEO dec-1008-080711-64fe step B: one small declared prelaunch row, withdrawn if it delays PACT work" \
        --demand mem_gb=2 --cpus 1 --timeout-s 1500 --progress-phase probe=900 --residency stage --data-manifest "$MANIFEST" \
        -- /usr/bin/python3 -c "print('pb1627 probe ran')" 2>&1 | tail -3)
  say "submit output: $out"
  key=$(echo "$out" | python3 -c "
import sys,json,re
t=sys.stdin.read()
m=re.findall(r'[0-9a-f]{64}',t)
print(m[-1] if m else '')")
  [ -n "$key" ] || { say "ABORT: no action key in the submit output"; exit 8; }
  echo "$key" > "$PROBE_KEY_FILE"; say "probe key $key"
  watch "$key"; rc=$?
  say "watch ended rc=$rc"
  exit $rc ;;
withdraw) withdraw_now "$(cat $PROBE_KEY_FILE)" ;;
*) echo "unknown step $step"; exit 2 ;;
esac
