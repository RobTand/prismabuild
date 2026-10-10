#!/usr/bin/env python3
"""Watch the PACT band resubmission through the stage tier (read-only). Author pb-integrator, 2026-10-08.

Finds new READY or CLAIMED consumers whose filed plan declares a resident_before_launch prefix, published after this
starts, and prints one line per 20 s: its state, the stage ledger, how many leads hold tokens, its events by name, and any
window-pressure-skipped row. It stops when the consumer is claimed, ended, or after WATCH_S. It writes nothing.
"""
import json, os, pathlib, sys, time
ROOT = '/home/rob/wt/lead-pb-integrator'
sys.path.insert(0, ROOT + '/tools/fleet'); sys.path.insert(0, ROOT + '/src')
from prismabuild import pool, residency_plan
Q = pool.PoolQueue(pathlib.Path('/mnt/shared/prismabuild-fleet/pb-queue'))
T = 'prismabuild-stage:dl380g10'
WATCH_S = int(os.environ.get('WATCH_S', '2700'))
start = time.time()
seen_before = set()
for st in (pool.READY, pool.CLAIMED):
    for p in (Q.root / st).glob('*.json'):
        seen_before.add(p.stem)
tracked = None
def line(**kw):
    print(time.strftime('%H:%M:%SZ', time.gmtime()), json.dumps(kw, default=str), flush=True)
line(msg='watching for a new declared-prelaunch consumer', ready_or_claimed_before=len(seen_before))
while time.time() - start < WATCH_S:
    led = Q.tier_ledger(T)
    tier = {'cap': led.capacity().get('stage_gib'), 'held': led.held().get('stage_gib'), 'free': led.available().get('stage_gib')}
    if tracked is None:
        for st in (pool.READY, pool.CLAIMED):
            for p in (Q.root / st).glob('*.json'):
                k = p.stem
                if k in seen_before:
                    continue
                try:
                    declared = residency_plan.filed_prelaunch_phases(Q, k)
                except Exception:
                    declared = None
                if declared:
                    tracked = k; line(msg='tracking', consumer=k, declared_phases=list(declared), tier=tier); break
            if tracked: break
        if tracked is None:
            line(waiting=True, tier=tier)
    if tracked:
        state = 'absent'
        for st in (pool.READY, pool.CLAIMED, pool.DONE, pool.FAILED, 'withdrawn'):
            if Q.item_path(st, tracked).exists():
                state = st; break
        plan = None
        try: plan = residency_plan.read(Q, tracked)
        except Exception: pass
        leads = residency_plan.leads_for(plan) if plan else []
        holding = sum(1 for l in leads if led.holder_tokens(l))
        ev = Q.consumer_events(tracked)
        names = {}
        for e in ev: names[e.get('event')] = names.get(e.get('event'), 0) + 1
        skips = [{k: e.get(k) for k in ('scope', 'reason', 'evictable_gib', 'free_gib', 'held_gib', 'shortfall_gib', 'cur_min_gib', 'capacity_gib', 'receiptless_holders', 'holders', 'decision')} for e in ev if e.get('event') == 'window-pressure-skipped']
        line(consumer=tracked[:12], state=state, tier=tier, leads=len(leads), leads_holding=holding, events=names, skips=skips[-2:])
        if state in (pool.CLAIMED, pool.DONE, pool.FAILED, 'withdrawn', 'absent'):
            line(msg='stopping: the consumer reached ' + state); break
    time.sleep(20)
else:
    line(msg='watch time over', tracked=tracked)
