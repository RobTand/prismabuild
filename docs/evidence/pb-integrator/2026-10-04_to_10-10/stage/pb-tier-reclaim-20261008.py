#!/usr/bin/env python3
"""Check the stage and RAM tiers on dl380g10 before a PACT capture; optionally release ended holders.

Standing instruction, CEO 2026-10-08 03:57Z: until the reclaim fix for issue 1627 is live, check both tiers before each PACT
capture, release holders of ended consumers under the 19:09Z rule (rep-1007-185336-023c), and record each release.
Ended = the holder's consumer is done, failed, withdrawn or absent from the queue, AND no live claim or owner, no shared
interest, no ready or claimed row naming the holder.  A prelaunch group holder is reported and never released here.

  pb-tier-reclaim-20261008.py            read-only: classify and print
  pb-tier-reclaim-20261008.py --release  release the ended holders (one stage_release.py egress each, as a PrismaBuild action)
Exit 0 = nothing to release or released ok; 2 = a re-check stopped the release; 3 = an egress failed.
"""
import argparse, json, pathlib, subprocess, sys, time
CO = '/home/rob/wt/pb-carry-gen7'          # a clean checkout of the live commit 54600c279f
sys.path.insert(0, CO + '/tools/fleet'); sys.path.insert(0, CO + '/src')
from prismabuild import pool, storage_tiers
import stage_release

R = '/mnt/shared/prismabuild-fleet/repo/tools'
LOG = '/home/rob/fleet/inventory/pb-stage-release-log-20261008.jsonl'
ROOTS = {'ram:dl380g10': '/ram/prewarm', 'prismabuild-stage:dl380g10': '/stage/prewarm'}
REASON = ('CEO standing instruction 03:57Z on issue 1627 under the 19:09Z rule: holder of an ended consumer (done, failed, '
          'withdrawn or absent), no live claim or owner, no shared interest; original stays on NFS')
q = pool.PoolQueue(pathlib.Path('/mnt/shared/prismabuild-fleet/pb-queue'))
ENDED = {'done', 'failed', 'withdrawn', 'absent'}


def state(key):
    for st in (pool.READY, pool.CLAIMED, pool.DONE, pool.FAILED, 'withdrawn'):
        if q.item_path(st, key).exists():
            return st
    return 'absent'


def classify():
    wanted, owners = stage_release.live_claims(q)
    out = []
    for tier, root in ROOTS.items():
        led, kind = q.tier_ledger(tier), storage_tiers.capacity_kind_of(tier)
        for h in led.held_keys():
            g = int(led.holder_tokens(h).get(kind, 0))
            if h.startswith('prelaunch-'):
                out.append(dict(tier=tier, holder=h, gib=g, kind='group', consumer=None, cstate='-', root=root)); continue
            mv = q.move_record(h) or {}
            c = mv.get('consumer_action_key')
            cs = state(c) if c else 'no-move-record'
            live = h in wanted or h in owners
            if not c:
                kind_ = 'unknown'
            elif live or cs in ('ready', 'claimed'):
                kind_ = 'live'
            elif cs in ENDED:
                kind_ = 'ended'
            else:
                kind_ = 'unknown'
            out.append(dict(tier=tier, holder=h, gib=g, kind=kind_, consumer=c, cstate=cs, root=root))
    return out


def recheck(t):
    h = t['holder']
    wanted, owners = stage_release.live_claims(q)
    if h in wanted or h in owners: return 'live claim or owner'
    si = stage_release.shared_interest(q, h)
    if si.get('interested') or si.get('unknown'): return f'shared interest {si}'
    for st in (pool.READY, pool.CLAIMED):
        for f in (q.root / st).glob('*.json'):
            if h in f.read_text(): return f'{st} row {f.name[:12]} names it'
    if state(t['consumer']) in (pool.READY, pool.CLAIMED): return 'consumer is live'
    if int(q.tier_ledger(t['tier']).holder_tokens(h).get(storage_tiers.capacity_kind_of(t['tier']), 0)) != t['gib']:
        return 'holder size changed'
    return None


ap = argparse.ArgumentParser(); ap.add_argument('--release', action='store_true'); ap.add_argument('--only-tier', default=None, help='release only this tier id'); a = ap.parse_args()
rows = classify()
print(time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()))
for tier in ROOTS:
    led, kind = q.tier_ledger(tier), storage_tiers.capacity_kind_of(tier)
    mine = [r for r in rows if r['tier'] == tier]
    tot = {k: sum(r['gib'] for r in mine if r['kind'] == k) for k in ('live', 'ended', 'group', 'unknown')}
    print(f"{tier}: capacity {led.capacity().get(kind)} held {led.held().get(kind, 0)} free {led.available().get(kind, 0)} | "
          f"live {tot['live']} GiB, ended {tot['ended']} GiB ({sum(1 for r in mine if r['kind']=='ended')} holders), "
          f"group {tot['group']} GiB, unknown {tot['unknown']} GiB")
for r in sorted(rows, key=lambda r: (r['kind'] != 'ended', -r['gib']))[:14]:
    print(f"  {r['tier'][:9]:9s} {r['kind']:8s} {r['gib']:4d} GiB {r['holder'][:16]} consumer={str(r['consumer'])[:12]} state={r['cstate']}")
todo = sorted([r for r in rows if r['kind'] == 'ended' and (a.only_tier is None or r['tier'] == a.only_tier)], key=lambda r: (r['tier'] != 'ram:dl380g10', r['gib']))
print(f"releasable under the rule: {sum(r['gib'] for r in todo)} GiB in {len(todo)} holders")
if not a.release or not todo:
    sys.exit(0)
for t in todo:
    why = recheck(t)
    if why: print('STOP before', t['holder'][:12], why); sys.exit(2)
    before = int(q.tier_ledger(t['tier']).available().get(storage_tiers.capacity_kind_of(t['tier']), 0)); t0 = time.time()
    cmd = ['python3', f'{R}/pbrun.py', '--cwd', CO, '--tag', 'dl380g10', '--cpus', '1', '--demand', 'mem_gb=1', '--priority', '10',
           '--timeout-s', '900', '--wait-s', '300', '--max-attempts', '1', '--', 'python3', 'tools/fleet/stage_release.py',
           '--pool-root', str(q.root), '--mover-action-key', t['holder'], '--consumer-action-key', t['consumer'], '--stage-root', t['root']]
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=1000, cwd=CO)
    after = int(q.tier_ledger(t['tier']).holder_tokens(t['holder']).get(storage_tiers.capacity_kind_of(t['tier']), 0))
    rec = dict(holder=t['holder'], tier=t['tier'], stage_root=t['root'], gib_before=t['gib'], gib_after=after, free_before=before,
               free_after=int(q.tier_ledger(t['tier']).available().get(storage_tiers.capacity_kind_of(t['tier']), 0)), rc=p.returncode,
               ok=(p.returncode == 0 and after == 0), reason=REASON, consumer_state=t['cstate'], seconds=round(time.time() - t0, 1))
    open(LOG, 'a').write(json.dumps(rec) + '\n')
    print(json.dumps({k: rec[k] for k in ('holder', 'tier', 'gib_before', 'gib_after', 'free_before', 'free_after', 'ok')})[:240])
    if not rec['ok']: print('STOP: egress failed'); sys.exit(3)
print('released', len(todo), 'holders')
