"""Release holders of ended consumers on dl380g10's stage and RAM tiers.

CEO decision on rep-1008-030528-30a4 (2026-10-08, under the 19:09Z rule of rep-1007-185336-023c): holders of
done or failed consumers that fill the room are released and each release is recorded.  One stage_release.py
egress per mover, run as a PrismaBuild action on dl380g10, from a checkout of the live commit.  Before each
release the driver re-checks: no live claim or owner, no shared interest, no ready/claimed row naming the
mover, the consumer is not ready or claimed, and the size is unchanged.  It stops on the first anomaly.
Excluded: any holder with a live claim or owner (the 40 GiB 063ae70e...).
"""
import json, pathlib, subprocess, sys, time
sys.path.insert(0, '/home/rob/prismabuild-wt/pb-carry-gen3/tools/fleet')
sys.path.insert(0, '/home/rob/prismabuild-wt/pb-carry-gen3/src')
from prismabuild import pool, storage_tiers
import stage_release

R = '/mnt/shared/prismabuild-fleet/repo/tools'
CO = '/home/rob/prismabuild-wt/pb-carry-gen3'
LOG = '/home/rob/fleet/inventory/pb-stage-release-log-20261008.jsonl'
REASON = ('CEO decision on rep-1008-030528-30a4 under the 19:09Z rule: holder of an ended consumer '
          '(done, failed, or a withdrawn group), no live claim or owner, no shared interest; original stays on NFS')
q = pool.PoolQueue(pathlib.Path('/mnt/shared/prismabuild-fleet/pb-queue'))
ROOTS = {'ram:dl380g10': '/ram/prewarm', 'prismabuild-stage:dl380g10': '/stage/prewarm'}


def state(key):
    for st in (pool.READY, pool.CLAIMED, pool.DONE, pool.FAILED):
        if q.item_path(st, key).exists():
            return st
    return 'absent'


def avail(tier):
    return int(q.tier_ledger(tier).available().get(storage_tiers.capacity_kind_of(tier), 0))


def gib(tier, holder):
    return int(q.tier_ledger(tier).holder_tokens(holder).get(storage_tiers.capacity_kind_of(tier), 0))


rows = json.load(open('/home/rob/fleet/inventory/pb-ram-holders-20261008.json')) + \
    json.load(open('/home/rob/fleet/inventory/pb-stage-holders-20261008.json'))
todo = []
for r in rows:
    mv = q.move_record(r['holder']) or {}
    tier = mv.get('tier_id')
    if tier not in ROOTS or not mv.get('consumer_action_key'):
        print('SKIP (no move record or unknown tier)', r['holder'][:12]); continue
    todo.append(dict(holder=r['holder'], gib=r['gib'], consumer=mv['consumer_action_key'], tier=tier, root=ROOTS[tier]))
# RAM copies first (a ram copy goes before its stage source), smallest first so the first one is the pilot.
todo.sort(key=lambda t: (t['tier'] != 'ram:dl380g10', t['gib']))


def recheck(t):
    h = t['holder']
    wanted, owners = stage_release.live_claims(q)
    if h in wanted or h in owners:
        return 'live claim or owner'
    si = stage_release.shared_interest(q, h)
    if si.get('interested') or si.get('unknown'):
        return f'shared interest {si}'
    for st in (pool.READY, pool.CLAIMED):
        for f in (q.root / st).glob('*.json'):
            if h in f.read_text():
                return f'{st} row {f.name[:12]} names it'
    if state(t['consumer']) in (pool.READY, pool.CLAIMED):
        return f"consumer is {state(t['consumer'])}"
    if gib(t['tier'], h) != t['gib']:
        return 'holder size changed'
    return None


done = 0
for t in todo:
    why = recheck(t)
    if why:
        if 'live claim' in why:
            print('KEEP', t['holder'][:12], t['gib'], 'GiB:', why)
            open(LOG, 'a').write(json.dumps(dict(holder=t['holder'], tier=t['tier'], gib_before=t['gib'], action='kept', reason=why)) + '\n')
            continue
        print('STOP before', t['holder'][:12], why); sys.exit(2)
    before, free_before, t0 = t['gib'], avail(t['tier']), time.time()
    cmd = ['python3', f'{R}/pbrun.py', '--cwd', CO, '--tag', 'dl380g10', '--cpus', '1', '--demand', 'mem_gb=1',
           '--priority', '10', '--timeout-s', '900', '--wait-s', '300', '--max-attempts', '1', '--',
           'python3', 'tools/fleet/stage_release.py', '--pool-root', str(q.root),
           '--mover-action-key', t['holder'], '--consumer-action-key', t['consumer'],
           '--stage-root', t['root']]
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=1000, cwd=CO)
    after = gib(t['tier'], t['holder'])
    rec = dict(holder=t['holder'], tier=t['tier'], stage_root=t['root'], gib_before=before, gib_after=after,
               free_before=free_before, free_after=avail(t['tier']), rc=p.returncode,
               ok=(p.returncode == 0 and after == 0), reason=REASON, seconds=round(time.time() - t0, 1),
               tail=(p.stdout[-250:] + p.stderr[-450:]))
    open(LOG, 'a').write(json.dumps(rec) + '\n')
    done += 1
    print(json.dumps({k: rec[k] for k in ('holder', 'tier', 'gib_before', 'gib_after', 'free_before', 'free_after', 'rc', 'ok', 'seconds')})[:260])
    if not rec['ok']:
        print('STOP: anomaly'); sys.exit(3)
print('done', done, 'releases; ram free', avail('ram:dl380g10'), 'stage free', avail('prismabuild-stage:dl380g10'))
