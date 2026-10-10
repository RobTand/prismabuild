"""Release the PACT source-9:15 stage holders (CEO decision 20:16Z, PACT parent).

One stage_release.py egress per mover, run as a PrismaBuild action on dl380g10.
Before each one it re-checks: no live claim or owner, no shared interest, no
ready/claimed row naming the mover.  It stops on the first anomaly.  The three
1 GiB holders are not in the decision and are left alone.
"""
import json, pathlib, subprocess, sys, time
sys.path.insert(0, '/mnt/shared/prismabuild-fleet/repo/tools')
sys.path.insert(0, '/mnt/shared/prismabuild-fleet/repo/src')
from prismabuild import pool
import stage_release

R = '/mnt/shared/prismabuild-fleet/repo/tools'
D = '/mnt/shared/fleet-ceo/pb-publish-20261007/canary-clone-731067d3b551'
T = 'prismabuild-stage:dl380g10'
LOG = '/home/rob/fleet/inventory/pb-stage-release-log-20261007b.jsonl'
REASON = ('CEO decision 20:16Z (PACT parent): source 9:15 capture 2d71bc9b rc 0, '
          'consumer absent from the queue, no live claim or owner, no shared interest')
q = pool.PoolQueue(pathlib.Path('/mnt/shared/prismabuild-fleet/pb-queue'))
plan = [r for r in json.load(open('/home/rob/fleet/inventory/pb-stage-release-plan-20261007b.json'))
        if r['gib'] >= 8]
plan.sort(key=lambda r: r['gib'])        # the 8 GiB holder is the pilot


def free():
    return int(q.tier_ledger(T).available().get('stage_gib', 0))


def recheck(row):
    holder = row['holder']
    wanted, owners = stage_release.live_claims(q)
    if holder in wanted or holder in owners:
        return 'live claim or owner'
    si = stage_release.shared_interest(q, holder)
    if si.get('interested') or si.get('unknown'):
        return f'shared interest {si}'
    for st in (pool.READY, pool.CLAIMED):
        for f in (q.root / st).glob('*.json'):
            if holder in f.read_text():
                return f'{st} row {f.name[:12]} names it'
    if int(q.tier_ledger(T).holder_tokens(holder).get('stage_gib', 0)) != row['gib']:
        return 'holder size changed'
    return None


for row in plan:
    holder = row['holder']
    why = recheck(row)
    if why:
        print('STOP before', holder[:12], why); sys.exit(2)
    before, free_before, t0 = row['gib'], free(), time.time()
    cmd = ['python3', f'{R}/pbrun.py', '--cwd', D,
           '--tag', 'dl380g10', '--cpus', '1', '--demand', 'mem_gb=1', '--priority', '10',
           '--timeout-s', '900', '--wait-s', '300', '--max-attempts', '1', '--',
           'python3', 'tools/fleet/stage_release.py', '--pool-root', str(q.root),
           '--mover-action-key', holder, '--consumer-action-key', row['consumer'],
           '--stage-root', '/stage/prewarm']
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=1000, cwd=D)
    after = int(q.tier_ledger(T).holder_tokens(holder).get('stage_gib', 0))
    rec = dict(holder=holder, gib_before=before, gib_after=after, free_before=free_before,
               free_after=free(), rc=p.returncode, ok=(p.returncode == 0 and after == 0),
               reason=REASON, seconds=round(time.time() - t0, 1),
               tail=(p.stdout[-300:] + p.stderr[-500:]))
    open(LOG, 'a').write(json.dumps(rec) + '\n')
    print(json.dumps({k: rec[k] for k in ('holder', 'gib_before', 'gib_after', 'free_before', 'free_after', 'rc', 'ok', 'seconds')}))
    if not rec['ok']:
        print('STOP: anomaly'); sys.exit(3)
