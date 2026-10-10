#!/usr/bin/env python3
"""prismabuild#1738 test band: commit, owner withdraw, same-key resubmit, renewed row claims. Author pb-integrator, 2026-10-10.
CEO host task (D64). One CPU consumer on dl380g10 through the real tier loop. Touches only its own rows and throwaway files under
/mnt/shared/fleet-ceo/pb-1738-testband. Records receipts after every step. Never touches a production row."""
import glob, json, os, subprocess, sys, time, datetime
Q = '/mnt/shared/prismabuild-fleet/pb-queue/'
PB = '/home/rob/tmp/pb-submit-celestia-20261003/bin/python'
PBRUN = '/mnt/shared/prismabuild-fleet/repo/tools/pbrun.py'
CWD = '/home/rob/tmp/pb-1738-job'
MAN = '/home/rob/tmp/pb-1738-manifest.json'
STAGE = Q + 'tiers/prismabuild-stage:dl380g10.json'
OUT = '/mnt/shared/fleet-ceo/pb-1738-testband/receipts-%s.json' % datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
R = {'steps': []}
def now(): return datetime.datetime.now(datetime.timezone.utc).strftime('%H:%M:%S.%f')[:-3] + 'Z'
def save(): json.dump(R, open(OUT, 'w'), indent=1)
def step(name, **kw):
    R['steps'].append(dict(at=now(), step=name, **kw)); save(); print(now(), name, json.dumps(kw)[:300], flush=True)
def stage_held():
    try: d = json.load(open(STAGE)); return dict(held_gib=d.get('held_gib'), landed_gib=d.get('landed_gib'), in_flight_gib=d.get('in_flight_gib'), free_gib=round(d.get('free_bytes', 0) / 2**30, 1))
    except Exception as e: return dict(error=str(e))
def group_files(key):
    out = {}
    for d in glob.glob(Q + 'prelaunch-groups/' + key + '.*'):
        out[os.path.basename(d)[-12:]] = {f: round(os.path.getmtime(d + '/' + f)) for f in sorted(os.listdir(d))}
    return out
def state_of(key):
    for s in ('ready', 'claimed', 'done', 'failed', 'withdrawn'):
        if os.path.exists(Q + s + '/' + key + '.json'): return s
    return None
def submit():
    cmd = [PB, PBRUN, '--cwd', CWD, '--transport', 'pool', '--tag', 'dl380g10', '--demand', 'cpu=1', '--demand', 'mem_gb=1',
           '--residency', 'stage', '--data-manifest', MAN, '--priority', '0', '--priority-reason',
           'prismabuild#1738 throwaway test band; CEO host task under D64; no production row', '--timeout-s', '300', '--progress-phase', 'prelaunch-test=600', '--detach', '--',
           'bash', '-c', 'echo prismabuild-1738-test-band-consumer; sleep 5']
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    line = [l for l in p.stdout.splitlines() if l.startswith('{') and 'action_key' in l]
    if p.returncode != 0 or not line:
        step('submit-failed', rc=p.returncode, stdout=p.stdout[-600:], stderr=p.stderr[-900:]); sys.exit(2)
    return json.loads(line[-1])['action_key']
def events(key):
    out = []
    for f in sorted(glob.glob(Q + 'residency-events/' + key + '*')):
        files = [f] if os.path.isfile(f) else sorted(glob.glob(f + '/*'))
        for g in files:
            try:
                for l in open(g):
                    l = l.strip()
                    if l.startswith('{'):
                        e = json.loads(l); out.append(dict(at=e.get('at') or e.get('unix'), event=e.get('event'), tier=str(e.get('tier') or e.get('tier_id') or '')[-24:], detail=str({k: v for k, v in e.items() if k not in ('event', 'at', 'unix', 'consumer')})[:200]))
            except Exception as e: out.append(dict(error=str(e)[:120], file=g[-60:]))
    return out
R['baseline'] = stage_held(); step('baseline', stage=R['baseline'], ready=len(glob.glob(Q + 'ready/*.json')), claimed=len(glob.glob(Q + 'claimed/*.json')))
t0 = time.time(); key = submit(); R['key'] = key; step('submitted-1', key=key, state=state_of(key))
committed_seen = None
while time.time() - t0 < 240:
    g = group_files(key)
    if any('committed.json' in v for v in g.values()): committed_seen = now(); break
    if state_of(key) in ('claimed', 'done', 'failed'): break
    time.sleep(0.25)
step('commit-poll-ended', committed_seen=committed_seen, state=state_of(key), group=group_files(key), stage=stage_held())
if not committed_seen or state_of(key) != 'ready':
    step('VOID', reason='no committed.json while the row was still READY (the claim won the race or no group began); nothing proven', state=state_of(key)); print('VOID'); sys.exit(3)
w = subprocess.run([PB, PBRUN, '--withdraw', key, '--reason', 'prismabuild#1738 test band: owner withdraw after commit'], capture_output=True, text=True, timeout=120)
step('withdraw-called', rc=w.returncode, out=(w.stdout + w.stderr)[-400:])
for _ in range(240):
    if state_of(key) == 'withdrawn': break
    time.sleep(0.5)
step('withdrawn', state=state_of(key), group=group_files(key), stage=stage_held())
# let the tier loop run a few cycles so the tokens come back as they did in the incident; observe, do not touch
for _ in range(60):
    if str(stage_held().get('held_gib')) == str(R['baseline'].get('held_gib')): break
    time.sleep(1)
step('after-token-return-wait', group=group_files(key), stage=stage_held(), events=events(key)[-12:])
key2 = submit(); step('resubmitted', key2=key2, same_key=(key2 == key), state=state_of(key2))
if key2 != key:
    step('VOID', reason='the resubmitted key differs; not the same unit'); sys.exit(4)
t1 = time.time(); claimed_at = None
while time.time() - t1 < 420:
    s = state_of(key)
    if s in ('claimed', 'done'): claimed_at = now(); break
    if s in ('failed', 'withdrawn'): break
    time.sleep(0.5)
step('claim-poll-ended', claimed_at=claimed_at, seconds=round(time.time() - t1, 1), state=state_of(key), group=group_files(key), stage=stage_held())
for _ in range(240):
    if state_of(key) in ('done', 'failed'): break
    time.sleep(1)
rec = {}
for s in ('claimed', 'done', 'failed'):
    p = Q + s + '/' + key + '.json'
    if os.path.exists(p):
        d = json.load(open(p)); rec = dict(file=s, status=d.get('status'), claimed_host=d.get('claimed_host'), finished_host=d.get('finished_host'), attempts=d.get('attempts'), residency_verdict=d.get('residency_verdict'), detail=str(d.get('detail'))[:300]); break
step('ended', state=state_of(key), record=rec)
for _ in range(180):
    if str(stage_held().get('held_gib')) == str(R['baseline'].get('held_gib')) and any('released.json' in v for v in group_files(key).values()): break
    time.sleep(1)
step('final', state=state_of(key), group=group_files(key), stage=stage_held(), events=events(key))
print('RECEIPTS', OUT); R['result'] = 'renewed row claimed' if claimed_at else 'renewed row did NOT claim'; save(); print(R['result'])
