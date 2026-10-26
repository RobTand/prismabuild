"""Read PB artifacts and check the frozen source/architecture qualification matrix."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import re

ROOT = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parent))

from collect_validation import collect

CAS = Path('/mnt/shared/prismabuild-fleet/cas')
source_repo = ROOT / 'qualification-source.git'
if not source_repo.exists():
    subprocess.run(['/usr/bin/git', 'init', '--bare', str(source_repo)], check=True, capture_output=True)
verified_bundles = {}
rows = []
keys = set()


def verify_source_bundle(key, checkout, head, files):
    receipt = json.loads((CAS / 'actions/v3' / key[:2] / (key + '.json')).read_text())
    inputs = [item for item in receipt['producer']['inputs']
              if item['id'] == 'pbrun.checkout-snapshot']
    if len(inputs) != 1:
        raise ValueError('missing unique attested source bundle')
    ref = inputs[0]
    digest = ref['sha256']
    bundle = CAS / 'blobs' / digest[:2] / digest
    if digest not in verified_bundles:
        payload = bundle.read_bytes()
        if len(payload) != ref['bytes'] or hashlib.sha256(payload).hexdigest() != digest:
            raise ValueError('source bundle integrity mismatch')
        lines = subprocess.check_output(['/usr/bin/git', 'bundle', 'list-heads', str(bundle)], text=True).splitlines()
        commits = [line.split()[0] for line in lines if line.endswith(' refs/heads/prismabuild-snapshot')]
        if len(commits) != 1:
            raise ValueError('source bundle has no unique snapshot ref')
        subprocess.run(['/usr/bin/git', '--git-dir', str(source_repo), 'fetch', str(bundle),
                        'refs/heads/prismabuild-snapshot:refs/verified/' + digest],
                       check=True, capture_output=True)
        verified_bundles[digest] = commits[0]
    snapshot = verified_bundles[digest]
    observed = subprocess.check_output(['/usr/bin/git', '--git-dir', str(source_repo),
        'ls-tree', '-r', '-z', snapshot]).split(b'\0')
    expected = subprocess.check_output(['/usr/bin/git', '-C', checkout,
        'ls-tree', '-r', '-z', head]).split(b'\0')
    extra = set(observed) - set(expected)
    if set(expected) - set(observed) or len(extra) != 1:
        raise ValueError('attested repository tree differs beyond its source stamp')
    stamp_path = next(iter(extra)).split(b'\t', 1)[1].decode()
    if not re.fullmatch(r'\.pbrun-closure\.[0-9a-f]{16}\.json', stamp_path):
        raise ValueError('unexpected injected source path')
    stamp = json.loads(subprocess.check_output(['/usr/bin/git', '--git-dir', str(source_repo),
        'show', snapshot + ':' + stamp_path]))
    if stamp != {'cwd': '.', 'head': head, 'dirty_sha256': hashlib.sha256(b'').hexdigest()}:
        raise ValueError('injected stamp does not bind the clean frozen checkout')
    for file in files:
        actual = subprocess.check_output(['/usr/bin/git', '--git-dir', str(source_repo), 'show', snapshot + ':' + file])
        expected = subprocess.check_output(['/usr/bin/git', '-C', checkout, 'show', head + ':' + file])
        if actual != expected:
            raise ValueError('attested source bundle differs at ' + file)
    return digest

for arch, machine in [('x86', 'x86_64'), ('gb10', 'aarch64'), ('delta', 'x86_64'),
                      ('ram-x86', 'x86_64'), ('ram-gb10', 'aarch64')]:
    if arch.startswith('ram-'):
        plan_file = f'validation-ram-positive-{arch[4:]}-4c0b7511.plan.json'
    elif arch == 'delta':
        plan_file = 'validation-delta-88d1d57d.plan.json'
    else:
        plan_file = f'validation-{arch}-b531027f.plan.json'
    plan = json.loads((ROOT / plan_file).read_text())
    for row in plan['rows']:
        report = Path(row['report'])
        item = dict(name=row['name'], report=str(report), head=plan['head'], state='pending')
        rows.append(item)
        if not report.exists():
            continue
        try:
            evidence = collect(report)
            if evidence['failures']:
                raise ValueError(f"test failures: {evidence['failures']}")
            expected = sorted(row['argv'][row['argv'].index('--json') + 2:])
            actual = sorted(file for action in evidence['actions'] for file in action['files'])
            if actual != expected:
                raise ValueError('file population differs from frozen plan')
            for action in evidence['actions']:
                key = action['action_key']
                if key in keys:
                    raise ValueError('action reused between required fresh repeats')
                keys.add(key)
                runtime = action['runtime']
                if not runtime['complete'] or runtime['kernel']['machine'] != machine:
                    raise ValueError('incomplete runtime or wrong architecture')
                if set(runtime['native_threads'].values()) != {'1'}:
                    raise ValueError('native threads not bounded to one')
                if not action['cleanup_complete']:
                    raise ValueError('cleanup incomplete')
                item.setdefault('source_bundles', []).append(verify_source_bundle(
                    key, plan['checkout'], plan['head'], action['files']))
                for file in action['files']:
                    source = subprocess.check_output(['/usr/bin/git', '-C', plan['checkout'],
                        'show', plan['head'] + ':' + file])
                    observed = runtime['selected_source_sha256'].get(file)
                    if observed is not None and hashlib.sha256(source).hexdigest() != observed:
                        raise ValueError('executed test source differs from frozen source')
            item.update(state='verified', counts=evidence['counts'],
                actions=[action['action_key'] for action in evidence['actions']],
                xdist='--workers-per-shard' in row['argv'] and
                    row['argv'][row['argv'].index('--workers-per-shard') + 1] == '2')
        except Exception as exc:
            item.update(state='unverified', error=str(exc))
summary = dict(schema='pri36.source_qualification.v1',
    head='4c0b7511c8e341e327ad98551b4a35df0ced0f79',
    state='verified' if len(rows) == 22 and all(r['state'] == 'verified' for r in rows)
        and all(sum(r.get('xdist', False) for r in rows if r['name'].startswith('repeat-' + arch)) >= 2
                for arch in ('x86', 'gb10'))
        and all(sum(r.get('xdist', False) for r in rows if r['name'].startswith('ram-positive-' + arch)) >= 2
                for arch in ('x86', 'gb10')) else 'pending', rows=rows)
(ROOT / 'qualification-4c0b7511.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps({k:v for k,v in summary.items() if k != 'rows'}))
for row in rows:
    print(json.dumps({k:v for k,v in row.items() if k not in ('actions', 'report', 'source_bundles')}))
