"""Check a completed PB report against its frozen source and architecture plan."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser(__doc__)
parser.add_argument('run_name')
parser.add_argument('--evidence-dir', type=Path, required=True)
args = parser.parse_args()
ROOT = args.evidence_dir
plan = json.loads((ROOT / 'validation-plan.json').read_text())
run = next(r for r in plan['runs'] if r['name'] == args.run_name)
report = json.loads(Path(run['report']).read_text())
audit = json.loads((ROOT / (args.run_name + '-audit.json')).read_text())
rows = audit['actions']
assert len(rows) == len(report) == len({r['action_key'] for r in rows})
assert sorted(f for r in report for f in r['files']) == sorted(run['files'])
assert all(r['returncode'] == 0 and r['status'] == 'executed' for r in rows)
assert all(r['verdict']['green'] and not r['timed_out'] and not r['skipped'] for r in report)
nodes = [n for r in report for n in r['test_completion']['collected']]
assert len(nodes) == len(set(nodes))
for row in rows:
    assert row['snapshot']['parent'] == run['head']
    assert row['resources']['cpu'] == run['resources']['cpu']
    assert row['resources']['mem_gb'] == run['resources']['mem_gb']
    expected_arch = {'x86': 'x86_64', 'gb10': 'aarch64'}.get(run['architecture'])
    if expected_arch:
        assert row['runtime']['kernel']['machine'] == expected_arch

def git(root, *parts):
    return subprocess.check_output(['git', '-C', str(root), *parts])

bare = ROOT / 'verified-snapshots.git'
if not bare.exists():
    subprocess.run(['git', 'init', '--bare', str(bare)], check=True, capture_output=True)
snapshots = {row['snapshot']['commit']: row['snapshot']['input'] for row in rows}
verified = []
for snapshot, ref in snapshots.items():
    blob = Path('/mnt/shared/prismabuild-fleet/cas/blobs') / ref['sha256'][:2] / ref['sha256']
    data = blob.read_bytes()
    assert len(data) == ref['bytes'] and hashlib.sha256(data).hexdigest() == ref['sha256']
    subprocess.run(['git', '-C', str(bare), 'fetch', '--no-tags', str(blob),
                    'refs/heads/prismabuild-snapshot'], check=True, capture_output=True)
    for tree in ('src', 'tools', 'tests'):
        assert git(bare, 'rev-parse', snapshot + ':' + tree) == git(run['checkout'], 'rev-parse', run['head'] + ':' + tree)
    for path in run['files']:
        expected = hashlib.sha256(git(run['checkout'], 'show', run['head'] + ':' + path)).hexdigest()
        assert hashlib.sha256(git(bare, 'show', snapshot + ':' + path)).hexdigest() == expected
        if run['head'] == plan['head']:
            assert expected == plan['source_blobs'][path]
    verified.append(dict(commit=snapshot, bundle_sha256=ref['sha256'],
                         source_tools_tests_equal=True))
result = dict(run=args.run_name, source_head=run['head'], population=audit['population'],
              snapshots=verified, architecture=run['architecture'],
              reports_and_resources_match=True, unique_population_sha256=hashlib.sha256(
                  json.dumps(sorted(nodes), separators=(',', ':')).encode()).hexdigest())
(ROOT / (args.run_name + '-source-audit.json')).write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result))
