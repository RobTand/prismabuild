"""Verify PRI-172 terminal, log, CAS, pytest reconciliation and sealed source evidence."""
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parent
CAS = Path('/mnt/shared/prismabuild-fleet/cas')


def collect(report):
    report = Path(report)
    rows = json.loads(report.read_text())
    keys = [row['action_key'] for row in rows]
    if not keys or any(not key for key in keys):
        raise ValueError('missing published action population')
    proc = subprocess.run(['python3', '/mnt/shared/prismabuild-fleet/repo/tools/fleet/pbwait.py',
        '--endings', '--wait-s', '0', *keys], text=True, capture_output=True, timeout=55)
    ending_path = report.with_suffix('.endings.json')
    ending_path.write_text(proc.stdout)
    endings = json.loads(proc.stdout)
    if not endings['complete']:
        raise ValueError('terminal population incomplete')
    by_key = {e['action_key']: e for e in endings['endings']}
    results = []
    for row in rows:
        key = row['action_key']
        ending = by_key[key]
        result = ending['result']
        if not result['verified'] or ending['exit_code'] != row['returncode']:
            raise ValueError(f'{key}: unverified or mismatched terminal result')
        for stream in ('stdout', 'stderr'):
            meta = result[stream]
            data = Path(meta['path']).read_bytes()
            if len(data) != meta['bytes'] or hashlib.sha256(data).hexdigest() != meta['sha256']:
                raise ValueError(f'{key}: {stream} integrity mismatch')
        receipt = None
        if ending['succeeded']:
            receipt_path = CAS / 'actions/v3' / key[:2] / (key + '.json')
            receipt = json.loads(receipt_path.read_text())
            if receipt['action_key'] != key or receipt['receipt_sha256'] != result['receipt_sha256']:
                raise ValueError(f'{key}: receipt binding mismatch')
            ref = receipt['result']
            payload = (CAS / 'blobs' / ref['sha256'][:2] / ref['sha256']).read_bytes()
            if len(payload) != ref['bytes'] or hashlib.sha256(payload).hexdigest() != ref['sha256']:
                raise ValueError(f'{key}: result blob integrity mismatch')
        verdict = row['verdict']
        if not verdict['failing_nodes'] and row['returncode'] != 0:
            raise ValueError(f'{key}: nonzero exit without an observed test failure')
        unexpected = [p for p in verdict['problems'] if p['code'] not in ('client_exit', 'test_failure')]
        if (verdict['unobserved_files'] or unexpected or verdict['bound_failures']
                or row['test_completion']['status'] != 'complete'
                or row['reconciliation']['problems']):
            raise ValueError(f'{key}: incomplete pytest evidence')
        runtime_lines = [line.removeprefix('prismabuild-test-runtime: ')
            for line in row['output'].splitlines() if line.startswith('prismabuild-test-runtime: ')]
        runtime = json.loads(runtime_lines[-1]) if runtime_lines else None
        results.append(dict(action_key=key, files=row['files'], summary=row['summary'],
            failing_nodes=verdict['failing_nodes'], host=ending['host'],
            runtime=runtime, cleanup_complete=ending['diagnostics'].get('cleanup', {}).get('complete'),
            receipt_sha256=None if receipt is None else receipt['receipt_sha256'],
            result_sha256=None if receipt is None else receipt['result']['sha256'],
            logs_verified=True, terminal_verified=True))
    summary = dict(report=str(report), shards=len(rows),
        counts={name: sum(int(n) for row in rows for n in re.findall(r'(\d+) '+name, row['summary'] or ''))
                for name in ('passed', 'failed', 'skipped', 'deselected')},
        failures=sorted({node for row in results for node in row['failing_nodes']}),
        actions=results)
    report.with_suffix('.evidence.json').write_text(json.dumps(summary, indent=2) + '\n')
    return summary



BASE = Path('/mnt/shared/paperclip-runs/PRI-172')
CAS = Path('/mnt/shared/prismabuild-fleet/cas')
ROOT = Path.cwd()
plan = json.loads(Path(__file__).with_name('issue1483_compat_validation_plan_2026-10-26.json').read_text())
source_repo = BASE / 'verified-source.git'
if not source_repo.exists():
    subprocess.run(['git', 'init', '--bare', str(source_repo)], check=True, capture_output=True)

def git(*args, repo=ROOT):
    return subprocess.check_output(['git', '-C', str(repo), *args])

def blob(ref):
    data = (CAS / 'blobs' / ref['sha256'][:2] / ref['sha256']).read_bytes()
    assert len(data) == ref['bytes'] and hashlib.sha256(data).hexdigest() == ref['sha256']
    return data

seen = set()
source_checks = []
reports = []
for report in plan['reports']:
    assert hashlib.sha256((BASE / report['file']).read_bytes()).hexdigest() == report['sha256']
    summary = collect(BASE / report['file'])
    assert summary['counts']['failed'] == report['failed'], summary
    reports.append(summary)
    for action in summary['actions']:
        key = action['action_key']
        request = json.loads((CAS / 'requests' / key[:2] / (key + '.json')).read_text())
        assert request['action_key'] == key
        snapshot = request['params']['checkout_snapshot']
        ref = snapshot['input']
        if ref['sha256'] not in seen:
            blob(ref)
            bundle = CAS / 'blobs' / ref['sha256'][:2] / ref['sha256']
            subprocess.run(['git', '-C', str(source_repo), 'fetch', str(bundle),
                            'refs/heads/prismabuild-snapshot:refs/verified/' + ref['sha256']],
                           check=True, capture_output=True)
            seen.add(ref['sha256'])
        snapshot_commit = snapshot['commit']
        assert git('rev-parse', 'refs/verified/' + ref['sha256'], repo=source_repo).decode().strip() == snapshot_commit
        head = report['source_head']
        paths = list(report['source_paths'])
        if 'tests' in report['include']:
            paths += action['files']
        verified = []
        for path in sorted(set(paths)):
            actual = git('ls-tree', '-r', '-z', snapshot_commit, '--', path, repo=source_repo)
            expected = git('ls-tree', '-r', '-z', head, '--', path)
            assert expected and actual == expected, (key, path, head)
            verified.append(dict(path=path, tree_listing_sha256=hashlib.sha256(actual).hexdigest()))
        source_checks.append(dict(action_key=key, snapshot=snapshot_commit,
            bundle_sha256=ref['sha256'], expected_head=head, verified_paths=verified))
proc = subprocess.run(['python3', '/mnt/shared/prismabuild-fleet/repo/tools/fleet/pbwait.py',
    '--endings', '--wait-s', '0', plan['capture_key']], text=True, capture_output=True, timeout=55)
endings = json.loads(proc.stdout)
assert endings['complete'] and len(endings['endings']) == 1
ending = endings['endings'][0]
assert ending['succeeded'] and ending['result']['verified']
for stream in ('stdout', 'stderr'):
    meta = ending['result'][stream]
    data = Path(meta['path']).read_bytes()
    assert len(data) == meta['bytes'] and hashlib.sha256(data).hexdigest() == meta['sha256']
    if stream == 'stdout':
        assert plan['capture_sha256'] in data.decode()
receipt = json.loads((CAS / 'actions/v3' / plan['capture_key'][:2] / (plan['capture_key'] + '.json')).read_text())
assert receipt['action_key'] == plan['capture_key']
assert receipt['receipt_sha256'] == ending['result']['receipt_sha256']
blob(receipt['result'])
assert hashlib.sha256((ROOT / 'tests/compat/goldens-floor.jsonl').read_bytes()).hexdigest() == plan['capture_sha256']
result = dict(reports=reports, source_checks=source_checks, capture=ending,
    checked_source_bundles=len(seen), passed=True)
(BASE / 'verified-evidence.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(dict(passed=True, counts=[r['counts'] for r in reports],
                     actions=len(source_checks), source_bundles=len(seen))))
