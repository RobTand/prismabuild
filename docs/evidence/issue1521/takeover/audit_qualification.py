"""Read immutable PB evidence and bind it to the repaired source; never submit work."""
import argparse
import hashlib
import json
import re
from pathlib import Path
import subprocess

ROOT = Path('/mnt/shared/paperclip-runs/PRI-47')
CAS = Path('/mnt/shared/prismabuild-fleet/cas')
HEAD = 'affe5b27eedd8e2ac55e9b07e52ad7b64e0e1db8'
CHECKOUT = Path('/home/rob/paperclip-worktrees/prismabuild/PRI-47')
SOURCE = ['src/prismabuild/numeric_fields.py', 'src/prismabuild/_gang.py', 'src/prismabuild/core.py',
          'src/prismabuild/residency_plan.py', 'tests/compat/field_sets.json',
          'tests/test_core_numbers_1521.py', 'docs/design.md']


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def blob(ref):
    path = CAS / 'blobs' / ref['sha256'][:2] / ref['sha256']
    with path.open('rb') as handle:
        assert hashlib.file_digest(handle, 'sha256').hexdigest() == ref['sha256']
    assert path.stat().st_size == ref['bytes']
    return path


def git(repo, *args):
    return subprocess.check_output(['git', '--git-dir', str(repo), *args], stderr=subprocess.PIPE)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('reports', nargs='+', type=Path)
    parser.add_argument('--out', required=True, type=Path)
    parser.add_argument('--head', default=HEAD)
    parser.add_argument('--checkout', type=Path, default=CHECKOUT)
    args = parser.parse_args()
    head, checkout = args.head, args.checkout
    rows = [dict(row, audit_report=report.name) for report in args.reports for row in json.loads(report.read_text())]
    keys = [row['action_key'] for row in rows]
    assert len(keys) == len(set(keys))
    observed = subprocess.check_output(['python3', '/mnt/shared/prismabuild-fleet/repo/tools/fleet/pbwait.py',
                                       '--endings', '--wait-s', '0', *keys])
    (args.out.parent / (args.out.stem + '-endings.json')).write_bytes(observed)
    observed_data = json.loads(observed)
    assert observed_data['complete']
    endings = {e['action_key']: e for e in observed_data['endings']}
    assert set(endings) == set(keys)
    entries, snapshots, nodeids = [], {}, set()
    for row in rows:
        key = row['action_key']
        e = endings[key]
        assert row['returncode'] == 0 and e['succeeded'] and e['exit_code'] == 0, key
        assert e['result']['verified'] and e['diagnostics']['cleanup']['complete'], key
        cleanup = e['diagnostics']['cleanup']['resource_scope']['export']
        assert cleanup['empty'] and cleanup['settled'], key
        texts, logs = {}, {}
        for stream in ('stdout', 'stderr'):
            ref = e['result'][stream]
            raw = Path(ref['path']).read_bytes()
            assert sha(raw) == ref['sha256'] and len(raw) == ref['bytes'], key
            logs[stream] = ref
            texts[stream] = raw.decode()
        lines = texts['stdout'].splitlines()
        def record(prefix):
            found = [json.loads(line[len(prefix):]) for line in lines if line.startswith(prefix)]
            assert len(found) == 1, (key, prefix)
            return found[0]
        c, o, r = [record(prefix) for prefix in ('pbtest-completion: ', 'pbtest-outcomes: ',
                                                'prismabuild-test-runtime: ')]
        assert c['collection_complete'] and not c['collection_errors'] and not c['collect_only']
        assert not c['duplicate_teardown'] and not o['uncounted'] and not o['file_selection_error']
        population = set(c['collected'])
        assert population and population == set(c['teardown_finished']) == set(c['outcome_nodeids'])
        assert population == set(o['collected']) == {outcome[0] for outcome in o['reports']}
        assert c['run_nonce'] == o['run_nonce']
        assert not o['bound_failures'] and all(outcome[2] == 'passed' for outcome in o['reports'])
        assert r['complete'] and r['exitstatus'] == 0 and r['executable_path_matches_running']
        assert r['native_threads'] == dict(OMP_NUM_THREADS='1', MKL_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1')
        request_path = CAS / 'requests' / key[:2] / (key + '.json')
        req = json.loads(request_path.read_bytes())
        assert req['action_key'] == key and req['params']['checkout_snapshot']['parent'] == head
        name = row['audit_report']
        if name.startswith('numeric-'):
            repeat = int(name.rsplit('-', 1)[1].split('.')[0])
            expected_demand = {'cpu': 2 if repeat >= 3 else 1, 'mem_gb': 3}
        else:
            expected_demand = {'cpu': 2, 'mem_gb': 4}
        assert req['params']['demand'] == expected_demand, (key, req['params']['demand'], expected_demand)
        assert req['params']['execution_timeout_s'] == 900
        assert len(set(r['affinity'])) == req['params']['demand']['cpu']
        assert req['environment']['variables']['CUDA_VISIBLE_DEVICES'] == ''
        assert req['params']['retry_policy'] == dict(max_attempts=3, retry_safe=True, retry_scope='interruption')
        publication, = [json.loads(line) for line in lines if line.startswith('{') and '"receipt":' in line]
        receipt = publication['receipt']
        unsigned = {k: v for k, v in receipt.items() if k != 'receipt_sha256'}
        assert receipt['action_key'] == key
        assert receipt['receipt_sha256'] == e['result']['receipt_sha256'] == sha(json.dumps(unsigned, sort_keys=True, separators=(',', ':')).encode())
        assert json.loads((CAS / 'actions/v3' / key[:2] / (key + '.json')).read_bytes()) == receipt
        assert receipt['producer']['inputs'] == req['inputs']
        blob(receipt['result'])
        snapshot, = [ref for ref in req['inputs'] if ref['id'] == 'pbrun.checkout-snapshot']
        snapshots[snapshot['sha256']] = snapshot
        nodeids.update(population)
        entries.append(dict(action_key=key, host=e['host'], status=e['status'], exit_code=0,
                            receipt_sha256=receipt['receipt_sha256'], request_sha256=sha(request_path.read_bytes()),
                            logs=logs, native_tests=len(population), nodeids=sorted(population),
                            cleanup_complete=True, source_snapshot=snapshot, runtime=r))
    bundles = []
    for ref in snapshots.values():
        bundle = blob(ref)
        repo = args.out.parent / ('snapshot-' + ref['sha256'][:12] + '.git')
        if not repo.exists():
            subprocess.run(['git', 'clone', '--bare', '--quiet', str(bundle), str(repo)], check=True)
        commit = git(repo, 'rev-parse', 'refs/heads/prismabuild-snapshot').decode().strip()
        assert git(repo, 'show', '-s', '--format=%P', commit).decode().strip() == head
        changes = git(repo, 'diff-tree', '--no-commit-id', '--name-status', '-r', head, commit).decode().splitlines()
        assert all(re.fullmatch(r'A\t\.pbrun-closure\.[0-9a-f]{16}\.json', change) for change in changes), changes
        source_hashes = {}
        for path in SOURCE:
            expected = subprocess.check_output(['git', '-C', str(checkout), 'show', head + ':' + path])
            actual = git(repo, 'show', commit + ':' + path)
            assert actual == expected
            source_hashes[path] = sha(actual)
        for entry in entries:
            if entry['source_snapshot']['sha256'] != ref['sha256']:
                continue
            runtime = entry['runtime']
            for path, digest in runtime['selected_source_sha256'].items():
                assert sha(git(repo, 'show', commit + ':' + path)) == digest
            assert sha(git(repo, 'show', commit + ':tests/conftest.py')) == runtime['conftest_sha256']
        bundles.append(dict(snapshot=ref, commit=commit, source_sha256=source_hashes))
    answer = dict(head=head, actions=len(entries), passing_executions=sum(e['native_tests'] for e in entries),
                  entries=entries, source_bundles=bundles,
                  limits='CPU component evidence only. Worker attestation signatures unchecked. No deployment, GPU or campaign claim.')
    args.out.write_text(json.dumps(answer, indent=2) + '\n')
    print(json.dumps({key: answer[key] for key in ('head', 'actions', 'passing_executions')}))


if __name__ == '__main__':
    main()
