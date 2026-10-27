"""Read PB evidence and bind native outcomes to the submitted source."""
import argparse
import collections
import hashlib
import json
from pathlib import Path
import subprocess

CAS = Path('/mnt/shared/prismabuild-fleet/cas')
PBWAIT = '/mnt/shared/prismabuild-fleet/repo/tools/fleet/pbwait.py'


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--head', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    reports = json.loads(args.report.read_text())
    keys = [row['action_key'] for row in reports]
    assert len(keys) == len(set(keys))
    observed = subprocess.run(['python3', PBWAIT, '--endings', '--wait-s', '0', *keys], capture_output=True)
    assert observed.returncode in (0, 1), observed.stderr
    raw = observed.stdout
    args.output.with_suffix('.endings.json').write_bytes(raw)
    endings = json.loads(raw)
    assert endings['complete']
    by_key = {row['action_key']: row for row in endings['endings']}
    entries = []
    snapshots = {}
    for report in reports:
        key = report['action_key']
        ending = by_key[key]
        assert ending['result']['verified']
        assert ending['diagnostics']['cleanup']['complete']
        cleanup = ending['diagnostics']['cleanup']['resource_scope']['export']
        assert cleanup['empty'] and cleanup['settled']
        request_raw = (CAS / 'requests' / key[:2] / (key + '.json')).read_bytes()
        request = json.loads(request_raw)
        assert request['action_key'] == key
        assert request['params']['checkout_snapshot']['parent'] == args.head
        assert request['params']['demand'] == {'cpu': 2, 'mem_gb': 4}
        assert request['params']['execution_timeout_s'] == 1200
        assert request['environment']['variables']['CUDA_VISIBLE_DEVICES'] == ''
        snapshot, = [ref for ref in request['inputs'] if ref['id'] == 'pbrun.checkout-snapshot']
        bundle = CAS / 'blobs' / snapshot['sha256'][:2] / snapshot['sha256']
        assert sha(bundle.read_bytes()) == snapshot['sha256']
        assert bundle.stat().st_size == snapshot['bytes']
        commit = request['params']['checkout_snapshot']['commit']
        if commit not in snapshots:
            repo = args.output.parent / ('source-' + commit[:12])
            if not repo.exists():
                subprocess.run(['git', 'init', '--bare', '-q', str(repo)], check=True)
            subprocess.run(['git', '-C', str(repo), 'fetch', '-q', str(bundle), commit], check=True)
            snapshots[commit] = repo
        logs = {}
        for stream in ('stdout', 'stderr'):
            ref = ending['result'][stream]
            stream_raw = Path(ref['path']).read_bytes()
            assert sha(stream_raw) == ref['sha256'] and len(stream_raw) == ref['bytes']
            logs[stream] = ref
        lines = Path(logs['stdout']['path']).read_text().splitlines()
        native = {}
        for kind, prefix in [('completion', 'pbtest-completion: '),
                             ('outcomes', 'pbtest-outcomes: '),
                             ('runtime', 'prismabuild-test-runtime: ')]:
            rows = [json.loads(line[len(prefix):]) for line in lines if line.startswith(prefix)]
            native[kind], = rows
        completion, outcomes, runtime = [native[name] for name in ('completion', 'outcomes', 'runtime')]
        assert completion['collection_complete'] and not completion['collection_errors']
        assert not completion['collect_only'] and not completion['duplicate_teardown']
        population = set(completion['collected'])
        assert len(population) == len(completion['collected'])
        assert population == set(completion['teardown_finished']) == set(completion['outcome_nodeids'])
        assert population == set(outcomes['collected']) == {row[0] for row in outcomes['reports']}
        assert completion['run_nonce'] == outcomes['run_nonce']
        assert not outcomes['uncounted'] and not outcomes['file_selection_error']
        assert runtime['complete'] and runtime['exitstatus'] == ending['exit_code']
        assert runtime['executable_path_matches_running']
        assert runtime['native_threads'] == {'OMP_NUM_THREADS': '1', 'MKL_NUM_THREADS': '1', 'OPENBLAS_NUM_THREADS': '1'}
        assert len(runtime['affinity']) == len(set(runtime['affinity'])) == 2
        expected_files = set(report['files'])
        assert set(runtime['files']) == expected_files
        assert set(outcomes['file_selection']['files']) == expected_files
        assert {node.split('::')[0] for node in population} == expected_files
        if not runtime['selected_files']:
            command = request['params']['command']
            assert '-n' in command and int(command[command.index('-n') + 1]) > 1
            assert runtime['selected_source_sha256'] == {}
        else:
            assert set(runtime['selected_files']) == expected_files
        for path in expected_files:
            source = subprocess.check_output(['git', '-C', str(snapshots[commit]), 'show', commit + ':' + path])
            parent_source = subprocess.check_output(['git', '-C', str(snapshots[commit]), 'show', args.head + ':' + path])
            assert source == parent_source
        conftest = subprocess.check_output(['git', '-C', str(snapshots[commit]), 'show', commit + ':tests/conftest.py'])
        assert sha(conftest) == runtime['conftest_sha256']
        for path, expected in runtime['selected_source_sha256'].items():
            source = subprocess.check_output(['git', '-C', str(snapshots[commit]), 'show', commit + ':' + path])
            assert sha(source) == expected
        failures = sorted({row[0] for row in outcomes['reports'] if row[2] in ('failed', 'error')})
        receipt_digest = ending['result']['receipt_sha256']
        if ending['succeeded']:
            assert not failures and not outcomes['bound_failures'] and ending['exit_code'] == 0
            publication, = [json.loads(line) for line in lines if line.startswith('{') and '"receipt":' in line]
            receipt = publication['receipt']
            assert receipt['action_key'] == key and receipt['receipt_sha256'] == receipt_digest
            assert sha(canonical({k: v for k, v in receipt.items() if k != 'receipt_sha256'})) == receipt_digest
            assert json.loads((CAS / 'executions/v1' / receipt_digest[:2] / (receipt_digest + '.json')).read_text()) == receipt
            assert receipt['producer']['inputs'] == request['inputs']
            ref = receipt['result']
            payload = CAS / 'blobs' / ref['sha256'][:2] / ref['sha256']
            assert sha(payload.read_bytes()) == ref['sha256'] and payload.stat().st_size == ref['bytes']
        else:
            assert failures or outcomes['bound_failures']
            assert not receipt_digest
        entries.append({'action_key': key, 'status': ending['status'], 'exit_code': ending['exit_code'],
                        'host': ending['host'], 'snapshot_commit': commit, 'snapshot': snapshot,
                        'request_sha256': sha(request_raw), 'logs': logs, 'runtime': runtime,
                        'native_complete': True, 'cleanup_complete': True,
                        'receipt_sha256': receipt_digest, 'failed_nodeids': failures,
                        'outcome_counts': dict(collections.Counter(row[1] + ':' + row[2] for row in outcomes['reports']))})
    result = {'source_parent': args.head, 'report': str(args.report), 'entries': entries,
              'worker_signing_keys': 'not checked; no cryptographic attestation claim'}
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'actions': len(entries), 'failed': sum(bool(row['failed_nodeids']) for row in entries),
                      'receipts': sum(bool(row['receipt_sha256']) for row in entries), 'output': str(args.output)}))


if __name__ == '__main__':
    main()
