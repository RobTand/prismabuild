"""Audit retained fleet bytes and native completion without executing test work."""
import argparse
import collections
import hashlib
import json
from pathlib import Path
import subprocess

FLEET = Path('/mnt/shared/prismabuild-fleet')
SOURCE = '7fb8c362e9e50ab05ecd9e9b08c54f22368e10fa'
CAPTURE_SOURCE = '7585278f635e4e1d7cd7dc8eb324125b07969f40'
CAPTURE = 'b65406ddc95db56dc6ee4832a31d493d59835387fb0eede657d8c4b85c6fdb93'


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def git(repo, *args):
    return subprocess.check_output(['git', '-C', str(repo), *args])


def verify_claim(digest):
    request = {'jsonrpc': '2.0', 'id': 1, 'method': 'tools/call',
               'params': {'name': 'pb_verify_claim', 'arguments': {'sha256': digest, 'hash_payload': True}}}
    answer = subprocess.run(['python3', str(FLEET / 'repo/tools/pbmcp.py'), '--deadline-s', '10'],
                            input=json.dumps(request) + '\n', capture_output=True, text=True, check=True)
    envelope = json.loads(answer.stdout)
    assert 'error' not in envelope and not envelope['result'].get('isError')
    result = json.loads(envelope['result']['content'][0]['text'])
    assert result['complete'] and not result['timed_out'] and not result['unavailable']
    assert result['checks_passed'] and result['attestation_verified'] is None
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence', type=Path, default=Path('/mnt/shared/paperclip-runs/PRI-243'))
    args = parser.parse_args()
    reports = []
    for name in ('integration-guards', 'compatibility'):
        assert json.loads((args.evidence / (name + '.controller-exit.json')).read_text())['exit_code'] == 0
        reports.extend(dict(row, report=name) for row in json.loads((args.evidence / (name + '.json')).read_text()))
    keys = [row['action_key'] for row in reports] + [CAPTURE]
    assert len(set(keys)) == len(keys) == 9
    observed = subprocess.run(['python3', str(FLEET / 'repo/tools/fleet/pbwait.py'),
                               '--endings', '--wait-s', '0', *keys], capture_output=True, check=True)
    (args.evidence / 'qualification.endings.json').write_bytes(observed.stdout)
    endings = json.loads(observed.stdout)
    assert endings['complete']
    by_key = {row['action_key']: row for row in endings['endings']}
    entries = []
    snapshots = {}
    for report in [*reports, dict(action_key=CAPTURE, report='capture')]:
        key = report['action_key']
        capture = key == CAPTURE
        parent = CAPTURE_SOURCE if capture else SOURCE
        ending = by_key[key]
        assert ending['succeeded'] and ending['exit_code'] == 0 and ending['result']['verified'], key
        assert ending['diagnostics']['cleanup']['complete']
        scope = ending['diagnostics']['cleanup']['resource_scope']['export']
        assert scope['empty'] and scope['settled'] and scope['released'] and not scope['tickets_pending']
        terminal_path = FLEET / 'pb-queue/done' / (key + '.json')
        terminal_raw = terminal_path.read_bytes()
        terminal = json.loads(terminal_raw)
        assert terminal['action_key'] == key and terminal['detail']['returncode'] == 0
        assert terminal['resource_scope_cleanup']['complete']
        request_path = FLEET / 'cas/requests' / key[:2] / (key + '.json')
        request_raw = request_path.read_bytes()
        request = json.loads(request_raw)
        assert request['action_key'] == key
        snapshot = request['params']['checkout_snapshot']
        assert snapshot['parent'] == parent and terminal['checkout_snapshot'] == snapshot
        assert request['params']['demand'] == {'cpu': 1, 'mem_gb': 3 if capture else 4}
        assert request['params']['execution_timeout_s'] == 600
        assert terminal['priority'] == -10 and not terminal['needs_gpu']
        variables = request['environment']['variables']
        assert variables['CUDA_VISIBLE_DEVICES'] == ''
        threads = {name: variables[name] for name in ('OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'OPENBLAS_NUM_THREADS')}
        assert set(threads.values()) == {'1'}
        ref, = [ref for ref in request['inputs'] if ref['id'] == 'pbrun.checkout-snapshot']
        bundle = FLEET / 'cas/blobs' / ref['sha256'][:2] / ref['sha256']
        assert sha(bundle.read_bytes()) == ref['sha256'] and bundle.stat().st_size == ref['bytes']
        commit = snapshot['commit']
        if commit not in snapshots:
            repo = args.evidence / ('audited-source-' + commit[:12] + '.git')
            if not repo.exists():
                subprocess.run(['git', 'init', '--bare', '-q', str(repo)], check=True)
            subprocess.run(['git', '-C', str(repo), 'fetch', '-q', str(bundle), commit], check=True)
            assert git(repo, 'rev-parse', commit + '^').decode().strip() == parent
            changed = git(repo, 'diff', '--name-only', parent, commit).decode().splitlines()
            generated = request['code_closure']['files']
            assert set(changed) == {item['path'] for item in generated}
            for item in generated:
                assert item['path'].startswith('.pbrun-closure.') and item['path'].endswith('.json')
                raw = git(repo, 'show', commit + ':' + item['path'])
                assert len(raw) == item['bytes'] and sha(raw) == item['sha256']
                assert not git(repo, 'ls-tree', '--name-only', parent, '--', item['path'])
            snapshots[commit] = repo
        repo = snapshots[commit]
        logs = {}
        for stream in ('stdout', 'stderr'):
            ref = ending['result'][stream]
            raw = Path(ref['path']).read_bytes()
            assert sha(raw) == ref['sha256'] and len(raw) == ref['bytes']
            logs[stream] = ref
        lines = Path(logs['stdout']['path']).read_text().splitlines()
        publication, = [json.loads(line) for line in lines if line.startswith('{') and '"receipt":' in line]
        receipt = publication['receipt']
        digest = ending['result']['receipt_sha256']
        assert receipt['action_key'] == key and receipt['receipt_sha256'] == digest
        assert sha(canonical({k: v for k, v in receipt.items() if k != 'receipt_sha256'})) == digest
        receipt_path = FLEET / 'cas/executions/v1' / digest[:2] / (digest + '.json')
        assert json.loads(receipt_path.read_text()) == receipt
        assert receipt['producer']['inputs'] == request['inputs']
        ref = receipt['result']
        payload = FLEET / 'cas/blobs' / ref['sha256'][:2] / ref['sha256']
        assert sha(payload.read_bytes()) == ref['sha256'] and payload.stat().st_size == ref['bytes']
        claim_digest = publication['local_result_claim_sha256']
        verified = verify_claim(claim_digest)
        assert verified['claim']['action_key'] == key
        assert verified['payload']['sha256'] == ref['sha256']
        assert json.loads(Path(verified['receipt_path']).read_text()) == receipt
        counts = {}
        runtime = None
        completion = None
        if not capture:
            native = {}
            for kind, prefix in [('completion', 'pbtest-completion: '), ('outcomes', 'pbtest-outcomes: '),
                                 ('runtime', 'prismabuild-test-runtime: ')]:
                native[kind], = [json.loads(line[len(prefix):]) for line in lines if line.startswith(prefix)]
            completion, outcomes, runtime = [native[name] for name in ('completion', 'outcomes', 'runtime')]
            assert completion['collection_complete'] and not completion['collection_errors']
            assert not completion['collect_only'] and not completion['duplicate_teardown']
            population = set(completion['collected'])
            assert len(population) == len(completion['collected']) > 0
            assert population == set(completion['teardown_finished']) == set(completion['outcome_nodeids'])
            assert population == set(outcomes['collected']) == {row[0] for row in outcomes['reports']}
            assert not outcomes['uncounted'] and not outcomes['file_selection_error'] and not outcomes['bound_failures']
            assert completion['run_nonce'] == outcomes['run_nonce'] == report['test_completion']['run_nonce']
            assert runtime['complete'] and runtime['exitstatus'] == 0 and runtime['executable_path_matches_running']
            assert runtime['native_threads'] == threads and len(set(runtime['affinity'])) == 1
            files = set(report['files'])
            assert set(runtime['files']) == set(runtime['selected_files']) == set(outcomes['file_selection']['files']) == files
            assert {node.split('::')[0] for node in population} == files
            assert sha(git(repo, 'show', commit + ':tests/conftest.py')) == runtime['conftest_sha256']
            assert set(runtime['selected_source_sha256']) == files
            for path, expected in runtime['selected_source_sha256'].items():
                assert sha(git(repo, 'show', commit + ':' + path)) == expected
            counts = dict(collections.Counter(row[1] + ':' + row[2] for row in outcomes['reports']))
            assert set(counts) == {'call:passed'}
            assert report['returncode'] == 0 and report['verdict']['green'] and not report['skipped']
            assert report['summary'].encode() in payload.read_bytes()
        entries.append(dict(action_key=key, report=report['report'], source_parent=parent,
                            source_snapshot=snapshot, source_bundle_verified=True,
                            source_tree_equals_parent_except_sealed_generated_closure=True,
                            host=ending['host'], status=ending['status'], exit_code=0,
                            terminal_path=str(terminal_path), terminal_sha256=sha(terminal_raw),
                            request_path=str(request_path), request_sha256=sha(request_raw), logs=logs,
                            receipt_sha256=digest, receipt_path=str(receipt_path), payload_path=str(payload),
                            claim_sha256=claim_digest, claim_checks_passed=True, attestation_verified=None,
                            payload_sha256=ref['sha256'], cleanup_complete=True, empty=True, settled=True, released=True,
                            outcome_counts=counts, runtime=runtime, completion=completion))
    result = dict(source_parent=SOURCE, actions=len(entries), entries=entries,
                  passed=sum(row['outcome_counts'].get('call:passed', 0) for row in entries),
                  failed=0, skipped=0, receipts=len(entries),
                  worker_signing_keys='unchecked; no cryptographic worker-attestation claim',
                  scope='Scoped source/component qualification; no runtime publication, activation, canary or campaign claim.')
    (args.evidence / 'qualification-audit.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({k:v for k,v in result.items() if k != 'entries'}))


if __name__ == '__main__':
    main()
