"""Read and verify PRI-184 fleet evidence. This does not submit or run tests."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(sys.argv[1] if len(sys.argv) > 1 else '/mnt/shared/paperclip-runs/PRI-184')
QUEUE = Path('/mnt/shared/prismabuild-fleet/pb-queue')
BASE = '6185a6ee60f32cea85e4c45144493767a7477998'
TEST = 'tests/test_an_arbitration_rereads_only_what_moved.py'
MANIFEST = json.loads(Path(__file__).with_name('actions.json').read_text())
server = subprocess.Popen(
    ['python3', '/mnt/shared/prismabuild-fleet/repo/tools/fleet/pbmcp.py'],
    stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
sequence = 0


def call(name, arguments):
    global sequence
    sequence += 1
    request = {'jsonrpc': '2.0', 'id': sequence, 'method': 'tools/call',
               'params': {'name': name, 'arguments': arguments}}
    server.stdin.write(json.dumps(request) + '\n')
    server.stdin.flush()
    wire = json.loads(server.stdout.readline())
    result = json.loads(wire['result']['content'][0]['text'])
    assert result['complete'] and not result['timed_out'], result
    assert not result.get('unavailable'), result.get('unavailable')
    return result


rows = []
try:
    for entry in MANIFEST:
        name = entry['name']
        path = ROOT / (name + '.json')
        report, = json.loads(path.read_text())
        strict = name == 'strict-x86'
        assert report['action_key'] == entry['action_key']
        assert report['returncode'] == int(strict), (name, report['returncode'])
        assert report['ran'] and report['attempts'] == 1 and not report['skipped']
        completion = report['test_completion']
        assert completion['status'] == 'complete' and not completion['problems']
        assert len(completion['collected']) == (5 if strict else 7)
        assert set(completion['collected']) == set(completion['outcome_nodeids'])
        assert set(completion['collected']) == set(completion['teardown_finished'])
        action = call('pb_action', {'key_prefix': report['action_key'], 'tail_lines': 0})
        (ROOT / ('action-' + name + '.json')).write_text(json.dumps(action, indent=2) + '\n')
        assert action['state'] == ('failed' if strict else 'done'), (name, action['state'])
        assert action['outcome']['returncode'] == int(strict)
        key = report['action_key']
        terminal_path = QUEUE / action['state'] / (key + '.json')
        terminal_bytes = terminal_path.read_bytes()
        terminal = json.loads(terminal_bytes)
        assert terminal['action_key'] == key and terminal['attempts'] == 1
        request_path = Path(action['sealed']['cas_root']) / 'requests' / key[:2] / (key + '.json')
        request = json.loads(request_path.read_text())
        argv = request['params']['command']
        xdist = name.endswith(('r4', 'r5'))
        assert ('-n' in argv) == xdist
        if xdist:
            assert argv[argv.index('-n') + 1] == '2'
        assert argv[-1] == TEST
        assert request['params']['execution_timeout_s'] == 600
        assert action['sealed']['needs_gpu'] is False
        assert action['sealed']['resources'] == {'cpu': 2 if name.endswith(('r4', 'r5')) else 1, 'mem_gb': 2}
        snapshot = action['sealed']['checkout_snapshot']
        assert snapshot['parent'] == BASE
        blob = snapshot['input']
        bundle = Path(action['sealed']['cas_root']) / 'blobs' / blob['sha256'][:2] / blob['sha256']
        data = bundle.read_bytes()
        assert len(data) == blob['bytes']
        assert hashlib.sha256(data).hexdigest() == blob['sha256']
        archive = ROOT / ('snapshot-' + snapshot['commit'] + '.git')
        if not archive.exists():
            subprocess.run(['git', 'clone', '--bare', str(bundle), str(archive)],
                           check=True, capture_output=True)
        source = subprocess.check_output(['git', '-C', str(archive), 'show',
                                         snapshot['commit'] + ':' + TEST])
        baseline = subprocess.check_output(['git', '-C', str(archive), 'show', BASE + ':' + TEST])
        changed = subprocess.check_output(['git', '-C', str(archive), 'diff', '--name-only',
                                          BASE, snapshot['commit']], text=True).splitlines()
        assert [p for p in changed if not p.startswith('.pbrun-closure.')] == ([TEST] if strict else [])
        marker = b'    expected = {root, *directories}\n'
        mutation = b'    assert not any(under for _kind, _name, under in events if _kind == "list"), "historical zero-listing assertion"\n'
        assert source == (baseline.replace(marker, mutation + marker, 1) if strict else baseline)
        attempt, = action['attempts_detail']
        log_rows = {}
        stdout = ''
        for stream, descriptor in attempt['logs'].items():
            content = (QUEUE / descriptor['path']).read_bytes()
            assert len(content) == descriptor['bytes']
            assert hashlib.sha256(content).hexdigest() == descriptor['sha256']
            log_rows[stream] = descriptor
            if stream == 'stdout':
                stdout = content.decode()
        if strict:
            assert 'historical zero-listing assertion' in stdout
            assert '4 failed, 1 passed, 2 deselected' in stdout
            for case in ['root', 'namespace', 'initial', 'both']:
                assert f'FAILED {TEST}::test_only_the_directory_that_refuses_trust_is_relisted[{case}]' in stdout
        else:
            assert '7 passed' in stdout
        if name.endswith(('r4', 'r5')):
            assert 'bringing up nodes...' in stdout
        outcome_lines = [line.removeprefix('pbtest-outcomes: ') for line in stdout.splitlines()
                         if line.startswith('pbtest-outcomes: ')]
        outcomes, = [json.loads(line) for line in outcome_lines]
        assert outcomes['run_nonce'] == completion['run_nonce']
        calls = {row[0]: row[2] for row in outcomes['reports'] if row[1] == 'call'}
        assert set(calls) == set(completion['collected'])
        for node, status in calls.items():
            assert status == ('failed' if strict and not node.endswith('[stable]') else 'passed')
        runtime_lines = [line.split('prismabuild-test-runtime: ', 1)[1]
                         for line in stdout.splitlines() if line.startswith('prismabuild-test-runtime: ')]
        assert runtime_lines
        for line in runtime_lines:
            runtime = json.loads(line)
            assert runtime['kernel']['machine'] == ('aarch64' if name.startswith('arm') else 'x86_64')
            if xdist:
                assert runtime['selected_source_sha256'] == {}
            else:
                assert runtime['selected_source_sha256'][TEST] == hashlib.sha256(source).hexdigest()
            assert all(value == '1' for value in runtime['native_threads'].values())
        result_lines = []
        for line in stdout.splitlines():
            try:
                value = json.loads(line)
            except ValueError:
                continue
            if isinstance(value, dict) and value.get('local_result_claim_sha256'):
                result_lines.append(value)
        claim = verification = None
        receipt = action['receipt']
        if strict:
            assert not result_lines and not receipt['present']
        else:
            result, = result_lines
            claim = result['local_result_claim_sha256']
            verification = call('pb_verify_claim', {'sha256': claim, 'hash_payload': True})
            assert verification['checks_passed'], verification
            assert receipt['present']
            raw_receipt = json.loads(Path(receipt['path']).read_text())
            assert raw_receipt['receipt_sha256'] == receipt['receipt_sha256']
            assert raw_receipt['action_key'] == report['action_key']
            payload = receipt['result']
            payload_path = Path(action['sealed']['cas_root']) / 'blobs' / payload['sha256'][:2] / payload['sha256']
            content = payload_path.read_bytes()
            assert len(content) == payload['bytes']
            assert hashlib.sha256(content).hexdigest() == payload['sha256']
            assert b'7 passed' in content
        rows.append({'name': name, 'action_key': report['action_key'],
                     'summary': report['summary'], 'host': action['outcome']['finished_host'],
                     'returncode': report['returncode'], 'source_sha256': hashlib.sha256(source).hexdigest(),
                     'snapshot': snapshot, 'logs': log_rows, 'receipt': receipt,
                     'terminal': {'path': str(terminal_path), 'sha256': hashlib.sha256(terminal_bytes).hexdigest()},
                     'request_path': str(request_path), 'pytest_argv': argv[3:],
                     'claim_sha256': claim, 'claim_checks': verification,
                     'completion': completion})
        print(name, report['summary'], action['outcome']['finished_host'],
              'terminal+logs+snapshot verified' if strict else 'terminal+logs+snapshot+CAS verified', flush=True)
finally:
    server.terminate()
    server.wait()
(ROOT / 'verified-evidence.json').write_text(json.dumps(rows, indent=2) + '\n')
print('verified', len(rows), 'of 11 actions')
