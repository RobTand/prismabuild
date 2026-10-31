"""Read retained evidence bytes and fixture provenance; never executes tests."""
import base64
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
TREE = ROOT.parents[3]


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    rows = [json.loads(line) for line in (ROOT / 'actions.jsonl').read_text().splitlines()]
    capture = json.loads((ROOT / 'capture-action.json').read_text())
    timeout = json.loads((ROOT / 'timeout.json').read_text())
    for row in [*rows, capture, timeout]:
        key = row['action_key']
        for kind in ('terminal', 'stdout'):
            assert digest(row[kind + '_path']) == row[kind + '_sha256'], (key, kind)
        terminal = json.loads(Path(row['terminal_path']).read_text())
        assert terminal['action_key'] == key
        assert terminal['checkout_snapshot'] == row['source_snapshot']
        assert terminal['checkout_snapshot']['parent'] == row['source_parent']
        assert terminal['detail']['returncode'] == row['expected_returncode']
        assert terminal['resource_scope_cleanup']['complete'], key
        if row is timeout:
            assert terminal['status'] == 'timeout' and not Path(row['receipt_path']).exists()
            continue
        stdout = Path(row['stdout_path']).read_text()
        if row is not capture:
            assert 'pbtest-completion:' in stdout and row['summary'] in stdout
        if row['state'] == 'done':
            assert digest(row['receipt_path']) == row['receipt_file_sha256'], key
            receipt = json.loads(Path(row['receipt_path']).read_text())
            assert receipt['action_key'] == key and receipt['receipt_sha256'] == row['receipt_sha256']
            assert receipt['result']['sha256'] == row['payload_sha256']
            assert digest(row['payload_path']) == row['payload_sha256'], key
            if row is not capture:
                assert row['summary'].encode() in Path(row['payload_path']).read_bytes()
            assert row['claim_sha256'] in stdout, key
        else:
            assert not Path(row['receipt_path']).exists(), key
    fixtures = json.loads((ROOT / 'fixtures.json').read_text())
    for path, expected in fixtures['sha256'].items():
        assert digest(TREE / path) == expected, path
    archive = TREE / fixtures['archive']
    before = {row['case']: row for line in (archive / 'goldens-rollout.jsonl').read_text().splitlines()
              if (row := json.loads(line))}
    current = {row['case']: row for line in (TREE / 'tests/compat/goldens-rollout.jsonl').read_text().splitlines()
               if (row := json.loads(line))}
    old_lines = {json.loads(line)['case']: line for line in (archive / 'goldens-rollout.jsonl').read_bytes().splitlines(keepends=True)}
    new_lines = {json.loads(line)['case']: line for line in (TREE / 'tests/compat/goldens-rollout.jsonl').read_bytes().splitlines(keepends=True)}
    assert before.keys() == current.keys()
    for case in before:
        if case != 'upgrade-intent/armed':
            assert old_lines[case] == new_lines[case], case
            continue
        old = json.loads(base64.b64decode(before[case]['raw']))
        new = json.loads(base64.b64decode(current[case]['raw']))
        assert new.pop('coordinator_sha256') == fixtures['publisher_sha256']
        assert old.pop('coordinator_sha256') == fixtures['prior_publisher_sha256']
        assert old == new
    assert digest(TREE / 'tools/fleet/publish_runtime.py') == fixtures['publisher_sha256']
    print(json.dumps({'terminal_log_pairs': len(rows) + 2,
                      'test_actions': len(rows), 'green_test_actions': sum(r['state'] == 'done' for r in rows),
                      'expected_test_failures': sum(r['state'] == 'failed' for r in rows),
                      'verified_capture_actions': 1, 'retained_timeouts': 1,
                      'receipt_payload_pairs': sum(r['state'] == 'done' for r in rows) + 1,
                      'fixture_source_identity_only': True, 'worker_attestations': 'unchecked'}))


if __name__ == '__main__':
    main()
