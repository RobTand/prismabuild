"""Read back PRI-30 source qualification without executing tests.

Run from the reviewed checkout on a fleet coordinator:
    python3 docs/evidence/issue2014/verify_acceptance.py
"""
import hashlib
import json
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[3]
RUNTIME = Path('/mnt/shared/prismabuild-fleet/repo')
QUEUE = Path('/mnt/shared/prismabuild-fleet/pb-queue')


def verify(record_path):
    record = json.loads(record_path.read_text())
    source = record['source']['commit']
    changed = subprocess.check_output(
        ['git', '-C', str(ROOT), 'diff', '--name-only', source, 'HEAD',
         '--', 'src', 'tools', 'tests'], text=True)
    assert not changed, changed
    sys.path.insert(0, str(RUNTIME / 'src'))
    from prismabuild import core
    cas = core.PrismaBuildCAS('/mnt/shared/prismabuild-fleet/cas')
    actions = {row['action_key']: row for row in record['evidence']}
    assert len(actions) == len(record['evidence'])
    observed = {}
    for report in record['reports']:
        data = Path(report['path']).read_bytes()
        assert hashlib.sha256(data).hexdigest() == report['sha256'], report['path']
        rows = json.loads(data)
        selected = set(report['selected_keys'])
        assert selected <= {row['action_key'] for row in rows}
        for row in rows:
            key = row['action_key']
            if key not in selected:
                continue
            assert row['returncode'] == 0 and row['record_binding'] == 1, key
            assert not row['skipped'] and not row['timed_out'], key
            completion = row['test_completion']
            assert completion['status'] == 'complete', key
            assert completion['collection_complete'] and not completion['collection_errors'], key
            assert not completion['collect_only'] and not completion['duplicate_teardown'], key
            assert not any(completion[name] for name in (
                'missing_teardown', 'missing_outcomes', 'unexpected_nodeids', 'problems')), key
            nodes = set(completion['collected'])
            assert nodes == set(completion['outcome_nodeids']), key
            assert nodes == set(completion['teardown_finished']), key
            assert len(nodes) == len(completion['collected']), key
            identity = (report['arch'], tuple(row['files']))
            assert identity not in observed or observed[identity] == nodes, identity
            observed[identity] = nodes
            request = cas.read_action_request(key)
            receipt = cas.lookup(request)
            audit = actions[key]
            assert receipt is not None and receipt['receipt_sha256'] == audit['receipt_sha256'], key
            assert audit['status'] == 'done' and audit['returncode'] == 0, key
            assert audit['terminal_status'] == 'executed', key
            for log in audit['verified_logs'].values():
                contents = (QUEUE / log['path']).read_bytes()
                assert len(contents) == log['bytes'], key
                assert hashlib.sha256(contents).hexdigest() == log['sha256'], key
    for arch in ('x86', 'arm'):
        assert sum(len(nodes) for (lane, _), nodes in observed.items() if lane == arch) == 355
        for name in record['repeated_files']:
            runs = [report for report in record['reports']
                    if report['arch'] == arch and name in report['selected_files']]
            assert len(runs) == 5, (arch, name, len(runs))
            assert sum(report['workers_per_shard'] == 2 for report in runs) == 2
    print(json.dumps({'source': source, 'verified_actions': len(actions),
                      'unique_tests_per_architecture': 355, 'repeat_gate': 'five; two xdist',
                      'deployment': 'not established'}))


if __name__ == '__main__':
    verify(Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).with_name('source-acceptance.json'))
