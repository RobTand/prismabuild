"""Read-only audit of PB terminal, immutable log and CAS bindings for a report."""
import argparse
import hashlib
import json
from pathlib import Path
import re

parser = argparse.ArgumentParser(__doc__)
parser.add_argument('report', type=Path)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
QUEUE = Path('/mnt/shared/prismabuild-fleet/pb-queue')
CAS = Path('/mnt/shared/prismabuild-fleet/cas')

def digest(data):
    return hashlib.sha256(data).hexdigest()

rows = []
for shard in json.loads(args.report.read_text()):
    key = shard['action_key']
    paths = [p for state in ('done', 'failed') if (p := QUEUE / state / (key + '.json')).exists()]
    assert len(paths) == 1, key
    raw = paths[0].read_bytes()
    terminal = json.loads(raw)
    detail = terminal['detail']
    code = detail.get('action_returncode', detail['returncode'])
    assert key == terminal['action_key'] and code == shard['returncode']
    row = dict(action_key=key, terminal=str(paths[0]), terminal_sha256=digest(raw),
               status=terminal['status'], returncode=code, host=terminal['claimed_host'],
               snapshot=terminal['checkout_snapshot'], resources=terminal['resources'],
               cleanup_complete=terminal['resource_scope_cleanup']['complete'], logs={})
    assert row['cleanup_complete'] is True
    for channel in ('stdout', 'stderr'):
        ref = detail[channel + '_log']
        path = QUEUE / ref['path']
        data = path.read_bytes()
        assert digest(data) == ref['sha256'] and len(data) == ref['bytes']
        row['logs'][channel] = dict(path=str(path), sha256=digest(data), bytes=len(data))
        if channel == 'stdout':
            log = data.decode()
    def marker(name):
        found = re.findall(re.escape(name) + r': (\{[^\n]*\})', log)
        assert len(found) == 1, (key, name, len(found))
        return json.loads(found[0])
    completion = marker('pbtest-completion')
    outcomes = marker('pbtest-outcomes')
    runtime = marker('prismabuild-test-runtime')
    assert completion['collection_complete'] and not completion['collection_errors']
    assert not completion['collect_only'] and not completion['duplicate_teardown']
    nodes = completion['collected']
    assert len(nodes) == len(set(nodes))
    assert set(nodes) == set(completion['teardown_finished']) == set(completion['outcome_nodeids'])
    assert nodes == outcomes['collected']
    assert not outcomes['bound_failures'] and not outcomes['uncounted']
    calls = [r for r in outcomes['reports'] if r[1] == 'call']
    assert len(calls) == len(nodes) and {r[0] for r in calls} == set(nodes)
    assert all(r[2] in ('passed', 'failed') for r in calls)
    assert runtime['complete'] and runtime['exitstatus'] == code
    assert all(runtime['native_threads'].get(k) == '1'
               for k in ('OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'OPENBLAS_NUM_THREADS'))
    allocation = detail['cpu_allocation']
    assert set(runtime['affinity']) == set(allocation['preferred'] + allocation['fallback'])
    row.update(collected=len(nodes), passed=sum(r[2] == 'passed' for r in calls),
               failed=sum(r[2] == 'failed' for r in calls), skipped=0,
               failed_nodes=[r[0] for r in calls if r[2] == 'failed'],
               runtime=runtime, files=shard['files'], summary=shard['summary'])
    receipt_path = CAS / 'actions/v3' / key[:2] / (key + '.json')
    if code == 0:
        assert terminal['status'] in ('executed', 'cache_hit')
        receipt = json.loads(receipt_path.read_bytes())
        body = {k: v for k, v in receipt.items() if k != 'receipt_sha256'}
        canonical = json.dumps(body, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()
        assert digest(canonical) == receipt['receipt_sha256'] and receipt['action_key'] == key
        assert row['snapshot']['input'] in receipt['producer']['inputs']
        payload = receipt['result']
        data = (CAS / 'blobs' / payload['sha256'][:2] / payload['sha256']).read_bytes()
        assert digest(data) == payload['sha256'] and len(data) == payload['bytes']
        row.update(receipt=str(receipt_path), receipt_sha256=receipt['receipt_sha256'],
                   payload_sha256=payload['sha256'])
    else:
        assert terminal['status'] == 'failed' and not receipt_path.exists()
        row['receipt'] = 'no success receipt expected'
    rows.append(row)
result = dict(report=str(args.report), actions=rows,
              population={k: sum(r[k] for r in rows) for k in ('collected', 'passed', 'failed', 'skipped')})
args.output.write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(dict(output=str(args.output), actions=len(rows), population=result['population'])))
