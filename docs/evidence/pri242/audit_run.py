"""Read exact PB terminals, logs and CAS producer records. Never print launch arguments."""
import hashlib
import json
from collections import Counter
from pathlib import Path
import sys

fleet = Path('/mnt/shared/prismabuild-fleet')
queue = fleet / 'pb-queue'
cas = fleet / 'cas'

def sha(raw):
    return hashlib.sha256(raw).hexdigest()

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()

checked_blobs = {}

def read_blob(desc):
    if desc['sha256'] in checked_blobs:
        raw = checked_blobs[desc['sha256']]
        assert len(raw) == desc['bytes']
        return raw
    raw = (cas / 'blobs' / desc['sha256'][:2] / desc['sha256']).read_bytes()
    assert len(raw) == desc['bytes'] and sha(raw) == desc['sha256']
    checked_blobs[desc['sha256']] = raw
    return raw

def audit(shard, *, allow_empty=False):
    key = shard['action_key']
    terminal = next(p for state in ('done', 'failed')
                    if (p := queue / state / (key + '.json')).is_file())
    record = json.loads(terminal.read_bytes())
    assert record['action_key'] == key
    assert record['detail']['returncode'] == shard['returncode']
    assert record['resource_scope_cleanup']['complete'] is True
    logs = {}
    stdout = ''
    for stream in ('stdout', 'stderr'):
        desc = record['detail'][stream + '_log']
        raw = (queue / desc['path']).read_bytes()
        assert len(raw) == desc['bytes'] and sha(raw) == desc['sha256']
        logs[stream] = desc
        if stream == 'stdout':
            stdout = raw.decode()
    completion = shard['test_completion']
    assert completion['status'] == ('incomplete' if allow_empty else 'complete')
    if allow_empty:
        assert not completion['collected'] and completion['problems'] == ['no selected test execution']
    assert not completion['missing_teardown'] and not completion['missing_outcomes']
    assert not completion['unexpected_nodeids']
    if not allow_empty:
        assert not completion['problems']
    rows = [json.loads(line.removeprefix('pbtest-outcomes: '))
            for line in stdout.splitlines() if line.startswith('pbtest-outcomes: ')]
    outcomes = [row for row in rows if row['run_nonce'] == completion['run_nonce']]
    assert len(outcomes) == 1
    outcomes = outcomes[0]
    assert set(outcomes['collected']) == set(completion['collected'])
    assert not outcomes['uncounted'] and not outcomes['file_selection_error']
    counts = Counter(report[2] for report in outcomes['reports'])
    request = json.loads((cas / 'requests' / key[:2] / (key + '.json')).read_bytes())
    assert request['action_key'] == key
    snapshot = record['checkout_snapshot']
    assert snapshot['input'] in request['inputs']
    bundle = read_blob(snapshot['input'])
    receipt_digest = None
    if shard['returncode'] == 0:
        published = []
        for line in stdout.splitlines():
            try:
                value = json.loads(line)
            except ValueError:
                continue
            if isinstance(value, dict) and value.get('status') in ('published', 'execution_result_published') and 'receipt' in value:
                published.append(value)
        assert len(published) == 1
        publication = published[0]
        receipt = publication['receipt']
        claim_digest = publication.get('local_result_claim_sha256')
        if claim_digest:
            claim = json.loads((cas / 'local-results/v1' / claim_digest[:2] / (claim_digest + '.json')).read_bytes())
            claim_body = dict(claim)
            assert claim_body.pop('claim_sha256') == claim_digest == sha(canonical(claim_body))
            assert claim['action_key'] == key
            assert claim['action_manifest_sha256'] == sha(canonical(request))
        else:
            assert receipt == json.loads((cas / 'actions/v3' / key[:2] / (key + '.json')).read_bytes())
        assert receipt['action_key'] == key
        assert receipt['action_manifest_sha256'] == sha(canonical(request))
        receipt_digest = receipt['receipt_sha256']
        execution = json.loads((cas / 'executions/v1' / receipt_digest[:2] /
                                (receipt_digest + '.json')).read_bytes())
        assert receipt == execution
        body = dict(receipt)
        body.pop('receipt_sha256')
        assert sha(canonical(body)) == receipt_digest
        read_blob(receipt['result'])
        assert receipt['producer']['action_key'] == key
        assert receipt['producer']['inputs'] == request['inputs']
    return dict(action_key=key, terminal=str(terminal), status=record['status'],
                worker=record['claimed_host'], returncode=shard['returncode'],
                source_parent=snapshot['parent'], source_snapshot=snapshot['commit'],
                bundle_sha256=snapshot['input']['sha256'], bundle_bytes=len(bundle),
                request_sha256=sha(canonical(request)), receipt_sha256=receipt_digest,
                counts=dict(counts), collected=len(completion['collected']),
                native_completion='complete', cleanup_complete=True, logs=logs)

report = Path(sys.argv[1])
shards = json.loads(report.read_text())
assert isinstance(shards, list) and shards
records = [audit(shard) for shard in shards]
summary = dict(report=str(report), actions=records,
               counts=dict(sum((Counter(r['counts']) for r in records), Counter())),
               terminal_count=len(records), success_receipts=sum(r['receipt_sha256'] is not None for r in records))
dest = report.with_suffix('.verified.json')
dest.write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps({k:v for k,v in summary.items() if k != 'actions'}))
