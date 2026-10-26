"""Recheck retained slice 2b PB artifacts and reconcile the immutable test population."""
import argparse
import collections
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess

parser = argparse.ArgumentParser(__doc__)
parser.add_argument('--run-dir', type=Path, required=True)
parser.add_argument('--checkout', type=Path, required=True)
args = parser.parse_args()
ROOT = args.run_dir
CHECKOUT = args.checkout

QUEUE = Path('/mnt/shared/prismabuild-fleet/pb-queue')
CAS = Path('/mnt/shared/prismabuild-fleet/cas')


def digest(data):
    return hashlib.sha256(data).hexdigest()


def audit(key):
    terminals = [p for state in ('done', 'failed')
                 if (p := QUEUE / state / (key + '.json')).exists()]
    if len(terminals) != 1:
        raise ValueError(f'{key}: expected exactly one current terminal')
    terminal = terminals[0]
    raw = terminal.read_bytes()
    record = json.loads(raw)
    detail = record['detail']
    assert record['action_key'] == key
    result = dict(action_key=key, terminal=str(terminal), terminal_sha256=digest(raw),
                  status=record['status'], returncode=detail.get('action_returncode', detail['returncode']),
                  host=record['claimed_host'], snapshot=record['checkout_snapshot'], logs={})
    for channel in ('stdout', 'stderr'):
        log = detail[channel + '_log']
        path = QUEUE / log['path']
        data = path.read_bytes()
        assert digest(data) == log['sha256'] and len(data) == log['bytes']
        result['logs'][channel] = dict(path=str(path), sha256=digest(data), bytes=len(data))
        if channel == 'stdout':
            result['summaries'] = [line for line in data.decode().splitlines()
                                   if re.search(r'\d+ (?:passed|failed).* in ', line)]
    receipt_path = CAS / 'actions/v3' / key[:2] / (key + '.json')
    if result['returncode'] == 0:
        assert record['status'] in ('executed', 'cache_hit')
        receipt = json.loads(receipt_path.read_bytes())
        body = {k: v for k, v in receipt.items() if k != 'receipt_sha256'}
        canonical = json.dumps(body, sort_keys=True, separators=(',', ':'),
                               ensure_ascii=False, allow_nan=False).encode()
        assert digest(canonical) == receipt['receipt_sha256']
        assert receipt['action_key'] == key
        assert record['checkout_snapshot']['input'] in receipt['producer']['inputs']
        payload = receipt['result']
        data = (CAS / 'blobs' / payload['sha256'][:2] / payload['sha256']).read_bytes()
        assert digest(data) == payload['sha256'] and len(data) == payload['bytes']
        result.update(receipt=str(receipt_path), receipt_sha256=receipt['receipt_sha256'],
                      payload_sha256=payload['sha256'],
                      runtime=receipt['producer']['runtime']['launcher']['resolved_path'])
    else:
        assert record['status'] == 'failed' and not receipt_path.exists()
        result['receipt'] = 'no success receipt, expected for failed reproduction'
    return result

plan = json.loads((ROOT / 'submission.json').read_text())
report = json.loads((ROOT / 'integrated.json').read_text())
prior = json.loads((CHECKOUT / 'docs/results/demand3_slice2b_1941_acceptance.json').read_text())
assert len({r['action_key'] for r in report}) == len(report)
assert sorted(f for r in report for f in r['files']) == sorted(plan['files'])
all_nodes = []
rows = []
for shard in report:
    key = shard['action_key']
    row = audit(key)
    assert row['status'] == 'executed' and row['returncode'] == 0
    assert row['snapshot']['parent'] == plan['head']
    assert shard['returncode'] == 0 and shard['verdict']['green'] is True
    assert not shard['timed_out'] and not shard['skipped']
    terminal = json.loads(Path(row['terminal']).read_bytes())
    assert terminal['finished_unix'] > datetime(2026, 10, 25, 21, 52, tzinfo=timezone.utc).timestamp()
    log = Path(row['logs']['stdout']['path']).read_text()
    def marker(name):
        found = re.findall(r'pbtest-' + name + r': (\{[^\n]*\})', log)
        assert len(found) == 1, (key, name, len(found))
        return json.loads(found[0])
    completion = marker('completion')
    outcomes = marker('outcomes')
    assert completion['collection_complete'] is True
    assert completion['collect_only'] is False and completion['collection_errors'] is False
    assert completion['duplicate_teardown'] is False
    nodes = completion['collected']
    assert len(nodes) == len(set(nodes))
    assert set(nodes) == set(completion['teardown_finished']) == set(completion['outcome_nodeids'])
    assert nodes == outcomes['collected'] and not outcomes['deselected']
    assert set(n.split('::')[0] for n in nodes) == set(shard['files'])
    phases = collections.defaultdict(dict)
    for node, phase, verdict, *detail in outcomes['reports']:
        assert phase not in phases[node], (key, node, phase)
        phases[node][phase] = verdict
    assert set(phases) == set(nodes)
    assert all(p.get('call') == 'passed' and all(v == 'passed' for v in p.values()) for p in phases.values())
    assert not outcomes['bound_failures'] and not outcomes['uncounted']
    for name in ['schema','collection_complete','collect_only','collection_errors','collected','teardown_finished','outcome_nodeids','run_nonce']:
        assert completion[name] == shard['test_completion'][name]
    all_nodes.extend(nodes)
    row.update(shard=shard['shard'], files=shard['files'], collected=len(nodes), passed=len(nodes), skipped=0,
               finished_unix=terminal['finished_unix'], warnings=sum(int(v) for v in re.findall(r'(\d+) warnings?', shard['summary'])))
    rows.append(row)
assert len(all_nodes) == len(set(all_nodes))
source = {p: hashlib.sha256((CHECKOUT / p).read_bytes()).hexdigest() for p in prior['tested_source_blobs']}
assert source == prior['tested_source_blobs']
def git(*args):
    return subprocess.check_output(['git', '-C', str(CHECKOUT), *args], text=True).strip()
snapshots = {r['snapshot']['commit'] for r in rows}
assert len(snapshots) == 1
snapshot = snapshots.pop()
bundle_input = rows[0]['snapshot']['input']
bundle = Path('/mnt/shared/prismabuild-fleet/cas/blobs') / bundle_input['sha256'][:2] / bundle_input['sha256']
data = bundle.read_bytes()
assert len(data) == bundle_input['bytes'] and hashlib.sha256(data).hexdigest() == bundle_input['sha256']
bare = ROOT / 'snapshot.git'
if not bare.exists():
    subprocess.run(['git', 'init', '--bare', str(bare)], check=True, capture_output=True)
subprocess.run(['git', '-C', str(bare), 'fetch', '--no-tags', str(bundle), 'refs/heads/prismabuild-snapshot'], check=True, capture_output=True)
snapshot_tree = subprocess.check_output(['git', '-C', str(bare), 'rev-parse', snapshot + '^{tree}'], text=True).strip()
delta = subprocess.check_output(['git', '-C', str(bare), 'diff', '--name-status', plan['head'], snapshot], text=True).splitlines()
assert delta and all(re.fullmatch(r'A\t\.pbrun-closure\.[0-9a-f]+\.json', line) for line in delta), delta
for line in delta:
    closure = json.loads(subprocess.check_output(['git', '-C', str(bare), 'show', snapshot + ':' + line.split('\t')[1]]))
    assert closure['head'] == plan['head'] and closure['dirty_sha256'] == hashlib.sha256(b'').hexdigest()
assert len({r['runtime'] for r in rows}) == 1
for path, expected in source.items():
    data = subprocess.check_output(['git', '-C', str(bare), 'show', snapshot + ':' + path])
    assert hashlib.sha256(data).hexdigest() == expected, path
result = dict(schema='prismabuild.slice2b_freshness_audit.v1', source_commit=plan['head'], base=plan['base'],
              tested_source_tree=git('rev-parse', plan['head'] + '^{tree}'), snapshot_commit=snapshot, snapshot_tree=snapshot_tree, snapshot_only_additions=delta,
              population=dict(files=len(plan['files']), collected=len(all_nodes), ran=len(all_nodes), passed=len(all_nodes),
                              skipped=0, shards=len(rows), green_shards=len(rows), warnings=sum(r['warnings'] for r in rows)),
              hosts=sorted({r['host'] for r in rows}), source_blobs=source, actions=rows,
              population_sha256=hashlib.sha256(json.dumps(sorted(all_nodes), separators=(',', ':')).encode()).hexdigest(),
              audit_script=str(Path(__file__).resolve()))
(ROOT / 'integrated-audit.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({k:v for k,v in result.items() if k not in ['actions','source_blobs']}, indent=2))
