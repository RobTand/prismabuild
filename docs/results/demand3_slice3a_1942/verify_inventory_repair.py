"""Read retained PB inventory-repair evidence without running tests."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

parser = argparse.ArgumentParser(__doc__)
parser.add_argument('--evidence-dir', type=Path, required=True)
args = parser.parse_args()
root = args.evidence_dir
scripts = Path(__file__).resolve().parent
plan = json.loads((root / 'validation-plan.json').read_text())
assert plan == json.loads((scripts / 'inventory-repair-plan.json').read_text())
queue = Path('/mnt/shared/prismabuild-fleet/pb-queue')
cas = Path('/mnt/shared/prismabuild-fleet/cas')

def git(checkout, *parts):
    return subprocess.check_output(['git', '-C', str(checkout), *parts])

results = []
for name in ['red'] + [run['name'] for run in plan['runs']]:
    subprocess.run([sys.executable, str(scripts / 'audit_terminals.py'),
                    str(root / (name + '.json')), '--output',
                    str(root / (name + '-audit.json'))], check=True)
    audit = json.loads((root / (name + '-audit.json')).read_text())
    assert len(audit['actions']) == 1
    action = audit['actions'][0]
    key = action['action_key']
    request = json.loads((cas / 'requests' / key[:2] / (key + '.json')).read_bytes())
    body = {k: v for k, v in request.items() if k != 'action_key'}
    canonical = json.dumps(body, sort_keys=True, separators=(',', ':'),
                           ensure_ascii=False, allow_nan=False).encode()
    assert hashlib.sha256(canonical).hexdigest() == key == request['action_key']
    assert request['params']['demand'] == {'cpu': 1, 'mem_gb': 2}
    assert request['params']['execution_timeout_s'] == 180
    state = 'failed' if name == 'red' else 'done'
    terminal = json.loads((queue / state / (key + '.json')).read_bytes())
    assert terminal['priority'] == -10 and terminal['needs_gpu'] is False
    assert terminal['attempts'] == 1 and len(terminal['attempt_history']) == 1
    if name == 'red':
        assert audit['population'] == dict(collected=1, passed=0, failed=1, skipped=0)
        assert action['failed_nodes'] == [
            'tests/test_wire_inventory.py::test_closed_field_sets_remain_pinned']
        assert action['snapshot']['parent'] == plan['before_head']
        log = Path(action['logs']['stdout']['path']).read_text()
        assert "At index 339 diff:" in log and 'DirtyHostInputs.confirm' in log
    else:
        assert audit['population'] == dict(collected=8, passed=8, failed=0, skipped=0)
        subprocess.run([sys.executable, str(scripts / 'audit_source.py'), name,
                        '--evidence-dir', str(root)], check=True)
    results.append(dict(run=name, population=audit['population'], action=action))

checkout = plan['runs'][0]['checkout']
before, after = plan['before_head'], plan['head']
assert git(checkout, 'diff', '--name-only', before, after).decode().splitlines() == [
    'tests/compat/field_sets.json']
old = json.loads(git(checkout, 'show', before + ':tests/compat/field_sets.json'))
new = json.loads(git(checkout, 'show', after + ':tests/compat/field_sets.json'))
assert len(old) == len(new) == 581
changes = [(a, b) for a, b in zip(old, new) if a != b]
assert len(changes) == 1
a, b = changes[0]
assert a == dict(path='src/prismabuild/queue_bell_host_reads.py',
                 owner='DirtyHostInputs.confirm', binding='keys',
                 unresolved='tuple(self._pending)')
assert b == dict(path='src/prismabuild/queue_bell_input_reads.py',
                 owner='DirtyInputs.confirm', binding='keys',
                 unresolved='tuple(self._pending)')
for tree in ('src', 'tools', 'tests'):
    assert git(checkout, 'rev-parse', after + ':' + tree) == git(
        checkout, 'rev-parse', 'HEAD:' + tree)
summary = dict(source_head=after, metadata_rows=581, changed_rows=1,
               unique_tests=8, passing_executions=16, results=results)
(root / 'verified-repair.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps({k: v for k, v in summary.items() if k != 'results'}))
