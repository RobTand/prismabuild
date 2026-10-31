"""Read back #1942 fleet evidence without submitting or running tests."""
import argparse
from collections import Counter
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
assert plan == json.loads((scripts / 'validation-plan.json').read_text())
queue = Path('/mnt/shared/prismabuild-fleet/pb-queue')
cas = Path('/mnt/shared/prismabuild-fleet/cas')
keys = set()
rows = []
qualifications = Counter()
integrated_nodes = set()
qualification_populations = set()
for run in plan['runs']:
    name = run['name']
    for command in (
        [sys.executable, str(scripts / 'audit_terminals.py'), run['report'],
         '--output', str(root / (name + '-audit.json'))],
        [sys.executable, str(scripts / 'audit_source.py'), name,
         '--evidence-dir', str(root)],
    ):
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode:
            raise RuntimeError(name + ': ' + result.stdout + result.stderr)
    audit = json.loads((root / (name + '-audit.json')).read_text())
    report = json.loads(Path(run['report']).read_text())
    nodes = {n for row in report for n in row['test_completion']['collected']}
    if name == 'integrated-final':
        integrated_nodes = nodes
    if name.startswith('qual-'):
        assert nodes <= integrated_nodes
        qualification_populations.add(frozenset(nodes))
    for action in audit['actions']:
        key = action['action_key']
        assert key not in keys
        keys.add(key)
        request_path = cas / 'requests' / key[:2] / (key + '.json')
        request = json.loads(request_path.read_bytes())
        body = {k: v for k, v in request.items() if k != 'action_key'}
        canonical = json.dumps(body, sort_keys=True, separators=(',', ':'),
                               ensure_ascii=False, allow_nan=False).encode()
        assert hashlib.sha256(canonical).hexdigest() == key == request['action_key']
        params = request['params']
        command = params['command']
        workers = int(command[command.index('-n') + 1]) if '-n' in command else 1
        assert workers == run['workers']
        assert params['execution_timeout_s'] == 180
        assert params['demand'] == {k: run['resources'][k] for k in ('cpu', 'mem_gb')}
        terminal = json.loads((queue / 'done' / (key + '.json')).read_bytes())
        assert terminal['priority'] == -10 and terminal['needs_gpu'] is False
        assert terminal['attempts'] == 1 and len(terminal['attempt_history']) == 1
        assert terminal['attempt_history'][0]['attempt'] == 1
        log = Path(action['logs']['stdout']['path']).read_text()
        assert ('bringing up nodes...' in log) == (workers == 2)
        if name.startswith('qual-'):
            for file in action['files']:
                qualifications[run['architecture'], file, workers] += 1
        action['sealed_request_verified'] = True
        action['xdist_workers'] = workers
    rows.append(dict(run=name, architecture=run['architecture'],
                     workers=run['workers'], population=audit['population'],
                     actions=audit['actions']))
    print(name, audit['population'], flush=True)

touched = next(r['files'] for r in plan['runs'] if r['name'] == 'qual-x86-1')
for architecture in ('x86', 'gb10'):
    for file in touched:
        assert qualifications[architecture, file, 1] == 3
        assert qualifications[architecture, file, 2] == 2
assert len(keys) == 66
assert len(integrated_nodes) == 386 and len(qualification_populations) == 1
qualified = [r for r in rows if r['run'].startswith('qual-')]
summary = dict(schema='pb.slice1942.validation.v1', source_head=plan['head'],
               unique_integrated_tests=len(integrated_nodes), qualification_passes=sum(
                   r['population']['passed'] for r in qualified),
               qualification_actions=sum(len(r['actions']) for r in qualified),
               total_actions=len(keys), runs=rows,
               limits='Component source evidence only; no live consumer or deployment proof.')
assert summary['qualification_passes'] == 1580
(root / 'verified-evidence.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps({k: v for k, v in summary.items() if k != 'runs'}))
