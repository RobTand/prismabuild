"""Recheck retained PB observations and their applicability to the integrated source."""
import argparse
from collections import Counter
import hashlib
import json
import pathlib
import subprocess

p = argparse.ArgumentParser()
p.add_argument('--evidence', type=pathlib.Path, default=pathlib.Path('/mnt/shared/paperclip-runs/PRI-242'))
p.add_argument('--checkout', type=pathlib.Path, default=pathlib.Path('/home/rob/paperclip-worktrees/prismabuild/v21-integrate-2219'))
p.add_argument('--fleet', type=pathlib.Path, default=pathlib.Path('/mnt/shared/prismabuild-fleet'))
a = p.parse_args()
qualified = '8be219c5b1d878311e82fc38e47cf1eb3903d326'
prior = '4eab1f69ed0c88638bfefbe43540bd9819cbca4c'
def git(*args):
    return subprocess.check_output(['git', '-C', str(a.checkout), *args])
def read(name):
    return json.loads((a.evidence / name).read_text())
def digest(data):
    return hashlib.sha256(data).hexdigest()
changed = git('diff', '--name-only', prior, qualified).decode().splitlines()
assert changed == ['tests/test_gpu_runtime_integration.py'], changed
for name in ('src', 'tools'):
    assert git('rev-parse', prior + ':' + name) == git('rev-parse', qualified + ':' + name)
assert all(not name.startswith(('src/', 'tools/', 'tests/')) for name in git('diff', '--name-only', qualified, 'HEAD').decode().splitlines())
observations = []
qualified_nodes = set()
reused_files = set()
all_nodes = set()
cleanup = []
for name, disposition in [('red', 'failed-reproduction'), ('integration', 'integrated-observation'), ('integration-repair', 'qualifies'), ('compatibility', 'empty-selection-unqualified'), ('compatibility-upgrade', 'qualifies')]:
    report = read(name + '.json')
    audited = read(name + '.audited.json')
    assert audited['all_primary_evidence_verified']
    assert {r['action_key'] for r in report} == {r['action_key'] for r in audited['actions']}
    for row in report:
        proof = next(r for r in audited['actions'] if r['action_key'] == row['action_key'])
        raw = pathlib.Path(proof['terminal']).read_bytes()
        terminal = json.loads(raw)
        scope = terminal['resource_scope_cleanup']
        exp = scope['export']
        assert scope['complete'] and exp['empty'] and exp['settled'] and exp['released'] and not exp['tickets_pending']
        cleanup.append({'action_key': row['action_key'], 'terminal_sha256': digest(raw), 'status': terminal['status'], 'complete': True, **{k: exp[k] for k in ('empty', 'settled', 'released', 'tickets_pending')}})
        stdout = (a.fleet / 'pb-queue' / proof['logs']['stdout']['path']).read_bytes()
        assert digest(stdout) == proof['logs']['stdout']['sha256']
        outcomes = [json.loads(line[len('pbtest-outcomes: '):]) for line in stdout.decode().splitlines() if line.startswith('pbtest-outcomes: ')]
        assert len(outcomes) == 1
        selected = row['test_completion']['collected']
        counts = Counter(r[2] for r in outcomes[0]['reports'])
        qualify = disposition == 'qualifies' or (name == 'integration' and row['verdict']['green'])
        if qualify:
            assert row['returncode'] == 0 and row['verdict']['green'] and selected
            assert counts == {'passed': len(selected)}, counts
            assert not qualified_nodes.intersection(selected)
            qualified_nodes.update(selected)
            for file in row['files']:
                assert git('show', audited['source'] + ':' + file) == git('show', qualified + ':' + file)
                if name == 'integration':
                    reused_files.add(file)
        all_nodes.update(selected)
        observations.append({'selection': name, 'disposition': 'qualified' if qualify else disposition, 'action_key': row['action_key'], 'counts': dict(counts), 'source': audited['source'], 'receipt_sha256': proof['receipt_sha256'], 'files': row['files']})
assert len(reused_files) == 29
assert len(qualified_nodes) == 565
compat = read('compatibility-upgrade.json')[0]['test_completion']['collected']
expected = read('qualification-prelaunch.json')['compatibility']['expected_nodes']
assert {v.split('::')[-1] for v in compat} == set(expected)
guards = ['test_design_doc_line_references.py', 'test_duplication_baseline.py', 'test_fleet_tool_flags_have_help.py', 'test_wire_inventory.py']
guard_counts = {name: sum('/' + name + '::' in node for node in qualified_nodes) for name in guards}
assert all(guard_counts.values())
ledger = json.loads(git('show', qualified + ':docs/staged_read_requirements_2026-09-20.json'))
rows = {r['id']: r for r in ledger['requirements']}
assert len(rows) == 69
for parent in ['6a73af3edd365e8d10939bc8998f2dd527b4fa72', '8be4d4056fbcd6285a932d3ac4bf5d8bf6015036']:
    old = json.loads(git('show', parent + ':docs/staged_read_requirements_2026-09-20.json'))
    for row in old['requirements']:
        assert all(rows[row['id']][key] == value for key, value in row.items())
axes = [{'id': row['id'], 'target': row.get('target_enforcement', row['text']), **{key: row[key] for key in ('implementation', 'validation', 'deployment', 'workload_proof')}} for row in ledger['requirements']]
result = {'qualified_source': qualified, 'original_integrated_source': prior, 'unique_qualified_tests': len(qualified_nodes), 'reused_unchanged_files': sorted(reused_files), 'guards': guard_counts, 'observations': observations, 'cleanup': cleanup, 'whole_requirement_axes_unchanged': axes, 'attestation_limit': 'Receipt/request/payload/runtime hashes verified; worker signing-key authority and signature not established.'}
(a.evidence / 'qualification.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({'unique_qualified_tests': len(qualified_nodes), 'reused_files': len(reused_files), 'guards': guard_counts, 'audited_actions': len(observations), 'ledger_rows_preserved': len(rows)}))
