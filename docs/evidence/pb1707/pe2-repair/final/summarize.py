"""Prepare a reviewable source-only qualification record from audited fleet artifacts."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

ROOT = Path("/mnt/shared/paperclip-runs/PRI-43/pe2-repair/qualification-v4")
CHECKOUT = Path('/home/rob/paperclip-worktrees/prismabuild/PRI-43-pe2-repair')
DEST = CHECKOUT / 'docs/evidence/pb1707/pe2-repair/final'
HEAD = 'abfb073bf1923ef6c05f5f450d75acb65306e011'


def git(*args):
    return subprocess.check_output(['git', '-C', str(CHECKOUT), *args])


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def main():
    assert not git('diff', '--name-only', HEAD, 'HEAD', '--', 'src', 'tools', 'tests')
    final = json.loads((ROOT / 'final-summary.json').read_text())
    assert (final['actions'], final['passed'], final['failed'], final['skipped']) == (123, 2419, 0, 0)
    assert len({e['receipt_sha256'] for e in final['entries']}) == 123
    plan = json.loads((ROOT / 'qualification-plan.json').read_text())
    controls = json.loads((ROOT.parent / 'supplemental/source-controls.json').read_text())
    for control in controls['unchanged_controls']:
        before = git('show', controls['before'] + ':' + control['path'])
        after = git('show', HEAD + ':' + control['path'])
        assert before == after
        assert hashlib.sha256(after).hexdigest() == control['sha256']
    rows = []
    for row in plan['rows']:
        audit_path = ROOT / 'final-audit' / (row['id'] + '-audit.json')
        audit = json.loads(audit_path.read_text())
        assert audit['source_parent'] == HEAD
        entries = audit['entries']
        command = row['command']
        files = sorted({f for e in entries for f in e['runtime']['files']})
        if row['id'].startswith(('x86_64-', 'aarch64-')):
            arch = row['id'].split('-')[0]
            assert all(e['runtime']['kernel']['machine'] == arch for e in entries)
        if row['id'] in [a + '-' + str(n) for a in ('x86_64', 'aarch64') for n in range(1, 6)]:
            assert len(files) == 8
            workers = 2 if row['id'].endswith(('-2', '-4')) else 1
            for entry in entries:
                key = entry['action_key']
                request = json.loads((Path('/mnt/shared/prismabuild-fleet/cas/requests') / key[:2] / (key + '.json')).read_text())
                sealed_command = request['params']['command']
                assert ('-n' in sealed_command) == (workers == 2)
                if workers == 2:
                    assert sealed_command[sealed_command.index('-n') + 1] == '2'
        else:
            workers = None
        rows.append({'id': row['id'], 'files': files, 'xdist_workers': workers,
                     'actions': len(entries), 'passed': sum(e['outcome_counts']['call:passed'] for e in entries),
                     'audit_sha256': hashlib.sha256(audit_path.read_bytes()).hexdigest()})
    assert len(next(r for r in rows if r['id'] == 'x86_64-integration')['files']) == 23
    assert len(next(r for r in rows if r['id'] == 'aarch64-integration')['files']) == 22
    ledger = json.loads((CHECKOUT / 'docs/staged_read_requirements_2026-09-20.json').read_text())
    reqs = []
    for req in ledger['requirements']:
        if req['id'] in ('PRG-03', 'SM-01', 'SC-03', 'LIVE-01'):
            reqs.append({'id': req['id'], 'target': req['target_enforcement'],
                         'whole_requirement_axes': {axis: req[axis]['state'] for axis in ('implementation', 'validation', 'deployment', 'workload_proof')},
                         'candidate': {'implementation': 'scoped component implemented', 'validation': 'scoped component qualified; independent review pending',
                                       'deployment': 'not deployed', 'workload_proof': 'not established'}})
    compact = {k:v for k,v in final.items() if k not in ('entries', 'rows')}
    compact.update({'schema': 'pb.1707_pe2_final_qualification.v1', 'rows': rows,
                    'qualified_subtrees': {p: git('rev-parse', HEAD + ':' + p).decode().strip() for p in ('src', 'tools', 'tests')},
                    'evidence_root': str(ROOT), 'requirement_scope': reqs,
                    'unsupported': plan['unsupported'], 'unchanged_controls': controls,
                    'historical_timeout': {'action_key': '0f159f34da6adcaa583963a84a2c94ad0e01e472283c9c9f58543a3726a93470',
                                           'status': 'timeout', 'qualification_credit': False,
                                           'reason': 'sealed 1200s execution deadline; no native completion or success receipt'},
                    'earlier_source': {'head': 'c8743b3798ce092d1f46759374737259249ae480', 'passed': 1941, 'actions': 97,
                                       'scope': 'historical source, before supplemental measurement and legacy diagnostics corrections'},
                    'entries': [{k:e[k] for k in ('action_key','host','snapshot_commit','request_sha256','receipt_sha256','outcome_counts')} for e in final['entries']]})
    DEST.mkdir(exist_ok=True)
    write(DEST / 'qualification.json', compact)
    keys = [e['action_key'] for e in final['entries']]
    acceptance = {'schema': 'pb.staged_read_acceptance.v1', 'step': 'merge',
                  'contract_commit': HEAD, 'ledger_commit': HEAD, 'scope': [r['id'] for r in reqs],
                  'prelaunch': {'status': 'retained separately', 'record': str(ROOT / 'prelaunch.json'),
                                'sealed_keys': keys, 'tier_policy': 'strict'},
                  'merge': {'branch': 'prismabuild-1707-v2', 'issue': 'https://github.com/RobTand/prismabuild/issues/1707',
                            'pull_request': 'https://github.com/RobTand/prismabuild/pull/2069', 'qualified_source': HEAD,
                            'decision': 'proposed; independent Principal standards/spec review required',
                            'validated_repairs': [{'id': r['id'], 'actions': keys,
                                                  'outcome': '123 executed actions, 2419 passed, zero failures/skips; all 123 success receipts and payloads re-read and verified; qualification.json binds each key and receipt'} for r in reqs],
                            'requirement_target_and_status': reqs,
                            'remaining_gaps': ['Independent changed-head Principal review and acceptance', 'Current-main composition and required checks on PRI-128',
                                               'ARM archival compatibility unsupported by the native harness; x86 archival queue file passed 185 tests',
                                               'All whole-requirement implementation, validation, deployment and workload gaps retain their incoming states',
                                               '#1707 closure waits for merged proof; PRI-69 remains downstream'],
                            'note': 'Source component qualification only. Earlier failed and crash-era records are retained and excluded from passing credit. No timeout or policy widening.'},
                  'deploy': {'status': 'pending; outside this task', 'generation': None, 'role_convergence': 'unknown', 'strict_reader_recheck': 'unknown'},
                  'complete': {'status': 'not established', 'application_gates': [], 'open_unknowns': ['No candidate runtime activation or live workload proof', 'No power-loss durability or worker signing-key validation']},
                  'exceptions': []}
    write(DEST / 'merge-acceptance.json', acceptance)
    shutil.copyfile(ROOT / 'audit_reports.py', DEST / 'audit_reports.py')
    reader = (ROOT / 'final_audit.py').read_text().replace('ROOT = Path("/mnt/shared/paperclip-runs/PRI-43/pe2-repair/qualification-v4")', 'ROOT = Path("' + str(ROOT) + '")')
    (DEST / 'reaudit.py').write_text(reader)
    shutil.copyfile(ROOT / 'qualification-plan.json', DEST / 'qualification-plan.json')
    shutil.copyfile(ROOT / 'qualification-state.json', DEST / 'qualification-state.json')
    print(json.dumps({'destination': str(DEST), 'actions': len(keys), 'passed': final['passed'], 'rows': len(rows)}))


if __name__ == '__main__':
    main()
