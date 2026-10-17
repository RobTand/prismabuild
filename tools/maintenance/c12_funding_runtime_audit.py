"""Audit retained PB process witnesses and physical scope exports with stdlib."""
import argparse
import hashlib
import json
from pathlib import Path


def c12_runtime_digest(raw):
    return hashlib.sha256(raw).hexdigest()


def c12_runtime_source(root):
    files = {}
    for directory in ('src', 'tools', 'tests'):
        for path in sorted((root / directory).rglob('*.py')):
            files[str(path.relative_to(root))] = c12_runtime_digest(path.read_bytes())
    raw = json.dumps(files, sort_keys=True, separators=(',', ':')).encode()
    return len(files), c12_runtime_digest(raw)


def c12_runtime_audit(evidence, phase):
    plan = json.loads((evidence / (phase + '-plan.json')).read_bytes())
    verified = json.loads((evidence / phase / 'verified.json').read_bytes())
    assert verified['all_verified'] and verified['source_head'] == plan['pin']
    selected = set(plan['files'])
    source_count, source_hash = c12_runtime_source(Path(plan['checkout']))
    observed, missing = [], []
    for action in verified['actions']:
        terminal_raw = Path(action['terminal_path']).read_bytes()
        assert c12_runtime_digest(terminal_raw) == action['terminal_sha256']
        terminal = json.loads(terminal_raw)
        cleanup = terminal['resource_scope_cleanup']
        physical = cleanup['export']
        assert cleanup['complete']
        assert all(physical[name] is True for name in
                   ('empty', 'ok', 'settled', 'released', 'stopped'))
        assert physical['tickets_pending'] is False
        log = action['logs']['stdout_log']
        raw = Path(log['source']).read_bytes()
        assert len(raw) == log['bytes'] and c12_runtime_digest(raw) == log['sha256']
        text = raw.decode()
        standard = [json.loads(line.removeprefix('pbtest-runtime: '))
                    for line in text.splitlines() if line.startswith('pbtest-runtime: ')]
        older = [json.loads(line.removeprefix('c13-interpreter: '))
                 for line in text.splitlines() if line.startswith('c13-interpreter: ')]
        report = dict(action_key=action['action_key'], host=action['host'],
                      scope={key: physical[key] for key in
                             ('scope_id', 'empty', 'ok', 'settled', 'released',
                              'stopped', 'tickets_pending')})
        if standard:
            assert len(standard) == 1
            witness = standard[0]
            assert witness['source_files'] == source_count
            assert witness['source_sha256'] == source_hash
            assert set(witness['selected_files']) <= selected
            assert witness['selected_files']
            native = witness['native_bounds']
            assert all(value in (None, '1') for value in native.values())
            assert all(native[name] == '1' for name in
                       ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS'))
            cpus = sorted(terminal['cpu_allocation']['preferred']
                          + terminal['cpu_allocation']['fallback'])
            assert witness['affinity'] == cpus
            assert witness['kernel'] and witness['machine']
            report['witness_kind'] = 'standard-process-record'
            report['source_hash_verified'] = True
        elif older:
            assert len(older) == 1
            witness = older[0]
            report['witness_kind'] = 'retained-executed-native-identity-control'
        else:
            missing.append(action['action_key'])
            report['version_credit'] = False
            report['reason'] = 'No actual Python process witness; collection/source/native credit only.'
            observed.append(report)
            continue
        executable_hash = witness.get('executable_sha256', witness.get('sha256'))
        executable_count = witness.get('executable_bytes', witness.get('bytes'))
        executable = evidence / 'runtime-bytes' / executable_hash
        payload = executable.read_bytes()
        assert len(payload) == executable_count and c12_runtime_digest(payload) == executable_hash
        assert witness['tmpdir'] == '/home/rob/pbt17'
        assert witness['executable'] == plan['python']
        expected = [3, 14] if plan['python'].endswith('python3.14') else [3, 12]
        assert plan['python'].endswith(('python3.12', 'python3.14'))
        assert witness['version_info'][:2] == expected
        assert witness['resolved'].endswith('python' + '.'.join(map(str, expected)))
        report.update(witness=witness, executable_bytes_verified=str(executable), version_credit=True)
        observed.append(report)
    return dict(phase=phase, source=plan['pin'], all_physical_scopes_closed=True,
                missing_process_witness=missing, actions=observed)


def c12_runtime_main():
    parser = argparse.ArgumentParser()
    parser.add_argument('evidence', type=Path)
    parser.add_argument('phases', nargs='+')
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    reports = [c12_runtime_audit(args.evidence, phase) for phase in args.phases]
    args.out.write_text(json.dumps(dict(reports=reports), indent=2) + '\n')
    print(json.dumps(dict(phases=len(reports), actions=sum(len(row['actions']) for row in reports),
                          missing_process_witness=sum(len(row['missing_process_witness']) for row in reports),
                          all_physical_scopes_closed=True)))


if __name__ == '__main__':
    c12_runtime_main()
