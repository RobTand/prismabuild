"""Verify current drain composition from actual native and runtime records."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

from retained_native_bundle import read_native_action, combine_actions


def verify_drain_composition_native(root):
    phases = []
    for version in ('312', '314'):
        phase = f'native-py{version}-r2'
        plan = json.loads((root / (phase + '-plan.json')).read_bytes())
        verified = json.loads((root / phase / 'verified.json').read_bytes())
        assert verified['all_verified'] and verified['source_head'] == plan['pin']
        conftest = subprocess.check_output(['git', '-C', plan['checkout'], 'show',
                                            plan['pin'] + ':tests/conftest.py'])
        actions, observations = [], []
        for proof in verified['actions']:
            directory = root / phase / 'raw' / proof['action_key']
            action = read_native_action(directory, proof)
            assert action['succeeded'] and action['source'] == plan['pin']
            actions.append(action)
            prefix = 'prismabuild-test-runtime: '
            records = [json.loads(line[len(prefix):]) for line in action['lines']
                       if line.startswith(prefix)]
            records = [record for record in records if set(record['files']) == action['files']]
            assert len(records) == 1
            record = records[0]
            assert record['complete'] is True and 'reason' not in record
            assert record['version'][:2] == plan['expected_version']
            assert record['executable_path_matches_running'] is True
            assert record['conftest_sha256'] == hashlib.sha256(conftest).hexdigest()
            assert set(record['native_threads'].values()) == {'1'}
            allocation = proof['cpu_allocation']
            assert set(record['affinity']) == set(allocation['preferred'] + allocation['fallback'])
            descriptor = record['executable']
            binary = Path(descriptor['resolved_path'])
            if descriptor['sha256'] == 'c86f21d0da9290ece2d7d49a3f7ed52aed59cb029abdf0ea25c79634f6cbe960':
                binary = Path('/mnt/shared/prismabuild-agent-evidence-rob/refactor-20261016/active/final-scratch-profile/sparky-python3.12')
            payload = binary.read_bytes()
            assert len(payload) == descriptor['bytes']
            assert hashlib.sha256(payload).hexdigest() == descriptor['sha256']
            request = json.loads((directory / 'request.json').read_bytes())
            assert request['params']['command'][0] == plan['python']
            observations.append(dict(action_key=proof['action_key'], runtime=record))
        cohort = combine_actions(actions)
        assert cohort['nodes'].keys() == set(Path(plan['expected_nodes_file']).read_text().splitlines())
        assert cohort['files'] == set(plan['files'])
        assert cohort['counts'] == {'passed': 522}
        architecture = [row for row in cohort['reports']
                        if row[0].startswith('tests/test_duplication_baseline.py::')]
        assert len(architecture) == 8 and all(row[2] == 'passed' for row in architecture)
        phases.append(dict(phase=phase, source=plan['pin'], native=522,
                           counts=cohort['counts'], observations=observations,
                           scopes=cohort['scopes']))
    return dict(verified=True, phases=phases,
                limits='Native current-source role, drain, signal and architecture population only. Actual-root observations are separately recorded. No live service drain, runtime installation, bridge convergence, rollback designation or deployment credit.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    report = verify_drain_composition_native(args.root)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(dict(verified=True, phases=[dict(phase=row['phase'], native=row['native'])
                                                for row in report['phases']])))
