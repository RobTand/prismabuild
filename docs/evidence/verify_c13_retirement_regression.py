"""Verify both runtime-specific causal pairs from retained PB data only."""
from pathlib import Path
import argparse
import hashlib
import json
import re

from retained_native_bundle import read_native_action, combine_actions

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--root', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--python312-copy', type=Path, required=True)
parser.add_argument('--python314-copy', type=Path, required=True)
args = parser.parse_args()
root = args.root
pins = {row['label']: row for row in json.loads((root / 'source-pins.json').read_bytes())}
phases = []
for version in ('312', '314'):
    for label in ('original', 'extracted'):
        phase = f'{label}-py{version}'
        location = root / phase
        plan = json.loads((root / (phase + '-plan.json')).read_bytes())
        primary = json.loads((location / 'verified.json').read_bytes())
        assert primary['all_verified']
        assert primary['source_head'] == pins[label]['pin'] == plan['pin']
        assert len(primary['actions']) == 1
        proof = primary['actions'][0]
        directory = location / 'raw' / proof['action_key']
        action = read_native_action(directory, proof)
        combined = combine_actions([action])
        assert set(combined['nodes']) == set(Path(plan['expected_nodes_file']).read_text().splitlines())
        assert len(combined['nodes']) == 11 and len(combined['files']) == 2
        expected = {'passed': 11} if label == 'original' else {'passed': 10, 'failed': 1}
        assert combined['counts'] == expected
        failures = [node for node, outcome in combined['nodes'].items() if outcome == 'failed']
        assert failures == ([] if label == 'original' else ['tests/test_empty_retirement_intent_is_filed_before_unlink.py::test_the_delete_intent_is_durable_before_unlink[corrupt-empty]'])
        assert action['succeeded'] == (label == 'original')
        assert (directory / 'receipt.json').exists() == (label == 'original')
        binary = args.python314_copy if version == '314' else args.python312_copy
        binary_bytes = binary.read_bytes()
        witness_lines = [re.fullmatch(r'[.F]*c13-f1-runtime: (\{.*\})', line)
                         for line in action['lines']]
        witnesses = [json.loads(match[1]) for match in witness_lines if match]
        assert len(witnesses) == 3
        allocation = proof['cpu_allocation']
        for witness in witnesses:
            assert witness['version_info'][:2] == plan['expected_python_major_minor']
            assert witness['executable'] == plan['python']
            assert witness['resolved'] == witness['mapped_executable']
            assert witness['bytes'] == len(binary_bytes)
            assert witness['sha256'] == hashlib.sha256(binary_bytes).hexdigest()
            assert witness['cpus'] == sorted(allocation['preferred'] + allocation['fallback'])
            assert witness['tmpdir'] == '/home/rob/pbt17'
        request = json.loads((directory / 'request.json').read_bytes())
        assert request['params']['command'][0] == plan['python']
        phases.append(dict(phase=phase, source=plan['pin'], action_key=proof['action_key'],
                           counts=expected, expected_failure_nodes=failures,
                           runtime_witnesses=witnesses, executable_copy=str(binary),
                           physical_scopes=combined['scopes'],
                           success_receipt_present=label == 'original'))
report = dict(verified=True, phases=phases,
              identical_new_control_sha256=pins['original']['test_sha256'],
              original_production_source_unchanged=True,
              extracted_production_source_unchanged=True,
              no_failed_scope_grants_pass_credit=True,
              limits='Two causal original/extracted pairs, each with three interruption cases and eight architecture controls. No corrected-source, combined/full, rollout, performance or deployment credit.')
args.output.write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(dict(verified=True, phases=[dict(phase=row['phase'], counts=row['counts']) for row in phases])))
