"""Compare every original assertion in the three repaired fixture cases."""
import ast
import json
from pathlib import Path
import subprocess

BASE = 'efff85c6f1c49d789afa733b36364be4a3b0d5c0'
CASES = {
    'tests/test_a_producers_export_rates_are_learned_per_family.py':
        'test_a_funded_export_runs_under_a_withhold_and_an_unfunded_one_waits',
    'tests/test_gpu_runtime_integration.py':
        'test_host_fault_evidence_preserves_an_explicit_deadline',
    'tests/test_a_producers_exports_run_on_its_allowance.py':
        'test_a_withholding_foreign_action_does_not_hold_the_export_out',
}


def assertions(source, name):
    function = next(node for node in ast.parse(source).body
                    if isinstance(node, ast.FunctionDef) and node.name == name)
    return [ast.dump(node, include_attributes=False)
            for node in ast.walk(function) if isinstance(node, ast.Assert)]


def main():
    result = {}
    for path, name in CASES.items():
        before = subprocess.check_output(['git', 'show', BASE + ':' + path], text=True)
        old = assertions(before, name)
        assert old == assertions(Path(path).read_text(), name), path
        result[path] = {'identical_assertions': len(old)}
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
