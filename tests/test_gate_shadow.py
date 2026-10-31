"""Shadow selection makes no accuracy claim without a complete full-population run."""
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('gate_shadow', ROOT / 'tools/ci/gate_shadow.py')
shadow = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(shadow)


def audit(tmp_path, *, failure='tests/test_b.py', verdict='fail', files=None, file_attribute=True):
    plan = {'mode': 'full', 'population': {'version': 'population-v1'},
            'selected': [{'path': 'tests/test_a.py'}, {'path': 'tests/test_b.py'}],
            'shadow_selected': [{'path': 'tests/test_a.py'}], 'deselect': []}
    records = [{'population_version': 'population-v1', 'files': files or ['tests/test_a.py', 'tests/test_b.py'],
                'verdict': verdict}]
    junit = tmp_path / 'junit.xml'
    attr = f'file="{failure}"' if file_attribute else ''
    other = 'tests/test_b.py' if failure == 'tests/test_a.py' else 'tests/test_a.py'
    junit.write_text(f'<testsuites><testsuite tests="2"><testcase file="{other}" name="test_ok"/>'
                     f'<testcase {attr} name="test_bad"><failure>regression</failure></testcase>'
                     '</testsuite></testsuites>')
    return shadow.audit(plan, records, [junit])


def test_shadow_detects_a_real_failure_outside_its_selection(tmp_path):
    result = audit(tmp_path)
    assert result['exit_code'] == 1
    assert result['missed_files'] == ['tests/test_b.py']
    assert result['verdict'] == 'missed_failure'


def test_caught_failure_does_not_claim_universal_accuracy(tmp_path):
    result = audit(tmp_path, failure='tests/test_a.py')
    assert result['exit_code'] == 0 and result['missed_files'] == []
    assert result['verdict'] == 'no_miss_observed'
    assert result['failure_files'] == 1


def test_missing_verdict_or_coverage_or_file_identity_is_inconclusive(tmp_path):
    assert audit(tmp_path, verdict=None)['exit_code'] == 75
    assert audit(tmp_path, files=['tests/test_a.py'])['exit_code'] == 75
    assert audit(tmp_path, file_attribute=False)['exit_code'] == 75


def test_actions_workflow_is_removed():
    assert not (ROOT / '.github/workflows/gate.yml').exists()


def test_assignment_claims_without_junit_coverage_are_inconclusive(tmp_path):
    plan = {'mode': 'full', 'population': {'version': 'v1'},
            'selected': [{'path': 'tests/a.py'}, {'path': 'tests/b.py'}],
            'shadow_selected': [{'path': 'tests/a.py'}]}
    records = [{'population_version': 'v1', 'files': ['tests/a.py', 'tests/b.py'], 'verdict': 'pass'}]
    junit = tmp_path / 'incomplete.xml'
    junit.write_text('<testsuites><testsuite><testcase file="tests/a.py" name="one"/></testsuite></testsuites>')
    assert shadow.audit(plan, records, [junit])['exit_code'] == 75
