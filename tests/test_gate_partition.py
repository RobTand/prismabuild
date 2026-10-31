"""Heavy and light lanes partition every eligible file without inventing missing costs."""
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('gate_partition', ROOT / 'tools/ci/gate_partition.py')
partition = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(partition)


def test_partition_is_disjoint_complete_and_unknown_costs_go_heavy():
    plan = {'selected': [{'path': path} for path in ['a', 'b', 'c', 'd']]}
    result = partition.partition(plan, {'files': {'a': 1, 'b': 5, 'c': 9}}, 5)
    assert result['heavy'] == ['b', 'c', 'd'] and result['light'] == ['a']
    assert result['unknown_files'] == ['d']
    assert result['heavy_cpu_seconds'] is None and result['light_cpu_seconds'] == 1
