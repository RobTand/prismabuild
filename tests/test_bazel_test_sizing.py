"""Measured test sizes reach the py_test rule without changing other tests."""
from pathlib import Path
import importlib.util
import re
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]


def starlark(path, **bindings):
    # These macros use the Python-compatible subset of Starlark. Execute the
    # real functions, replacing only Bazel's rule and package primitives.
    source = (ROOT / path).read_text()
    source = re.sub(r'^load\([^\n]*\)\n', '', source, flags=re.MULTILINE)
    namespace = {'struct': SimpleNamespace, **bindings}
    exec(compile(source, path, 'exec'), namespace)
    return namespace


@pytest.fixture
def sizing():
    return starlark('tools/bazel/test_sizing.bzl')['TEST_SIZING']


def rule(name):
    captured = []
    native = SimpleNamespace(package_name=lambda: 'tests', package_relative_label=str)
    bindings = dict(native=native)
    for path in ('tools/bazel/nonhermetic.bzl', 'tools/bazel/test_sizing.bzl'):
        if (ROOT / path).exists():
            bindings.update(starlark(path, native=native))
    bindings.update(py_test=lambda **kwargs: captured.append(kwargs), TEST_INPUTS={},
                    all_requirements=[])
    macro = starlark('tools/bazel/python.bzl', **bindings)['pb_py_test']
    macro(name=name, path=f'tests/{name}.py', library=f':{name}_lib')
    return captured[0]


def test_sized_targets_pass_the_measured_properties_to_py_test(sizing):
    assert sizing, 'calibration must identify at least one target needing an override'
    for label, entry in sizing.items():
        properties = rule(label.split(':')[1])['exec_properties']
        assert {key: properties[key] for key in entry.exec_properties} == entry.exec_properties, label


@pytest.mark.parametrize('name', [
    'test_a_produced_namespace_is_not_a_consumer',
    'test_a_ready_gpu_action_is_not_starved_by_cpu_shards',
    'test_an_unproven_ending_is_refused_terminally_at_its_bound',
    'test_claim_denials',
    'test_client_proof_bridge_recovery',
    'test_docker_shim_relays_termination_1599',
    'test_output_reconciliation_cycle',
    'test_pbwait_finds_a_later_submission',
    'test_preemption_review_boundaries',
    'test_queue_bell',
    'test_the_claim_pass_times_its_transition_holds',
])
def test_quarantined_process_fixtures_reserve_cpu_and_memory(name):
    # These fixtures launch census, installed-reader, mover or waiter children.
    # A low whole-action average does not remove the concurrent child demand.
    properties = rule(name)['exec_properties']
    assert properties['EstimatedComputeUnits'] == '1'
    assert float(properties['EstimatedCPU']) >= 2
    assert properties['EstimatedMemory'].endswith('GB')
    assert float(properties['EstimatedMemory'][:-2]) >= 0.5


def test_a_quarantined_in_process_mover_keeps_dynamic_sizing():
    properties = rule('test_a_kill_between_a_movers_two_documents_blocks_nothing')['exec_properties']
    assert properties == {}


def test_runner_calibration_excludes_preexisting_and_incomplete_cgroups(tmp_path):
    spec = importlib.util.spec_from_file_location('calibrate_tests', ROOT / 'tools/bazel/calibrate_tests.py')
    calibration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(calibration)
    existing = tmp_path / 'previous-test'
    current = tmp_path / 'current-test'
    incomplete = tmp_path / 'not-a-test'
    for group in (existing, current, incomplete):
        group.mkdir()
    for group in (existing, current):
        (group / 'cpu.stat').write_text('usage_usec 0\n')
    assert calibration.runner_cgroups(tmp_path, {existing}) == [current]
    assert calibration.runner_cgroups(tmp_path / 'absent', set()) == []


def test_every_size_has_measurements_a_reason_and_an_existing_test_target(sizing):
    build = (ROOT / 'tests/BUILD.bazel').read_text()
    targets = set(re.findall(r'pb_py_test\(\n    name = "([^"]+)"', build))
    assert sizing
    for label, entry in sizing.items():
        assert label.startswith('//tests:') and label.split(':')[1] in targets, label
        assert entry.burst_cpu > 0 and entry.processes >= 2, label
        assert len(entry.reason.strip()) > 20 and 'TBD' not in entry.reason, label
        assert set(entry.exec_properties) == {'EstimatedComputeUnits', 'EstimatedCPU', 'EstimatedMemory'}, label
        assert entry.exec_properties['EstimatedComputeUnits'] == '1', label
        assert float(entry.exec_properties['EstimatedCPU']) > 1, label


@pytest.mark.parametrize('name', ['test_bazel_workspace', 'test_action_resource_profile'])
def test_unsized_targets_keep_their_properties_tags_and_environment(name):
    native = SimpleNamespace(package_name=lambda: 'tests')
    tags, properties, environment = starlark('tools/bazel/nonhermetic.bzl', native=native)['nonhermetic'](name)
    target = rule(name)
    assert target['exec_properties'] == properties
    assert target['tags'] == tags
    assert target['env'] == environment


def test_derivation_uses_bursts_instead_of_averages_and_rounds_memory():
    spec = importlib.util.spec_from_file_location('sizing_report', ROOT / 'tools/bazel/sizing_report.py')
    report = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(report)
    execution = {'execution': [{
        'actionMnemonic': 'TestRunner', 'targetLabel': '//tests:test_example',
        'executedActionMetadata': {
            'queuedTimestamp': '2026-11-02T00:00:00Z',
            'workerStartTimestamp': '2026-11-02T00:00:01Z',
            'executionStartTimestamp': '2026-11-02T00:00:02Z',
            'executionCompletedTimestamp': '2026-11-02T00:00:12Z',
            'usageStats': {'cpuNanos': '5000000000', 'peakMemoryBytes': '1100000000'},
        },
    }]}
    samples = {'//tests:test_example': [{'burst_cpu': 2.8, 'processes': 6}]}
    assert report.summarize(execution, samples) == [{
        'label': '//tests:test_example', 'burst_cpu': 2.8, 'processes': 6,
        'peak_memory_bytes': 1100000000, 'average_cpu': 0.5,
        'wall_seconds': 10.0, 'queue_seconds': 1.0,
        'exec_properties': {'EstimatedComputeUnits': '1', 'EstimatedCPU': '3', 'EstimatedMemory': '1.5GB'},
    }]
    samples['//tests:test_example'][0]['burst_cpu'] = 1.05
    assert report.summarize(execution, samples)[0]['exec_properties'] == {}
    # Source-confirmed child work needs room even when a short sample misses
    # overlap. Launcher counts alone must not opt every target into this rule.
    assert report.summarize(
        execution, samples, multi_process_labels={'//tests:test_example'},
    )[0]['exec_properties'] == {
        'EstimatedComputeUnits': '1', 'EstimatedCPU': '2', 'EstimatedMemory': '1.5GB',
    }
    assert report.summarize(
        execution, samples, multi_process_labels={'//tests:test_other'},
    )[0]['exec_properties'] == {}
    # An unisolated runner sample has no remote TestRunner execution record.
    assert report.summarize({'execution': []}, samples) == []
