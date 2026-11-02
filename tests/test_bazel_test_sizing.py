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
    # An unisolated runner sample has no remote TestRunner execution record.
    assert report.summarize({'execution': []}, samples) == []
