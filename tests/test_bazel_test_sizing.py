"""Measured test sizes reach the py_test rule without changing other tests."""
from pathlib import Path
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
        assert properties['EstimatedCPU'] == entry.exec_properties['EstimatedCPU'], label
        if 'EstimatedMemory' in entry.exec_properties:
            assert properties['EstimatedMemory'] == entry.exec_properties['EstimatedMemory'], label


def test_every_size_has_measurements_a_reason_and_an_existing_test_target(sizing):
    build = (ROOT / 'tests/BUILD.bazel').read_text()
    targets = set(re.findall(r'pb_py_test\(\n    name = "([^"]+)"', build))
    assert sizing
    for label, entry in sizing.items():
        assert label.startswith('//tests:') and label.split(':')[1] in targets, label
        assert entry.burst_cpu > 0 and entry.processes >= 2, label
        assert len(entry.reason.strip()) > 20 and 'TBD' not in entry.reason, label
        assert set(entry.exec_properties) <= {'EstimatedCPU', 'EstimatedMemory'}, label
        assert float(entry.exec_properties['EstimatedCPU']) > 1, label


@pytest.mark.parametrize('name', ['test_core', 'test_action_resource_profile'])
def test_unsized_targets_keep_their_properties_tags_and_environment(name):
    native = SimpleNamespace(package_name=lambda: 'tests')
    tags, properties, environment = starlark('tools/bazel/nonhermetic.bzl', native=native)['nonhermetic'](name)
    target = rule(name)
    assert target['exec_properties'] == properties
    assert target['tags'] == tags
    assert target['env'] == environment
