"""Apply the unchanged gate's population and node quarantine during map builds.

Load alongside tia_record through PYTEST_PLUGINS. No test discovery or admission
is done here. PB's own file partitioner owns the submitted population.
"""
from dataclasses import replace
import importlib.util
import sys
import json
from pathlib import Path


def population_pytest_configure(config):
    name = config.pluginmanager.get_name(sys.modules[__name__])
    config.invocation_params = replace(config.invocation_params, args=('-p', name, *config.invocation_params.args))


def population_pytest_collection_modifyitems(config, items):
    root = Path.cwd()
    path = root / 'tools/ci/gate_select.py'
    spec = importlib.util.spec_from_file_location('tia_population_gate', path)
    gate = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gate)
    exclusions = gate.exclusion_entries(json.loads((path.parent / 'gate_quarantine.json').read_text()))
    excluded = {row['target'] for row in exclusions}
    kept, removed = [], []
    for item in items:
        (removed if item.nodeid in excluded or item.nodeid.split('::', 1)[0] in excluded else kept).append(item)
    items[:] = kept
    if removed:
        config.hook.pytest_deselected(items=removed)

population_pytest_configure.pytest_impl = {'specname': 'pytest_configure'}

population_pytest_collection_modifyitems.pytest_impl = {'specname': 'pytest_collection_modifyitems'}
