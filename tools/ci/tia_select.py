#!/usr/bin/env python3
"""Select files by observed function calls, with explainable static fallbacks."""
from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import sys

try:
    from .tia_bytes import raw_sha256, compact_ascii_json_bytes, sorted_json
except ImportError:
    from tia_bytes import raw_sha256, compact_ascii_json_bytes, sorted_json


def load_tia_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def select(mapping: dict, diff: dict, *, file_hashes: dict, fast: list[str],
           changed_tests: list[str], population: list[str], static) -> dict:
    plans = {}
    def static_plan(paths):
        key = tuple(sorted(paths))
        if key not in plans:
            plans[key] = static(list(key))
        return plans[key]
    eligible = set(population)
    reasons: dict[str, set[str]] = {}
    nodes: dict[str, set[str]] = {}
    whole = set()
    def pick(path, why, node=None):
        if path in eligible:
            reasons.setdefault(path, set()).add(why)
            if node is None:
                whole.add(path)
            else:
                nodes.setdefault(path, set()).add(node)
    for path in fast:
        pick(path, 'fast tier')
    for path in changed_tests:
        pick(path, 'changed test file')
    functions = mapping['functions']
    known = set(functions)
    reverse: dict[str, set[str]] = {}
    for node, ids in mapping['tests'].items():
        for index in ids:
            reverse.setdefault(functions[index], set()).add(node)
    fallbacks = {}
    for path, changes in sorted(diff['python'].items()):
        if path.startswith('tests/'):
            fallbacks[path] = 'test helper change'
            continue
        names = changes['changed'] + changes['added'] + changes['deleted']
        if mapping['files'].get(path) != file_hashes.get(path) or path not in file_hashes:
            fallbacks[path] = 'stale source'
        elif changes['module_changed']:
            fallbacks[path] = 'module change'
        elif 'unattributed' not in mapping:
            fallbacks[path] = 'map lacks outside test evidence'
        elif any(path + '::' + name in mapping['unattributed'] for name in names):
            fallbacks[path] = 'function executed outside test'
        elif any(path + '::' + name not in known for name in names):
            fallbacks[path] = 'unmapped function'
        for function, tests in reverse.items():
            if function.startswith(path + '::') and (changes['module_changed'] or function.split('::', 1)[1] in names):
                for test in tests:
                    pick(test.split('::', 1)[0], ('module change: ' + path) if changes['module_changed'] else 'function: ' + function, test)
    for path in diff['non_python']:
        fallbacks[path] = 'non-Python path rule'
    # Infrastructure and configuration always retain the gate's whole-population triggers.
    for path in [*diff['python'], *diff['non_python']]:
        if path in {'conftest.py', 'tests/conftest.py', 'pyproject.toml', 'tools/verify.py'} or path.startswith(('tools/ci/', 'tests/test_tia_', 'tests/test_gate_')):
            fallbacks[path] = 'selection infrastructure'
            for test in eligible:
                pick(test, 'whole population: ' + path)
    for path, reason in sorted(fallbacks.items()):
        for row in static_plan([path])['selected']:
            for why in row['reasons']:
                pick(row['path'], f'{reason}: {path}; {why}')
        # A stale module can affect tests observed dynamically but absent from static imports.
        for function, tests in reverse.items():
            if function.startswith(path + '::'):
                for test in tests:
                    pick(test.split('::', 1)[0], f'{reason}: {path}; observed module')
    # Source text and imported test helpers can affect assertions without calling
    # the changed function. Retain the static path and helper evidence for them.
    for path in sorted(diff['python']):
        if path.startswith('tests/'):
            continue
        for row in static_plan([path])['selected']:
            for why in row['reasons']:
                if why.startswith(('literal path reference:', 'helper dependency:')):
                    pick(row['path'], f'source reader or helper consumer: {path}; {why}')
    unmapped = mapping.get('unmapped', {})
    if unmapped:
        changed_paths = sorted([*diff['python'], *diff['non_python']])
        for row in static_plan(changed_paths)['selected']:
            if row['path'] in unmapped:
                detail = unmapped[row['path']]
                reason = detail.get('reason') if isinstance(detail, dict) else detail
                for why in row['reasons']:
                    pick(row['path'], f'unmapped test: {reason}; {why}')
    return {'schema': 'prismabuild.tia_plan.v1', 'map_commit': mapping['source_commit'],
            'selected': [{'path': path, 'reasons': sorted(why),
                          'nodeids': None if path in whole else sorted(nodes[path])}
                         for path, why in sorted(reasons.items())],
            'targets': sorted(target for path in reasons for target in ([path] if path in whole else nodes[path])),
            'fallbacks': [{'path': path, 'reason': reason} for path, reason in sorted(fallbacks.items())]}


def prepare_selection(root: Path, gate_dir: Path, *, exclude: set[str] | None = None, duration_updates: dict | None = None) -> dict:
    gate = load_tia_module(gate_dir / 'gate_select.py', 'tia_gate_select')
    durations = gate.read_json(gate_dir / 'gate_durations.json')
    fills = {name: value for name, value in (duration_updates or {}).items() if name not in durations['files']}
    durations = dict(durations, files={**durations['files'], **fills},
                     source={**durations.get('source', {}), 'tia_map_missing_duration_fills': sorted(fills)})
    quarantine = gate.read_json(gate_dir / 'gate_quarantine.json')
    index = {name: value for name, value in gate.dependency_index(root).items() if name not in (exclude or set())}
    graph = gate.helper_importers(index, [])
    def static(paths, mode='selection'):
        return gate.create_plan(root, paths, durations=durations, quarantine=quarantine,
                                index=index, helper_graph=graph, mode=mode)
    full = static([], 'full')
    population = [row['path'] for row in full['selected']]
    fast, _ = gate.tier_declarations(root)
    return {'static': static, 'full': full, 'population': population, 'fast': fast, 'durations': durations, 'gate': gate}


def plan(root: Path, gate_dir: Path, mapping: dict, diff: dict, *, context: dict | None = None) -> dict:
    prepared = prepare_selection(root, gate_dir, duration_updates=mapping.get('duration_updates')) if context is None else context
    static, full = prepared['static'], prepared['full']
    population, fast = prepared['population'], prepared['fast']
    durations, gate = prepared['durations'], prepared['gate']
    changed = [*diff['python'], *diff['non_python']]
    recorded = {node.split('::', 1)[0] for node in mapping['tests']}
    unmapped = {path: 'test file absent from map' for path in set(population) - recorded}
    unmapped.update(mapping.get('unmapped', {}))
    result = select(dict(mapping, unmapped=unmapped), diff, file_hashes=diff['base_hashes'], fast=sorted(fast),
                    changed_tests=[name for name in changed if name.startswith('tests/')],
                    population=population, static=static)
    selected = {row['path']: row for row in result['selected']}
    result.update(estimate=gate.estimate(sorted(selected), durations), population=full['population'],
                  deferred=full['deferred'], deselect=full['deselect'])
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path.cwd())
    parser.add_argument('--gate-dir', type=Path, default=Path(__file__).parent)
    parser.add_argument('--map', type=Path, required=True)
    parser.add_argument('--base', required=True)
    parser.add_argument('--head', default='HEAD')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    diff = load_tia_module(Path(__file__).with_name('tia_diff.py'), 'tia_diff')
    maps = load_tia_module(Path(__file__).with_name('tia_map.py'), 'tia_map')
    result = plan(args.root, args.gate_dir, maps.read_impact_map(args.map), diff.analyze_changes(args.root, args.base, args.head))
    rendered = sorted_json(result) + '\n'
    if args.output:
        args.output.write_text(rendered)
    else:
        print(rendered, end='')


if __name__ == '__main__':
    main()
