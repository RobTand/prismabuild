#!/usr/bin/env python3
"""Import cycles among PrismaBuild's own modules may shrink and never grow (PB #2582).

A module in an import cycle depends on every other module in it. Bazel, and
the gate's own selector, then rerun a test that touches any member whenever
any member changes. This finds the strongly connected components of the
import graph over ``src/prismabuild/`` and ``tools/``. An edge is any import
statement, at module level, inside a function or under ``TYPE_CHECKING``, and
any ``import_module`` or ``__import__`` call that names a literal module. Names
resolve the way the gate's selector resolves them
(``gate_select.module_names``); a dotted name also loads each package
``__init__`` above it.

``tests/fixtures/import_cycle_baseline.json`` records every component, as its
members and the import edges between them. ``tests/test_import_cycles.py``
fails when a component has a member or an internal edge the baseline doesn't
record: a new cycle, a component that grows, or a cut edge that comes back.
It also fails when the baseline records a member or an edge that is no longer
in a cycle. Each cut shrinks the baseline in the same change, by running
``python tools/ci/import_cycles.py --write-baseline``, which refuses to record
a regression.
"""
from __future__ import annotations

import argparse
import ast
import importlib.util
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
BASELINE = ROOT / 'tests/fixtures/import_cycle_baseline.json'
SCANNED = ('src/prismabuild', 'tools')
LOADERS = frozenset({'import_module', '__import__'})

SELECT_SPEC = importlib.util.spec_from_file_location('gate_select', Path(__file__).with_name('gate_select.py'))
gate_select = importlib.util.module_from_spec(SELECT_SPEC)
SELECT_SPEC.loader.exec_module(gate_select)


def scanned_modules(root: Path) -> list[str]:
    return sorted(path.relative_to(root).as_posix()
                  for top in SCANNED if (root / top).is_dir()
                  for path in (root / top).rglob('*.py')
                  if '__pycache__' not in path.relative_to(root).parts)


def module_imports(tree: ast.AST, path: str) -> set[str]:
    """Every dotted name an import statement or a literal loader call in ``tree`` names."""
    package = PurePackage(path)
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            base = package.absolute(node.module, node.level)
            if base:
                found.add(base)
            found.update(f'{base}.{alias.name}' if base else alias.name
                         for alias in node.names if alias.name != '*')
        elif isinstance(node, ast.Call):
            callee = node.func.attr if isinstance(node.func, ast.Attribute) else getattr(node.func, 'id', None)
            if callee in LOADERS and node.args and isinstance(node.args[0], ast.Constant) \
                    and isinstance(node.args[0].value, str) and not node.args[0].value.startswith('.'):
                found.add(node.args[0].value)
    return found


class PurePackage:
    """The package a scanned file belongs to, for resolving its relative imports."""

    def __init__(self, path: str):
        self.parts = list(Path(path.removeprefix('src/')).parent.parts)

    def absolute(self, module: str | None, level: int) -> str:
        if not level:
            return module or ''
        anchor = self.parts[:len(self.parts) - level + 1]
        return '.'.join([*anchor, *([module] if module else [])])


def import_graph(root: Path) -> dict[str, set[str]]:
    """Each scanned module's edges to the other scanned modules it imports."""
    root = Path(root)
    files = scanned_modules(root)
    owners: dict[str, set[str]] = {}
    for path in files:
        for name in gate_select.module_names(path):
            owners.setdefault(name, set()).add(path)
    graph = {}
    for path in files:
        tree = ast.parse((root / path).read_text(encoding='utf-8'), filename=path)
        targets = set()
        for name in module_imports(tree, path):
            parts = name.split('.')
            for end in range(1, len(parts) + 1):
                targets.update(owners.get('.'.join(parts[:end]), ()))
        graph[path] = targets - {path}
    return graph


def strong_components(graph: dict[str, set[str]]) -> list[list[str]]:
    """Tarjan's components with more than one member, each sorted, largest first."""
    index: dict[str, int] = {}
    low: dict[str, int] = {}
    stack: list[str] = []
    on_stack: set[str] = set()
    found = []
    for start in sorted(graph):
        if start in index:
            continue
        work = [(start, iter(sorted(graph[start])))]
        index[start] = low[start] = len(index)
        stack.append(start)
        on_stack.add(start)
        while work:
            node, successors = work[-1]
            advanced = False
            for successor in successors:
                if successor not in index:
                    index[successor] = low[successor] = len(index)
                    stack.append(successor)
                    on_stack.add(successor)
                    work.append((successor, iter(sorted(graph[successor]))))
                    advanced = True
                    break
                if successor in on_stack:
                    low[node] = min(low[node], index[successor])
            if advanced:
                continue
            work.pop()
            if work:
                low[work[-1][0]] = min(low[work[-1][0]], low[node])
            if low[node] == index[node]:
                members = []
                while True:
                    member = stack.pop()
                    on_stack.discard(member)
                    members.append(member)
                    if member == node:
                        break
                if len(members) > 1:
                    found.append(sorted(members))
    return sorted(found, key=lambda members: (-len(members), members))


def cycle_state(root: Path = ROOT) -> dict:
    """Every import cycle: its members and the import edges inside it."""
    graph = import_graph(root)
    components = []
    for members in strong_components(graph):
        inside = set(members)
        components.append({
            'members': members,
            'edges': sorted(f'{source} -> {target}' for source in members
                            for target in graph[source] if target in inside),
        })
    return {'components': components}


def _recorded(state: dict, key: str) -> set[str]:
    return {entry for component in state['components'] for entry in component[key]}


def cycle_regressions(live: dict, baseline: dict) -> list[str]:
    """What the live graph adds to the baseline. Each one fails the ratchet."""
    found = []
    for key, label in (('members', 'module joined an import cycle'), ('edges', 'import edge inside a cycle')):
        found.extend(f'new {label}: {entry}' for entry in sorted(_recorded(live, key) - _recorded(baseline, key)))
    largest = max((len(component['members']) for component in baseline['components']), default=0)
    for component in live['components']:
        if len(component['members']) > largest:
            found.append(f"an import cycle of {len(component['members'])} modules exceeds the largest recorded, {largest}")
    return found


def unrecorded_gains(live: dict, baseline: dict) -> list[str]:
    """What the baseline records that is gone. Each one must shrink it in the same change."""
    found = []
    for key, label in (('members', 'module no longer in a cycle'), ('edges', 'edge no longer inside a cycle')):
        found.extend(f'{label}: {entry}' for entry in sorted(_recorded(baseline, key) - _recorded(live, key)))
    return found


def cycle_summary(state: dict) -> str:
    sizes = [len(component['members']) for component in state['components']]
    lines = [f'{len(sizes)} import cycles, sizes {sizes}']
    for component in state['components']:
        lines.append(f"  {len(component['members'])} modules, {len(component['edges'])} edges: "
                     + ', '.join(component['members']))
    return '\n'.join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n', 1)[0])
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--write-baseline', action='store_true',
                        help='record a gain in the committed baseline; refuses a regression')
    args = parser.parse_args(argv)
    live = cycle_state(args.root)
    print(cycle_summary(live))
    if args.write_baseline:
        path = args.root / BASELINE.relative_to(ROOT)
        if path.exists():
            worse = cycle_regressions(live, json.loads(path.read_text(encoding='utf-8')))
            if worse:
                print('refusing to record a regression:\n  ' + '\n  '.join(worse), file=sys.stderr)
                return 1
        path.write_text(json.dumps(live, indent=1) + '\n', encoding='utf-8')
    return 0


if __name__ == '__main__':
    sys.exit(main())
