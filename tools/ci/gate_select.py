#!/usr/bin/env python3
"""Explain the merge gate selection without importing or executing test code."""
from __future__ import annotations

import argparse
import ast
import hashlib
import heapq
import json
import math
from pathlib import Path, PurePosixPath
import re
import subprocess

POLICY = 'direct-imports-v1'
OWNER = 2428


def read_json(path: Path) -> dict:
    return json.loads(path.read_text())


def changed_files(root: Path, base: str, head: str) -> list[str]:
    """Include deleted and renamed module names as well as new names."""
    if not all(re.fullmatch(r'[0-9a-fA-F]{40,64}', sha) for sha in (base, head)):
        raise ValueError('base and head must be full Git object IDs')
    # Compare the event's trees, including queued-ahead changes in its head.
    # This needs only those two objects, even in a shallow checkout.
    # --no-renames retains both sides of every rename.
    result = subprocess.run(['git', 'diff', '--name-only', '--no-renames', '-z',
                             base, head, '--'], cwd=root,
                            capture_output=True, check=True)
    return sorted(set(result.stdout.decode().rstrip('\0').split('\0')) - {''})


def tier_declarations(root: Path) -> tuple[set[str], dict[str, str]]:
    """Read authoritative verify.py constants without its CLI import side effects."""
    declarations = {}
    for node in ast.parse((root / 'tools/verify.py').read_text()).body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in {'FAST', 'GPU'}:
                    declarations[target.id] = set(ast.literal_eval(node.value))
    fast, gpu = declarations['FAST'], declarations['GPU']
    profile = read_json(root / 'tests/proof_execution.json')
    roots = set(profile['root']['files'])
    receipt = root / 'tests/receipt_execution.json'
    if receipt.exists():
        roots.update(read_json(receipt)['root_files'])
    return fast, {**{name: 'root population requires the pinned proof runtime' for name in roots},
                  **{name: 'GPU population requires CUDA qualification' for name in gpu}}


def module_names(path: str) -> set[str]:
    if not path.endswith('.py') or not path.startswith(('src/prismabuild/', 'tools/')):
        return set()
    module = path.removesuffix('.py').removeprefix('src/').replace('/', '.')
    module = module.removesuffix('.__init__')
    names = {module}
    if path.startswith('tools/'):
        names.add(module.removeprefix('tools.'))
        names.add(module.rsplit('.', 1)[-1])
    return names


def literal_path(node: ast.AST) -> str | None:
    """Resolve literal Path joins used by file-loading tests, without evaluation."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Call) and node.args:
        name = node.func.id if isinstance(node.func, ast.Name) else getattr(node.func, 'attr', '')
        if name in {'Path', 'PurePath', 'PurePosixPath'}:
            parts = [literal_path(arg) for arg in node.args]
            if all(part is not None for part in parts):
                return '/'.join(parts)
    if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and
        node.func.attr == 'joinpath'):
        base = literal_path(node.func.value)
        parts = [literal_path(arg) for arg in node.args]
        if all(part is not None for part in parts):
            return '/'.join(([base] if base else []) + parts)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
        left, right = literal_path(node.left), literal_path(node.right)
        if right is not None:
            return left.rstrip('/') + '/' + right.lstrip('/') if left else right
    return None


def dependencies(source: str) -> tuple[set[str], set[str]]:
    parsed = ast.parse(source)
    trees = [parsed]
    # Subprocess Python snippets are direct evidence too.
    for node in ast.walk(parsed):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and 'import ' in node.value:
            try:
                trees.append(ast.parse(node.value))
            except SyntaxError:
                pass
    modules, paths = set(), set()
    for tree in trees:
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules.add(node.module)
                modules.update(f'{node.module}.{alias.name}' for alias in node.names)
            value = literal_path(node)
            if value is not None:
                paths.add(value.removeprefix('./'))
                if not value.endswith('.py') and re.fullmatch(r'[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*', value):
                    modules.add(value)
    return modules, paths


def dependency_index(root: Path) -> dict[str, tuple[set[str], set[str]]]:
    return {path.relative_to(root).as_posix(): dependencies(path.read_text())
            for path in sorted((root / 'tests').rglob('test_*.py'))}


def estimate(files: list[str], durations: dict, workers: int = 5) -> dict:
    values = durations['files']
    missing = sorted(set(files) - values.keys())
    if missing:
        return {'serial_seconds': None, 'wall_seconds': None, 'workers': workers,
                'missing_files': missing, 'source': durations.get('source')}
    costs = [values[name] for name in files]
    if any(isinstance(value, bool) or not isinstance(value, (int, float)) or
           not math.isfinite(value) or value < 0 for value in costs):
        raise ValueError('file durations must be finite nonnegative seconds')
    # Conservative file-level packing. xdist loadfile never splits a test file.
    slots = [0.0] * workers
    for cost in sorted(costs, reverse=True):
        heapq.heappush(slots, heapq.heappop(slots) + cost)
    return {'serial_seconds': round(sum(costs), 3), 'wall_seconds': round(max(slots), 3),
            'workers': workers, 'missing_files': [], 'source': durations.get('source')}


def exclusion_entries(quarantine: dict) -> list[dict]:
    entries = [dict(row, kind=kind) for kind in ('entries', 'cost_exclusions')
               for row in quarantine.get(kind, [])]
    for row in entries:
        target = row.get('target', '')
        path = PurePosixPath(target.split('::', 1)[0])
        issue = row.get('issue')
        if (not target or not str(path).startswith('tests/') or path.is_absolute() or
            '..' in path.parts or not str(path).endswith('.py') or
            not isinstance(issue, int) or isinstance(issue, bool) or issue <= 0 or
            not isinstance(row.get('reason'), str) or not row['reason'].strip()):
            raise ValueError(f'invalid or unowned gate exclusion: {target!r}')
        if row['kind'] == 'cost_exclusions' and '::' in target:
            raise ValueError('cost exclusions must name complete files')
    if len({row['target'] for row in entries}) != len(entries):
        raise ValueError('duplicate gate exclusions')
    return entries


def create_plan(root: Path, changed: list[str], *, durations: dict, quarantine: dict,
                index: dict | None = None) -> dict:
    root = Path(root)
    index = dependency_index(root) if index is None else index
    all_tests = set(index)
    fast, other_lanes = tier_declarations(root)
    if fast - all_tests:
        raise ValueError(f'fast tier names missing files: {sorted(fast - all_tests)}')
    exclusions = exclusion_entries(quarantine)
    by_file = {row['target']: row for row in exclusions if '::' not in row['target']}
    population = all_tests - other_lanes.keys() - by_file.keys()
    version_body = {'policy': POLICY, 'files': sorted(population), 'fast': sorted(fast),
                    'other_lanes': other_lanes, 'exclusions': exclusions, 'durations': durations}
    version = hashlib.sha256(json.dumps(version_body, sort_keys=True).encode()).hexdigest()
    triggers = sorted(path for path in changed if
        path in {'conftest.py', 'pyproject.toml', 'tools/verify.py',
                 'tests/proof_execution.json', 'tests/receipt_execution.json',
                 '.github/workflows/gate.yml'} or
        path.startswith('tools/ci/gate_') or
        (path.startswith('tests/') and path.endswith('.py') and
         (Path(path).name == 'conftest.py' or not Path(path).name.startswith('test_'))))
    reasons: dict[str, list[str]] = {}
    for name, (imports, paths) in index.items():
        why = []
        if name in fast:
            why.append('fast tier')
        if triggers:
            why.append('whole population: ' + ', '.join(triggers))
        if name in changed:
            why.append('changed test file')
        for path in sorted(set(changed)):
            modules = module_names(path)
            if modules and any(dependency == module or dependency.startswith(module + '.')
                               for dependency in imports for module in modules):
                why.append('direct import: ' + path)
            elif path in paths:
                why.append('literal path reference: ' + path)
        if why:
            reasons[name] = why
    selected = [{'path': name, 'reasons': reasons[name],
                 'estimated_seconds': durations['files'].get(name)}
                for name in sorted(population & reasons.keys())]
    deferred = [{'path': name, 'reason': reason, 'issue': 1831 if reason.startswith('root') else OWNER, 'lane': True}
                for name, reason in sorted(other_lanes.items()) if name in all_tests]
    deferred += [{'path': row['target'], 'reason': row['reason'], 'issue': row['issue'],
                  'kind': row['kind'], 'selected_by': reasons.get(row['target'].split('::')[0], [])}
                 for row in exclusions]
    deselect = sorted(row['target'] for row in exclusions if '::' in row['target'] and
                      row['target'].split('::')[0] in {item['path'] for item in selected})
    files = [row['path'] for row in selected]
    return {'schema': 'prismabuild.gate_plan.v1', 'policy': POLICY,
            'population': {'size': len(population), 'version': version,
                           'estimate': estimate(sorted(population), durations)},
            'changed_files': sorted(set(changed)), 'selected': selected, 'deferred': deferred,
            'deselect': deselect, 'estimate': estimate(files, durations),
            'budget': {'target_wall_seconds': 300, 'maximum_wall_seconds': 600,
                       'includes_pickup': False, 'policy': 'never truncate relevant coverage'}}


def summary(plan: dict) -> str:
    def cell(value):
        return str(value).replace('|', '\\|').replace('\n', ' ')
    lines = ['## Merge gate selection', '', f"Population `{plan['population']['version']}`",
             f"Selected {len(plan['selected'])} of {plan['population']['size']} files.",
             f"Estimated test wall seconds at 5 workers: {plan['estimate']['wall_seconds']!s}.",
             'Runner pickup and setup are additional. Missing durations mean an unknown estimate.', '',
             '| Test file | Reasons | Estimated seconds |', '| --- | --- | ---: |']
    lines.extend(f"| {cell(row['path'])} | {cell('; '.join(row['reasons']))} | {row['estimated_seconds']} |"
                 for row in plan['selected'])
    lines += ['', '<details><summary>Other lanes and exclusions</summary>', '',
              '| Test | Reason | Owning issue |', '| --- | --- | --- |']
    lines.extend(f"| {cell(row['path'])} | {cell(row['reason'])} | #{row['issue']} |"
                 for row in plan['deferred'])
    lines += ['', '</details>', '']
    return '\n'.join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path.cwd())
    parser.add_argument('--base', required=True)
    parser.add_argument('--head', required=True)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--summary', type=Path)
    args = parser.parse_args(argv)
    root = args.root.resolve()
    plan = create_plan(root, changed_files(root, args.base, args.head),
                       durations=read_json(root / 'tools/ci/gate_durations.json'),
                       quarantine=read_json(root / 'tools/ci/gate_quarantine.json'))
    plan.update(base_sha=args.base, head_sha=args.head)
    output = json.dumps(plan, sort_keys=True, indent=2) + '\n'
    if args.output:
        args.output.write_text(output)
    else:
        print(output, end='')
    if args.summary:
        with args.summary.open('a') as handle:
            handle.write(summary(plan))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
