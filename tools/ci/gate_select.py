#!/usr/bin/env python3
"""Explain the merge gate selection without importing or executing test code."""
from __future__ import annotations

import argparse
from collections import Counter
import ast
import importlib.util
import heapq
import json
import math
from pathlib import Path, PurePosixPath
import re
import subprocess

DIGEST_SPEC = importlib.util.spec_from_file_location('gate_digest', Path(__file__).resolve().parents[2] / 'src/prismabuild/digest_primitives.py')
digest = importlib.util.module_from_spec(DIGEST_SPEC)
DIGEST_SPEC.loader.exec_module(digest)

PATH_SPEC = importlib.util.spec_from_file_location('gate_paths', Path(__file__).with_name('gate_paths.py'))
gate_paths = importlib.util.module_from_spec(PATH_SPEC)
PATH_SPEC.loader.exec_module(gate_paths)

POLICY = 'direct-imports-bound-paths-helper-closure-v2'


def load_gate_json(path: Path) -> dict:
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
                if isinstance(target, ast.Name) and target.id in {'FAST', 'GPU', 'TWO_CPU'}:
                    declarations[target.id] = set(ast.literal_eval(node.value))
    fast, gpu = declarations['FAST'], declarations['GPU']
    profile = load_gate_json(root / 'tests/proof_execution.json')
    roots = set(profile['root']['files'])
    receipt = root / 'tests/receipt_execution.json'
    if receipt.exists():
        roots.update(load_gate_json(receipt)['root_files'])
    return fast, {**{name: {'lane': 'root', 'reason': 'root population requires the pinned proof runtime'} for name in roots},
                  **{name: {'lane': 'gpu', 'reason': 'GPU population requires CUDA qualification'} for name in gpu},
                  **{name: {'lane': 'two_cpu', 'reason': 'two-CPU population requires dedicated CPU allocations'}
                     for name in declarations['TWO_CPU']}}


def module_names(path: str) -> set[str]:
    if not path.endswith('.py') or not path.startswith(('src/prismabuild/', 'tools/', 'tests/')):
        return set()
    module = path.removesuffix('.py').removeprefix('src/').replace('/', '.')
    module = module.removesuffix('.__init__')
    names = {module}
    if path.startswith(('tools/', 'tests/')):
        names.add(module.split('.', 1)[1] if '.' in module else module)
        names.add(module.rsplit('.', 1)[-1])
    return names


def dependencies(source: str, path: str = '') -> tuple[set[str], set[str]]:
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
        paths.update(gate_paths.references(tree, path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                prefix = node.module or ''
                if node.level and path:
                    package = list(PurePosixPath(path).parent.parts)
                    package = package[:len(package) - node.level + 1]
                    prefix = '.'.join([*package, *([prefix] if prefix else [])])
                if prefix:
                    modules.add(prefix)
                modules.update(f'{prefix}.{alias.name}' if prefix else alias.name for alias in node.names)
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                value = node.value
                if not value.endswith('.py') and re.fullmatch(r'[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*', value):
                    modules.add(value)
    return modules, paths


def dependency_index(root: Path) -> dict[str, tuple[set[str], set[str]]]:
    return {path.relative_to(root).as_posix(): dependencies(path.read_text(), path.relative_to(root).as_posix())
            for path in sorted((root / 'tests').rglob('*.py'))}


def helper_importers(index: dict, changed: list[str], existing: dict | None = None) -> dict[str, set[str]]:
    targets_to_index = {name for name in changed if name.startswith('tests/') and name.endswith('.py')}
    targets_to_index = (set(index) | targets_to_index) if existing is None else targets_to_index - index.keys()
    if not targets_to_index:
        return existing if existing is not None else {}
    aliases: dict[str, set[str]] = {}
    for path in targets_to_index:
        for alias in module_names(path):
            aliases.setdefault(alias, set()).add(path)
    reverse: dict[str, set[str]] = {} if existing is None else dict(existing)
    for path, (imports, references) in index.items():
        targets = {target for module in imports for target in aliases.get(module, ())}
        targets.update(references & targets_to_index)
        patterns = {reference for reference in references if '**' in reference}
        if patterns:
            targets.update(target for target in targets_to_index if gate_paths.matches(target, patterns))
        for target in targets - {path}:
            reverse.setdefault(target, set()).add(path)
    return reverse


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
    entries = [dict(row, kind=kind) for kind in ('entries', 'cost_exclusions', 'environment_lanes')
               for row in quarantine.get(kind, [])]
    for row in entries:
        target = row.get('target', '')
        path = PurePosixPath(target.split('::', 1)[0])
        issue = row.get('issue')
        if (not target or not str(path).startswith('tests/') or path.is_absolute() or
            '..' in path.parts or not str(path).endswith('.py') or
            not isinstance(issue, int) or isinstance(issue, bool) or issue <= 0 or issue == 2428 or
            not isinstance(row.get('reason'), str) or not row['reason'].strip()):
            raise ValueError(f'invalid gate exclusion or missing owner: {target!r}')
        if row['kind'] == 'environment_lanes' and (not isinstance(row.get('lane'), str) or not row['lane'].strip()):
            raise ValueError('environment lane needs a named capability')
        if row['kind'] == 'cost_exclusions' and '::' in target:
            raise ValueError('cost exclusions must name complete files')
    if len({row['target'] for row in entries}) != len(entries):
        raise ValueError('duplicate gate exclusions')
    return entries


def create_plan(root: Path, changed: list[str], *, durations: dict, quarantine: dict,
                index: dict | None = None, mode: str = 'selection', helper_graph: dict | None = None) -> dict:
    root = Path(root)
    triggers = sorted(path for path in set(changed) if
        path in {'conftest.py', 'tests/conftest.py', 'pyproject.toml', 'tools/verify.py',
                 'tests/proof_execution.json', 'tests/receipt_execution.json', '.github/workflows/gate.yml'} or
        path.startswith(('tools/ci/gate_', 'tests/test_gate_')))
    if triggers:
        # Every discovered test is already selected. Parsing imports and helper
        # edges cannot add coverage, including to the shadow selection.
        names = index if index is not None else (path.relative_to(root).as_posix()
                for path in sorted((root / 'tests').rglob('*.py')))
        index = {name: (set(), set()) for name in names}
    else:
        index = dependency_index(root) if index is None else index
    if mode not in {'selection', 'full'}:
        raise ValueError('mode must be selection or full')
    all_tests = {path for path in index if Path(path).name.startswith('test_')}
    fast, other_lanes = tier_declarations(root)
    if fast - all_tests:
        raise ValueError(f'fast tier names missing files: {sorted(fast - all_tests)}')
    exclusions = exclusion_entries(quarantine)
    by_file = {row['target']: row for row in exclusions if '::' not in row['target']}
    population = all_tests - other_lanes.keys() - by_file.keys()
    version_body = {'policy': POLICY, 'files': sorted(population), 'fast': sorted(fast),
                    'other_lanes': other_lanes, 'exclusions': exclusions, 'durations': durations}
    version = digest.canonical_sha256(version_body)
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
            modules = module_names(path) if not path.startswith('tests/') else set()
            if modules and any(dependency == module or dependency.startswith(module + '.')
                               for dependency in imports for module in modules):
                why.append('direct import: ' + path)
            elif gate_paths.matches(path, paths):
                why.append('literal path reference: ' + path)
        if why:
            reasons[name] = why
    reverse = helper_importers(index, changed, existing=helper_graph)
    pending = sorted({name for name, why in reasons.items() if any(reason != 'fast tier' for reason in why)} |
                     {path for path in changed if path.startswith('tests/')})
    seen = set(pending)
    while pending:
        helper = pending.pop()
        for importer in sorted(reverse.get(helper, ())):
            reasons.setdefault(importer, []).append('helper dependency: ' + helper)
            if importer not in seen:
                seen.add(importer)
                pending.append(importer)
    shadow = [{'path': name, 'reasons': reasons[name],
               'estimated_seconds': durations['files'].get(name)}
              for name in sorted(population & reasons.keys())]
    selected = shadow if mode == 'selection' else [
        {'path': name, 'reasons': ['full queue population'],
         'estimated_seconds': durations['files'].get(name)} for name in sorted(population)]
    deferred = [{'path': name, 'issue': None, **lane}
                for name, lane in sorted(other_lanes.items()) if name in all_tests]
    deferred += [{'path': row['target'], 'reason': row['reason'], 'issue': row['issue'],
                  'kind': row['kind'], **({'lane': row['lane']} if row['kind'] == 'environment_lanes' else {}), 'selected_by': reasons.get(row['target'].split('::')[0], [])}
                 for row in exclusions]
    deselect = sorted(row['target'] for row in exclusions if '::' in row['target'] and
                      row['target'].split('::')[0] in {item['path'] for item in selected})
    files = [row['path'] for row in selected]
    return {'schema': 'prismabuild.gate_plan.v1', 'policy': POLICY,
            'population': {'size': len(population), 'version': version,
                           'estimate': estimate(sorted(population), durations)},
            'mode': mode, 'changed_files': sorted(set(changed)), 'selected': selected,
            'shadow_selected': shadow, 'deferred': deferred,
            'deselect': deselect, 'estimate': estimate(files, durations),
            'budget': {'target_wall_seconds': 300, 'maximum_wall_seconds': 600,
                       'includes_pickup': False, 'policy': 'never truncate relevant coverage'}}


def summary(plan: dict) -> str:
    """A bounded preview; plan.json is the complete machine-readable record."""
    preview_entries = 50
    def cell(value):
        text = str(value).replace('|', '\\|').replace('\n', ' ')
        return text[:500] + ('...' if len(text) > 500 else '')
    counts = Counter(reason.split(':', 1)[0] for row in plan['selected'] for reason in row['reasons'])
    lines = ['## Merge gate selection', '', f"Population `{cell(plan['population']['version'])}`",
             f"Selected {len(plan['selected'])} of {plan['population']['size']} files.",
             f"Estimated test wall seconds at 5 workers: {plan['estimate']['wall_seconds']!s}.",
             'Runner pickup and setup are additional. Missing durations mean an unknown estimate.',
             'Complete selection, reasons and exclusions are in the plan.json artifact.', '',
             'Selection reasons: ' + '; '.join(f'{cell(reason)}: {count}' for reason, count in sorted(counts.items())[:50]), '',
             '| Test file | Reasons | Estimated seconds |', '| --- | --- | ---: |']
    lines.extend(f"| {cell(row['path'])} | {cell('; '.join(row['reasons']))} | {cell(row['estimated_seconds'])} |"
                 for row in plan['selected'][:preview_entries])
    if len(plan['selected']) > preview_entries:
        lines += ['', f"{len(plan['selected']) - preview_entries} more selected files in plan.json."]
    lines += ['', '<details><summary>Other lanes and exclusions</summary>', '',
              f"{len(plan['deferred'])} entries; complete record in plan.json.", '',
              '| Test | Reason | Owning issue |', '| --- | --- | --- |']
    lines.extend(f"| {cell(row['path'])} | {cell(row['reason'])} | {('#' + str(row['issue'])) if row['issue'] else 'separate lane'} |"
                 for row in plan['deferred'][:preview_entries])
    lines += ['', '</details>', '']
    return '\n'.join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path.cwd())
    parser.add_argument('--base', required=True)
    parser.add_argument('--head', required=True)
    parser.add_argument('--mode', choices=['selection', 'full'], default='selection')
    parser.add_argument('--files-output', type=Path, help='newline-separated files for a CI splitter')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--summary', type=Path)
    args = parser.parse_args(argv)
    root = args.root.resolve()
    plan = create_plan(root, changed_files(root, args.base, args.head),
                       durations=load_gate_json(root / 'tools/ci/gate_durations.json'),
                       quarantine=load_gate_json(root / 'tools/ci/gate_quarantine.json'), mode=args.mode)
    plan.update(base_sha=args.base, head_sha=args.head)
    output = json.dumps(plan, indent=2) + '\n'
    if args.output:
        args.output.write_text(output)
    else:
        print(output, end='')
    if args.files_output:
        args.files_output.write_text(''.join(row['path'] + '\n' for row in plan['selected']))
    if args.summary:
        with args.summary.open('a') as handle:
            handle.write(summary(plan))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
