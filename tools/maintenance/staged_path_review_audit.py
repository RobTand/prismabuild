"""Freeze C09 review seams and compare complete source occurrences to ec634 and a8."""
from __future__ import annotations
import argparse
import ast
from collections import Counter
import json
from pathlib import Path
import subprocess
from staged_path_migrate import staged_inventory, staged_sources, staged_symbols, staged_references
from staged_path_audit import staged_checks, staged_body

REVIEW_BASE = 'ec634ac510c1a7465052f9111d51cfa928bb439d'
ORIGINAL_BASE = 'a8ba9c0c7beeeb379908924a60c6933e997c8df6'


def review_sources(root, ref):
    paths = [p for p in subprocess.check_output(
        ['git', '-C', str(root), 'ls-tree', '-r', '--name-only', ref], text=True).splitlines()
        if p.endswith('.py')]
    raw = subprocess.run(['git', '-C', str(root), 'cat-file', '--batch'],
        input=''.join(ref + ':' + p + '\n' for p in paths).encode(), capture_output=True, check=True).stdout
    out, offset = {}, 0
    for path in paths:
        end = raw.index(b'\n', offset)
        size = int(raw[offset:end].split()[-1])
        offset = end + 1
        out[path] = raw[offset:offset + size].decode()
        offset += size + 1
    return out


def review_occurrences(sources):
    private, raw_writers, progress, definitions = [], [], [], []
    for path, source in sources.items():
        if path.startswith(('tests/compat/', 'tools/maintenance/')):
            continue
        tree = ast.parse(source, filename=path)
        parents = {c: n for n in ast.walk(tree) for c in ast.iter_child_nodes(n)}
        for n in ast.walk(tree):
            scope, p = [], parents.get(n)
            while p:
                if isinstance(p, (ast.FunctionDef, ast.ClassDef)):
                    scope.append(p.name)
                p = parents.get(p)
            row = {'path': path, 'line': getattr(n, 'lineno', 0), 'scope': '.'.join(reversed(scope))}
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                definitions.append({**row, 'name': n.name, 'end': n.end_lineno,
                    'decorators': [ast.dump(d, include_attributes=False) for d in n.decorator_list]})
            if isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name):
                if n.value.id == 'staged_path' and n.attr.startswith('_'):
                    private.append({**row, 'expression': ast.unparse(n), 'parent_kind': type(parents.get(n)).__name__})
            if isinstance(n, ast.Call):
                name = ast.unparse(n.func)
                if name in ('reader_lease.write_material', 'residency_map.write_fragment'):
                    raw_writers.append({**row, 'call': ast.unparse(n)})
                if (isinstance(n.func, ast.Attribute) and n.func.attr == 'progress_phase'
                        or any(isinstance(a, ast.Constant) and a.value == 'progress_phase' for a in n.args)):
                    progress.append({**row, 'call': ast.unparse(n)})
    return {'private_staged_attributes': private, 'raw_pair_writers': raw_writers,
            'retired_progress_calls': progress, 'definition_occurrences': definitions}


def review_cli():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', type=Path, default=Path.cwd())
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--freeze', action='store_true')
    args = p.parse_args()
    root = args.root.resolve()
    original = review_sources(root, ORIGINAL_BASE)
    previous = review_sources(root, REVIEW_BASE)
    paths = subprocess.check_output(['git', '-C', str(root), 'ls-files', '*.py'], text=True).splitlines()
    current = {f: (root / f).read_text() for f in paths}
    selected = staged_symbols(original)
    result = {'base': REVIEW_BASE, 'original_base': ORIGINAL_BASE,
              'head': subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD'], text=True).strip(),
              'root': str(root), 'snapshots': {}, 'comparisons': {}}
    for label, sources in [('a8', original), ('ec', previous), ('current', current)]:
        rows = review_occurrences(sources)
        result['snapshots'][label] = {'python_files': len(sources), **rows}
    for label, sources in [('a8', original), ('ec', previous)]:
        missing, changed = [], []
        for path, old in sources.items():
            if path not in current:
                missing.append({'path': path, 'removed_file': True})
                continue
            new = current[path]
            if old != new:
                changed.append({'path': path, 'old_module_ast': ast.dump(ast.parse(old), include_attributes=False),
                    'new_module_ast': ast.dump(ast.parse(new), include_attributes=False)})
            if path.startswith('tests/'):
                expected = staged_checks(staged_references(old, selected) if label == 'a8' else old)
                loss = expected - staged_checks(new)
                if loss:
                    missing.append({'path': path, 'checks': dict(loss)})
        result['comparisons'][label] = {'changed_module_ASTs': changed, 'missing_test_checks': missing}
    historical = subprocess.check_output(['git', '-C', str(root), 'diff', '--name-only', REVIEW_BASE, '--', 'tests/compat'], text=True).splitlines()
    result['changed_historical_captures'] = historical
    result['limits'] = 'Source/AST occurrence evidence only; native outcomes require admitted execution. Every duplicate definition/decorator is retained as an occurrence.'
    if args.freeze:
        assert result['head'] == REVIEW_BASE and not subprocess.check_output(['git', '-C', str(root), 'status', '--porcelain', '--untracked-files=no'])
        result['frozen_review_occurrences'] = {path: staged_inventory(path, text) for path, text in previous.items()
            if not path.startswith('tests/compat/')}
    args.output.write_text(json.dumps(result, separators=(',', ':')) + '\n')
    print(json.dumps({'head': result['head'], 'old_private_attributes': len(result['snapshots']['ec']['private_staged_attributes']),
        'current_private_attributes': len(result['snapshots']['current']['private_staged_attributes']),
        'retired_progress_calls': len(result['snapshots']['current']['retired_progress_calls']),
        'missing_checks': {k: len(v['missing_test_checks']) for k, v in result['comparisons'].items()}}))


if __name__ == '__main__':
    review_cli()
