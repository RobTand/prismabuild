#!/usr/bin/env python3
"""Verify C29 source ownership against the pinned base inside a PB action."""
from __future__ import annotations

import argparse
import ast
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile

from c29_move import BASE, MODULES, MOVES, migrate, migrate_imports, movement_body, produced_output


def git(root: Path, *argv: str) -> str:
    return subprocess.check_output(['git', '-C', str(root), *argv], text=True)


def definitions(tree: ast.AST) -> dict[str, ast.AST]:
    result = {}

    def visit(node: ast.AST, prefix: str = '') -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                name = prefix + child.name
                result[name] = child
                visit(child, name + '.')
            else:
                visit(child, prefix)
    visit(tree)
    return result


def form(node: ast.AST) -> str:
    return ast.dump(node, include_attributes=False)


def owner_source(source: str, path: str) -> str:
    source = migrate_imports(source)
    if path in {'tests/test_prewarm_receipt_recovery.py', 'tests/test_prewarm_requires_known_receipts.py'}:
        source = source.replace('repository / "tools" / "fleet" / "prewarm_loop.py"',
                                'repository / "src" / "prismabuild" / "prewarm_loop.py"')
    return source


def imports(tree: ast.AST) -> list[dict]:
    rows = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            rows.extend({'line': node.lineno, 'module': a.name, 'alias': a.asname} for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            rows.append({'line': node.lineno, 'module': node.module,
                         'names': [a.name for a in node.names]})
    return rows


def run(root: Path) -> dict:
    paths = git(root, 'ls-tree', '-r', '--name-only', BASE).splitlines()
    evidence = {'base': BASE, 'moves': MOVES, 'functions': [], 'assertions': [],
                'imports_before': [], 'imports_after': [], 'core_manifest_calls': [],
                'literal_entry_names_preserved': {}, 'historical_locator': []}
    for old, new in MOVES.items():
        before = git(root, 'show', f'{BASE}:{old}')
        after = (root / new).read_text()
        original = definitions(ast.parse(movement_body(owner_source(before, old))))
        current = definitions(ast.parse(after))
        assert original.keys() == current.keys(), (old, original.keys() ^ current.keys())
        for name in original:
            a, b = form(original[name]), form(current[name])
            assert a == b, (old, new, name)
            evidence['functions'].append({'old': f'{old}::{name}', 'new': f'{new}::{name}',
                                          'normalized_ast_sha256': hashlib.sha256(a.encode()).hexdigest()})
        for node in ast.walk(ast.parse(after)):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name) and node.func.value.id == 'pb' and 'manifest' in node.func.attr.lower():
                evidence['core_manifest_calls'].append({'old_module': old, 'new_module': new,
                                                         'line': node.lineno, 'symbol': 'pb.' + node.func.attr})
        if old.endswith('stage_rounding.py'):
            assert not (root / old).exists()
        else:
            wrapper = ast.parse((root / old).read_text())
            assert not definitions(wrapper), old
            assert f'from prismabuild.{Path(old).stem} import main' in (root / old).read_text()
    for path in paths:
        if not path.endswith('.py') or path.startswith('refactor/'):
            continue
        target = root / MOVES.get(path, path)
        if not target.exists():
            continue
        before = git(root, 'show', f'{BASE}:{path}')
        after = target.read_text()
        a, b = ast.parse(owner_source(before, path)), ast.parse(after)
        for row in imports(ast.parse(before)):
            if row.get('module') in MODULES:
                evidence['imports_before'].append({'path': path, **row})
        for row in imports(b):
            module = row.get('module') or ''
            if module == 'prismabuild' and any(n in MODULES for n in row.get('names', [])) or any(module == 'prismabuild.' + name for name in MODULES):
                evidence['imports_after'].append({'path': MOVES.get(path, path), **row})
            if row.get('module') in MODULES:
                assert path == 'tools/maintenance/verify_manifest_refactor.py', (path, row)
                evidence['historical_locator'].append({'path': path, **row})
        old_assertions = Counter(form(n) for n in ast.walk(a) if isinstance(n, ast.Assert))
        new_assertions = Counter(form(n) for n in ast.walk(b) if isinstance(n, ast.Assert))
        assert not old_assertions - new_assertions, (path, old_assertions - new_assertions)
        # Embedded Python assertions are source lines, including f-string programs.
        old_lines = Counter(line.strip() for line in owner_source(before, path).splitlines() if line.lstrip().startswith('assert '))
        new_lines = Counter(line.strip() for line in after.splitlines() if line.lstrip().startswith('assert '))
        assert not old_lines - new_lines, (path, old_lines - new_lines)
        if old_assertions or old_lines:
            evidence['assertions'].append({'old': path, 'new': MOVES.get(path, path),
                                          'ast_count': sum(old_assertions.values()),
                                          'source_assert_lines': sum(old_lines.values())})
        if path not in MOVES:
            for name in MODULES:
                pattern = rf'(?m)^\s*(?:import {name}\b|from {name} import\b)'
                if re.search(pattern, after):
                    assert path == 'tools/maintenance/verify_manifest_refactor.py', path
    path = 'src/prismabuild/produced_output.py'
    original = git(root, 'show', f'{BASE}:{path}')
    assert (root / path).read_text() == produced_output(original)
    for name in ('STAGE_MOVER_SCRIPT', 'RAM_PROMOTE_SCRIPT', 'STAGE_RELEASE_SCRIPT'):
        path = 'src/prismabuild/movement_actions.py'
        a = ast.parse(git(root, 'show', f'{BASE}:{path}'))
        b = ast.parse((root / path).read_text())
        def assignment(tree):
            return next(form(n) for n in tree.body if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in n.targets))
        assert assignment(a) == assignment(b)
        evidence['literal_entry_names_preserved'][name] = assignment(b)
    base_inventory = json.loads(git(root, 'show', f'{BASE}:tests/fixtures/duplication_baseline.json'))
    normalized = json.dumps(base_inventory, sort_keys=True)
    for old, new in MOVES.items():
        normalized = normalized.replace(old, new)
    assert json.loads(normalized) == json.loads((root / 'tests/fixtures/duplication_baseline.json').read_text())
    evidence['architecture_inventory_only_owner_renames'] = True
    with tempfile.TemporaryDirectory(prefix='c29-replay-') as scratch:
        replay = Path(scratch) / 'checkout'
        subprocess.run(['git', 'clone', '--shared', '--no-checkout', str(root), str(replay)],
                       check=True, capture_output=True)
        subprocess.run(['git', '-C', str(replay), 'checkout', '--detach', BASE],
                       check=True, capture_output=True)
        first = migrate(replay)
        expected_paths = set(git(root, 'diff', '--name-only', '--no-renames', BASE, 'HEAD').splitlines())
        expected_paths = {p for p in expected_paths if not p.startswith(('docs/', 'refactor/', '.pbrun-closure.')) and p != 'tests/test_movement_package.py'}
        assert set(first) == expected_paths, (set(first) - expected_paths, expected_paths - set(first))
        for path in expected_paths:
            left, right = replay / path, root / path
            assert left.exists() == right.exists(), path
            if left.exists():
                assert left.read_bytes() == right.read_bytes(), path
        assert migrate(replay) == [], 'Source move is not idempotent'
        evidence['replay'] = {'base': BASE, 'first_changed_paths': first,
                              'source_bytes_exact': True, 'second_changed_paths': []}
    evidence['normalization'] = {
        'module_imports': {name: 'prismabuild.' + name for name in MODULES},
        'dependency_unavailable_paths': ['_StagedPublisher._judge_owners', '_StagedPublisher._live_claim_cover', '_shared_consumers', 'retire_conflicted_window', '_produced_hold_verified'],
        'produced_output_forks': ['retire_batch', 'retire_staged_batches', '_dead_staged_batch_ready'],
        'assertion_owner_paths': ['tests/test_prewarm_receipt_recovery.py', 'tests/test_prewarm_requires_known_receipts.py'],
        'native_setup_mapping': {'tests/test_produced_output_tier_host_egress.py::test_an_owner_without_the_fleet_tool_retires_through_the_action_anywhere': 'old import failure on local tier -> explicit announced remote tier; original assertion ASTs retained'},
    }
    evidence['total_functions'] = len(evidence['functions'])
    evidence['total_old_assertions'] = sum(r['ast_count'] for r in evidence['assertions'])
    evidence['total_old_assert_lines_including_embedded'] = sum(r['source_assert_lines'] for r in evidence['assertions'])
    evidence['removed_api'] = ['bare module imports: ' + ', '.join(MODULES),
                               'three produced-output import forks and five movement dependency-unavailable paths', 'library tools/fleet/stage_rounding.py']
    return evidence


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    evidence = run(args.root)
    args.out.write_text(json.dumps(evidence, indent=2) + '\n')
    print('c29-source-proof: ' + json.dumps(evidence))


if __name__ == '__main__':
    main()
