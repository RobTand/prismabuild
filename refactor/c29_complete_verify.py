#!/usr/bin/env python3
"""Audit the exact remaining C29 move, preserving every definition occurrence."""
from __future__ import annotations

import argparse
import ast
from collections import Counter
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile

from c29_complete import BASE, ENTRY, MOVES, OWNERS, PROTECTED, current_source, owner_source


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def definitions(tree):
    rows = []
    seen = Counter()

    def visit(node, prefix=''):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                name = prefix + child.name
                seen[name] += 1
                rows.append((name, seen[name], child))
                visit(child, name + '.')
            else:
                visit(child, prefix)
    visit(tree)
    return rows


def form(node):
    return ast.dump(node, include_attributes=False)


def audit(root):
    def git(*args):
        return subprocess.check_output(['git', '-C', str(root), *args])
    with tarfile.open(fileobj=io.BytesIO(git('archive', BASE))) as archive:
        sources = {item.name: archive.extractfile(item).read()
                   for item in archive if item.isfile()}
    paths = sorted(sources)
    result = {'base': BASE, 'moves': MOVES, 'definitions': [], 'files': [],
              'native_definitions': [], 'assertions': [], 'imports': [],
              'c31_current_bindings': [], 'protected': [], 'unresolved': []}
    for path in paths:
        if path.startswith(PROTECTED) or path == 'docs/refactor_requirements_93424_2026-10-15.tsv':
            old, new = sources[path], (root / path).read_bytes()
            assert old == new, ('protected bytes changed', path)
            result['protected'].append({'path': path, 'sha256': digest(old)})
        if not path.endswith('.py') or path.startswith(('refactor/', 'tools/maintenance/')):
            continue
        source = sources[path].decode()
        new_path = MOVES.get(path, path)
        if path in MOVES:
            expected = owner_source(source, path)
            if path.endswith('/tier_loop.py'):
                assert (root / path).read_text() == ENTRY
            else:
                assert not (root / path).exists(), path
        elif path.startswith(PROTECTED):
            expected = source
        else:
            expected = current_source(source, path)
        actual = (root / new_path).read_text()
        assert expected == actual, ('replay source mismatch', path, new_path)
        before, normalized, after = ast.parse(source), ast.parse(expected), ast.parse(actual)
        old_defs, normalized_defs, new_defs = definitions(before), definitions(normalized), definitions(after)
        assert [(n, i) for n, i, _ in old_defs] == [(n, i) for n, i, _ in new_defs], path
        assert [form(n) for _, _, n in normalized_defs] == [form(n) for _, _, n in new_defs], path
        old_asserts = [n for n in ast.walk(before) if isinstance(n, ast.Assert)]
        new_asserts = [n for n in ast.walk(after) if isinstance(n, ast.Assert)]
        assert len(old_asserts) == len(new_asserts), ('assertion lost', path)
        if source != actual or path in MOVES:
            result['files'].append({'old': path, 'new': new_path, 'old_sha256': digest(source.encode()),
                                    'current_sha256': digest(actual.encode()), 'normalized_source_exact': True})
            for (name, occurrence, old), (_, _, new) in zip(old_defs, new_defs):
                result['definitions'].append({'old': path + '::' + name, 'new': new_path + '::' + name,
                    'occurrence': occurrence, 'decorators': [form(n) for n in new.decorator_list],
                    'original_ast_sha256': digest(form(old).encode()), 'normalized_ast_sha256': digest(form(new).encode())})
            result['assertions'].append({'old': path, 'new': new_path, 'count': len(old_asserts),
                                         'normalized_full_bodies_equal': True})
        if path.startswith('tests/'):
            result['native_definitions'].extend({'path': path, 'symbol': name, 'occurrence': occurrence}
                for name, occurrence, _ in old_defs if name.split('.')[-1].startswith('test_'))
        for node in ast.walk(after):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                text = ast.unparse(node)
                if any(name in text for name in OWNERS.values()):
                    result['imports'].append({'path': new_path, 'line': node.lineno, 'binding': text})
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if path.startswith('tests/compat/') and any(name in node.value for name in OWNERS):
                    result['c31_current_bindings'].append({'path': path, 'line': node.lineno,
                        'literal': node.value, 'current_only': {old: 'prismabuild.' + new for old, new in OWNERS.items()
                                                            if old in node.value}})
    baseline = sources['tests/fixtures/duplication_baseline.json'].decode()
    for old, new in MOVES.items():
        baseline = baseline.replace(old, new)
    assert (root / 'tests/fixtures/duplication_baseline.json').read_text() == baseline
    result['architecture_allowlist'] = 'exact existing owner substitutions only'
    result['entrypoint'] = {'old': 'tools/fleet/tier_loop.py', 'current': 'tools/fleet/tier_loop.py',
                            'owner': 'prismabuild.tier_cycle', 'body': 'main',
                            'binding': 'main(entrypoint=__file__)', 'lock_stem': 'tier_loop'}
    result['shipping'] = {'removed_fleet_rows': [name + '.py' for name in OWNERS if name != 'tier_loop'],
        'package_owners': list(MOVES.values()), 'standalone_source_map': {
            'tools/runtime_paths.py': 'src/prismabuild/runtime_paths.py',
            'tools/fleet/runtime_paths.py': 'src/prismabuild/runtime_paths.py'},
        'c15_pair': ['tools/generation_receipt.py', 'tools/digest_primitives.py'],
        'installed_members_changed': False, 'protocol_changed': False}
    result['totals'] = {name: len(result[name]) for name in (
        'definitions', 'files', 'native_definitions', 'assertions', 'imports', 'protected')}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.root)
    args.out.write_text(json.dumps(result, indent=2) + '\n')
    print('c29-source-proof: ' + json.dumps(result['totals']))


if __name__ == '__main__':
    main()
