#!/usr/bin/env python3
"""Compare Python functions by AST, and identify definition-time changes."""
from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
import subprocess

try:
    from .tia_bytes import raw_sha256, compact_ascii_json_bytes, sorted_json
except ImportError:
    from tia_bytes import raw_sha256, compact_ascii_json_bytes, sorted_json


def function_definitions(source: str) -> tuple[dict[str, list[str]], str]:
    tree = ast.parse(source)
    functions: dict[str, list[str]] = {}

    def visit(node, prefix=''):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                name = prefix + child.name
                functions.setdefault(name, []).append(ast.dump(child, include_attributes=False))
                visit(child, name + '.<locals>.')
            elif isinstance(child, ast.ClassDef):
                visit(child, prefix + child.name + '.')
            else:
                visit(child, prefix)
    visit(tree)

    class ModuleCode(ast.NodeTransformer):
        def visit_FunctionDef(self, node):
            # Defaults, annotations and decorators can execute arbitrary calls at import.
            expressions = [*node.decorator_list, *node.args.defaults,
                           *(value for value in node.args.kw_defaults if value is not None)]
            expressions += [arg.annotation for arg in [*node.args.posonlyargs, *node.args.args,
                            *node.args.kwonlyargs, *([node.args.vararg] if node.args.vararg else []),
                            *([node.args.kwarg] if node.args.kwarg else [])] if arg.annotation]
            if node.returns:
                expressions.append(node.returns)
            expressions += list(getattr(node, 'type_params', []))
            return ast.Expr(value=ast.Tuple(elts=[ast.Constant(value=node.name), *expressions], ctx=ast.Load()))
        visit_AsyncFunctionDef = visit_FunctionDef
    skeleton = ModuleCode().visit(tree)
    return functions, ast.dump(skeleton, include_attributes=False)


def compare_functions(before: str, after: str) -> dict:
    try:
        old, old_module = function_definitions(before)
        new, new_module = function_definitions(after)
    except SyntaxError as error:
        return {'changed': [], 'added': [], 'deleted': [], 'renamed': [],
                'module_changed': True, 'parse_error': str(error)}
    changed = sorted(name for name in old.keys() & new.keys() if old[name] != new[name])
    added, deleted = sorted(new.keys() - old.keys()), sorted(old.keys() - new.keys())
    def normalized(entries):
        result = []
        for entry in entries:
            # AST dumps begin with the definition name. Rename only that first field.
            result.append(entry.split('name=', 1)[0] + entry.split(',', 1)[1])
        return result
    renamed = [{'before': a, 'after': b} for a in deleted for b in added
               if normalized(old[a]) == normalized(new[b])]
    return {'changed': changed, 'added': added, 'deleted': deleted, 'renamed': renamed,
            'module_changed': old_module != new_module}


def git_bytes(root: Path, *args: str) -> bytes:
    return subprocess.check_output(['git', *args], cwd=root)


def source(root: Path, revision: str, path: str) -> bytes:
    result = subprocess.run(['git', 'show', f'{revision}:{path}'], cwd=root, capture_output=True)
    if result.returncode:
        # A missing path is an addition/deletion; other git errors must not look empty.
        listing = git_bytes(root, 'ls-tree', '-z', revision, '--', path)
        if listing:
            raise ValueError(result.stderr.decode(errors='replace'))
        return b''
    return result.stdout


def analyze_changes(root: Path, base: str, head: str, *, merge_base: bool = True) -> dict:
    base = git_bytes(root, 'merge-base', base, head).decode().strip() if merge_base else base
    names = git_bytes(root, 'diff', '--name-only', '--no-renames', '-z', base, head, '--').decode().split('\0')
    python, other, hashes = {}, [], {}
    for path in sorted(set(names) - {''}):
        if path.endswith('.py'):
            before, after = source(root, base, path), source(root, head, path)
            python[path] = compare_functions(before.decode(), after.decode())
            hashes[path] = raw_sha256(before)
        else:
            other.append(path)
    return {'schema': 'prismabuild.tia_diff.v1', 'base': base, 'head': head,
            'python': python, 'non_python': other, 'base_hashes': hashes}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path.cwd())
    parser.add_argument('--base', required=True)
    parser.add_argument('--head', default='HEAD')
    args = parser.parse_args()
    print(sorted_json(analyze_changes(args.root, args.base, args.head)))


if __name__ == '__main__':
    main()
