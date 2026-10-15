#!/usr/bin/env python3
"""Inspect the closed directory-capture migration with Git, text and AST only."""
from __future__ import annotations

import argparse
import ast
import collections
import csv
import hashlib
import json
from pathlib import Path
import re
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path.cwd())
    parser.add_argument('--base', required=True)
    parser.add_argument('--tests-file', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    root = args.repo.resolve()
    args.out.mkdir(parents=True, exist_ok=True)

    def git(*words):
        return subprocess.check_output(['git', '-C', str(root), *words])

    def table(name, headings, rows):
        with (args.out / name).open('w', newline='') as stream:
            writer = csv.writer(stream, delimiter='\t', lineterminator='\n')
            writer.writerow(headings)
            writer.writerows(rows)

    names = sorted(set(git('ls-files', '-c', '--others', '--exclude-standard').decode().splitlines()))
    blobs = {name: (root / name).read_bytes() for name in names if (root / name).is_file()}
    sources = {name: data.decode() for name, data in blobs.items() if name.endswith('.py')}
    trees = {name: ast.parse(source, filename=name) for name, source in sources.items()}
    failures = []
    legacy = re.compile(r'\b(?:trusted_directory_stamp|current_directory_version|'
                        r'_directory_version_at|version_fence|keepable_version)\b')
    for name, source in sources.items():
        if name.startswith(('src/', 'tools/fleet/', 'tests/')):
            for number, line in enumerate(source.splitlines(), 1):
                if legacy.search(line):
                    failures.append(f'{name}:{number}: old trust spelling')
    callers = []
    for name, tree in trees.items():
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            called = ast.unparse(node.func)
            if any(word in called for word in (
                    'acquire', 'commit', 'file_fence', 'file_version',
                    'directory_before', 'directory_current', 'directory_evidence',
                    'directory_observations', 'ancestor_evidence', 'scrape_view')):
                callers.append((name, node.lineno, called))
            if (name.startswith(('src/', 'tools/fleet/'))
                    and name != 'src/prismabuild/kept_reads.py'
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr in {'directory_before', 'directory_current'}):
                failures.append(f'{name}:{node.lineno}: caller owns raw directory proof')
    table('capture-callers.tsv', ['path', 'line', 'operation'], sorted(callers))

    def cases(tree):
        return {node.name: node for node in ast.walk(tree)
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name.startswith('test_')}

    base_tests = set(git('ls-tree', '-r', '--name-only', args.base, '--', 'tests').decode().splitlines())
    assertions = []
    for name in git('diff', '--name-only', args.base, '--', 'tests').decode().splitlines():
        if not name.endswith('.py'):
            continue
        prior = (ast.parse(git('show', f'{args.base}:{name}').decode(), filename=name)
                 if name in base_tests else ast.parse(''))
        before = cases(prior)
        after = cases(trees[name]) if name in trees else {}
        for case in sorted(before.keys() | after.keys()):
            old, new = before.get(case), after.get(case)
            count = lambda node: sum(isinstance(item, ast.Assert) for item in ast.walk(node)) if node else 0
            assertions.append((name, case, count(old), count(new),
                               'preserved' if old and new else 'added' if new else 'removed'))
            if old and not new:
                failures.append(f'{name}: old test template removed: {case}')
            if old and new and count(new) < count(old):
                failures.append(f'{name}:{case}: assertion count decreased')
    table('assertion-inventory.tsv', ['path', 'test_template', 'old_assertions', 'new_assertions', 'status'], assertions)

    index = collections.defaultdict(set)
    for name in sources:
        module = name.removesuffix('.py').replace('/', '.')
        for alias in {module, module.removeprefix('src.'), module.removeprefix('tests.'),
                      module.removeprefix('tools.fleet.')}:
            index[alias].add(name)
            if alias.endswith('.__init__'):
                index[alias.removesuffix('.__init__')].add(name)

    def resolve(module):
        while module:
            if module in index:
                return index[module]
            module = module.rpartition('.')[0]
        return set()

    suffixes = collections.defaultdict(set)
    for name in blobs:
        parts = name.split('/')
        for offset in range(len(parts)):
            suffixes['/'.join(parts[offset:])].add(name)

    edges = collections.defaultdict(set)
    reasons = []
    assets = collections.defaultdict(set)
    loaders = []
    for name, tree in trees.items():
        for node in ast.walk(tree):
            targets = set()
            kind = literal = ''
            if isinstance(node, ast.Import):
                kind = 'import'
                for alias in node.names:
                    targets.update(resolve(alias.name))
            elif isinstance(node, ast.ImportFrom):
                kind = 'from-import'
                module = node.module or ''
                if node.level:
                    package = name.removesuffix('.py').split('/')[:-1]
                    package = package[:len(package) - node.level + 1]
                    module = '.'.join(package + ([module] if module else []))
                targets.update(resolve(module))
                for alias in node.names:
                    targets.update(resolve('.'.join(filter(None, (module, alias.name)))))
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                literal = node.value
                if literal.endswith('.py'):
                    kind = 'literal-python-file'
                    targets.update(suffixes[literal] & sources.keys())
                elif literal and '\n' not in literal:
                    assets[name].update(suffixes[literal] - sources.keys())
            elif isinstance(node, ast.Call):
                called = ast.unparse(node.func)
                if called.endswith(('import_module', '__import__')) and node.args:
                    argument = node.args[0]
                    if isinstance(argument, ast.Constant) and isinstance(argument.value, str):
                        kind, literal = 'literal-module-loader', argument.value
                        targets.update(resolve(literal))
                    else:
                        loaders.append((name, node.lineno, called, ast.unparse(argument)))
                elif called.endswith(('spec_from_file_location', 'SourceFileLoader')):
                    literals = {item.value for item in ast.walk(node)
                                if isinstance(item, ast.Constant) and isinstance(item.value, str)
                                and item.value.endswith('.py')}
                    targets.update(path for literal in literals for path in suffixes[literal] if path in sources)
                    loaders.append((name, node.lineno, called, ','.join(sorted(literals))))
            for target in targets:
                edges[name].add(target)
                reasons.append((name, target, kind, getattr(node, 'lineno', 0), literal))
    seeds = set(args.tests_file.read_text().splitlines())
    seeds.update(name for name in git('diff', '--name-only', args.base, '--', 'tests').decode().splitlines()
                 if name in trees and cases(trees[name]))
    configs = {'conftest.py', 'tests/conftest.py', 'pyproject.toml'} & blobs.keys()
    code = seeds | {name for name in configs if name.endswith('.py')}
    pending = list(code)
    while pending:
        name = pending.pop()
        for target in edges[name] - code:
            code.add(target)
            pending.append(target)
    tests = {name for name in code if name.startswith('tests/')}
    test_files = sorted(name for name in tests if Path(name).name.startswith('test_') and cases(trees[name]))
    extra_assets = set(configs) | {asset for name in code for asset in assets[name]}
    extra_assets -= sources.keys()
    paths = sorted(code | extra_assets)
    table('case-templates.tsv', ['path', 'qualified_template', 'line', 'decorators'],
          [(name, case, node.lineno, ' | '.join(ast.unparse(item) for item in node.decorator_list))
           for name in test_files for case, node in cases(trees[name]).items()])
    table('all-paths.tsv', ['path', 'role', 'bytes', 'sha256'], [
        (name, 'test' if name in test_files else 'test-helper' if name in tests else
         'production-support' if name in code else 'asset-config', len(blobs[name]),
         hashlib.sha256(blobs[name]).hexdigest()) for name in paths])
    table('test-import-literal-closure.tsv', ['from', 'to', 'kind', 'line', 'literal'],
          sorted(row for row in reasons if row[0] in tests or row[0] in configs))
    table('loader-sites.tsv', ['path', 'line', 'called', 'expression'],
          sorted(row for row in loaders if row[0] in code))
    table('asset-reference-roster.tsv', ['from', 'asset'],
          sorted((name, asset) for name in code for asset in assets[name]))
    (args.out / 'files.txt').write_text('\n'.join(test_files) + '\n')
    # The repaired Linux resolver must remain byte-identical to the frozen base.
    name = 'src/prismabuild/kept_reads.py'
    old = git('show', f'{args.base}:{name}').decode()
    resolver = lambda text: text[text.index('def _mount_table_changed'):text.index('\n\ndef trusted_directory_stamp') if '\n\ndef trusted_directory_stamp' in text else text.index('\n\nclass FilesystemTrust')]
    if resolver(old) != resolver(sources[name]):
        failures.append('Linux resolver bytes changed')
    def body(source, function, owner=None):
        tree = ast.parse(source)
        parent = (next(node for node in tree.body
                       if isinstance(node, ast.ClassDef) and node.name == owner)
                  if owner else tree)
        node = next(node for node in parent.body
                    if isinstance(node, ast.FunctionDef) and node.name == function)
        nodes = node.body
        if (isinstance(nodes[0], ast.Expr) and isinstance(nodes[0].value, ast.Constant)
                and isinstance(nodes[0].value.value, str)):
            nodes = nodes[1:]
        return ast.dump(ast.Module(body=nodes, type_ignores=[]), include_attributes=False)

    for before, after in [('trusted_directory_stamp', 'directory_before'),
                          ('version_fence', 'file_fence'),
                          ('keepable_version', 'file_version')]:
        if body(old, before) != body(sources[name], after, 'LinuxFilesystemTrust'):
            failures.append(f'qualification body changed: {before}')
    old_stage = git('show', f'{args.base}:tools/fleet/stage_release.py').decode()
    if body(old_stage, '_directory_version') != body(sources[name], '_directory_metadata'):
        failures.append('bare directory observation body changed')
    summary = {'base': args.base, 'head': git('rev-parse', 'HEAD').decode().strip(),
               'python_files_parsed': len(trees), 'test_files': len(test_files),
               'old_templates_preserved': sum(row[-1] == 'preserved' for row in assertions),
               'new_templates': sum(row[-1] == 'added' for row in assertions),
               'closure_files': len(paths), 'failures': failures,
               'execution': 'Git, text, AST only. No project/test imports or runtime checks.'}
    (args.out / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary, indent=2))
    return int(bool(failures))


if __name__ == '__main__':
    raise SystemExit(main())
