"""List the source-derived CPU lending test closure without importing product code.

This coordinator helper does not execute tests. Pass its files to the published
pbtest client with the architecture, interpreter and resources of the lane.
"""
from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
import subprocess


def lending_validation_files(root, base):
    changed = subprocess.check_output(
        ['git', '-C', str(root), 'diff', '--name-only', base], text=True).splitlines()
    changed += subprocess.check_output(
        ['git', '-C', str(root), 'ls-files', '--others', '--exclude-standard'], text=True).splitlines()
    modules = {Path(name).stem for name in changed if name.endswith('.py')}
    selected, dependencies = set(), {}
    for path in sorted((root / 'tests').glob('*.py')):
        relative = str(path.relative_to(root))
        tree = ast.parse(path.read_text())
        imported, strings = set(), set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.rsplit('.', 1)[-1] for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported.add((node.module or '').rsplit('.', 1)[-1])
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                strings.add(node.value)
        dependencies[relative] = imported
        if (imported & modules or relative in changed
                or any(module + '.py' in strings or 'prismabuild.' + module in strings
                       for module in modules)):
            selected.add(relative)
    while True:
        names = {Path(name).stem for name in selected}
        expanded = selected | {name for name, imports in dependencies.items() if names & imports}
        if expanded == selected:
            break
        selected = expanded
    selected.update(str(path.relative_to(root)) for path in (root / 'tests').glob('test_*.py')
                    if any(fragment in path.stem for fragment in
                           ('adaptive_cpu', 'resource_broker', 'resource_scope', 'worker_loop', 'pbstatus')))
    selected.update('tests/' + name for name in (
        'test_duplication_baseline.py', 'test_design_doc_line_references.py',
        'test_wire_inventory.py', 'test_fleet_tool_flags_have_help.py',
        'test_client_sdk_surface.py', 'test_publish_runtime.py'))
    return sorted(name for name in selected if Path(name).name.startswith('test_'))


def lending_validation_main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument('--base', required=True)
    parser.add_argument('--json', type=Path, required=True)
    args = parser.parse_args()
    files = lending_validation_files(args.root, args.base)
    args.json.write_text(json.dumps(files, indent=2) + '\n')
    print(f'{len(files)} test files; no test execution')


if __name__ == '__main__':
    lending_validation_main()
