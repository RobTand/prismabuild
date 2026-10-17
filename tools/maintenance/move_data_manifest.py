"""Move the cold manifest parser and its callers without executing the project."""
from __future__ import annotations

import argparse
import ast
from collections import Counter
import json
from pathlib import Path
import re

FUNCTIONS = {'_manifest_path', 'validate_data_manifest', 'read_data_manifest',
             'load_data_manifest'}
OWNER = 'manifest_codec'
IMPORT = 'from prismabuild import data_manifest as manifest_codec'
SCOPES = (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)


def constants(tree):
    return {target.id for node in tree.body if isinstance(node, ast.Assign)
            for target in node.targets if isinstance(target, ast.Name)
            and (target.id.startswith(('DATA_MANIFEST_', '_DATA_MANIFEST_'))
                 or target.id == 'PBCAMPAIGN_DATA_MANIFEST_INPUT_ID')}


def positions(text):
    lines = text.splitlines(keepends=True)
    starts = [0]
    for line in lines:
        starts.append(starts[-1] + len(line))
    return lines, starts


def edit(text, changes):
    end = len(text)
    for first, last, replacement in sorted(changes, reverse=True):
        assert 0 <= first <= last <= end, (first, last, end)
        text = text[:first] + replacement + text[last:]
        end = first
    ast.parse(text)
    return text


def bounds(node, lines, starts):
    def offset(line, column):
        return starts[line - 1] + len(lines[line - 1].encode()[:column].decode())
    return offset(node.lineno, node.col_offset), offset(node.end_lineno, node.end_col_offset)


def owner_import(node):
    if isinstance(node, ast.ImportFrom):
        if node.module == 'prismabuild' or node.module is None and node.level == 1:
            return {alias.asname or alias.name: 'core' for alias in node.names
                    if alias.name == 'core'}
    if isinstance(node, ast.Import):
        return {alias.asname or alias.name: ('core' if alias.name == 'prismabuild.core'
                                          else 'pbrun')
                for alias in node.names if alias.name in ('prismabuild.core', 'pbrun')}
    return {}


def migrate(path, members, source=None):
    text = path.read_text() if source is None else source
    tree = ast.parse(text)
    parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
    bindings = {}
    assignments = {}

    def scope(node):
        while node in parents:
            node = parents[node]
            if isinstance(node, SCOPES):
                return node
        return tree

    for node in ast.walk(tree):
        current = scope(node)
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for name, kind in owner_import(node).items():
                bindings.setdefault((current, name), []).append((kind, node))
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            assignments.setdefault(current, set()).add(node.id)
        elif isinstance(node, ast.arg):
            assignments.setdefault(current, set()).add(node.arg)

    def resolve(value):
        if isinstance(value, ast.Name):
            name, kind = value.id, 'core'
        elif isinstance(value, ast.Attribute) and value.attr == 'pb' and isinstance(value.value, ast.Name):
            name, kind = value.value.id, 'pbrun'
        else:
            return []
        current = scope(value)
        while True:
            found = bindings.get((current, name))
            if found:
                assert name not in assignments.get(current, set()), (path, name, value.lineno)
                assert all(actual == kind for actual, _ in found), (path, name)
                return [node for _, node in found]
            if name in assignments.get(current, set()) or current is tree:
                return []
            current = scope(current)

    lines, starts = positions(text)
    changes = []
    imports = set()
    references = []
    for node in ast.walk(tree):
        target = None
        if isinstance(node, ast.Attribute) and node.attr in members:
            target = node.value
        elif (isinstance(node, ast.Call) and len(node.args) >= 2
              and isinstance(node.args[1], ast.Constant)
              and node.args[1].value in members
              and (isinstance(node.func, ast.Attribute) and node.func.attr in ('setattr', 'object')
                   or isinstance(node.func, ast.Name) and node.func.id in ('getattr', 'setattr'))):
            target = node.args[0]
        if target is None:
            continue
        origins = resolve(target)
        if not origins:
            continue
        first, last = bounds(target, lines, starts)
        changes.append((first, last, OWNER))
        imports.update(origins)
        references.append(dict(line=node.lineno, before=text[first:last], member=(
            node.attr if isinstance(node, ast.Attribute) else node.args[1].value)))
    if not changes:
        return text, []
    existing = {scope(node) for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
                and (node.module == 'prismabuild' or node.module is None and node.level == 1)
                and any(alias.name == 'data_manifest' and alias.asname == OWNER for alias in node.names)}
    assert not any(isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store) and node.id == OWNER
                   or isinstance(node, ast.arg) and node.arg == OWNER for node in ast.walk(tree)), path
    if not existing:
        assert not any(isinstance(node, ast.Name) and node.id == OWNER for node in ast.walk(tree)), path
    for node in imports:
        if scope(node) in existing:
            continue
        line_end = starts[node.end_lineno]
        indent = lines[node.lineno - 1][:node.col_offset]
        directive = ('from . import data_manifest as manifest_codec'
                     if path.parent.name == 'prismabuild' and path.parent.parent.name == 'src'
                     else IMPORT)
        changes.append((line_end, line_end, indent + directive + '\n'))
    updated = edit(text, changes)
    return updated, references


def normalize_assertions(text, members):
    class Owner(ast.NodeTransformer):
        def visit_Attribute(self, node):
            if node.attr in members:
                return ast.Attribute(value=ast.Name(id='_manifest_owner', ctx=ast.Load()),
                                     attr=node.attr, ctx=node.ctx)
            return self.generic_visit(node)
    return Counter(ast.dump(Owner().visit(node), include_attributes=False)
                   for node in ast.walk(ast.parse(text)) if isinstance(node, ast.Assert))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    core = args.root / 'src/prismabuild/core.py'
    original = core.read_text()
    tree = ast.parse(original)
    names = constants(tree)
    moved = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in FUNCTIONS]
    destination = core.with_name('data_manifest.py')
    already_migrated = not moved
    if already_migrated:
        assert destination.is_file() and not names
        original = destination.read_text()
        tree = ast.parse(original)
        names = constants(tree)
        moved = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in FUNCTIONS]
    members = FUNCTIONS | names
    assert {node.name for node in moved} == FUNCTIONS and len(names) == 13
    chunks = []
    removals = []
    lines, starts = positions(original)
    for node in tree.body:
        selected = isinstance(node, ast.FunctionDef) and node.name in FUNCTIONS
        if isinstance(node, ast.Assign):
            selected |= any(isinstance(target, ast.Name) and target.id in names for target in node.targets)
        if not selected:
            continue
        first = node.lineno - 1
        while first and lines[first - 1].startswith('#:'):
            first -= 1
        chunks.append(''.join(lines[first:node.end_lineno]))
        removals.append((starts[first], starts[node.end_lineno], ''))
    for node in tree.body:
        if (isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == '__all__'
                                                for target in node.targets)):
            for value in node.value.elts:
                if isinstance(value, ast.Constant) and value.value in members:
                    removals.append((starts[value.lineno - 1], starts[value.end_lineno], ''))
    module = ('"""Bounded data-manifest parsing for submitters and storage roles."""\n'
              'from __future__ import annotations\n\n'
              'from collections.abc import Mapping\nimport json\nimport os\n'
              'from pathlib import Path\nimport posixpath\nimport stat\nimport zlib\n\n'
              'from .core import (\n    ActionContractError, _exact_mapping, _fail,\n'
              '    _nonnegative_integer, _sha256, _text,\n)\n\n\n'
              + '\n\n'.join(chunks) + '\n\n\n__all__ = [\n'
              + ''.join('    ' + repr(name) + ',\n' for name in sorted(members) if not name.startswith('_'))
              + ']\n')
    ast.parse(module)
    updates = {} if already_migrated else {core: edit(original, removals), destination: module}
    uses = []
    old_assertions = 0
    embedded = []
    documentation = []
    qualified = re.compile(r'\b(?:pb|_core|core|core_mod|pb_core|pbrun\.pb)\.(?:'
                           + '|'.join(re.escape(name) for name in sorted(members)) + r')\b')
    for path in sorted(args.root.rglob('*.py')):
        if '.git' in path.parts or path == core or path == destination or path == Path(__file__):
            continue
        before = path.read_text()
        if not any(name in before for name in members):
            continue
        parsed = ast.parse(before)
        docstrings = {node.body[0].value for node in ast.walk(parsed)
                      if isinstance(node, SCOPES[:-1]) and node.body
                      and isinstance(node.body[0], ast.Expr)
                      and isinstance(node.body[0].value, ast.Constant)
                      and isinstance(node.body[0].value.value, str)}
        doc_edits = []
        doc_lines, doc_starts = positions(before)
        for node in ast.walk(parsed):
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and qualified.search(node.value):
                observation = dict(path=str(path.relative_to(args.root)), line=node.lineno)
                if node not in docstrings:
                    embedded.append(dict(observation, text=node.value[:180]))
                    continue
                first, last = bounds(node, doc_lines, doc_starts)
                replacement = qualified.sub(lambda match: 'data_manifest.' + match.group().rsplit('.', 1)[1],
                                            before[first:last])
                doc_edits.append((first, last, replacement))
                documentation.append(observation)
        after_docs = edit(before, doc_edits)
        after, references = migrate(path, members, source=after_docs)
        if references or after != before:
            assert normalize_assertions(before, members) == normalize_assertions(after, members), path
            old_assertions += sum(normalize_assertions(before, members).values())
            uses.append(dict(path=str(path.relative_to(args.root)), references=references))
            updates[path] = after
    assert not embedded, embedded
    report = dict(functions=sorted(FUNCTIONS), constants=sorted(names),
                  changed_paths=[str(path.relative_to(args.root)) for path in updates],
                  callers=uses, references=sum(len(row['references']) for row in uses),
                  original_assertions_preserved=old_assertions,
                  embedded_qualified_manifest_references=embedded,
                  documentation_owner_updates=documentation,
                  already_migrated=already_migrated,
                  applied=args.apply)
    args.report.write_text(json.dumps(report, indent=2) + '\n')
    if args.apply:
        for path, text in updates.items():
            path.write_text(text)
    print(json.dumps({key: report[key] for key in ('references', 'original_assertions_preserved', 'applied')}))
    print('paths', len(updates))


if __name__ == '__main__':
    main()
