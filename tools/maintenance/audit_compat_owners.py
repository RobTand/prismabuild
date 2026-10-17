"""Resolve current catalogue owners from source without importing the project."""
import argparse
import ast
from functools import lru_cache
import json
from pathlib import Path


class Definitions(ast.NodeVisitor):
    def __init__(self):
        self.prefix = ''
        self.names = set()

    def visit_ClassDef(self, node):
        previous = self.prefix
        name = previous + node.name
        self.names.add(name)
        self.prefix = name + '.'
        for child in node.body:
            self.visit(child)
        self.prefix = previous

    def visit_FunctionDef(self, node):
        previous = self.prefix
        name = previous + node.name
        self.names.add(name)
        self.prefix = name + '.<locals>.'
        for child in node.body:
            self.visit(child)
        self.prefix = previous

    visit_AsyncFunctionDef = visit_FunctionDef


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()

    @lru_cache(maxsize=None)
    def definitions(module):
        relative = module.replace('.', '/')
        paths = [args.root / base / (relative + '.py')
                 for base in ('src', 'tools/fleet', 'tools')]
        paths += [args.root / base / relative / '__init__.py'
                  for base in ('src', 'tools/fleet', 'tools')]
        if module == 'docker':
            paths.insert(0, args.root / 'tools/fleet/docker')
        path = next((path for path in paths if path.is_file()), None)
        if path is None:
            return None, set()
        visitor = Definitions()
        visitor.visit(ast.parse(path.read_text()))
        return str(path.relative_to(args.root)), visitor.names

    tree = ast.parse((args.root / 'src/prismabuild/record_kinds.py').read_text())
    required, historical = [], []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == 'RecordKind'):
            continue
        kind = ast.literal_eval(node.args[0])
        required.append((kind, 'reader', ast.literal_eval(node.args[4])))
        required.extend((kind, 'writer', ref) for ref in ast.literal_eval(node.args[5]))
        for keyword in node.keywords:
            if keyword.arg in ('variant_readers', 'variant_writers'):
                required.extend((kind, keyword.arg, ref)
                                for _, ref in ast.literal_eval(keyword.value))
            elif keyword.arg in ('legacy_readers', 'legacy_writers'):
                historical.extend((kind, keyword.arg, ref)
                                  for ref in ast.literal_eval(keyword.value))
    rows = []
    for kind, role, reference in required + historical:
        module, name = reference.split(':')
        path, names = definitions(module)
        rows.append(dict(kind=kind, role=role, reference=reference, path=path,
                         definition_present=name in names, historical=role.startswith('legacy_')))
    missing = [row for row in rows if not row['historical'] and not row['definition_present']]
    inline = []
    for adapter in sorted((args.root / 'tests/compat/support').glob('*.py')):
        for node in ast.walk(ast.parse(adapter.read_text())):
            if not (isinstance(node, ast.Attribute) and isinstance(node.value, ast.Call)
                    and isinstance(node.value.func, ast.Name) and node.value.func.id == 'load'
                    and len(node.value.args) == 1 and isinstance(node.value.args[0], ast.Constant)
                    and isinstance(node.value.args[0].value, str) and not node.attr.startswith('__')):
                continue
            module = node.value.args[0].value
            path, names = definitions(module)
            bindings = set(names)
            if path is not None:
                for statement in ast.parse((args.root / path).read_text()).body:
                    if isinstance(statement, (ast.Import, ast.ImportFrom)):
                        bindings.update(alias.asname or alias.name.split('.')[0]
                                        for alias in statement.names)
                    elif isinstance(statement, (ast.Assign, ast.AnnAssign)):
                        targets = statement.targets if isinstance(statement, ast.Assign) else [statement.target]
                        bindings.update(target.id for target in targets if isinstance(target, ast.Name))
            inline.append(dict(adapter=str(adapter.relative_to(args.root)), line=node.lineno,
                               module=module, attribute=node.attr, present=node.attr in bindings))
    missing_inline = [row for row in inline if not row['present']]
    report = dict(current_refs=len(required), historical_refs=len(historical),
                  missing_current=missing, references=rows, inline_attributes=inline,
                  missing_inline_attributes=missing_inline,
                  scope='Source ownership only; retained execution, source attestation and byte compatibility require PB evidence')
    args.report.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(dict(current_refs=len(required), historical_refs=len(historical),
                          missing_current=missing, missing_inline_attributes=missing_inline)))
    raise SystemExit(bool(missing or missing_inline))


if __name__ == '__main__':
    main()
