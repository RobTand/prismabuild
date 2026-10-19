"""Move the measurement fixture family to an explicit shared clock."""
import argparse
import ast
from collections import defaultdict
import copy
import json
from pathlib import Path
import subprocess


def clock_tree(node):
    return ast.dump(node, include_attributes=False)


def clock_migration():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--base', default='55270a27011e98d9a675f11b376e14c6391cec79')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()

    def source_at(path):
        return subprocess.check_output(
            ['git', '-C', str(args.root), 'show', args.base + ':' + path], text=True)

    paths = subprocess.check_output(
        ['git', '-C', str(args.root), 'ls-tree', '-r', '--name-only', args.base, 'tests'],
        text=True).splitlines()
    paths = [p for p in paths if Path(p).parent == Path('tests') and p.endswith('.py')]
    sources = {p: source_at(p) for p in paths}
    trees = {p: ast.parse(s) for p, s in sources.items()}
    modules = {Path(p).stem: p for p in paths}
    readers = defaultdict(set)
    for path, tree in trees.items():
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                module = (node.module or '').removeprefix('tests.').split('.')[0]
                if module in modules:
                    readers[modules[module]].add(path)
    primary = 'tests/test_measurement_drains_gpu_backfill.py'
    closure = {primary, 'tests/test_measurement_foreign_ambient_withholds.py',
               'tests/test_measurement_foreign_load_starvation.py'}
    while True:
        expanded = closure | {p for dependency in closure for p in readers[dependency]}
        if expanded == closure:
            break
        closure = expanded
    report = dict(source_only=True, base=args.base, files=[], whole_files=sorted(closure),
                  complete_module_AST_restored=True, original_assertions=0,
                  clock_accesses=0, constructors=0)
    for path in sorted(closure):
        source, original = sources[path], trees[path]
        lines = source.splitlines(keepends=True)
        offsets = [0]
        for line in lines:
            offsets.append(offsets[-1] + len(line))

        def span(node):
            return (offsets[node.lineno - 1] + node.col_offset,
                    offsets[node.end_lineno - 1] + node.end_col_offset)

        subs = [n for n in ast.walk(original) if isinstance(n, ast.Subscript)
                and isinstance(n.value, ast.Name) and n.value.id in ('clock', 'now')
                and isinstance(n.slice, ast.Constant) and n.slice.value == 0]
        constructors = [n for n in ast.walk(original) if isinstance(n, ast.Assign)
                        and len(n.targets) == 1 and isinstance(n.targets[0], ast.Name)
                        and n.targets[0].id in ('clock', 'now')
                        and isinstance(n.value, ast.List) and len(n.value.elts) == 1]
        if not subs and not constructors:
            continue
        edits, excluded = [], []
        if path == primary:
            fixture = next(n for n in original.body if isinstance(n, ast.FunctionDef)
                           and n.name == 'fleet')
            expected = ast.parse('''clock = [T0]
monkeypatch.setattr(time, "time", lambda: clock[0])
monkeypatch.setattr(pool.socket, "gethostname", lambda: "sparklina")
queue = pool.PoolQueue(tmp_path / "pb-queue")
queue.records.ensure_layout()
''').body
            assert [clock_tree(n) for n in fixture.body[:5]] == [clock_tree(n) for n in expected]
            replacement = '''clock = Clock(now=T0)
    clock.install(monkeypatch)
    FleetWorld.use_host(monkeypatch, "sparklina")
    world = FleetWorld(tmp_path, clock=clock)
    queue = world.queue'''
            start, _ = span(fixture.body[0])
            _, end = span(fixture.body[4])
            excluded.append((start, end))
            edits.append((start, end, replacement))
            imports = [n for n in original.body if isinstance(n, ast.ImportFrom)]
            anchor = imports[-1]
            _, end = span(anchor)
            edits.append((end, end, '\nfrom fleet_world import Clock, FleetWorld'))
        elif constructors:
            assert len(constructors) == 1 and constructors[0].targets[0].id == 'now', path
            import_node = next(n for n in original.body if isinstance(n, ast.ImportFrom)
                               and n.module == 'fleet_world')
            start, end = span(import_node)
            assert [n.name for n in import_node.names] == ['FleetWorld']
            edits.append((start, end, 'from fleet_world import Clock, FleetWorld'))
        for node in subs:
            start, end = span(node)
            if not any(a <= start < b for a, b in excluded):
                edits.append((start, end, node.value.id + '.now'))
        for node in constructors:
            start, end = span(node.value)
            if not any(a <= start < b for a, b in excluded):
                edits.append((start, end, 'Clock(now=' + ast.unparse(node.value.elts[0]) + ')'))
        edited = source
        for start, end, replacement in sorted(edits, reverse=True):
            edited = edited[:start] + replacement + edited[end:]
        current = ast.parse(edited)

        class RestoreClock(ast.NodeTransformer):
            def visit_Attribute(self, node):
                if (isinstance(node.value, ast.Name) and node.value.id in ('clock', 'now')
                        and node.attr == 'now'):
                    return ast.Subscript(copy.deepcopy(node.value), ast.Constant(0), node.ctx)
                return self.generic_visit(node)

            def visit_Call(self, node):
                if isinstance(node.func, ast.Name) and node.func.id == 'Clock':
                    assert not node.args and [k.arg for k in node.keywords] == ['now']
                    return ast.List([copy.deepcopy(node.keywords[0].value)], ast.Load())
                return self.generic_visit(node)

        restored = RestoreClock().visit(copy.deepcopy(current))
        if path == primary:
            new_fixture = next(n for n in restored.body if isinstance(n, ast.FunctionDef)
                               and n.name == 'fleet')
            expected_new = ast.parse('''clock = [T0]
clock.install(monkeypatch)
FleetWorld.use_host(monkeypatch, "sparklina")
world = FleetWorld(tmp_path, clock=clock)
queue = world.queue
''').body
            assert [clock_tree(n) for n in new_fixture.body[:5]] == [clock_tree(n) for n in expected_new]
            new_fixture.body[:5] = copy.deepcopy(fixture.body[:5])
            restored.body = [n for n in restored.body if not (
                isinstance(n, ast.ImportFrom) and n.module == 'fleet_world')]
        else:
            for node in restored.body:
                if isinstance(node, ast.ImportFrom) and node.module == 'fleet_world':
                    node.names = [n for n in node.names if n.name != 'Clock']
        assert clock_tree(restored) == clock_tree(original), path
        actual = (args.root / path).read_text()
        assert actual in (source, edited), (path, 'unexpected existing edits')
        if args.apply and actual != edited:
            (args.root / path).write_text(edited)
        count = sum(isinstance(n, ast.Assert) for n in ast.walk(original))
        report['files'].append(dict(path=path, clock_accesses=len(subs),
                                    constructors=len(constructors), assertions=count,
                                    state='already_applied' if actual == edited else 'planned'))
        report['original_assertions'] += count
        report['clock_accesses'] += len(subs)
        report['constructors'] += len(constructors)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({k: v for k, v in report.items() if k not in ('files', 'whole_files')}))


if __name__ == '__main__':
    clock_migration()
