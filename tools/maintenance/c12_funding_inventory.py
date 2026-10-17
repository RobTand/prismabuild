"""Export source-only ownership, effect, lock and implicit caller occurrences."""
import argparse
import ast
import hashlib
import json
from pathlib import Path
import subprocess


def c12_inventory_git(root, *args):
    return subprocess.check_output(['git', '-C', str(root), *args])


def c12_inventory_definitions(raw):
    result = []

    def c12_inventory_walk(body, prefix=''):
        for node in body:
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                name = prefix + node.name
                if not isinstance(node, ast.ClassDef):
                    result.append((name, node))
                c12_inventory_walk(node.body, name + '.')

    c12_inventory_walk(ast.parse(raw).body)
    return result


def c12_inventory_effects(raw, node):
    calls = sorted((part for part in ast.walk(node) if isinstance(part, ast.Call)),
                   key=lambda part: (part.lineno, part.col_offset))
    locks = [ast.unparse(part.context_expr) for part in ast.walk(node)
             if isinstance(part, ast.withitem)]
    catches = [ast.unparse(part.type) if part.type else 'bare'
               for part in ast.walk(node) if isinstance(part, ast.ExceptHandler)]
    source = ast.get_source_segment(raw.decode(), node)
    return dict(line=node.lineno, end_line=node.end_lineno,
                signature=ast.unparse(node.args),
                decorators=[ast.unparse(value) for value in node.decorator_list],
                sha256=hashlib.sha256(source.encode()).hexdigest(),
                ordered_calls=[dict(line=part.lineno, target=ast.unparse(part.func))
                               for part in calls],
                locks=locks, catches=catches)


def c12_inventory_main():
    parser = argparse.ArgumentParser()
    parser.add_argument('root', type=Path)
    parser.add_argument('out', type=Path)
    parser.add_argument('--base', default='dac2006656264b4bd55e4e3837331205a9740fd3')
    parser.add_argument('--head', default='HEAD')
    args = parser.parse_args()
    config = json.loads((Path(__file__).parent / 'c12_funding_migration.json').read_bytes())
    head = c12_inventory_git(args.root, 'rev-parse', args.head).decode().strip()
    owner_path = 'src/prismabuild/funding.py'
    owner_raw = c12_inventory_git(args.root, 'show', head + ':' + owner_path)
    owner_defs = dict(c12_inventory_definitions(owner_raw))
    maps = [('pool', 'src/prismabuild/pool.py', config['methods']),
            ('prelaunch', 'src/prismabuild/prelaunch_group.py', config['prelaunch']),
            ('tier_loop', 'tools/fleet/tier_loop.py', config['tier'])]
    moves = []
    for group, path, mapping in maps:
        raw = c12_inventory_git(args.root, 'show', args.base + ':' + path)
        definitions = c12_inventory_definitions(raw)
        for former, current in mapping.items():
            original = [(name, node) for name, node in definitions
                        if name == ('PoolQueue.' + former if group == 'pool' else former)]
            for occurrence, (name, node) in enumerate(original, 1):
                actual = owner_defs['FundingLedger.' + current]
                moves.append(dict(group=group, occurrence=occurrence,
                    old=dict(path=path, name=name, **c12_inventory_effects(raw, node)),
                    current=dict(path=owner_path, name='FundingLedger.' + current,
                                 **c12_inventory_effects(owner_raw, actual))))
    names = set().union(*(set(mapping) | set(mapping.values()) for _, _, mapping in maps))
    occurrences, constants = [], []
    for path in c12_inventory_git(args.root, 'ls-tree', '-r', '--name-only', head).decode().splitlines():
        if not path.endswith('.py'):
            continue
        raw = c12_inventory_git(args.root, 'show', head + ':' + path)
        if not any(name.encode() in raw for name in names):
            continue
        tree = ast.parse(raw)
        parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
        for node in ast.walk(tree):
            value = (node.attr if isinstance(node, ast.Attribute) else
                     node.id if isinstance(node, ast.Name) else
                     node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None)
            hits = ([name for name in names if name in value]
                    if isinstance(node, ast.Constant) and value else [value] if value in names else [])
            if not hits:
                continue
            scope, parent, annotation = [], node, False
            while parent in parents:
                parent = parents[parent]
                if isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    scope.append(parent.name)
                annotation |= isinstance(parent, (ast.arg, ast.AnnAssign))
            occurrences.append(dict(path=path, line=node.lineno, kind=type(node).__name__,
                scope='.'.join(reversed(scope)), annotation=annotation,
                symbols=sorted(hits), source=ast.unparse(node)))
        if path in ('src/prismabuild/pool.py', owner_path):
            for node in tree.body:
                if isinstance(node, (ast.Assign, ast.AnnAssign)):
                    text = ast.unparse(node)
                    if 'FUNDING' in text or '_BINDING' in text:
                        constants.append(dict(path=path, line=node.lineno, source=text))
    args.out.mkdir(parents=True, exist_ok=True)
    result = dict(base=args.base, head=head, moves=moves, constants=constants,
                  occurrences=occurrences,
                  limit='Source occurrence/effect inventory, not behavioral or lock-equivalence proof. '
                        'Historical strings and retained driver maps remain reviewable occurrences.')
    (args.out / 'owner-caller-effects.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(dict(head=head, moved_occurrences=len(moves),
                          caller_occurrences=len(occurrences), constants=len(constants))))


if __name__ == '__main__':
    c12_inventory_main()
