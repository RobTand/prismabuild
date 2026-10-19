"""Source-only return and result-context proof for the C30 fixture migration."""
from __future__ import annotations

import argparse
import ast
from collections import Counter, defaultdict
from copy import deepcopy
import json
from pathlib import Path

from fleet_fixture_inventory import BASE, fixture_blob, fixture_inventory, fixture_nodes, fixture_sources
from fleet_fixture_migrate import FixtureSubstitute, fixture_bind


def fixture_form(node):
    return ast.dump(node, include_attributes=False)


def fixture_index(source):
    tree = ast.parse(source)
    nodes = list(fixture_nodes(tree))
    return nodes, {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}


def fixture_context(node, parents, scopes):
    current, chain = node, []
    while current in parents:
        parent = parents[current]
        for field, value in ast.iter_fields(parent):
            if value is current:
                chain.append([type(parent).__name__, field, None])
            elif isinstance(value, list) and any(v is current for v in value):
                chain.append([type(parent).__name__, field, next(i for i, v in enumerate(value) if v is current)])
        if isinstance(parent, ast.stmt):
            break
        current = parent
    statement = parents.get(current, current)

    class Mask(ast.NodeTransformer):
        def visit(self, value):
            if (type(value) is type(node) and getattr(value, 'lineno', None) == node.lineno
                    and getattr(value, 'col_offset', None) == node.col_offset
                    and fixture_form(value) == fixture_form(node)):
                return ast.Name(id='C30_RESULT', ctx=ast.Load())
            return super().visit(value)

    masked = Mask().visit(deepcopy(statement))
    return dict(observed=not (isinstance(parents.get(node), ast.Expr) and parents[node].value is node),
                chain=chain, statement_line=statement.lineno, statement=ast.unparse(statement),
                masked=fixture_form(masked), scope=list(scopes[id(node)]))


def fixture_adapter_result(sources):
    tree = ast.parse(sources['tests/fleet_world.py'])
    owner = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'ActionRows')
    method = next(n for n in owner.body if isinstance(n, ast.FunctionDef) and n.name == 'publish')
    returns = [n for n in ast.walk(method) if isinstance(n, ast.Return)]
    if not returns:
        return 'None'
    assert len(returns) == 1 and returns[0] is method.body[0]
    assert isinstance(returns[0].value, ast.Call) and ast.unparse(returns[0].value.func) == 'self.queue.publish'
    assert ast.unparse(method.returns) == 'Path'
    return 'PoolQueue.publish result'


def fixture_adapter_consumers(sources, issues):
    result = []
    for path, source in sorted(sources.items()):
        nodes, parents = fixture_index(source)
        row_names, world_names = set(), set()
        for node, _ in nodes:
            if isinstance(node, ast.ImportFrom) and node.module in {'fleet_world', 'tests.fleet_world'}:
                row_names.update(a.asname or a.name for a in node.names if a.name == 'ActionRows')
                world_names.update(a.asname or a.name for a in node.names if a.name == 'FleetWorld')
        if path == 'tests/fleet_world.py':
            row_names.add('ActionRows')
            world_names.add('FleetWorld')
        if not row_names and not world_names:
            continue
        scopes = {id(n): s for n, s in nodes}
        worlds = {target.id for node, _ in nodes if isinstance(node, ast.Assign)
                  and isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name)
                  and node.value.func.id in world_names for target in node.targets if isinstance(target, ast.Name)}
        accounted = set()
        for node, scope in nodes:
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute) or node.func.attr != 'publish':
                continue
            receiver = node.func.value
            direct = (isinstance(receiver, ast.Call) and isinstance(receiver.func, ast.Name)
                      and receiver.func.id in row_names)
            member = (isinstance(receiver, ast.Attribute) and receiver.attr == 'actions'
                      and isinstance(receiver.value, ast.Name) and receiver.value.id in worlds)
            if direct or member:
                if direct:
                    accounted.add(id(receiver))
                else:
                    accounted.add(id(receiver))
                result.append(dict(path=path, line=node.lineno, scope=scope, source=ast.unparse(node),
                                   context=fixture_context(node, parents, scopes)))
        for node, scope in nodes:
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load) and node.id in row_names:
                parent = parents.get(node)
                if not isinstance(parent, ast.Call) or parent.func is not node:
                    issues.append(dict(path=path, line=node.lineno, issue='ActionRows class escapes a direct constructor', source=ast.unparse(parent)))
                elif id(parent) not in accounted:
                    container = parents.get(parent)
                    stored = (path == 'tests/fleet_world.py' and isinstance(container, ast.Assign)
                              and [ast.unparse(t) for t in container.targets] == ['self.actions'])
                    if not stored:
                        issues.append(dict(path=path, line=node.lineno, issue='ActionRows instance escapes the enumerated receivers', source=ast.unparse(container)))
            if (isinstance(node, ast.Attribute) and node.attr == 'actions'
                    and isinstance(node.value, ast.Name) and node.value.id in worlds and id(node) not in accounted):
                issues.append(dict(path=path, line=node.lineno, issue='FleetWorld.actions escapes the enumerated receivers', source=ast.unparse(parents.get(node, node))))
    return result


def fixture_return_values(node):
    return [fixture_form(n.value) if n.value else 'None'
            for n, scope in fixture_nodes(node) if isinstance(n, ast.Return) and len(scope) == 1]


def fixture_operations(node):
    kinds = (ast.Call, ast.Attribute, ast.Subscript, ast.Compare, ast.If, ast.IfExp,
             ast.Assert, ast.Raise, ast.Return, ast.BinOp, ast.BoolOp, ast.AugAssign)
    return [dict(line=getattr(n, 'lineno', None), kind=type(n).__name__, source=ast.unparse(n))
            for n in ast.walk(node) if isinstance(n, kinds)]


def fixture_recipe_bodies(original, current, migration, indexes, mapped, issues):
    """Account for complete bodies, including ignored return evaluation."""
    recipes = {f['path']: f['recipes'] for f in migration['files']}
    result = []
    for occurrence in fixture_inventory(original):
        if occurrence['kind'] != 'helper':
            continue
        path, scope = occurrence['path'], tuple(occurrence['scope'])
        old = next(n for n, s in fixture_nodes(ast.parse(original[path]))
                   if s == scope and isinstance(n, ast.FunctionDef))
        local = recipes.get(path, [])
        removed = next((r for r in local if r['kind'] == 'helper' and r['line'] == old.lineno), None)
        row = dict(id=occurrence['id'], path=path, original_line=old.lineno,
                   full_original_body=ast.unparse(old), all_original_operations=fixture_operations(old),
                   returns=fixture_return_values(old))
        if removed:
            body = old.body[1:] if isinstance(old.body[0], ast.Expr) and isinstance(old.body[0].value, ast.Constant) else old.body
            mode = removed['mode']
            if mode == 'publish':
                assert len(body) == 1 and isinstance(body[0], (ast.Expr, ast.Return))
                assert isinstance(body[0].value, ast.Call) and body[0].value.func.attr == 'publish'
                witness = 'The entire body is this one publication expression, including its argument reads/calls. Exact bindings are independently checked by the383 argument proof; result-use contexts and real Path forwarding are checked here.'
            elif mode == 'ordered-publish':
                assert len(body) in {2, 3} and isinstance(body[-1], ast.Return)
                assert isinstance(body[-2], ast.Expr) and isinstance(body[-2].value, ast.Call)
                if len(body) == 3:
                    assert isinstance(body[0], ast.AugAssign)
                witness = 'Every prefix operation, publication, and final return expression is evaluated in order, including when discarded. Bound return expressions and their exception opportunities are matched at every caller; no returned expression is treated as pure.'
            elif mode == 'constructor':
                assert len(body) == 1 and isinstance(body[0], ast.Return)
                witness = 'The entire returned constructor expression is substituted at the original call position. There is no ensure_layout step; exact constructor expression and caller result context remain in the source mapping.'
            else:
                assert mode == 'layout' and len(body) == 3
                assert isinstance(body[0], ast.Assign) and isinstance(body[2], ast.Return)
                name = ast.unparse(body[2].value)
                assert ast.unparse(body[1]) == name + '.records.ensure_layout()'
                witness = 'The exact constructor/admission then ensure_layout then queue return move into the unchanged FleetWorld constructor at this call position. Root path and queue_name are explicit in every site. Path parent/name projections are lexical construction on the cited tmp_path/shared_root Path inputs; no filesystem read or return expression is discarded.'
            row.update(disposition='removed', mode=mode, body_witness=witness,
                       sites=[r for r in mapped if r['path'] == path and r['helper_line'] == old.lineno],
                       evaluation_witnesses=[])
            for site in removed['calls']:
                original_call = next(n for n in ast.walk(ast.parse(site['old']))
                                     if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == old.name)
                inputs = [*original_call.args, *(k.value for k in original_call.keywords)]
                bound, extras = fixture_bind(old, original_call)
                expanded = ast.Module(body=[FixtureSubstitute(bound).visit(deepcopy(statement))
                                           for statement in body], type_ignores=[])
                row['evaluation_witnesses'].append(dict(
                    original_line=site['line'], original_call=site['old'],
                    original_argument_operations=[op for value in inputs for op in fixture_operations(value)],
                    bound_original_body=ast.unparse(expanded),
                    bound_original_operations=fixture_operations(expanded),
                    kwargs_binding=[dict(name=k.arg, value=ast.unparse(k.value)) for k in extras],
                    replacement_statements=site['new'],
                    replacement_operations=fixture_operations(ast.parse(site['new'])),
                    local_binding_witness='Helper parameters and the fresh Python **kwargs dict are local bindings. Literal-key pop with an explicit default operates on that local dict; every selected value and original argument expression is pinned here. Production read/call/guard/return evaluation is not excluded on the basis of a discarded result.'))
                input_calls = [n for value in inputs for n in ast.walk(value) if isinstance(n, ast.Call)]
                replacement_calls = Counter(fixture_form(n) for n in ast.walk(ast.parse(site['new'])) if isinstance(n, ast.Call))
                for expression in input_calls:
                    if replacement_calls[fixture_form(expression)] != sum(fixture_form(n) == fixture_form(expression) for n in input_calls):
                        issues.append(dict(path=path, line=site['line'], issue='Caller argument call evaluation count changed', source=ast.unparse(expression)))
            if mode == 'constructor':
                for site in removed['calls']:
                    call = next(n for n in ast.walk(ast.parse(site['old'])) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == old.name)
                    bound, extras = fixture_bind(old, call)
                    assert not extras
                    expected = FixtureSubstitute(bound).visit(deepcopy(body[0].value))
                    assert fixture_form(expected) == fixture_form(ast.parse(site['new']).body[0].value)
        else:
            new = next(n for n, s in indexes[path][0] if s == scope and isinstance(n, ast.FunctionDef))
            reverse = {}
            layouts = []
            for recipe in local:
                if recipe['kind'] in {'composed-publish', 'composed-queue', 'clock', 'filed-record'}:
                    reverse[fixture_form(ast.parse(recipe['new']).body[0].value)] = ast.parse(recipe['old']).body[0].value
                    if recipe['kind'] == 'composed-queue':
                        for i, statement in enumerate(old.body[:-1]):
                            if isinstance(statement, ast.Assign) and fixture_form(statement.value) == fixture_form(ast.parse(recipe['old']).body[0].value):
                                layouts.append((fixture_form(statement), old.body[i + 1]))
            class Undo(ast.NodeTransformer):
                def visit(self, node):
                    replacement = reverse.get(fixture_form(node))
                    return deepcopy(replacement) if replacement is not None else super().visit(node)
            restored = Undo().visit(deepcopy(new))
            for form, layout in layouts:
                position = next(i for i, statement in enumerate(restored.body) if fixture_form(statement) == form)
                restored.body.insert(position + 1, deepcopy(layout))
            exact = fixture_form(old) == fixture_form(restored)
            if not exact:
                issues.append(dict(path=path, line=old.lineno, issue='Full retained recipe body has an unaccounted change', original=ast.unparse(old), current=ast.unparse(new), restored=ast.unparse(restored)))
            row.update(disposition='retained', current_line=new.lineno, full_current_body=ast.unparse(new),
                       all_current_operations=fixture_operations(new), full_body_after_adapter_inversion_matches=exact)
        result.append(row)
    assert len(result) == 122
    # These accepted shared constructors add only local object/field binding;
    # production queue, admission and layout evaluation remain source-pinned.
    old_world = next(n for n in ast.parse(original['tests/fleet_world.py']).body if isinstance(n, ast.ClassDef) and n.name == 'FleetWorld')
    new_world = next(n for n in ast.parse(current['tests/fleet_world.py']).body if isinstance(n, ast.ClassDef) and n.name == 'FleetWorld')
    assert fixture_form(old_world) == fixture_form(new_world)
    return result


def fixture_audit(root, migration, frozen, current_ref=None):
    original, current = fixture_sources(root, BASE), fixture_sources(root, current_ref)
    frozen_sources = fixture_sources(root, frozen)
    old_indexes = {p: fixture_index(s) for p, s in original.items()}
    new_indexes = {p: fixture_index(s) for p, s in current.items()}
    imports_by_path = {p: [(n, s) for n, s in nodes if isinstance(n, (ast.Import, ast.ImportFrom))]
                       for p, (nodes, _) in old_indexes.items()}
    adapter = fixture_adapter_result(current)
    issues, mapped, retained, callers = [], [], [], []
    removed = set()
    matched_current = set()
    qualification = {'tests/test_fleet_world.py', 'tests/test_duplication_baseline.py'}
    for file in migration['files']:
        path = file['path']
        old_nodes, old_parents = old_indexes[path]
        new_nodes, new_parents = new_indexes[path]
        old_scopes, new_scopes = ({id(n): s for n, s in ns} for ns in (old_nodes, new_nodes))
        for recipe in file['recipes']:
            if recipe['kind'] != 'helper':
                continue
            function = ast.parse(recipe['old']).body[0]
            removed.add((path, recipe['line']))
            returns = fixture_return_values(function)
            for site in recipe['calls']:
                old_call = next(n for n, _ in old_nodes if isinstance(n, ast.Call)
                                and isinstance(n.func, ast.Name) and n.func.id == function.name and n.lineno == site['line'])
                context = fixture_context(old_call, old_parents, old_scopes)
                replacement = ast.parse(site['new'])
                mode = recipe['mode']
                contract = 'PoolQueue.publish result' if returns and mode == 'publish' else 'None' if mode == 'publish' else mode
                if mode == 'ordered-publish':
                    bound, extras = fixture_bind(function, old_call)
                    assert not extras
                    expression = FixtureSubstitute(bound).visit(deepcopy(function.body[-1].value))
                    final = replacement.body[-1]
                    kept = fixture_form(final.value) == fixture_form(expression)
                    if not kept:
                        issues.append(dict(path=path, line=site['line'], issue='Original return expression evaluation was dropped', expected=ast.unparse(expression), new=site['new']))
                    target = final.value
                    qualification.add(path)
                else:
                    assert len(replacement.body) == 1
                    target = replacement.body[0].value
                    if mode == 'publish' and context['observed'] and contract != adapter:
                        issues.append(dict(path=path, line=site['line'], issue='Observed helper result changed', original=contract, current=adapter))
                    if mode == 'publish' and returns:
                        qualification.add(path)
                matches = [n for n, s in new_nodes if s == old_scopes[id(old_call)]
                           and fixture_form(n) == fixture_form(target) and id(n) not in matched_current]
                assert matches, (path, site['line'], site['new'])
                matching_context = [n for n in matches if fixture_context(n, new_parents, new_scopes)['masked'] == context['masked']]
                new_call = min(matching_context or matches, key=lambda n: (n.lineno, n.col_offset))
                matched_current.add(id(new_call))
                current_context = fixture_context(new_call, new_parents, new_scopes)
                if context['masked'] != current_context['masked'] or context['chain'] != current_context['chain']:
                    issues.append(dict(path=path, line=site['line'], issue='Call result-use context changed', original=context, current=current_context))
                mapped.append(dict(path=path, original_line=site['line'], current_line=new_call.lineno,
                                   helper_line=recipe['line'], helper=function.name, mode=mode,
                                   original_returns=returns, original_result=contract,
                                   current_adapter_result=adapter if mode == 'publish' else None,
                                   original_context=context, current_context=current_context))
    bodies = fixture_recipe_bodies(original, current, migration, new_indexes, mapped, issues)
    occurrences = [r for r in fixture_inventory(original) if r['kind'] == 'helper']
    for occurrence in occurrences:
        path, scope = occurrence['path'], tuple(occurrence['scope'])
        old_nodes, old_parents = old_indexes[path]
        function = next(n for n, s in old_nodes if s == scope and isinstance(n, ast.FunctionDef))
        if (path, occurrence['line']) not in removed:
            replacement = next(n for n, s in new_indexes[path][0] if s == scope and isinstance(n, ast.FunctionDef))
            old_returns, new_returns = fixture_return_values(function), fixture_return_values(replacement)
            if old_returns != new_returns:
                issues.append(dict(path=path, line=function.lineno, issue='Retained domain recipe return changed', original=old_returns, current=new_returns))
            retained.append(dict(id=occurrence['id'], path=path, original_line=function.lineno,
                                 current_line=replacement.lineno, returns=old_returns))
        for caller_path, (nodes, parents) in old_indexes.items():
            if caller_path != path:
                imports = [n for n, _ in imports_by_path[caller_path] if isinstance(n, ast.ImportFrom)
                           and n.module in {Path(path).stem, 'tests.' + Path(path).stem}]
                aliases = {a.asname or a.name for n in imports for a in n.names if a.name == function.name}
                modules = {a.asname or a.name for n, _ in imports_by_path[caller_path] if isinstance(n, ast.Import)
                           for a in n.names if a.name in {Path(path).stem, 'tests.' + Path(path).stem}}
            else:
                aliases, modules = {function.name}, set()
            if not aliases and not modules:
                continue
            scopes = {id(n): s for n, s in nodes}
            for node, caller_scope in nodes:
                if not isinstance(node, ast.Call):
                    continue
                bare = isinstance(node.func, ast.Name) and node.func.id in aliases
                qualified = (isinstance(node.func, ast.Attribute) and node.func.attr == function.name
                             and ast.unparse(node.func.value) in modules)
                if bare or qualified:
                    external = caller_path != path
                    if external and (path, occurrence['line']) in removed:
                        issues.append(dict(path=caller_path, line=node.lineno, issue='Removed helper has external caller', definition=occurrence['id']))
                    callers.append(dict(definition=occurrence['id'], path=caller_path, line=node.lineno,
                                        external=external, source=ast.unparse(node),
                                        context=fixture_context(node, parents, scopes)))
    base_consumers = fixture_adapter_consumers(original, issues)
    frozen_consumers = fixture_adapter_consumers(frozen_sources, issues)
    current_consumers = fixture_adapter_consumers(current, issues)
    for consumer in base_consumers:
        if consumer['context']['observed']:
            issues.append(dict(path=consumer['path'], line=consumer['line'], issue='Existing None-return consumer observes broadened adapter result', source=consumer['source']))
    for consumer in current_consumers:
        if consumer['context']['observed']:
            qualification.add(consumer['path'])
    key = lambda r: (r['path'], tuple(r['scope']), r['source'], r['context']['masked'])
    assert Counter(map(key, frozen_consumers)) == Counter(map(key, current_consumers))
    return dict(verified=not issues, base=BASE, frozen=frozen, source_only=True, product_imports=False,
                source_files=len(original), mapped_modules=len(migration['files']), helper_occurrences=len(occurrences),
                adapter_result=adapter, mapped_calls=mapped, retained_helper_returns=retained,
                all_original_helper_callers=callers, base_adapter_consumers=base_consumers,
                frozen_adapter_consumers=frozen_consumers, current_adapter_consumers=current_consumers,
                complete_recipe_bodies=bodies,
                source_pins={p: fixture_blob(s) for p, s in current.items()},
                qualification_files=sorted(qualification), issues=issues)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path.cwd())
    parser.add_argument('--migration', type=Path, required=True)
    parser.add_argument('--frozen', default='89106849f98741682b089ebc376ee1d8bde21975')
    parser.add_argument('--current-ref')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = fixture_audit(args.root, json.loads(args.migration.read_bytes()), args.frozen, args.current_ref)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(dict(verified=result['verified'], mapped_calls=len(result['mapped_calls']),
                         helper_callers=len(result['all_original_helper_callers']), issues=result['issues'],
                         qualification_files=result['qualification_files'])))
    raise SystemExit(0 if result['verified'] else 1)


if __name__ == '__main__':
    main()
