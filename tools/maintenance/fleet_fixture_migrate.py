"""Replay the reviewed C30 clock, row and layout recipes from the frozen base."""
from __future__ import annotations

import argparse
import ast
from collections import Counter
from copy import deepcopy
import json
from pathlib import Path
import re

from fleet_fixture_inventory import BASE, fixture_sources, fixture_checks, fixture_blob

READ_ONLY = ("tests/compat/", "tests/test_current_tier_compat_composition.py")
DEFAULTS = {"cas_root": "/cas", "checkout_root": "/co", "worker_script": "/w.py"}
# Exact-base occurrences reviewed as positive filed-record setup. Fault,
# historical-generation, wire-byte and partial-write controls are separate.
FILED_RECORDS = {
    "tests/test_a_busy_transition_lock_never_stops_the_tier_loop.py": {110},
    "tests/test_a_failed_consumer_stops_staging.py": {110, 120},
    "tests/test_a_retry_is_not_superseded_by_its_predecessors_cleanup.py": {65},
    "tests/test_pbmetrics_starvation.py": {79, 81, 98, 101},
    "tests/test_pbstatus_starvation.py": {103, 105},
    "tests/test_pbrun_bounded_attachment.py": {100},
    "tests/test_pbrun_bounded_pool_wait.py": {60, 123, 179, 286},
    "tests/test_pbrun_client_events.py": {84},
    "tests/test_pbrun_watches_failed.py": {37},
    "tests/test_pbwait_bounded_observation.py": {113, 141, 253, 322},
    "tests/test_typed_pool_client.py": {24, 47},
    "tests/test_withdrawal_retires_the_plan_it_minted.py": {118, 125},
    "tests/test_withdrawn_and_receiptless_owners_are_retired.py": {141},
}


def fixture_pretty(node, indent=0):
    single = ast.unparse(node)
    if len(single) + indent <= 100:
        return single
    pad, inner = " " * indent, " " * (indent + 4)
    if isinstance(node, ast.Call):
        parts = [fixture_pretty(value, indent + 4) for value in node.args]
        parts.extend((keyword.arg + "=" if keyword.arg else "**")
                     + fixture_pretty(keyword.value, indent + 4)
                     for keyword in node.keywords)
        return ast.unparse(node.func) + "(\n" + "".join(inner + part + ",\n" for part in parts) + pad + ")"
    if isinstance(node, ast.Dict):
        parts = [(ast.unparse(key) + ": " if key else "**") + fixture_pretty(value, indent + 4)
                 for key, value in zip(node.keys, node.values)]
        return "{\n" + "".join(inner + part + ",\n" for part in parts) + pad + "}"
    return single


class FixtureText:
    def __init__(self, source):
        self.raw = source.encode()
        self.offsets = [0]
        for line in self.raw.splitlines(keepends=True):
            self.offsets.append(self.offsets[-1] + len(line))
        self.edits = []

    def span(self, node):
        return (self.offsets[node.lineno - 1] + node.col_offset,
                self.offsets[node.end_lineno - 1] + node.end_col_offset)

    def replace(self, node, replacement):
        start, end = self.span(node)
        self.edits.append((start, end, replacement.encode()))

    def expression(self, node, replacement):
        self.replace(node, fixture_pretty(ast.parse(replacement, mode="eval").body, node.col_offset))

    def delete_statement(self, node):
        start = self.offsets[node.lineno - 1]
        end = self.offsets[node.end_lineno]
        if isinstance(node, ast.FunctionDef) and node.col_offset == 0:
            for index in range(node.end_lineno, min(node.end_lineno + 2, len(self.offsets) - 1)):
                if self.raw[self.offsets[index]:self.offsets[index + 1]].strip():
                    break
                end = self.offsets[index + 1]
        self.edits.append((start, end, b""))

    def finish(self):
        raw = self.raw
        previous = len(raw)
        for start, end, replacement in sorted(self.edits, reverse=True):
            if end > previous:
                raise ValueError("overlapping fixture recipes")
            raw = raw[:start] + replacement + raw[end:]
            previous = start
        return raw.decode()


class FixtureSubstitute(ast.NodeTransformer):
    def __init__(self, bindings):
        self.bindings = bindings

    def visit_Name(self, node):
        return deepcopy(self.bindings.get(node.id, node))


def fixture_bind(function, call):
    parameters = [*function.args.posonlyargs, *function.args.args]
    if function.args.vararg or any(isinstance(v, ast.Starred) for v in call.args):
        raise ValueError("starred positional input")
    if len(call.args) > len(parameters):
        raise ValueError("too many positional inputs")
    bindings = {p.arg: v for p, v in zip(parameters, call.args)}
    names = {p.arg for p in [*parameters, *function.args.kwonlyargs]}
    rest = []
    for keyword in call.keywords:
        if keyword.arg in names:
            if keyword.arg in bindings:
                raise ValueError("duplicate argument")
            bindings[keyword.arg] = keyword.value
        else:
            rest.append(keyword)
    defaults = dict(zip([p.arg for p in parameters[-len(function.args.defaults):]],
                        function.args.defaults)) if function.args.defaults else {}
    defaults.update((p.arg, v) for p, v in zip(function.args.kwonlyargs,
                                            function.args.kw_defaults) if v is not None)
    for name in names - bindings.keys():
        if name not in defaults:
            raise ValueError("missing argument " + name)
        bindings[name] = defaults[name]
    if rest and function.args.kwarg is None:
        raise ValueError("unknown keyword")
    return bindings, rest


def fixture_publish(function, invocation):
    bindings, extras = fixture_bind(function, invocation)
    statement = function.body[0]
    call = deepcopy(statement.value)
    if not isinstance(call, ast.Call) or not isinstance(call.func, ast.Attribute):
        raise ValueError("not a publish call")
    call = FixtureSubstitute(bindings).visit(call)
    supplied = {k.arg: k.value for k in extras if k.arg is not None}
    dynamic = [k for k in extras if k.arg is None]
    kwarg = function.args.kwarg.arg if function.args.kwarg else None

    class Pops(ast.NodeTransformer):
        def visit_Call(self, node):
            if (isinstance(node.func, ast.Attribute) and node.func.attr == "pop"
                    and isinstance(node.func.value, ast.Name) and node.func.value.id == kwarg):
                if dynamic or len(node.args) != 2 or not isinstance(node.args[0], ast.Constant):
                    raise ValueError("dynamic keyword pop")
                return deepcopy(supplied.pop(node.args[0].value, node.args[1]))
            return self.generic_visit(node)

    call = Pops().visit(call)
    keywords = []
    key = None
    for keyword in call.keywords:
        if keyword.arg == "action_key":
            key = keyword.value
        elif (keyword.arg is None and isinstance(keyword.value, ast.Name)
              and keyword.value.id == kwarg):
            keywords.extend(ast.keyword(arg=name, value=value) for name, value in supplied.items())
            keywords.extend(dynamic)
        elif keyword.arg in DEFAULTS and isinstance(keyword.value, ast.Constant) and keyword.value.value == DEFAULTS[keyword.arg]:
            continue
        else:
            keywords.append(keyword)
    if key is None:
        raise ValueError("no action identity")
    call.func = ast.Attribute(ast.Call(ast.Name("ActionRows", ast.Load()),
                                      [call.func.value], []), "publish", ast.Load())
    call.args = [key]
    call.keywords = keywords
    return ast.unparse(call)


def fixture_layout(call):
    admission = []
    if isinstance(call.func, ast.Name) and call.func.id == "AdmittedQueueFixture":
        admission = call.keywords
        call = call.args[0]
    if (not isinstance(call, ast.Call) or ast.unparse(call.func) != "pool.PoolQueue"
            or len(call.args) != 1 or call.keywords):
        raise ValueError("not a plain explicit queue")
    root = call.args[0]
    if isinstance(root, ast.BinOp) and isinstance(root.op, ast.Div):
        base, name = ast.unparse(root.left), ast.unparse(root.right)
    elif isinstance(root, ast.Name):
        base, name = root.id + ".parent", root.id + ".name"
    else:
        raise ValueError("queue root needs an explicit layout recipe")
    options = [base]
    if name not in {"'pb-queue'", '"pb-queue"'}:
        options.append("queue_name=" + name)
    if admission:
        options.append("admission=Admission(" + ", ".join(
            k.arg + "=" + ast.unparse(k.value) for k in admission) + ")")
    return "FleetWorld(" + ", ".join(options) + ").queue"


def fixture_migrate(path, source, sources):
    if path.startswith(READ_ONLY):
        return source, []
    tree = ast.parse(source)
    text = FixtureText(source)
    rows, imports = [], set()
    removed = set()
    definitions = [n for n in tree.body if isinstance(n, ast.FunctionDef)]
    parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
    # Only the original monkeypatch protocol is replaced. Scoped mock objects
    # retain their context and mock identity; embedded programs retain bytes.
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "setattr" and len(node.args) == 3
                and isinstance(node.args[1], ast.Constant) and node.args[1].value == "_now"
                and ast.unparse(node.args[0]) in {"pool", "pool_module"}):
            replacement = ("FleetClock(source=" + ast.unparse(node.args[2]) + ").install("
                           + ast.unparse(node.func.value) + ', scope="pool")')
            text.replace(node, replacement)
            imports.add("Clock as FleetClock")
            rows.append(dict(kind="clock", line=node.lineno, old=ast.unparse(node), new=replacement,
                             reader=ast.dump(node.args[2]), target=ast.unparse(node.args[0])))
    for function in definitions:
        if function.name not in {"_publish", "_queue"} or function.decorator_list:
            continue
        # A test importing a helper keeps ownership of that distinct recipe.
        external = [p for p, s in sources.items() if p != path
                    and Path(path).stem in s and re_helper_reference(s, Path(path).stem, function.name)]
        if external:
            continue
        body = function.body
        if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
            body = body[1:]
        mode, expression = None, None
        if len(body) == 1 and isinstance(body[0], (ast.Expr, ast.Return)):
            call = body[0].value
            if (function.name == "_publish" and isinstance(call, ast.Call)
                    and isinstance(call.func, ast.Attribute) and call.func.attr == "publish"):
                mode = "publish"
            elif function.name == "_queue" and isinstance(body[0], ast.Return):
                expression = ast.unparse(call)
                mode = "constructor"
        if (function.name == "_queue" and len(body) == 3 and isinstance(body[0], ast.Assign)
                and len(body[0].targets) == 1 and isinstance(body[0].targets[0], ast.Name)):
            local = body[0].targets[0].id
            if (ast.unparse(body[1]) == local + ".records.ensure_layout()"
                    and isinstance(body[2], ast.Return) and ast.unparse(body[2].value) == local):
                try:
                    expression = fixture_layout(body[0].value)
                    mode = "layout"
                except ValueError:
                    pass
        if mode is None:
            # Inline the other repeated publisher shape as statements: the
            # caller-owned ordering clock, publication, and returned key/row.
            if (function.name != "_publish" or len(body) not in {2, 3}
                    or not isinstance(body[-1], ast.Return)
                    or not isinstance(body[-2], ast.Expr)
                    or not isinstance(body[-2].value, ast.Call)
                    or not isinstance(body[-2].value.func, ast.Attribute)
                    or body[-2].value.func.attr != "publish"
                    or len(body) == 3 and not isinstance(body[0], ast.AugAssign)):
                continue
            mode = "ordered-publish"
        references = [n for n in ast.walk(tree) if isinstance(n, ast.Name)
                      and n.id == function.name]
        calls = [parents[n] for n in references if isinstance(parents.get(n), ast.Call)
                 and parents[n].func is n]
        if len(calls) != len(references):
            continue
        replacements = []
        try:
            for call in calls:
                if mode in {"publish", "ordered-publish"}:
                    # Do not change an assertion's vocabulary to claim that its
                    # exact original AST has survived.
                    ancestor = parents.get(call)
                    while ancestor is not None:
                        if isinstance(ancestor, ast.Assert):
                            raise ValueError("asserted helper call")
                        ancestor = parents.get(ancestor)
                    if mode == "publish":
                        replacement = fixture_publish(function, call)
                    else:
                        parent = parents[call]
                        if (not isinstance(parent, (ast.Expr, ast.Assign, ast.AnnAssign))
                                or parent.value is not call
                                or any(isinstance(n, (ast.Call, ast.NamedExpr))
                                       for value in [*call.args, *(k.value for k in call.keywords)]
                                       for n in ast.walk(value))):
                            raise ValueError("ordered recipe needs stable statement arguments")
                        bindings, extras = fixture_bind(function, call)
                        if any(k.arg is None for k in extras):
                            raise ValueError("ordered recipe has dynamic kwargs")
                        publishing = deepcopy(function)
                        publishing.body = [body[-2]]
                        statements = []
                        if len(body) == 3:
                            statements.append(ast.unparse(FixtureSubstitute(bindings).visit(deepcopy(body[0]))))
                        statements.append(fixture_publish(publishing, call))
                        if isinstance(parent, ast.Expr):
                            statements.append(ast.unparse(FixtureSubstitute(bindings).visit(deepcopy(body[-1].value))))
                        else:
                            assignment = deepcopy(parent)
                            assignment.value = FixtureSubstitute(bindings).visit(deepcopy(body[-1].value))
                            statements.append(ast.unparse(assignment))
                        replacement = "\n".join(statements)
                        call = parent
                else:
                    bindings, rest = fixture_bind(function, call)
                    if rest:
                        raise ValueError("constructor extras")
                    replacement = ast.unparse(FixtureSubstitute(bindings).visit(ast.parse(expression, mode="eval").body))
                replacements.append((call, replacement))
        except ValueError:
            continue
        text.delete_statement(function)
        removed.add(function)
        for call, replacement in replacements:
            if mode == "ordered-publish":
                statements = ast.parse(replacement).body
                formatted = [fixture_pretty(n.value, call.col_offset) if isinstance(n, ast.Expr)
                             else ast.unparse(n) for n in statements]
                text.replace(call, ("\n" + " " * call.col_offset).join(formatted))
            else:
                text.expression(call, replacement)
        imports.update({"ActionRows"} if mode in {"publish", "ordered-publish"} else
                       ({"FleetWorld", "Admission"} if "Admission(" in expression else
                        {"FleetWorld"} if "FleetWorld(" in expression else set()))
        rows.append(dict(kind="helper", line=function.lineno, name=function.name, mode=mode,
                         old=ast.unparse(function), calls=[dict(line=n.lineno, old=ast.unparse(n), new=r)
                                                         for n, r in replacements]))
    for function in definitions:
        if function in removed or function.name not in {"_queue", "_publish"}:
            continue
        if function.name == "_queue":
            for index, statement in enumerate(function.body[:-1]):
                if (not isinstance(statement, ast.Assign) or len(statement.targets) != 1
                        or not isinstance(statement.targets[0], ast.Name)):
                    continue
                following = function.body[index + 1]
                local = statement.targets[0].id
                if ast.unparse(following) != local + ".records.ensure_layout()":
                    continue
                try:
                    expression = fixture_layout(statement.value)
                except ValueError:
                    continue
                text.replace(statement.value, expression)
                text.delete_statement(following)
                imports.add("FleetWorld")
                if "Admission(" in expression:
                    imports.add("Admission")
                rows.append(dict(kind="composed-queue", line=statement.lineno,
                                 old=ast.unparse(statement.value), new=expression,
                                 retained_recipe=ast.unparse(function)))
        else:
            for node in ast.walk(function):
                if (not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute)
                        or node.func.attr != "publish" or ast.unparse(node.func.value) not in {"q", "queue", "fleet.queue"}):
                    continue
                call = deepcopy(node)
                key = next((k.value for k in call.keywords if k.arg == "action_key"), None)
                if key is None:
                    continue
                call.keywords = [k for k in call.keywords if k.arg != "action_key"]
                call.args = [key]
                call.func = ast.Attribute(ast.Call(ast.Name("ActionRows", ast.Load()),
                                                  [call.func.value], []), "publish", ast.Load())
                replacement = ast.unparse(call)
                text.expression(node, replacement)
                imports.add("ActionRows")
                rows.append(dict(kind="composed-publish", line=node.lineno,
                                 old=ast.unparse(node), new=replacement,
                                 retained_recipe=ast.unparse(function)))
    selected = FILED_RECORDS.get(path, set())
    found = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or node.lineno not in selected:
            continue
        if not isinstance(node.func, ast.Attribute) or node.func.attr != "write_text":
            continue
        address = node.func.value
        if (not isinstance(address, ast.Call) or not isinstance(address.func, ast.Attribute)
                or not isinstance(address.func.value, ast.Attribute)
                or address.func.value.attr != "records" or len(node.args) != 1):
            raise ValueError("filed record lost its Store address: " + path)
        encoder = node.args[0]
        if (not isinstance(encoder, ast.Call) or ast.unparse(encoder.func) != "json.dumps"
                or len(encoder.args) != 1 or encoder.keywords):
            raise ValueError("filed record gained a wire-format contract: " + path)
        replacement = (ast.unparse(address.func.value) + ".replace_record_at("
                       + ast.unparse(address) + ", " + ast.unparse(encoder.args[0]) + ")")
        text.expression(node, replacement)
        rows.append(dict(kind="filed-record", line=node.lineno, old=ast.unparse(node), new=replacement,
                         payload=ast.dump(encoder.args[0]), address=ast.dump(address)))
        found.add(node.lineno)
    if found != selected:
        raise ValueError("unresolved filed record: " + path + " " + str(selected - found))
    result = text.finish()
    if imports:
        current = ast.parse(result)
        imports -= {alias.name + (" as " + alias.asname if alias.asname else "")
                    for node in current.body if isinstance(node, ast.ImportFrom)
                    and node.module == "fleet_world" for alias in node.names}
        # Insert beside imports only after the docstring and __future__ rows.
        at = 0
        for node in current.body:
            if ((isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant)
                 and isinstance(node.value.value, str))
                    or isinstance(node, ast.ImportFrom) and node.module == "__future__"):
                at = node.end_lineno
            else:
                break
        lines = result.splitlines(keepends=True)
        if imports:
            lines.insert(at, "from fleet_world import " + ", ".join(sorted(imports)) + "\n")
        result = "".join(lines)
    return result, rows


def re_helper_reference(source, module, name):
    tree = ast.parse(source)
    aliases = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and (node.module or "").split(".")[-1] == module:
            if any(alias.name == name for alias in node.names):
                return True
        if isinstance(node, ast.Import):
            aliases.update(alias.asname or alias.name for alias in node.names
                           if alias.name.split(".")[-1] == module)
    return any(isinstance(node, ast.Attribute) and node.attr == name
               and isinstance(node.value, ast.Name) and node.value.id in aliases
               for node in ast.walk(tree))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    sources = fixture_sources(args.root, BASE)
    rows = []
    for path, original in sorted(sources.items()):
        result, recipes = fixture_migrate(path, original, sources)
        if not recipes:
            continue
        current = (args.root / path).read_text()
        old_checks, new_checks = fixture_checks(original), fixture_checks(result)
        checks = lambda values: Counter((tuple(re.sub(r"embedded@\d+", "embedded", s) for s in v["scope"]),
                                         v["kind"], v["form"]) for v in values)
        if checks(old_checks) != checks(new_checks):
            raise ValueError("migration changes original checks: " + path)
        if args.apply:
            if current not in (original, result):
                raise ValueError("unowned source change: " + path)
            (args.root / path).write_text(result)
        rows.append(dict(path=path, original_git_blob=fixture_blob(original),
                         current_git_blob=fixture_blob(result),
                         complete_current_source_matches=current == result, recipes=recipes))
    args.output.write_text(json.dumps(dict(base=BASE, files=rows, applied=args.apply), indent=2) + "\n")
    print(json.dumps(dict(files=len(rows), recipes=dict(Counter(r["kind"] for row in rows for r in row["recipes"])),
                          source_only=True)))


if __name__ == "__main__":
    main()
