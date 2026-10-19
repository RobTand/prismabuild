"""Move repeated current test setup to explicit world adapters, with AST replay."""
import argparse
import ast
import copy
from collections import Counter
import hashlib
import json
from pathlib import Path
import subprocess


def dump(node):
    return ast.dump(node, include_attributes=False)


def body(node):
    return dump(ast.Module(body=node.body, type_ignores=[]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--base", default="b26d6bc94abb3c977bbf04650ac27d0d6535277a")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    def git(*words):
        return subprocess.check_output(["git", "-C", str(args.root), *words])

    paths = git("ls-tree", "-r", "--name-only", args.base, "tests").decode().splitlines()
    paths = [p for p in paths if Path(p).parent == Path("tests") and p.endswith(".py")]
    originals = {p: git("show", args.base + ":" + p).decode() for p in paths}
    trees = {p: ast.parse(s) for p, s in originals.items()}
    exemplar = next(n for n in trees["tests/test_pool.py"].body
                    if isinstance(n, ast.FunctionDef) and n.name == "_publish")
    queue_plans, publishes = {}, {}
    for path, tree in trees.items():
        pools = {alias.asname or alias.name for node in tree.body
                 if isinstance(node, ast.ImportFrom) and node.module == "prismabuild"
                 for alias in node.names if alias.name == "pool"}
        for node in tree.body:
            if not isinstance(node, ast.FunctionDef):
                continue
            if body(node) == body(exemplar) and dump(node.args) == dump(exemplar.args):
                publishes[path] = node
            if len(node.body) != 3 or not isinstance(node.body[0], ast.Assign):
                continue
            assignment, ensure, returned = node.body
            if (len(assignment.targets) != 1
                    or not isinstance(assignment.targets[0], ast.Name)
                    or not isinstance(assignment.value, ast.Call)):
                continue
            local, call = assignment.targets[0].id, assignment.value
            if (dump(ensure) != dump(ast.parse(local + ".records.ensure_layout()").body[0])
                    or not isinstance(returned, ast.Return)
                    or not isinstance(returned.value, ast.Name)
                    or returned.value.id != local):
                continue
            admission = None
            if isinstance(call.func, ast.Name) and call.func.id == "AdmittedQueueFixture":
                if len(call.args) != 1 or {k.arg for k in call.keywords} != {"capacity", "default_demand"}:
                    continue
                admission = call.keywords
                call = call.args[0]
            if (not isinstance(call, ast.Call) or not isinstance(call.func, ast.Attribute)
                    or not isinstance(call.func.value, ast.Name) or call.func.value.id not in pools
                    or call.func.attr != "PoolQueue" or len(call.args) != 1 or call.keywords):
                continue
            root = call.args[0]
            if (not isinstance(root, ast.BinOp) or not isinstance(root.op, ast.Div)
                    or not isinstance(root.right, ast.Constant) or not isinstance(root.right.value, str)):
                continue
            options = []
            if root.right.value != "pb-queue":
                options.append("queue_name=" + repr(root.right.value))
            if admission is not None:
                options.append("admission=Admission(" + ", ".join(
                    k.arg + "=" + ast.unparse(k.value) for k in admission) + ")")
            expression = "FleetWorld(" + ast.unparse(root.left)
            if options:
                expression += ", " + ", ".join(options)
            expression += ").queue"
            queue_plans.setdefault(path, {})[node.name] = (node, expression)

    exports = {Path(p).stem: n.name for p, n in publishes.items()}
    call_targets = {p: {n.name} for p, n in publishes.items()}
    imported_helpers = {}
    for path, tree in trees.items():
        for node in tree.body:
            if not isinstance(node, ast.ImportFrom):
                continue
            module = (node.module or "").removeprefix("tests.")
            target = exports.get(module)
            if target:
                matches = [a for a in node.names if a.name == target]
                if matches:
                    assert len(node.names) == 1, (path, "mixed helper import needs explicit port")
                    call_targets.setdefault(path, set()).update(a.asname or a.name for a in matches)
                    imported_helpers.setdefault(path, []).append(node)

    report = {"source_only": True, "base": args.base, "files": [], "changed_files": 0,
              "queue_factories": 0, "removed_helpers": 0, "call_sites": 0,
              "all_original_assertions_preserved": True, "complete_module_AST_restored": True}
    for path in sorted(set(queue_plans) | set(call_targets)):
        source, original = originals[path], trees[path]
        lines = source.splitlines(keepends=True)
        starts = [0]
        for line in lines:
            starts.append(starts[-1] + len(line))

        def span(node):
            return starts[node.lineno - 1] + node.col_offset, starts[node.end_lineno - 1] + node.end_col_offset

        edits, imports, replacements = [], set(), {}
        plans = queue_plans.get(path, {})
        for name, (node, expression) in plans.items():
            start, _ = span(node.body[0])
            _, stop = span(node.body[-1])
            edits.append((start, stop, "return " + expression))
            imports.add("FleetWorld")
            if "admission=" in expression:
                imports.add("Admission")
        targets = call_targets.get(path, set())
        own = publishes.get(path)
        if own:
            shadowed = [n for n in ast.walk(original) if isinstance(n, ast.FunctionDef)
                        and n is not own and n.name == own.name]
            assert not shadowed, (path, "helper shadowing")
            start, stop = span(own)
            edits.append((start, stop, ""))
        for node in imported_helpers.get(path, []):
            start, stop = span(node)
            edits.append((start, stop, ""))
        consumed = set()
        for call in ast.walk(original):
            if not isinstance(call, ast.Call) or not isinstance(call.func, ast.Name) or call.func.id not in targets:
                continue
            assert call.args and not isinstance(call.args[0], ast.Starred), (path, ast.unparse(call))
            rewritten = ast.Call(
                func=ast.Attribute(ast.Call(ast.Name("ActionRows", ast.Load()),
                                           [copy.deepcopy(call.args[0])], []), "publish", ast.Load()),
                args=copy.deepcopy(call.args[1:]), keywords=copy.deepcopy(call.keywords))
            old, new = dump(call), dump(rewritten)
            assert new not in replacements or dump(replacements[new]) == old
            replacements[new] = copy.deepcopy(call)
            consumed.add(id(call.func))
            start, stop = span(call)
            edits.append((start, stop, ast.unparse(rewritten)))
            imports.add("ActionRows")
        unexpected = [n for n in ast.walk(original) if isinstance(n, ast.Name)
                      and isinstance(n.ctx, ast.Load) and n.id in targets and id(n) not in consumed]
        assert not unexpected, (path, "non-call reference to removed helper")
        assert not any(isinstance(n, ast.Constant) and isinstance(n.value, str)
                       and any(module + "." + name in n.value for module, name in exports.items())
                       for n in ast.walk(original)), (path, "embedded retired helper")
        first = next(n for n in original.body if isinstance(n, (ast.FunctionDef, ast.ClassDef)))
        first_line = min([first.lineno, *[n.lineno for n in getattr(first, "decorator_list", [])]])
        edits.append((starts[first_line - 1], starts[first_line - 1],
                      "from fleet_world import " + ", ".join(sorted(imports)) + "\n\n\n"))
        desired = source
        for start, stop, replacement in sorted(edits, reverse=True):
            desired = desired[:start] + replacement + desired[stop:]
        changed = ast.parse(desired)

        class Restore(ast.NodeTransformer):
            def visit_Call(self, node):
                if dump(node) in replacements:
                    return copy.deepcopy(replacements[dump(node)])
                return self.generic_visit(node)

            def visit_FunctionDef(self, node):
                if node.name in plans:
                    previous = plans[node.name][0]
                    assert dump(node.args) == dump(previous.args)
                    assert [dump(x) for x in node.decorator_list] == [dump(x) for x in previous.decorator_list]
                    node.body = copy.deepcopy(previous.body)
                return self.generic_visit(node)

        restored = Restore().visit(copy.deepcopy(changed))
        restored.body = [n for n in restored.body
                         if not (isinstance(n, ast.ImportFrom) and n.module == "fleet_world")]
        removed = [n for n in original.body if n is own or n in imported_helpers.get(path, [])]
        for previous in removed:
            restored.body.insert(original.body.index(previous), copy.deepcopy(previous))
        assert dump(restored) == dump(original), (path, "complete-module replay differs")
        old_asserts = Counter(dump(n) for n in ast.walk(original) if isinstance(n, ast.Assert))
        new_asserts = Counter(dump(n) for n in ast.walk(Restore().visit(copy.deepcopy(changed))) if isinstance(n, ast.Assert))
        assert old_asserts == new_asserts, path
        current = (args.root / path).read_text()
        if current != source and current != desired:
            manual = ast.parse(current)
            if path != "tests/test_pool.py":
                raise AssertionError((path, "unrecognized current edits"))
            baseline_functions = {n.name: n for n in original.body if isinstance(n, ast.FunctionDef)}
            for node in manual.body:
                if isinstance(node, ast.FunctionDef) and node.name in plans:
                    assert dump(node.body[0]) == dump(ast.parse("return " + plans[node.name][1]).body[0])
                    node.body = copy.deepcopy(baseline_functions[node.name].body)
            manual.body = [n for n in manual.body if not (isinstance(n, ast.ImportFrom) and n.module == "fleet_world")]
            assert dump(manual) == dump(original), "first manual unit differs"
        if args.apply:
            (args.root / path).write_text(desired)
        row = dict(path=path, before_sha256=hashlib.sha256(source.encode()).hexdigest(),
                   after_sha256=hashlib.sha256(desired.encode()).hexdigest(),
                   queue_factories=sorted(plans), removed_helper=own.name if own else None,
                   call_sites=len(consumed), assertions=sum(old_asserts.values()),
                   full_module_restored=True)
        report["files"].append(row)
        report["queue_factories"] += len(plans)
        report["removed_helpers"] += own is not None
        report["call_sites"] += len(consumed)
    report["changed_files"] = len(report["files"])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k != "files"}))


if __name__ == "__main__":
    main()
