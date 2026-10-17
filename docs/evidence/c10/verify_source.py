#!/usr/bin/env python3
"""Audit original test predicates, aliases and bounded C10 commits from source."""

import argparse
import ast
from collections import Counter, defaultdict
import io
import json
from pathlib import Path
import runpy
import subprocess


def assertion_keys(tree):
    parents = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parents[child] = node
    found = Counter()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assert):
            continue
        parent = parents.get(node)
        names = []
        while parent is not None:
            if isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                names.append(parent.name)
            parent = parents.get(parent)
        found[".".join(reversed(names)), ast.dump(node, include_attributes=False)] += 1
    return found


def declarations(tree):
    return Counter((node.name, tuple(ast.dump(d) for d in node.decorator_list))
                   for node in ast.walk(tree)
                   if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)))


def original_sources(root, base, paths):
    batch = subprocess.check_output(["git", "cat-file", "--batch"], cwd=root,
                                    input="".join(f"{base}:{p}\n" for p in paths).encode())
    stream = io.BytesIO(batch)
    for path in paths:
        header = stream.readline().decode().split()
        if len(header) != 3 or header[1] != "blob":
            raise ValueError((path, header))
        source = stream.read(int(header[2])).decode()
        if stream.read(1) != b"\n":
            raise ValueError(path)
        yield path, source


def prerequisite_assertions(before, prerequisite):
    """Derive exact assertion-owner moves from the concrete imported source."""
    old, new = assertion_keys(before), assertion_keys(prerequisite)
    removed, added = old - new, new - old
    mapping = {}
    for key, count in removed.items():
        candidates = [(replacement, number) for replacement, number in added.items()
                      if replacement[0] == key[0]]
        if count != 1 or len(candidates) != 1 or candidates[0][1] != 1:
            raise ValueError(("ambiguous prerequisite assertion move", key, candidates))
        mapping[key] = candidates[0][0]
    parents = {}
    for node in ast.walk(before):
        for child in ast.iter_child_nodes(node):
            parents[child] = node
    for node in ast.walk(before):
        if not isinstance(node, ast.Assert):
            continue
        names, parent = [], parents.get(node)
        while parent is not None:
            if isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                names.append(parent.name)
            parent = parents.get(parent)
        key = ".".join(reversed(names)), ast.dump(node, include_attributes=False)
        if key in mapping:
            replacement = next(n for n in ast.walk(prerequisite)
                               if isinstance(n, ast.Assert)
                               and ast.dump(n, include_attributes=False) == mapping[key][1])
            node.test, node.msg = replacement.test, replacement.msg
    return [dict(function=key[0], original=key[1], prerequisite=value[1])
            for key, value in mapping.items()]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--prerequisite", help="Concrete imported owner prerequisite commit")
    args = parser.parse_args()
    baseline = json.loads(args.baseline.read_text())
    migration = runpy.run_path(str(args.root / "docs/evidence/c10/migrate_source.py"))
    paths = [path for path in baseline["files"] if path.startswith("tests/")]
    predicate_changes, declaration_changes = {}, {}
    original_count = retained_count = 0
    prerequisite_moves = {}
    imported = (dict(original_sources(args.root, args.prerequisite, paths))
                if args.prerequisite else {})
    for path, source in original_sources(args.root, baseline["revision"], paths):
        before = ast.parse(source, filename=path)
        if imported:
            moves = prerequisite_assertions(before, ast.parse(imported[path], filename=path))
            if moves:
                prerequisite_moves[path] = moves
        after = ast.parse((args.root / path).read_text(), filename=path)
        removed = declarations(before) - declarations(after)
        if removed:
            declaration_changes[path] = [list(key) + [count] for key, count in removed.items()]
        scopes, aliases = migration["caller_aliases"](before)
        normalized = migration["CallerMove"](scopes, aliases).visit(before)
        old, new = assertion_keys(normalized), assertion_keys(after)
        original_count += old.total()
        retained_count += (old & new).total()
        missing, extra = old - new, new - old
        if missing:
            predicate_changes[path] = {
                "missing": [dict(function=key[0], assertion=key[1], count=count)
                            for key, count in missing.items()],
                "extra": [dict(function=key[0], assertion=key[1], count=count)
                          for key, count in extra.items()]}
    retired = set(migration["MOVED"]) | set(migration["AGGREGATE"]) | {"tier_ledger"}
    residual, unknown, parse_errors = [], defaultdict(list), []
    owner = ast.parse((args.root / "src/prismabuild/tier_holdings.py").read_text())
    owner_class = next(n for n in owner.body if isinstance(n, ast.ClassDef) and n.name == "TierLedger")
    methods = {n.name for n in owner_class.body if isinstance(n, ast.FunctionDef)} | {"tier_id", "records"}
    for top in ("src", "tools", "tests"):
        for path in sorted((args.root / top).rglob("*.py")):
            relative = str(path.relative_to(args.root))
            try:
                tree = ast.parse(path.read_text(), filename=relative)
            except SyntaxError as exc:
                parse_errors.append(dict(path=relative, error=str(exc)))
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.Attribute) and node.attr in retired:
                    residual.append(dict(path=relative, line=node.lineno, name=node.attr))
                if isinstance(node, ast.FunctionDef) and node.name in retired:
                    residual.append(dict(path=relative, line=node.lineno, name=node.name))
                if isinstance(node, ast.Call):
                    name = (node.func.id if isinstance(node.func, ast.Name) else
                            node.func.attr if isinstance(node.func, ast.Attribute) else "")
                    if name in {"getattr", "setattr", "delattr"}:
                        for argument in node.args:
                            if isinstance(argument, ast.Constant) and argument.value in retired:
                                residual.append(dict(path=relative, line=node.lineno,
                                                     name=argument.value, dynamic=True))
            if relative == "src/prismabuild/tier_holdings.py":
                continue
            scopes, aliases = migration["caller_aliases"](tree)
            for node in ast.walk(tree):
                if not isinstance(node, ast.Attribute):
                    continue
                bound = (migration["tier_call"](node.value)
                         or isinstance(node.value, ast.Name)
                         and node.value.id in aliases[scopes[node]])
                if bound and node.attr not in methods:
                    unknown[node.attr].append(f"{relative}:{node.lineno}")
    commits, oversize = [], []
    for commit in subprocess.check_output(
            ["git", "rev-list", "--reverse", f"{baseline['revision']}..HEAD"],
            cwd=args.root, text=True).splitlines():
        parents = subprocess.check_output(["git", "rev-list", "--parents", "-n", "1", commit],
                                          cwd=args.root, text=True).split()[1:]
        flags = []
        if len(parents) > 1:
            if len(parents) != 2 or parents[1] != args.prerequisite:
                raise ValueError(("undeclared integration merge", commit, parents))
            flags = ["--remerge-diff"]
        lines = subprocess.check_output(["git", "show", *flags, "--format=", "--numstat", commit],
                                        cwd=args.root, text=True).splitlines()
        changed = sum(int(a) + int(b) for a, b, _ in (line.split("\t") for line in lines)
                      if a != "-" and b != "-")
        row = dict(commit=commit, paths=len(lines), changed_lines=changed)
        if flags:
            row.update(imported_prerequisite=parents[1], scope="actual merge resolutions")
        commits.append(row)
        if len(lines) > 6 or changed > 900:
            oversize.append(row)
    report = dict(source_only=True, base=baseline["revision"],
                  head=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=args.root, text=True).strip(),
                  original_assertions=original_count, automatically_mapped_assertions=retained_count,
                  predicate_changes=predicate_changes, declaration_changes=declaration_changes,
                  prerequisite=args.prerequisite, prerequisite_assertion_moves=prerequisite_moves,
                  residual_retired_apis=residual, unknown_tier_attributes=dict(unknown),
                  parse_errors=parse_errors, commits=commits, oversized_commits=oversize)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("head", "original_assertions", "automatically_mapped_assertions")}))
    print(json.dumps({"predicate_files": list(predicate_changes),
                      "declaration_files": list(declaration_changes), "residual": residual,
                      "unknown": dict(unknown), "parse_errors": parse_errors,
                      "oversized": oversize}))


if __name__ == "__main__":
    main()
