"""Pin census source occurrences and their transitive consumer population.

Source/AST only. Test collection and runtime controls run inside PB actions.
"""
from __future__ import annotations

import argparse
import ast
from collections import Counter
import json
import io
from pathlib import Path
import re
import subprocess
import tarfile

RENDERERS = tuple(f"tools/fleet/{name}.py" for name in ("pbstatus", "pbmetrics", "pbmcp"))


def census_source_blob(value):
    return subprocess.check_output(["git", "hash-object", "--stdin"], input=value.encode()).decode().strip()


def census_definitions(tree, scope=""):
    counts = Counter()
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            counts[node.name] += 1
            name = f"{scope}.{node.name}" if scope else node.name
            yield name, counts[node.name], node
            if isinstance(node, ast.ClassDef):
                yield from census_definitions(node, name)


def census_inventory(path, source):
    tree = ast.parse(source, filename=path)
    parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
    out = []
    for name, occurrence, node in census_definitions(tree):
        calls = []
        assertions = []
        for child in ast.walk(node):
            if isinstance(child, ast.Call):
                guards = []
                parent = parents.get(child)
                while parent is not None and parent is not node:
                    if isinstance(parent, (ast.If, ast.For, ast.While, ast.With, ast.Try, ast.IfExp)):
                        guards.append(ast.unparse(parent.test) if isinstance(parent, (ast.If, ast.While, ast.IfExp))
                                      else type(parent).__name__)
                    parent = parents.get(parent)
                calls.append({"line": child.lineno, "call": ast.unparse(child.func), "guards": guards})
            if isinstance(child, ast.Assert):
                assertions.append({"line": child.lineno, "assertion": ast.unparse(child.test)})
            if isinstance(child, ast.Call) and (isinstance(child.func, ast.Attribute)
                    and (child.func.attr.startswith("assert") or ast.unparse(child.func) == "pytest.raises")):
                assertions.append({"line": child.lineno, "assertion": ast.unparse(child)})
        out.append({"name": name, "occurrence": occurrence, "line": node.lineno,
                    "end": node.end_lineno, "decorators": [ast.unparse(d) for d in node.decorator_list],
                    "signature": ast.unparse(node.args) if hasattr(node, "args") else None,
                    "ast_git_blob": census_source_blob(ast.dump(node, include_attributes=False)),
                    "calls": sorted(calls, key=lambda c: c["line"]), "assertions": assertions})
    return {"git_blob": census_source_blob(source), "definitions": out,
            "native_strings": [{"line": n.lineno, "text": n.value} for n in ast.walk(tree)
                               if isinstance(n, ast.Constant) and isinstance(n.value, str)
                               and any(s in n.value for s in ("python", "pytest", "pbstatus", "pbmetrics", "pbmcp"))]}


def census_population(sources):
    dependencies = {}
    for path, source in sources.items():
        try:
            tree = ast.parse(source)
        except SyntaxError:
            continue
        docs = {id(n.value) for n in ast.walk(tree)
                if isinstance(n, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
                and n.body and isinstance(n.body[0], ast.Expr)
                and isinstance(n.body[0].value, ast.Constant)
                for n in [n.body[0]]}
        names = set()
        parents = {child: n for n in ast.walk(tree) for child in ast.iter_child_nodes(n)}
        used_status = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)
                       and isinstance(n.value, ast.Name) and n.value.id == "pbstatus"}
        census_status = {"read_pool", "read_starvation", "read_endings", "ending_row", "main",
                         "read_blocked_origins", "_pool_claim_denials", "_starvation_plan_entry"}
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names.update(part for alias in node.names for part in alias.name.split(".")
                             if part != "pbstatus" or used_status & census_status)
            if isinstance(node, ast.ImportFrom):
                if node.module == "pbstatus" and not ({a.name for a in node.names} & census_status):
                    continue
                names.update((node.module or "").split("."))
                names.update(alias.name for alias in node.names)
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docs:
                # Inventory membership is not an import. Follow code paths and
                # embedded programs, never runtime script lists or log notices.
                if isinstance(parents.get(node), ast.BinOp):
                    names.update(Path(s).stem for s in re.findall(r"[\w/.-]+\.py\b", node.value))
                names.update(re.findall(r"(?:from|import)\s+(\w+)", node.value))
        dependencies[path] = names
    wanted = set(RENDERERS)
    wanted.update(path for path in sources if path.startswith("src/prismabuild/census"))
    changed = True
    while changed:
        changed = False
        stems = {Path(path).stem for path in wanted
                 if not path.startswith("tests/test_") and Path(path).stem != "conftest"}
        for path, source in sources.items():
            if path in wanted:
                continue
            if dependencies.get(path, set()) & stems:
                wanted.add(path)
                changed = True
    # Include fixtures and test-module dependencies of every importer. These
    # are dependencies, not changed owners that pull all their other users in.
    test_modules = {Path(p).stem: p for p in sources if p.startswith("tests/")}
    pending = list(wanted)
    while pending:
        path = pending.pop()
        for name in dependencies.get(path, set()):
            dependency = test_modules.get(name)
            if dependency is not None and dependency not in wanted:
                wanted.add(dependency)
                pending.append(dependency)
    wanted.add("tests/conftest.py")
    return sorted(wanted)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--ref", default="WORKTREE")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    paths = subprocess.check_output(["git", "-C", str(args.root), "ls-files"], text=True).splitlines()
    archived = {}
    if args.ref != "WORKTREE":
        raw = subprocess.check_output(["git", "-C", str(args.root), "archive", args.ref])
        with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
            archived = {m.name: archive.extractfile(m).read().decode()
                        for m in archive.getmembers() if m.isfile() and m.name.endswith(".py")}
    if args.ref == "WORKTREE":
        paths.extend(str(p.relative_to(args.root)) for p in args.root.glob("src/prismabuild/census*.py"))
    sources = {}
    for path in sorted(set(paths)):
        if not path.endswith(".py") or path.startswith(("tests/compat/", "evidence/")):
            continue
        try:
            sources[path] = ((args.root / path).read_text() if args.ref == "WORKTREE"
                             else archived[path])
        except (FileNotFoundError, KeyError):
            continue
    members = census_population(sources)
    result = {"ref": args.ref, "root": str(args.root), "population": members,
              "whole_test_files": [p for p in members if p.startswith("tests/test_")],
              "files": {p: census_inventory(p, sources[p]) for p in members},
              "limit": "AST/source pin only; no collection or execution credit"}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"output": str(args.output), "files": len(members),
                      "whole_test_files": len(result["whole_test_files"])}))


if __name__ == "__main__":
    main()
