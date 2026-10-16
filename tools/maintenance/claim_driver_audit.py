"""Inventory claim ownership and source deltas without importing the project."""
from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
import subprocess
import builtins
import symtable

ROOT = Path(__file__).resolve().parents[2]
POOL = "src/prismabuild/pool.py"
OWNERS = (POOL, "src/prismabuild/candidate_policy.py",
          "src/prismabuild/local_scratch.py", "src/prismabuild/_gang.py",
          "src/prismabuild/_measurement_reservation.py")


def claim_audit_git(*args):
    return subprocess.check_output(["git", "-C", str(ROOT), *args])


def claim_audit_label(node):
    return ast.unparse(node)


def claim_source_inventory(source):
    tree = ast.parse(source)
    functions = {}
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        functions.setdefault(node.name, []).append({
            "line": node.lineno, "physical": node.end_lineno - node.lineno + 1,
            "statements": sum(isinstance(child, ast.stmt) for child in ast.walk(node)),
            "ast_object": subprocess.check_output(["git", "hash-object", "--stdin"],
                                                 input=ast.dump(node, include_attributes=False).encode()).decode().strip(),
        })
    calls = []

    def visit(node, guards=(), owner="module"):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            owner = node.name
        if isinstance(node, ast.Call):
            calls.append({"line": node.lineno, "owner": owner,
                          "call": claim_audit_label(node.func), "expression": claim_audit_label(node),
                          "guards": guards})
        if isinstance(node, ast.If):
            visit(node.test, guards, owner)
            for child in node.body:
                visit(child, (*guards, claim_audit_label(node.test)), owner)
            for child in node.orelse:
                visit(child, (*guards, "else " + claim_audit_label(node.test)), owner)
            return
        if isinstance(node, ast.Try):
            for child in node.body:
                visit(child, (*guards, "try"), owner)
            for handler in node.handlers:
                for child in handler.body:
                    visit(child, (*guards, "except " + (claim_audit_label(handler.type) if handler.type else "bare")), owner)
            for child in node.orelse:
                visit(child, (*guards, "try else"), owner)
            for child in node.finalbody:
                visit(child, (*guards, "finally"), owner)
            return
        if isinstance(node, (ast.With, ast.AsyncWith)):
            for entry in node.items:
                visit(entry.context_expr, guards, owner)
            for child in node.body:
                visit(child, (*guards, "with " + ", ".join(claim_audit_label(x.context_expr) for x in node.items)), owner)
            return
        for child in ast.iter_child_nodes(node):
            visit(child, guards, owner)

    visit(tree)
    return {"functions": functions, "calls": calls}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--final", action="store_true")
    args = parser.parse_args()
    current = {}
    manifests = []
    for entry in claim_audit_git("ls-files", "--stage").decode().splitlines():
        metadata, name = entry.split("\t", 1)
        mode, blob, stage = metadata.split()
        manifests.append({"path": name, "git_blob": blob, "mode": mode, "stage": stage})
    for name in OWNERS:
        current[name] = claim_source_inventory((ROOT / name).read_text())
    old = claim_source_inventory(claim_audit_git("show", args.base + ":" + POOL).decode())
    references = []
    retired = ("_carried_withhold", "_claim_pass", "_claim", "claim")
    for name in claim_audit_git("ls-files", "*.py").decode().splitlines():
        path = ROOT / name
        if not path.is_file():
            continue
        try:
            tree = ast.parse(path.read_text())
        except (SyntaxError, UnicodeError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                target = node.func.attr if isinstance(node.func, ast.Attribute) else getattr(node.func, "id", "")
                if target in retired:
                    references.append({"path": name, "line": node.lineno, "call": claim_audit_label(node.func)})
    symbols = symtable.symtable((ROOT / POOL).read_text(), POOL, "exec")
    known = set(symbols.get_identifiers()) | set(dir(builtins))
    queue_symbols = next(child for child in symbols.get_children() if child.get_name() == "PoolQueue")
    changed = {name for name, rows in current[POOL]["functions"].items()
               if rows != old["functions"].get(name)}
    unknown_globals = {}
    for function in queue_symbols.get_children():
        if function.get_name() in changed:
            missing = [symbol.get_name() for symbol in function.get_symbols()
                       if symbol.is_referenced() and symbol.is_global()
                       and symbol.get_name() not in known]
            if missing:
                unknown_globals[function.get_name()] = missing
    shape = current[POOL]["functions"]["_claim_pass"][0]
    diff = claim_audit_git("diff", "--numstat", args.base).decode().splitlines()
    report = {"head": claim_audit_git("rev-parse", "HEAD").decode().strip(),
              "tree": claim_audit_git("rev-parse", "HEAD^{tree}").decode().strip(),
              "dirty": claim_audit_git("status", "--porcelain=v1").decode().splitlines(),
              "base": args.base, "source_manifest": manifests,
              "unknown_globals": unknown_globals,
              "owners": current, "baseline_pool": old, "callers": references,
              "source_delta_numstat": diff, "driver": shape,
              "limits_pass": shape["physical"] <= 240 and shape["statements"] < 100}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"output": str(args.output), "head": report["head"],
                      "driver": shape, "limits_pass": report["limits_pass"]}))
    if args.final and (not report["limits_pass"] or unknown_globals):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
