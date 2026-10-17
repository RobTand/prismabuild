#!/usr/bin/env python3
"""Pin C10 source contracts without importing or executing product code."""

import argparse
import ast
from collections import Counter
import hashlib
import json
from pathlib import Path
import subprocess


DOMAIN = {
    "tier_ledger", "tier_mint_lock", "tier_ids", "tier_holdings",
    "release_tier_rate_reservations", "hold_tier_host_memory",
    "release_tier_host_memory", "_ram_tier_host", "take_tier_advance",
    "cancel_tier_fence", "release_tier_holder", "reconcile_ram_host_holds",
    "release_tier_reservations", "transfer_tier_reservation",
    "transfer_tier_reservation_count", "mint_tier_capacity",
    "_apply_tier_capacity", "_reclaim_dead_markers",
    "mint_tier_capacity_guarded", "release_tier_holder_for_egress",
}


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def syntax(node):
    return ast.dump(node, include_attributes=False)


def mentions(node):
    return any(isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
               and n.func.attr == "tier_ledger" for n in ast.walk(node))


def inspect_file(path, source):
    tree = ast.parse(source, filename=path)
    functions, assertions, sites, strings = [], [], [], []
    parents = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[child] = parent
    aliases = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            if node.value is not None and mentions(node.value):
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                for target in targets:
                    aliases.add(ast.unparse(target))
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            parent = parents.get(node)
            name = ((parent.name + ".") if isinstance(parent, ast.ClassDef) else "") + node.name
            calls = sorted((n for n in ast.walk(node) if isinstance(n, ast.Call)),
                           key=lambda n: (n.lineno, n.col_offset))
            functions.append({"name": name, "line": node.lineno,
                              "ast": digest(syntax(node)),
                              "signature": ast.unparse(node.args),
                              "decorators": [ast.unparse(d) for d in node.decorator_list],
                              "asserts": [digest(syntax(n)) for n in ast.walk(node)
                                          if isinstance(n, ast.Assert)]})
            if path == "src/prismabuild/pool.py" and node.name in DOMAIN:
                functions[-1]["source"] = ast.get_source_segment(source, node)
                functions[-1]["ordered_calls"] = [ast.unparse(c.func) for c in calls]
                functions[-1]["control"] = [
                    {"line": n.lineno, "kind": type(n).__name__,
                     "test": ast.unparse(n.test) if isinstance(n, ast.If) else ""}
                    for n in ast.walk(node)
                    if isinstance(n, (ast.If, ast.With, ast.Try, ast.Raise, ast.Return))]
        if isinstance(node, ast.Assert):
            assertions.append({"line": node.lineno, "ast": syntax(node)})
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            receiver = ast.unparse(node.func.value)
            if (node.func.attr in DOMAIN or mentions(node.func.value)
                    or receiver in aliases):
                sites.append({"line": node.lineno, "operation": node.func.attr,
                              "receiver": receiver, "call": ast.unparse(node)})
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if any(s in node.value for s in DOMAIN) or "TIER_RESERVATIONS" in node.value:
                strings.append({"line": node.lineno, "value": node.value})
    return {"sha256": digest(source), "aliases": sorted(aliases),
            "functions": functions, "assertions": assertions,
            "sites": sorted(sites, key=lambda x: x["line"]), "strings": strings}


def capture(root, revision):
    paths = subprocess.check_output(["git", "ls-tree", "-r", "--name-only", revision],
                                    cwd=root, text=True).splitlines()
    files = {}
    for path in paths:
        if not path.endswith(".py"):
            continue
        source = subprocess.check_output(["git", "show", f"{revision}:{path}"],
                                         cwd=root, text=True)
        if (path.startswith("tests/") or "tier_ledger" in source
                or any(name in source for name in DOMAIN)):
            files[path] = inspect_file(path, source)
    return {"revision": subprocess.check_output(
        ["git", "rev-parse", revision], cwd=root, text=True).strip(), "files": files}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--revision", default="HEAD")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = capture(args.root, args.revision)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    files = report["files"]
    counts = Counter(site["operation"] for data in files.values() for site in data["sites"])
    print(json.dumps({"revision": report["revision"], "files": len(files),
                      "functions": sum(len(d["functions"]) for d in files.values()),
                      "assertions": sum(len(d["assertions"]) for d in files.values()),
                      "operations": dict(counts), "output": str(args.output)}, sort_keys=True))


if __name__ == "__main__":
    main()
