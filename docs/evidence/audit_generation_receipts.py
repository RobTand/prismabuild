#!/usr/bin/env python3
"""Source-only C15 inventory and assertion-preservation audit. Imports no product code."""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
from pathlib import Path
import re
import subprocess


TERMS = re.compile(r"RUNTIME_VERSION|runtime_version|generation_receipt|"
                   r"runtime_version\.v1|CLIENT_UPGRADE_\w*PROTOCOL|"
                   r"MAINTENANCE_DURABLE_PROTOCOL")
OWNERS = {"generation_receipt", "runtime_publication", "resource_scope",
          "upgrade_client", "publish_runtime", "supervise", "role_session",
          "compatibility_window", "submission", "pbrun", "fleet_membership",
          "pbcanary_watch", "generation_fixture", "client_proof_bridge_fixture",
          "movement_publication_support", "receipt_reader_fixture"}


def sha(data):
    return hashlib.sha256(data).hexdigest()


def syntax(node):
    return sha(ast.dump(node, include_attributes=False).encode())


def inventory(root):
    tracked = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=root)
    paths = sorted(set(tracked.decode().rstrip("\0").split("\0")))
    occurrences, contracts, modules, sources = [], {}, {}, {}
    for relative in paths:
        if relative.startswith("qualification_"):
            continue
        path = root / relative
        if not path.is_file() or path.suffix not in (".py", ".sh"):
            continue
        raw = path.read_bytes()
        text = raw.decode("utf-8")
        hits = [(number, line) for number, line in enumerate(text.splitlines(), 1)
                if TERMS.search(line)]
        tree = ast.parse(text) if path.suffix == ".py" else None
        imports, symbols, embedded, aliases, reads = [], [], [], set(), []
        if tree:
            for node in ast.walk(tree):
                if isinstance(node, ast.Assign) and any(
                        isinstance(child, ast.Constant) and isinstance(child.value, str)
                        and TERMS.search(child.value) for child in ast.walk(node.value)):
                    aliases.update(child.id for target in node.targets for child in ast.walk(target)
                                   if isinstance(child, ast.Name))
            grew = True
            while grew:
                before = set(aliases)
                for node in ast.walk(tree):
                    if isinstance(node, ast.Assign) and any(
                            isinstance(child, ast.Name) and child.id in aliases for child in ast.walk(node.value)):
                        aliases.update(child.id for target in node.targets for child in ast.walk(target)
                                       if isinstance(child, ast.Name))
                grew = aliases != before
            for node in ast.walk(tree):
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    imports.extend([node.module or ""] if isinstance(node, ast.ImportFrom)
                                   else [alias.name for alias in node.names])
                    imports.extend(alias.name for alias in node.names)
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    symbols.append((node.lineno, node.end_lineno, node.name))
                if isinstance(node, ast.Call) and any(
                        isinstance(child, ast.Name) and child.id in aliases for child in ast.walk(node)):
                    reads.append({'line': node.lineno, 'call': ast.unparse(node)[:320]})
                if isinstance(node, ast.Constant) and isinstance(node.value, str) and TERMS.search(node.value):
                    if not any(number == node.lineno for number, _ in hits):
                        hits.append((node.lineno, repr(node.value)[:320]))
                if isinstance(node, ast.Constant) and isinstance(node.value, str):
                    if TERMS.search(node.value) and "\n" in node.value:
                        try:
                            embedded_tree = ast.parse(node.value)
                        except SyntaxError:
                            continue
                        embedded.append({"line": node.lineno, "sha256": syntax(embedded_tree),
                                         "lines": len(node.value.splitlines())})
            if relative.startswith("tests/"):
                contracts[relative] = {
                    "assertions": [syntax(node) for node in ast.walk(tree) if isinstance(node, ast.Assert)],
                    "tests": [{"name": node.name,
                               "decorators": [syntax(item) for item in node.decorator_list]}
                              for node in ast.walk(tree)
                              if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                              and node.name.startswith("test_")]}
        modules[relative] = imports
        if hits:
            sources[relative] = sha(raw)
            occurrences.append({'path': relative, 'line': 0, 'symbol': '<alias-map>',
                                'aliases': sorted(aliases), 'dependent_calls': reads})
            for number, line in hits:
                enclosing = [(end - start, name) for start, end, name in symbols
                             if start <= number <= end]
                occurrences.append({"path": relative, "line": number,
                                    "symbol": min(enclosing)[1] if enclosing else "<module>",
                                    "source": line.strip(), "embedded": embedded})
    selected = {path for path, imports in modules.items()
                if path.startswith("tests/test_") and any(
                    item.rsplit(".", 1)[-1] in OWNERS for item in imports)}
    selected.update(item["path"] for item in occurrences if item["path"].startswith("tests/test_"))
    selected.add("tests/test_duplication_baseline.py")
    dependencies = {Path(path).stem for path in selected}
    changed = True
    while changed:
        added = {path for path, imports in modules.items()
                 if path.startswith("tests/") and path not in selected
                 and any(item.rsplit(".", 1)[-1] in dependencies for item in imports)}
        changed = bool(added)
        selected.update(added)
        dependencies.update(Path(path).stem for path in added)
    return {"schema": "c15.source-inventory.v1", "product_imports": False,
            "head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root).decode().strip(),
            "branch": subprocess.check_output(["git", "branch", "--show-current"], cwd=root).decode().strip(),
            "checkout": str(root), "occurrences": occurrences,
            "sources": sources, "test_contracts": contracts,
            "affected_test_files": sorted(path for path in selected if Path(path).name.startswith("test_"))}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkout", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--baseline", type=Path)
    args = parser.parse_args()
    report = inventory(args.checkout.resolve())
    if args.baseline:
        before = json.loads(args.baseline.read_bytes())["test_contracts"]
        after = report["test_contracts"]
        report["changed_original_test_contracts"] = [path for path in before if before[path] != after.get(path)]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"output": str(args.output), "occurrences": len(report["occurrences"]),
                      "affected_test_files": len(report["affected_test_files"]),
                      "changed_original_test_contracts": report.get("changed_original_test_contracts")}))


if __name__ == "__main__":
    main()
