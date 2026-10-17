"""Audit source, assertions and publication occurrences without product imports."""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
from pathlib import Path
import subprocess


def review_git(root, *arguments):
    return subprocess.check_output(["git", "-C", str(root), *arguments])


def review_test_contract(tree):
    result = {}

    def visit(node, owners=()):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            owners = (*owners, node.name)
            if node.name.startswith("test_"):
                result["::".join(owners)] = {
                    "assertions": [ast.dump(part, include_attributes=False)
                                   for part in ast.walk(node) if isinstance(part, ast.Assert)],
                    "decorators": [ast.dump(part, include_attributes=False)
                                   for part in node.decorator_list],
                }
        for child in ast.iter_child_nodes(node):
            visit(child, owners)

    visit(tree)
    return result


def review_occurrences(tree):
    result = []

    def visit(node, owner="<module>"):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            owner = node.name
        if isinstance(node, ast.Call):
            name = ast.unparse(node.func)
            if any(word in name.lower() for word in (
                    "publish", "read", "write", "lookup", "request", "submit",
                    "seal", "floor", "operation", "supersed", "attach", "refus")):
                result.append({"owner": owner, "line": node.lineno, "call": name})
        for child in ast.iter_child_nodes(node):
            visit(child, owner)

    visit(tree)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--base", default="8e8572ee324b8508b9a7dd6a6dcb7d3548b663a7")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    paths = review_git(args.root, "ls-files", "-z").decode().split("\0")
    base_paths = set(review_git(args.root, "ls-tree", "-r", "--name-only", args.base).decode().splitlines())
    changed = set(review_git(args.root, "diff", "--name-only", args.base).decode().splitlines())
    preserved, inventory, parsed = {}, {}, 0
    for name in sorted(path for path in paths if path.endswith(".py")):
        raw = (args.root / name).read_bytes()
        tree = ast.parse(raw, filename=name)
        parsed += 1
        if name in base_paths and name.startswith("tests/"):
            before = ast.parse(review_git(args.root, "show", f"{args.base}:{name}"))
            old, new = review_test_contract(before), review_test_contract(tree)
            assert old.keys() <= new.keys(), (name, "original test removed")
            for key in old:
                assert old[key] == new[key], (name, key, "assertion or decorator changed")
            preserved[name] = {"tests": len(old), "assertions": sum(len(row["assertions"]) for row in old.values())}
        if name in changed or name in {
                "src/prismabuild/submission.py", "src/prismabuild/submission_types.py",
                "src/prismabuild/d38_gate.py", "tools/fleet/submission_frontend.py"}:
            inventory[name] = {"sha256": hashlib.sha256(raw).hexdigest(),
                               "bytes": len(raw), "occurrences": review_occurrences(tree)}
    forbidden = [name for name in changed if name.startswith("tests/fixtures/")
                 or "golden" in name or "schema" in Path(name).name]
    assert not forbidden, forbidden
    result = {"base": args.base,
              "head": review_git(args.root, "rev-parse", "HEAD").decode().strip(),
              "tree": review_git(args.root, "rev-parse", "HEAD^{tree}").decode().strip(),
              "parsed_python_files": parsed, "preserved": preserved,
              "original_test_count": sum(row["tests"] for row in preserved.values()),
              "original_assertion_count": sum(row["assertions"] for row in preserved.values()),
              "inventory": inventory, "changed_paths": sorted(changed)}
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: value for key, value in result.items()
                      if key not in {"preserved", "inventory", "changed_paths"}}))


if __name__ == "__main__":
    main()
