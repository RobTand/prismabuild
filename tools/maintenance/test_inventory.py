"""Inventory test structures that need review; findings are not failure verdicts."""
from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path


def inventory(root: Path) -> dict[str, object]:
    findings: dict[str, list[dict[str, object]]] = {
        "sleep_calls": [], "reflexive_assertions": [],
        "source_inspections": [], "shadowed_tests": [],
    }
    files = 0
    tests = 0
    for path in sorted(root.rglob("test_*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        files += 1
        relative = path.relative_to(root).as_posix()
        for scope in (tree, *(node for node in ast.walk(tree)
                              if isinstance(node, ast.ClassDef))):
            definitions: dict[str, int] = {}
            for node in scope.body:
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                if not node.name.startswith("test_"):
                    continue
                if node.name in definitions:
                    findings["shadowed_tests"].append({
                        "file": relative, "test": node.name,
                        "line": definitions[node.name], "replaced_at": node.lineno,
                    })
                definitions[node.name] = node.lineno
        for test in ast.walk(tree):
            if not isinstance(test, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if not test.name.startswith("test_"):
                continue
            tests += 1
            for node in ast.walk(test):
                kind = None
                if isinstance(node, ast.Call):
                    target = ast.unparse(node.func)
                    if target in {"time.sleep", "sleep", "asyncio.sleep"}:
                        kind = "sleep_calls"
                    elif target in {"inspect.getsource", "inspect.getsourcelines",
                                   "ast.parse"}:
                        kind = "source_inspections"
                elif isinstance(node, ast.Assert) and isinstance(node.test, ast.Compare):
                    compare = node.test
                    if (len(compare.comparators) == 1
                            and isinstance(compare.ops[0], (ast.Eq, ast.Is))
                            and ast.dump(compare.left) == ast.dump(compare.comparators[0])):
                        kind = "reflexive_assertions"
                if kind:
                    findings[kind].append({
                        "file": relative, "test": test.name, "line": node.lineno,
                        "expression": ast.unparse(node),
                    })
    return {"files": files, "tests": tests,
            "counts": {kind: len(rows) for kind, rows in findings.items()},
            "findings": findings}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", nargs="?", type=Path, default=Path("tests"))
    parser.add_argument("--json", type=Path, help="write the full inventory here")
    args = parser.parse_args()
    report = inventory(args.root)
    if args.json:
        args.json.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: value for key, value in report.items()
                      if key != "findings"}, sort_keys=True))


if __name__ == "__main__":
    main()
