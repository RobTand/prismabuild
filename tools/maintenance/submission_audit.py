"""Rerunnable owner, native test and assertion census for C14."""
from __future__ import annotations
import argparse
import ast
import hashlib
import json
from pathlib import Path
import subprocess

BASE = "f0bcb21e195378e7fb6e85e1bbfde24c45672e6b"
PROTECTED = ("src/prismabuild/action_client.py", "src/prismabuild/action_wait.py",
    "src/prismabuild/client_channel.py", "src/prismabuild/client_recovery.py",
    "src/prismabuild/capability_eligibility.py", "src/prismabuild/contracts.py")


def inspect(root: Path) -> dict:
    errors = []
    assertions = {}
    tests = {}
    legacy = []
    forbidden = []
    for folder in ("src", "tools", "tests"):
        for path in sorted((root / folder).rglob("*.py")):
            relative = path.relative_to(root).as_posix()
            source = path.read_text()
            try:
                tree = ast.parse(source)
            except SyntaxError as exc:
                errors.append({"path": relative, "error": str(exc)})
                continue
            if folder == "tests":
                tests[relative] = [node.name for node in ast.walk(tree)
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and node.name.startswith("test")]
                assertions[relative] = [ast.dump(node, include_attributes=False)
                    for node in ast.walk(tree) if isinstance(node, ast.Assert)]
            for node in ast.walk(tree):
                if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
                    if node.value.id == "pbrun" and node.attr not in {
                            "main", "parse_args", "configured_store", "render", "__file__",
                            "RUNTIME_ROOT", "fleet_roster", "request_from_args"}:
                        legacy.append([relative, node.lineno, node.attr])
                if relative in {"src/prismabuild/submission.py", "src/prismabuild/submission_types.py",
                                "src/prismabuild/d38_gate.py", "src/prismabuild/target_evidence.py"}:
                    if isinstance(node, (ast.Import, ast.ImportFrom)):
                        names = [alias.name for alias in node.names]
                        if getattr(node, "module", "") in {"pbrun", "argparse", "runpy"} or any(
                                name in {"pbrun", "argparse", "runpy"} for name in names):
                            forbidden.append([relative, node.lineno, ast.unparse(node)])
                    if isinstance(node, ast.Attribute) and ast.unparse(node) == "sys.argv":
                        forbidden.append([relative, node.lineno, "sys.argv"])
    protected = {}
    for relative in PROTECTED:
        actual = (root / relative).read_bytes()
        baseline = subprocess.check_output(["git", "show", f"{BASE}:{relative}"], cwd=root)
        protected[relative] = {"sha256": hashlib.sha256(actual).hexdigest(), "unchanged": actual == baseline}
    return {"syntax_errors": errors, "legacy_references": legacy, "forbidden": forbidden,
        "protected": protected, "tests": tests, "assertions": assertions}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = inspect(args.root)
    raw = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.write_text(raw)
    print(json.dumps({"syntax_errors": result["syntax_errors"],
        "legacy_references": len(result["legacy_references"]),
        "forbidden": result["forbidden"],
        "protected": all(row["unchanged"] for row in result["protected"].values()),
        "test_files": len(result["tests"]),
        "test_functions": sum(map(len, result["tests"].values())),
        "assertions": sum(map(len, result["assertions"].values()))}))
    return bool(result["syntax_errors"] or result["forbidden"] or not all(
        row["unchanged"] for row in result["protected"].values()))


if __name__ == "__main__":
    raise SystemExit(main())
