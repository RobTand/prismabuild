"""Check fix PRs for changed Python test behavior without executing PR code."""
from __future__ import annotations

import argparse
import ast
from dataclasses import asdict, dataclass
import json
from pathlib import Path, PurePosixPath
import re


_FIX_TITLE = re.compile(
    r"^\s*(?:\[[^\]]+\]\s*)*(?:fix(?:es|ed)?|repair|bugfix|regression)"
    r"(?:\([^)]*\))?(?:\s|:|$)", re.IGNORECASE,
)
_FIX_LABELS = {"bug", "fix", "bugfix", "regression", "type:bug", "type:fix"}


@dataclass(frozen=True)
class Verdict:
    allowed: bool
    reason: str
    tests: tuple[str, ...] = ()


def is_test_file(filename: str) -> bool:
    path = PurePosixPath(filename)
    return ("tests" in path.parts and path.suffix == ".py"
            and (path.name.startswith("test_") or path.name.endswith("_test.py")))


def is_fix(pull: dict) -> bool:
    labels = {label["name"].strip().lower() for label in pull["labels"]}
    return bool(_FIX_TITLE.search(pull["title"]) or labels & _FIX_LABELS
                or re.search(r"(?im)^\s*change type:\s*(?:fix|bugfix)\s*$", pull["body"] or ""))


def _has_assertion(node: ast.AST) -> bool:
    return any(isinstance(child, ast.Assert) or (
        isinstance(child, ast.Call) and (
            isinstance(child.func, ast.Attribute) and (
                child.func.attr.startswith("assert") or
                ast.unparse(child.func) in {"pytest.raises", "pytest.fail", "pytest.warns"})
        )) for child in ast.walk(node))


def _test_bodies(source: str) -> dict[str, tuple[str, bool]]:
    tree = ast.parse(source)
    tests = {}
    for scope in [tree, *(node for node in tree.body if isinstance(node, ast.ClassDef)
                          and (node.name.startswith("Test") or any(
                              ast.unparse(base).endswith("TestCase") for base in node.bases)))]:
        prefix = scope.name + "." if isinstance(scope, ast.ClassDef) else ""
        for node in scope.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or not node.name.startswith("test_"):
                continue
            if any(ast.unparse(decorator).split("(", 1)[0].endswith("fixture")
                   for decorator in node.decorator_list):
                continue
            name = prefix + node.name
            node.name = "test"
            if (node.body and isinstance(node.body[0], ast.Expr)
                    and isinstance(node.body[0].value, ast.Constant)
                    and isinstance(node.body[0].value.value, str)):
                node.body.pop(0)
            proof = _has_assertion(node)
            tests[name] = (ast.dump(node, include_attributes=False), proof)
    return tests


def evaluate(pull: dict, files: list[dict]) -> Verdict:
    if not is_fix(pull):
        return Verdict(True, "Not classified as a fix by title, labels or Change type.")
    if len(files) != pull["changed_files"] or len(files) > 3000:
        return Verdict(False, "Incomplete PR file inventory; rerun after reducing the PR or repairing the API read.")
    changed = []
    for file in files:
        if (not is_test_file(file["filename"]) or file["status"] not in {"added", "modified", "renamed"}
                or file["additions"] == 0):
            continue
        if not isinstance(file.get("before"), str) or not isinstance(file.get("after"), str):
            return Verdict(False, f"Complete test source unavailable: {file['filename']}.")
        try:
            before = _test_bodies(file["before"])
            after = _test_bodies(file["after"])
        except SyntaxError:
            return Verdict(False, f"Cannot parse Python tests: {file['filename']}.")
        previous_bodies = {body for body, _ in before.values()}
        for name, (body, proof) in after.items():
            if proof and body not in previous_bodies:
                changed.append(f"{file['filename']}::{name}")
    if changed:
        return Verdict(True, "Changed executable test behavior found; reviewer must verify relevance and red-to-green evidence.", tuple(changed))
    return Verdict(False, "Fix PR requires an added or modified Python test with an assertion or expected-exception check. Fixtures, comments, renames and deletions alone do not qualify.")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="JSON with pull and files; test files include before/after source")
    args = parser.parse_args()
    try:
        data = json.loads(args.input.read_text())
        pull, files = data["pull"], data["files"]
        if not (isinstance(pull["title"], str) and isinstance(pull.get("body"), (str, type(None)))
                and isinstance(pull["labels"], list) and all(isinstance(x["name"], str) for x in pull["labels"])
                and type(pull["changed_files"]) is int and pull["changed_files"] >= 0
                and isinstance(files, list) and all(isinstance(f["filename"], str)
                    and isinstance(f["status"], str) and type(f["additions"]) is int
                    and f["additions"] >= 0 for f in files)):
            raise ValueError("invalid PR metadata")
        verdict = evaluate(pull, files)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(json.dumps(asdict(Verdict(False, f"Gate input unavailable or invalid: {exc}"))))
        return 2
    print(json.dumps(asdict(verdict)))
    return 0 if verdict.allowed else 1


if __name__ == "__main__":
    raise SystemExit(main())
