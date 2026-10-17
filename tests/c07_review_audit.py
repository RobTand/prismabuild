"""Retain review-round source, assertions, callers and conservative import closure.

Stdlib source inspection only. Native collection and outcomes require PB.
"""
import argparse
import ast
from collections import Counter
import hashlib
import json
from pathlib import Path
import subprocess


BASE = "b26d6bc94abb3c977bbf04650ac27d0d6535277a"
ROOT = Path(__file__).resolve().parents[1]
SEAMS = ("_expired_claim", "_recover_cancelled_claim", "_complete_cancellation",
         "_commit_claim", "_retire_recorded_terminal", "withdrawn_attempt",
         "qualify_claim_recovery", "claim_identity")


def git(*args):
    return subprocess.check_output(["git", "-C", str(ROOT), *args])


def _c07_review_sha(value):
    return hashlib.sha256(value).hexdigest()


def atoms(raw):
    tree = ast.parse(raw)
    return Counter(ast.dump(n, include_attributes=False) for n in ast.walk(tree)
                   if isinstance(n, ast.Assert)), Counter(
        ast.dump(d, include_attributes=False) for n in ast.walk(tree)
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
        for d in n.decorator_list), Counter(n.name for n in ast.walk(tree)
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    paths = git("ls-files").decode().splitlines()
    sources = {p: (ROOT / p).read_bytes() for p in paths if p.endswith(".py")}
    old_paths = git("ls-tree", "-r", "--name-only", BASE).decode().splitlines()
    retained, mismatches, original = [], [], {}
    for path in old_paths:
        if not path.startswith("tests/") or not path.endswith(".py"):
            continue
        raw = git("show", BASE + ":" + path)
        before = atoms(raw)
        after = atoms(sources.get(path, b""))
        original[path] = {name: dict(values) for name, values in
                          zip(("assertions", "decorators", "definitions"), before)}
        missing = {name: dict(a - b) for name, a, b in
                   zip(("assertions", "decorators", "definitions"), before, after) if a - b}
        if missing:
            mismatches.append({"path": path, "missing": missing})
        retained.append({"path": path, "before_sha256": _c07_review_sha(raw),
                         "after_sha256": _c07_review_sha(sources[path]), "byte_equal": raw == sources[path]})
    (args.out / "original-test-atoms.json").write_text(json.dumps(original, sort_keys=True) + "\n")
    callers, imports, fixtures = [], {}, {}
    modules = {p: p.removeprefix("src/").removesuffix(".py").replace("/", ".") for p in sources}
    lookup = {module: p for p, module in modules.items()}
    for path in sources:
        lookup.setdefault(Path(path).stem, path)
    for path, raw in sources.items():
        text = raw.decode()
        tree = ast.parse(raw)
        edges = set()
        for node in ast.walk(tree):
            names = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                prefix = node.module or ""
                if node.level:
                    parts = modules[path].split(".")[:-node.level]
                    prefix = ".".join([*parts, prefix]).rstrip(".")
                names = [prefix, *(prefix + "." + a.name for a in node.names)]
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                # Embedded child code, dynamic imports, plugin and script paths.
                names = [m for m in lookup if m and m in node.value]
            edges.update(lookup[name] for name in names if name in lookup and lookup[name] != path)
        imports[path] = sorted(edges)
        callers.extend({"path": path, "line": i, "text": line.strip()}
                       for i, line in enumerate(text.splitlines(), 1) if any(s in line for s in SEAMS))
        fixtures[path] = [str(p) for p in Path(path).parents
                          if str(p) != "." and str(p / "conftest.py") in sources]
        if path.startswith("tests/"):
            for directory in fixtures[path]:
                imports[path].append(directory + "/conftest.py")
    closure = {"src/prismabuild/lifecycle.py", "tools/fleet/qualify_claim_recovery.py"}
    while True:
        expanded = closure | {p for p, deps in imports.items() if closure.intersection(deps)}
        if expanded == closure:
            break
        closure = expanded
    tests = sorted(p for p in closure if p.startswith("tests/") and Path(p).name.startswith("test_"))
    result = {"base": BASE, "head": git("rev-parse", "HEAD").decode().strip(),
              "tree": git("rev-parse", "HEAD^{tree}").decode().strip(),
              "hashes": {p: _c07_review_sha(raw) for p, raw in sources.items()},
              "test_retention": retained, "missing_atoms": mismatches,
              "callers": callers, "imports": imports, "implicit_conftests": fixtures,
              "conservative_transitive_tests": tests,
              "closure_limit": "AST imports plus literal module/path matches and ancestor conftests; "
                               "runtime fixture/plugin discovery and native roster are separate PB evidence"}
    (args.out / "audit.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    (args.out / "root-final-whole-files.txt").write_text("\n".join(tests) + "\n")
    print(json.dumps({"head": result["head"], "old_test_files": len(retained),
                      "byte_equal": sum(r["byte_equal"] for r in retained),
                      "missing_atoms": len(mismatches), "transitive_test_files": len(tests)}))
    raise SystemExit(bool(mismatches))


if __name__ == "__main__":
    main()
