"""Account for every assertion occurrence across the C27 source migration."""
from __future__ import annotations

import argparse
import ast
from collections import Counter
import io
import json
from pathlib import Path
import subprocess
import tarfile

BASE = "8e8572ee324b8508b9a7dd6a6dcb7d3548b663a7"
NAMES = {"read_pool": "action_rows", "read_starvation": "starvation",
         "read_blocked_origins": "blocked_origins", "read_endings": "endings",
         "_number": "nonnegative_number"}


class CensusAssertionOwner(ast.NodeTransformer):
    def visit_Attribute(self, node):
        if (node.attr == "value" and isinstance(node.value, ast.Call)
                and isinstance(node.value.func, ast.Attribute)
                and node.value.func.attr in {"rows", "action_rows", "blocked_origins"}):
            return self.visit(node.value)
        text = ast.unparse(node)
        if text == "pbmetrics._number":
            return ast.Attribute(ast.Name("facts", ast.Load()), "nonnegative_number", ast.Load())
        if text.endswith(".read_rollout_summary"):
            return ast.Attribute(ast.Name("facts", ast.Load()), "read_rollout_summary", ast.Load())
        prefixes = ("pbstatus", "fleet_census", "pbmcp.pbstatus", "pbmetrics.pbstatus")
        if ast.unparse(node.value) in prefixes:
            return ast.Attribute(ast.Name("facts", ast.Load()), NAMES.get(node.attr, node.attr), ast.Load())
        return self.generic_visit(node)


def census_assertion_form(node):
    return ast.dump(CensusAssertionOwner().visit(ast.parse(ast.unparse(node))), include_attributes=False)


def census_assertion_occurrences(source):
    result = []
    tree = ast.parse(source)
    parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
    for node in ast.walk(tree):
        if (isinstance(node, ast.Assert) or isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and (node.func.attr.startswith("assert") or ast.unparse(node.func) == "pytest.raises")):
            scope, parent = [], parents.get(node)
            while parent is not None:
                if isinstance(parent, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                    scope.append(parent.name)
                parent = parents.get(parent)
            result.append({"scope": ".".join(reversed(scope)), "line": node.lineno,
                           "text": ast.unparse(node), "form": census_assertion_form(node)})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--scope", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    raw = subprocess.check_output(["git", "-C", str(args.root), "archive", BASE, "tests"])
    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
        baseline = {m.name: archive.extractfile(m).read().decode()
                    for m in archive.getmembers() if m.isfile() and m.name.endswith(".py")}
    scope = json.loads(args.scope.read_text())
    paths = set(scope.get("population", scope.get("files", [])))
    paths.update(subprocess.check_output(["git", "-C", str(args.root), "diff", "--name-only", BASE], text=True).splitlines())
    rows, lost = [], 0
    for path in sorted(p for p in paths if p.startswith("tests/") and p.endswith(".py")):
        before = census_assertion_occurrences(baseline.get(path, ""))
        after = census_assertion_occurrences((args.root / path).read_text())
        remaining = Counter((row["scope"], row["form"]) for row in after)
        dispositions = []
        for row in before:
            key = row["scope"], row["form"]
            found = remaining[key] > 0
            if found:
                remaining[key] -= 1
            else:
                lost += 1
            dispositions.append({**row, "disposition": "preserved with explicit owner normalization" if found else "UNRESOLVED"})
        rows.append({"path": path, "before": len(before), "after": len(after), "occurrences": dispositions,
                     "added": sum(remaining.values())})
    report = {"base": BASE, "paths": rows, "unresolved": lost,
              "before": sum(r["before"] for r in rows), "after": sum(r["after"] for r in rows)}
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k != "paths"}))
    return bool(lost)


if __name__ == "__main__":
    raise SystemExit(main())
