"""Compare C09 occurrences, bodies, assertions and import ownership using ASTs."""
from __future__ import annotations

import argparse
import ast
from collections import Counter
from copy import deepcopy
import json
from pathlib import Path
import re

from staged_path_migrate import (
    STAGED_BASE, STAGED_MODULES, STAGED_OWNER, STAGED_PUBLIC, staged_references,
    staged_sources, staged_symbols,
)


class StagedBodyForm(ast.NodeTransformer):
    def __init__(self, selected):
        self.selected = selected

    def visit_Name(self, node):
        if node.id == "PRODUCED_OUTPUT_REF_SCHEMA_V1":
            return ast.Name("output_ref_schema", ast.Load())
        node.id = STAGED_PUBLIC.get(node.id, node.id)
        return node

    def visit_FunctionDef(self, node):
        node.name = STAGED_PUBLIC.get(node.name, node.name)
        return self.generic_visit(node)

    def visit_ClassDef(self, node):
        node.name = STAGED_PUBLIC.get(node.name, node.name)
        return self.generic_visit(node)

    def visit_ImportFrom(self, node):
        module = (node.module or "").split(".")[-1]
        if module in self.selected and all(a.name in self.selected[module] for a in node.names):
            return None
        return node

    def visit_Attribute(self, node):
        if isinstance(node.value, ast.Name):
            if node.value.id == "output_vocabulary" and node.attr == "PRODUCED_OUTPUT_REF_SCHEMA_V1":
                return ast.Name("output_ref_schema", ast.Load())
            if node.value.id == "staged_path":
                return ast.Name(STAGED_PUBLIC.get(node.attr, node.attr), ast.Load())
            if node.value.id == "produced_output" and node.attr in self.selected["produced_output"]:
                node.value.id = "output_vocabulary"
        return self.generic_visit(node)

    def visit_Constant(self, node):
        if node.value == "prismabuild.produced_output_ref.v1":
            return ast.Name("output_ref_schema", ast.Load())
        return node


def staged_body(node, selected):
    return ast.dump(StagedBodyForm(selected).visit(deepcopy(node)), include_attributes=False)


def staged_checks(source):
    tree = ast.parse(source)
    checks = Counter()
    for node in ast.walk(tree):
        if isinstance(node, ast.Assert) or (isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and (node.func.attr.startswith("assert") or node.func.attr == "raises")):
            checks[ast.dump(node, include_attributes=False)] += 1
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            for decorator in node.decorator_list:
                checks["decorator:" + ast.dump(decorator, include_attributes=False)] += 1
            if node.name.startswith("test"):
                checks["native:" + node.name] += 1
    return checks


def staged_audit_cli():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    sources = staged_sources(args.root, STAGED_BASE)
    selected = staged_symbols(sources)
    new_tree = ast.parse((args.root / STAGED_OWNER).read_text())
    definitions = {n.name: n for n in new_tree.body if isinstance(n, (ast.FunctionDef, ast.ClassDef))}
    moves = []
    for module, path in STAGED_MODULES.items():
        if module == "produced_output":
            continue
        for node in ast.parse(sources[path]).body:
            if not isinstance(node, (ast.FunctionDef, ast.ClassDef)) or node.name not in selected[module]:
                continue
            name = STAGED_PUBLIC.get(node.name, node.name)
            current = definitions.get(name)
            moves.append({"old": f"{path}::{node.name}", "new": f"{STAGED_OWNER}::{name}",
                          "old_line": node.lineno, "new_line": current.lineno if current else None,
                          "ast_equal": current is not None and staged_body(node, selected) == staged_body(current, selected)})
    missing_checks, retired, parse_errors, changed = [], [], [], []
    expected_tests = current_tests = 0
    for path, original in sources.items():
        current_path = args.root / path
        if not current_path.exists():
            continue
        current = current_path.read_text()
        try:
            tree = ast.parse(current, filename=path)
        except SyntaxError as exc:
            parse_errors.append([path, str(exc)])
            continue
        if original != current:
            changed.append(path)
        if path.startswith("tests/"):
            expected = staged_checks(staged_references(original, selected) if original != current else original)
            actual = staged_checks(current)
            delta = expected - actual
            if delta:
                missing_checks.append({"path": path, "missing": dict(delta)})
            expected_tests += sum(v for k, v in expected.items() if k.startswith("native:"))
            current_tests += sum(v for k, v in actual.items() if k.startswith("native:"))
        aliases = {m: m for m in selected}
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "prismabuild":
                aliases.update({a.asname or a.name: a.name for a in node.names if a.name in selected})
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
                module = aliases.get(node.value.id)
                if module in selected and node.attr in selected[module]:
                    retired.append([path, node.lineno, ast.unparse(node)])
            if isinstance(node, ast.ImportFrom):
                module = (node.module or "").split(".")[-1]
                if module in selected:
                    retired.extend([path, node.lineno, f"import {a.name}"] for a in node.names if a.name in selected[module])
    result = {"base": STAGED_BASE, "root": str(args.root), "moves": moves,
              "body_changes": [r for r in moves if not r["ast_equal"]], "changed": changed,
              "missing_checks": missing_checks, "retired_executable_references": retired,
              "parse_errors": parse_errors, "original_test_definition_occurrences": expected_tests,
              "current_test_definition_occurrences": current_tests,
              "limit": "Source comparison, not runtime proof. Deliberate later protocol changes are reported, never hidden."}
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({k: len(result[k]) for k in ("moves", "body_changes", "changed", "missing_checks", "retired_executable_references", "parse_errors")}))


if __name__ == "__main__":
    staged_audit_cli()
