"""Compare C09 occurrences, bodies, assertions and import ownership using ASTs."""
from __future__ import annotations

import argparse
import ast
from collections import Counter
from copy import deepcopy
import json
from pathlib import Path
import re
import subprocess
import sys

from staged_path_migrate import (
    STAGED_BASE, STAGED_MODULES, STAGED_OWNER, STAGED_PUBLIC, staged_references,
    staged_sources, staged_symbols, staged_exports, staged_alias_bindings, staged_reference_module,
)


class StagedBodyForm(ast.NodeTransformer):
    def __init__(self, selected):
        self.selected = selected

    def visit_Name(self, node):
        if node.id == "PRODUCED_OUTPUT_REF_SCHEMA_V1":
            return ast.Name("output_ref_schema", ast.Load())
        if node.id in ("stage_release", "staged_path"):
            return ast.Name("staged_authority", ast.Load())
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
            if node.value.id == "staged_path" or (
                    node.value.id in self.selected
                    and node.attr in self.selected[node.value.id]
                    and node.value.id != "produced_output"):
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


def staged_baseline_relocation(root, selected):
    sys.path.insert(0, str(root / "tools"))
    import duplication_inventory as inventory
    from maintenance.submission_owners import baseline_at_submission_owners

    path = root / "tests/fixtures/duplication_baseline.json"
    base = baseline_at_submission_owners(json.loads(path.read_text()))
    live = inventory.scan(root)

    def relocate(value):
        for module, old_path in STAGED_MODULES.items():
            prefix = old_path + "::"
            if prefix not in value:
                continue
            before, rest = value.split(prefix, 1)
            name, dot, tail = rest.partition(".")
            if name in selected[module]:
                destination = ("src/prismabuild/output_vocabulary.py" if module == "produced_output" else STAGED_OWNER)
                return before + destination + "::" + STAGED_PUBLIC.get(name, name) + dot + tail
        return value

    expected_groups, exemptions = {}, {}
    for name, paths in base["same_name_helpers"].items():
        for old_path in paths:
            module, symbol = relocate(old_path + "::" + name).split("::", 1)
            expected_groups.setdefault(symbol, set()).add(module)
    distinct = []
    for row in base["same_name_distinct"]:
        by_name = {}
        for old_path in row["modules"]:
            module, symbol = relocate(old_path + "::" + row["name"]).split("::", 1)
            by_name.setdefault(symbol, []).append(module)
            exemptions.setdefault(symbol, set()).add(module)
        distinct.extend({**row, "name": name, "modules": sorted(modules)} for name, modules in by_name.items())
    growth = {
        "pairs": sorted({tuple(pair) for pair in live["near_duplicates"]}
                        - {tuple(sorted(map(relocate, pair))) for pair in base["near_duplicates"]}),
        "digest_sites": sorted(set(live["primitive_digest_sites"]) - set(map(relocate, base["primitive_digest_sites"]))),
        "helpers": inventory.same_name_growth(live["same_name_helpers"], expected_groups, exemptions),
    }
    if any(growth.values()):
        raise SystemExit("C09 adds architecture permissions: " + json.dumps(growth))
    raw_groups = live["same_name_helpers"]
    live["same_name_helpers"] = {name: [p for p in paths if p not in exemptions.get(name, set())]
        for name, paths in raw_groups.items() if set(paths) - exemptions.get(name, set())}
    live["same_name_distinct"] = [row for row in distinct if all(p in raw_groups.get(row["name"], ()) for p in row["modules"])]
    pairs = {tuple(pair) for pair in live["near_duplicates"]}
    live["must_differ"] = [{**row, "pair": sorted(map(relocate, row["pair"]))}
        for row in base["must_differ"] if tuple(sorted(map(relocate, row["pair"]))) in pairs]
    path.write_text(json.dumps({key: live[key] for key in sorted(live)}, indent=1) + "\n")
    return {"growth": growth, "counts": {k: len(v) for k, v in live.items()}}


def staged_audit_cli():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--relocate-baseline", action="store_true")
    args = parser.parse_args()
    sources = staged_sources(args.root, STAGED_BASE)
    selected = staged_symbols(sources)
    if args.relocate_baseline:
        result = staged_baseline_relocation(args.root, selected)
        args.output.write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result))
        return
    original_exports = staged_exports(sources)
    current_sources = {p: (args.root / p).read_text() for p in sources if (args.root / p).exists()}
    current_exports = staged_exports(current_sources)
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
            expected = staged_checks(staged_references(original, selected, exports=original_exports)
                                     if original != current else original)
            actual = staged_checks(current)
            delta = expected - actual
            if delta:
                missing_checks.append({"path": path, "missing": dict(delta)})
            expected_tests += sum(v for k, v in expected.items() if k.startswith("native:"))
            current_tests += sum(v for k, v in actual.items() if k.startswith("native:"))
        aliases = staged_alias_bindings(tree, selected, current_exports)
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute):
                module = staged_reference_module(node.value, aliases, current_exports)
                if module in selected and node.attr in selected[module]:
                    retired.append([path, node.lineno, ast.unparse(node)])
            if isinstance(node, ast.ImportFrom):
                module = (node.module or "").split(".")[-1]
                if module in selected:
                    retired.extend([path, node.lineno, f"import {a.name}"] for a in node.names if a.name in selected[module])
    current_paths = subprocess.check_output(
        ["git", "ls-files", "*.py"], cwd=args.root, text=True).splitlines()
    all_sources = {p: (args.root / p).read_text() for p in current_paths}
    all_parse_errors, remaining_rewrites = [], []
    for path, source in all_sources.items():
        try:
            ast.parse(source, filename=path)
        except SyntaxError as exc:
            all_parse_errors.append([path, str(exc)])
        if path.startswith(("tests/compat/", "tools/maintenance/")):
            continue
        local = next((m for m, p in STAGED_MODULES.items() if p == path), None)
        if staged_references(source, selected, local=local, exports=current_exports) != source:
            remaining_rewrites.append(path)
    original_assertions, current_assertions = Counter(), Counter()
    for population, counter in ((sources, original_assertions), (all_sources, current_assertions)):
        for path, source in population.items():
            if path.startswith(("src/", "tools/")):
                counter.update(staged_body(node, selected) for node in ast.walk(ast.parse(source))
                               if isinstance(node, ast.Assert))
    preserved_captures = subprocess.check_output(
        ["git", "diff", "--name-only", STAGED_BASE, "--", "tests/compat"],
        cwd=args.root, text=True).splitlines()
    result = {"base": STAGED_BASE, "root": str(args.root), "moves": moves,
              "body_changes": [r for r in moves if not r["ast_equal"]], "changed": changed,
              "missing_checks": missing_checks, "retired_executable_references": retired,
              "parse_errors": parse_errors, "original_test_definition_occurrences": expected_tests,
              "current_test_definition_occurrences": current_tests,
              "tracked_python_files": len(all_sources), "all_parse_errors": all_parse_errors,
              "remaining_migration_rewrites": remaining_rewrites,
              "original_production_assertions": original_assertions.total(),
              "current_production_assertions": current_assertions.total(),
              "missing_production_assertions": dict(original_assertions - current_assertions),
              "assertion_binding_migration": "produced_output's existing in-process module-availability assert now names staged_path; all original assertion conditions remain.",
              "changed_historical_captures": preserved_captures,
              "limit": "Source comparison, not runtime proof. Deliberate later protocol changes are reported, never hidden."}
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({k: len(result[k]) for k in ("moves", "body_changes", "changed", "missing_checks", "retired_executable_references", "parse_errors", "remaining_migration_rewrites", "missing_production_assertions", "changed_historical_captures")}))


if __name__ == "__main__":
    staged_audit_cli()
