"""Inventory claim ownership and source deltas without importing the project."""
from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
import subprocess
import builtins
import symtable

ROOT = Path(__file__).resolve().parents[2]
POOL = "src/prismabuild/pool.py"
OWNERS = (POOL, "src/prismabuild/candidate_policy.py",
          "src/prismabuild/local_scratch.py", "src/prismabuild/_gang.py",
          "src/prismabuild/_measurement_reservation.py")


def claim_audit_git(*args):
    return subprocess.check_output(["git", "-C", str(ROOT), *args])


def claim_audit_label(node):
    return ast.unparse(node)


def claim_source_inventory(source):
    tree = ast.parse(source)
    functions = {}
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        functions.setdefault(node.name, []).append({
            "line": node.lineno, "physical": node.end_lineno - node.lineno + 1,
            "statements": sum(isinstance(child, ast.stmt) for child in ast.walk(node)),
            "ast_object": subprocess.check_output(["git", "hash-object", "--stdin"],
                                                 input=ast.dump(node, include_attributes=False).encode()).decode().strip(),
        })
    calls = []

    def visit(node, guards=(), owner="module"):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            owner = node.name
        if isinstance(node, ast.Call):
            calls.append({"line": node.lineno, "owner": owner,
                          "call": claim_audit_label(node.func), "expression": claim_audit_label(node),
                          "guards": guards})
        if isinstance(node, ast.BoolOp):
            for index, value in enumerate(node.values):
                earlier = node.values[:index]
                condition = (" and " if isinstance(node.op, ast.And) else " or ").join(
                    claim_audit_label(part) for part in earlier)
                if condition and isinstance(node.op, ast.Or):
                    condition = "not (" + condition + ")"
                visit(value, (*guards, condition) if condition else guards, owner)
            return
        if isinstance(node, ast.IfExp):
            visit(node.test, guards, owner)
            visit(node.body, (*guards, claim_audit_label(node.test)), owner)
            visit(node.orelse, (*guards, "else " + claim_audit_label(node.test)), owner)
            return
        if isinstance(node, ast.If):
            visit(node.test, guards, owner)
            for child in node.body:
                visit(child, (*guards, claim_audit_label(node.test)), owner)
            for child in node.orelse:
                visit(child, (*guards, "else " + claim_audit_label(node.test)), owner)
            return
        if isinstance(node, ast.Try):
            for child in node.body:
                visit(child, (*guards, "try"), owner)
            for handler in node.handlers:
                for child in handler.body:
                    visit(child, (*guards, "except " + (claim_audit_label(handler.type) if handler.type else "bare")), owner)
            for child in node.orelse:
                visit(child, (*guards, "try else"), owner)
            for child in node.finalbody:
                visit(child, (*guards, "finally"), owner)
            return
        if isinstance(node, (ast.With, ast.AsyncWith)):
            for entry in node.items:
                visit(entry.context_expr, guards, owner)
            for child in node.body:
                visit(child, (*guards, "with " + ", ".join(claim_audit_label(x.context_expr) for x in node.items)), owner)
            return
        for child in ast.iter_child_nodes(node):
            visit(child, guards, owner)

    visit(tree)
    return {"functions": functions, "calls": calls}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--final", action="store_true")
    args = parser.parse_args()
    current = {}
    manifests = []
    for entry in claim_audit_git("ls-files", "--stage").decode().splitlines():
        metadata, name = entry.split("\t", 1)
        mode, blob, stage = metadata.split()
        manifests.append({"path": name, "git_blob": blob, "mode": mode, "stage": stage})
    for name in OWNERS:
        current[name] = claim_source_inventory((ROOT / name).read_text())
    old = claim_source_inventory(claim_audit_git("show", args.base + ":" + POOL).decode())
    references = []
    claim_targets = ("_carried_withhold", "_claim_pass", "_claim", "claim")
    for name in claim_audit_git("ls-files", "*.py").decode().splitlines():
        path = ROOT / name
        if not path.is_file():
            continue
        try:
            tree = ast.parse(path.read_text())
        except (SyntaxError, UnicodeError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                target = node.func.attr if isinstance(node.func, ast.Attribute) else getattr(node.func, "id", "")
                if target in claim_targets:
                    references.append({"path": name, "line": node.lineno, "call": claim_audit_label(node.func)})
    symbols = symtable.symtable((ROOT / POOL).read_text(), POOL, "exec")
    known = set(symbols.get_identifiers()) | set(dir(builtins))
    queue_symbols = next(child for child in symbols.get_children() if child.get_name() == "PoolQueue")
    changed = {name for name, rows in current[POOL]["functions"].items()
               if rows != old["functions"].get(name)}
    unknown_globals = {}
    for function in queue_symbols.get_children():
        if function.get_name() in changed:
            missing = [symbol.get_name() for symbol in function.get_symbols()
                       if symbol.is_referenced() and symbol.is_global()
                       and symbol.get_name() not in known]
            if missing:
                unknown_globals[function.get_name()] = missing
    old_tree = ast.parse(claim_audit_git("show", args.base + ":" + POOL).decode())
    new_tree = ast.parse((ROOT / POOL).read_text())
    old_classes = {node.name: node for node in old_tree.body if isinstance(node, ast.ClassDef)}
    new_classes = {node.name: node for node in new_tree.body if isinstance(node, ast.ClassDef)}
    unchanged_contracts = {}
    for name in ("_ClaimReservation", "_ClaimScanReads"):
        unchanged_contracts[name] = ast.dump(old_classes[name], include_attributes=False) == ast.dump(
            new_classes[name], include_attributes=False)
    old_methods = {node.name: node for node in old_classes["PoolQueue"].body if isinstance(node, ast.FunctionDef)}
    new_methods = {node.name: node for node in new_classes["PoolQueue"].body if isinstance(node, ast.FunctionDef)}
    modified_existing = [name for name in old_methods if name in new_methods and
                         ast.dump(old_methods[name], include_attributes=False) !=
                         ast.dump(new_methods[name], include_attributes=False)]
    removed_methods = sorted(set(old_methods) - set(new_methods))
    driver = new_methods["_claim_pass"]
    uncertain = [node for node in ast.walk(driver) if isinstance(node, ast.Try)
                 and any(isinstance(handler.type, ast.Name) and handler.type.id == "BaseException"
                         for handler in node.handlers)]
    census_blocks = [node for node in ast.walk(driver) if isinstance(node, ast.With)
                     and any(claim_audit_label(item.context_expr).startswith(
                         "measurement_reservation.pass_admission_census(") for item in node.items)]
    transition_blocks = [node for node in ast.walk(driver) if isinstance(node, ast.With)
                         and any(claim_audit_label(item.context_expr).startswith(
                             "self._timed_transition_hold(") for item in node.items)]
    scope_checks = {"single_uncertain": len(uncertain) == 1,
                    "canonical_census_visible": len(census_blocks) == 1,
                    "candidate_transition_visible": len(transition_blocks) == 1}
    if len(uncertain) == 1 and len(census_blocks) == 1:
        guarded_calls = {claim_audit_label(node.func) for statement in uncertain[0].body
                         for node in ast.walk(statement) if isinstance(node, ast.Call)}
        census_calls = {claim_audit_label(node.func) for node in ast.walk(census_blocks[0])
                        if isinstance(node, ast.Call)}
        scope_checks.update({
            "reserve_under_census": "self._reserve_claim_host" in census_calls,
            "accounting_after_census": "self._account_host_refusal" not in census_calls,
            "take_and_commit_uncertain": {"self._take_claim_tiers", "self._rename_and_commit_claim"} <= guarded_calls,
            "publication_outside_uncertain": "self._persist_committed_claim" not in guarded_calls,
            "uncertain_cleanup_exact": ast.dump(uncertain[0].handlers[0], include_attributes=False)
                == ast.dump(ast.parse("try: pass\nexcept BaseException:\n"
                                      "    reservation.abandon(_ClaimExit.UNCERTAIN)\n    raise").body[0].handlers[0],
                            include_attributes=False),
        })
    body = ast.Module(body=driver.body[1:], type_ignores=[])
    scope_checks["no_nested_decision_or_encoding"] = not any(
        isinstance(node, (ast.Lambda, ast.FunctionDef, ast.DictComp, ast.ListComp, ast.BinOp, ast.Subscript))
        or isinstance(node, ast.Constant) and isinstance(node.value, str)
        for node in ast.walk(body))
    canonical_census = {}
    module = "src/prismabuild/_measurement_reservation.py"
    old_census = ast.parse(claim_audit_git("show", args.base + ":" + module).decode())
    new_census = ast.parse((ROOT / module).read_text())
    for name in ("PassCensus", "locked_census", "admission_census", "pass_admission_census"):
        before = next(node for node in old_census.body if getattr(node, "name", None) == name)
        after = next(node for node in new_census.body if getattr(node, "name", None) == name)
        canonical_census[name] = ast.dump(before, include_attributes=False) == ast.dump(after, include_attributes=False)
    shape = current[POOL]["functions"]["_claim_pass"][0]
    diff = claim_audit_git("diff", "--numstat", args.base).decode().splitlines()
    report = {"head": claim_audit_git("rev-parse", "HEAD").decode().strip(),
              "tree": claim_audit_git("rev-parse", "HEAD^{tree}").decode().strip(),
              "dirty": claim_audit_git("status", "--porcelain=v1").decode().splitlines(),
              "base": args.base, "source_manifest": manifests,
              "unknown_globals": unknown_globals,
              "unchanged_contracts": unchanged_contracts,
              "modified_existing_methods": sorted(modified_existing),
              "removed_methods": removed_methods,
              "driver_scope_checks": scope_checks, "canonical_census_unchanged": canonical_census,
              "owners": current, "baseline_pool": old, "callers": references,
              "source_delta_numstat": diff, "driver": shape,
              "limits_pass": shape["physical"] <= 240 and shape["statements"] < 100}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"output": str(args.output), "head": report["head"],
                      "driver": shape, "limits_pass": report["limits_pass"]}))
    if args.final and (not report["limits_pass"] or unknown_globals or not all(unchanged_contracts.values())
                       or not all(scope_checks.values()) or not all(canonical_census.values())):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
