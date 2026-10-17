"""Audit C21 source ownership or replay its exact reviewed patch.

Only source, Git objects and stdlib ASTs are read. Runtime qualification and
native collection belong to admitted PrismaBuild actions.
"""
from __future__ import annotations

import argparse
import ast
from collections import Counter
import json
from pathlib import Path
import subprocess

C21_BASE = "a8ba9c0c7beeeb379908924a60c6933e997c8df6"
C21_OWNERS = (
    "src/prismabuild/submission.py", "src/prismabuild/residency_plan.py",
    "src/prismabuild/queue_publication.py", "src/prismabuild/movement_actions.py",
    "src/prismabuild/record_store.py", "src/prismabuild/census.py",
    "src/prismabuild/stage_release.py", "src/prismabuild/stage_rounding.py",
    "tools/fleet/tier_loop.py", "tools/fleet/manifest_promotion.py",
    "tools/fleet/pbcampaign.py", "tools/fleet/prelaunch_tier.py",
    "tools/fleet/stage_reclaim.py", "tools/fleet/retire_residency_plans.py",
)
C21_MOVES = {
    "submission.residency_stage_rows": "residency_plan.prepare_window",
    "submission.publication_row": "queue_publication.RowPublication.row",
    "submission.container_owner": "movement_actions.submission_container_owner",
    "submission._git_identity": "movement_actions.checkout_identity",
    "residency_plan.window": "residency_plan.ConsumerWindow.decision",
    "residency_plan.advance_needs": "residency_plan.ConsumerWindow.advance",
    "record_store.RecordStore.residency_supersession_keys": "residency_plan.active_supersessions",
    "manifest_promotion.superseded_consumers": "residency_plan.retired_consumers",
    "tier_loop._shared_movers": "residency_plan.shared_movers",
    "tier_loop._stage_leg_rows": "residency_plan.stage_leg_rows",
    "tier_loop._ram_leg_rows": "residency_plan.ram_leg_rows",
    "tier_loop._stage_source_staged": "residency_plan.stage_source_resident",
    "retire_residency_plans.candidates": "residency_plan.concluded_consumers",
}


def c21_git(root, *args, data=None):
    return subprocess.check_output(["git", "-C", str(root), *args], input=data)


def c21_blob(root, source):
    return c21_git(root, "hash-object", "--stdin", data=source.encode()).decode().strip()


def c21_occurrences(source, path):
    tree = ast.parse(source, filename=path)
    parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
    counts = Counter()
    definitions, assertions, calls, imports, strings = [], [], [], [], []
    def walk_scope(node, scope=()):
        for child in ast.iter_child_nodes(node):
            inner = scope
            if isinstance(child, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                key = (*scope, child.name)
                counts[key] += 1
                inner = (*scope, f"{child.name}#{counts[key]}")
                definitions.append(dict(scope=list(inner), line=child.lineno, end=child.end_lineno,
                    signature=ast.unparse(child.args) if hasattr(child, "args") else None,
                    decorators=[ast.dump(d, include_attributes=False) for d in child.decorator_list],
                    ast=ast.dump(child, include_attributes=False)))
            if isinstance(child, ast.Assert):
                assertions.append(dict(scope=list(scope), line=child.lineno,
                                       ast=ast.dump(child, include_attributes=False), source=ast.unparse(child)))
            if isinstance(child, ast.Call):
                guards = []
                parent = parents.get(child)
                while parent is not None:
                    if isinstance(parent, (ast.If, ast.While, ast.IfExp)):
                        guards.append(dict(line=parent.lineno, test=ast.unparse(parent.test)))
                    elif isinstance(parent, (ast.With, ast.Try, ast.For, ast.ExceptHandler)):
                        guards.append(dict(line=parent.lineno, kind=type(parent).__name__,
                            header=ast.unparse(parent.items[0].context_expr) if isinstance(parent, ast.With)
                            else ast.unparse(parent.type) if isinstance(parent, ast.ExceptHandler) and parent.type else None))
                    parent = parents.get(parent)
                calls.append(dict(scope=list(scope), line=child.lineno, call=ast.unparse(child.func),
                    source=ast.unparse(child), guards=guards))
            if isinstance(child, (ast.Import, ast.ImportFrom)):
                imports.append(dict(scope=list(scope), line=child.lineno, source=ast.unparse(child)))
            if isinstance(child, ast.Constant) and isinstance(child.value, str):
                strings.append(dict(scope=list(scope), line=child.lineno, value=child.value))
            walk_scope(child, inner)
    walk_scope(tree)
    return dict(definitions=definitions, assertions=assertions, calls=calls, imports=imports, strings=strings)


def c21_sources(root, pin):
    paths = c21_git(root, "ls-tree", "-r", "--name-only", pin).decode().splitlines()
    return {path: c21_git(root, "show", f"{pin}:{path}").decode()
            for path in paths if path.endswith(".py")}


def c21_audit(root, out, base):
    out.mkdir(parents=True, exist_ok=True)
    original = c21_sources(root, base)
    tracked = c21_git(root, "ls-files", "*.py").decode().splitlines()
    untracked = c21_git(root, "ls-files", "--others", "--exclude-standard", "*.py").decode().splitlines()
    current = {path: (root / path).read_text()
               for path in dict.fromkeys([*tracked, *untracked]) if (root / path).exists()}
    for path in C21_OWNERS:
        if (root / path).exists():
            current[path] = (root / path).read_text()
    changed = sorted(path for path in original.keys() | current.keys()
                     if original.get(path) != current.get(path))
    before, after = {}, {}
    total_old = total_new = 0
    for label, sources, output in (("before", original, before), ("after", current, after)):
        for path, source in sources.items():
            ast.parse(source, filename=path)
            if path in C21_OWNERS or path in changed or path.startswith("tests/"):
                facts = c21_occurrences(source, path)
                if path.startswith("tests/"):
                    if label == "before": total_old += len(facts["assertions"])
                    else: total_new += len(facts["assertions"])
                if path in C21_OWNERS or path in changed:
                    output[path] = dict(blob=c21_blob(root, source), **facts)
    (out / "before.json").write_text(json.dumps(before, indent=2) + "\n")
    (out / "after.json").write_text(json.dumps(after, indent=2) + "\n")
    # Pair by file, complete scope occurrence and position, never by function name alone.
    locators = []
    registry = json.loads((root / "tools/maintenance/residency_bindings.json").read_text())
    bindings = {(row["path"], tuple(row["scope"]), row["occurrence"]): row
                for row in registry["bindings"]}
    for path in changed:
        if not path.startswith("tests/"): continue
        old = before.get(path, {}).get("assertions", [])
        new = after.get(path, {}).get("assertions", [])
        def grouped(values):
            out = {}
            for entry in values: out.setdefault(tuple(entry["scope"]), []).append(entry)
            return out
        old_groups, new_groups = grouped(old), grouped(new)
        for scope, entries in old_groups.items():
            candidates = new_groups.get(scope, [])
            if len(entries) != len(candidates):
                raise ValueError(f"assertion occurrence count changed at {path}::{scope}")
            for index, (left, right) in enumerate(zip(entries, candidates)):
                exact = left["ast"] == right["ast"]
                if not exact:
                    bound = bindings.get((path, scope, index))
                    if (bound is None or bound["original_ast_blob"] != c21_blob(root, left["ast"])
                            or bound["current_ast_blob"] != c21_blob(root, right["ast"])):
                        raise ValueError(f"unreviewed assertion binding at {path}::{scope}#{index}")
                locators.append(dict(path=path, scope=scope, occurrence=index,
                    original=left, current=right, exact_ast=exact))
        old_defs = {tuple(row["scope"]): row for row in before.get(path, {}).get("definitions", [])}
        new_defs = {tuple(row["scope"]): row for row in after.get(path, {}).get("definitions", [])}
        for scope, definition in old_defs.items():
            if scope not in new_defs or definition["decorators"] != new_defs[scope]["decorators"]:
                raise ValueError(f"a function/decorator occurrence changed at {path}::{scope}")
    if total_new < total_old:
        raise ValueError("an original assertion occurrence disappeared")
    (out / "assertion-locators.json").write_text(json.dumps(locators, indent=2) + "\n")
    report = dict(base=base, head=c21_git(root, "rev-parse", "HEAD").decode().strip(),
        changed=changed, original_assertions=total_old, current_assertions=total_new,
        original_python_files=len(original), current_python_files=len(current),
        paired_changed_assertions=len(locators), exact_assertions=sum(x["exact_ast"] for x in locators),
        explicit_owner_bindings=[x for x in locators if not x["exact_ast"]], moves=C21_MOVES,
        scope="Source audit only; no runtime or architecture-test pass credit")
    (out / "manifest.json").write_text(json.dumps(report, indent=2) + "\n")
    return {key: value for key, value in report.items() if key != "explicit_owner_bindings"}


def c21_replay(root, patch, patch_blob, tree):
    raw = patch.read_bytes()
    if c21_git(root, "hash-object", "--stdin", data=raw).decode().strip() != patch_blob:
        raise ValueError("the C21 patch differs from its reviewed Git object")
    actual = c21_git(root, "write-tree").decode().strip()
    if actual == tree:
        return dict(status="already applied", tree=actual)
    if c21_git(root, "status", "--porcelain").strip() or c21_git(root, "rev-parse", "HEAD").decode().strip() != C21_BASE:
        raise ValueError("replay requires the clean exact composition base")
    subprocess.run(["git", "-C", str(root), "apply", "--index", "--binary", str(patch)], check=True)
    actual = c21_git(root, "write-tree").decode().strip()
    if actual != tree: raise ValueError("replayed C21 tree differs from the reviewed tree")
    return dict(status="applied", tree=actual, patch_blob=patch_blob, base=C21_BASE)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--base", default=C21_BASE)
    parser.add_argument("--patch", type=Path)
    parser.add_argument("--patch-blob")
    parser.add_argument("--tree")
    args = parser.parse_args()
    result = (c21_replay(args.root, args.patch, args.patch_blob, args.tree) if args.patch
              else c21_audit(args.root, args.output, args.base))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
