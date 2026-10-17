"""Audit C24 source preservation without importing product code.

Run from a checkout with its base commit available. This compares moved bodies,
all remaining profile helpers, queue callers, assertions and test decorators.
"""
from __future__ import annotations

import ast
import json
from pathlib import Path
import subprocess

SCRATCH_AUDIT_BASE = "b34dbd4220adc8f9f81f3321f87bef26bce9317b"
SCRATCH_AUDIT_ROOT = Path(__file__).resolve().parents[2]
SCRATCH_AUDIT_MOVED = {
    "build_scratch_lifetime_selection": "build_scratch_lifetime_selection",
    "_scratch_lifetime_selections": "selections",
    "_scratch_lifetime_publication": "publication",
    "_scratch_lifetime_record": "validate_record",
    "_scratch_lifetime_intent": "_intent",
    **{name: name for name in (
        "_directory_birthtime", "_directory_identity", "_check_directory_link",
        "_check_scratch_links", "_check_scratch_permissions", "_prepare_scratch_directory",
        "_scratch_directory_identity", "_open_scratch_identity_chain", "_create_scratch_directory",
        "_uncreated_scratch_directory", "_check_registered_scratch_directory",
        "_remove_scratch_contents", "_clean_scratch_directory")},
}
SCRATCH_AUDIT_METHODS = {
    "_record_scratch_lifetimes": "register", "_scratch_lifetime_owner": "durable_owner",
    "_persist_scratch_lifetime": "_persist", "_finalize_scratch_lifetimes": "finalize",
}


class ScratchOwnershipNormalizer(ast.NodeTransformer):
    def visit_Name(self, node):
        node.id = SCRATCH_AUDIT_MOVED.get(node.id, node.id)
        return node

    def visit_Attribute(self, node):
        if (isinstance(node.value, ast.Name)
                and node.value.id in {"local_scratch", "_local_scratch", "scratch", "scratch_lifetime", "naming"}):
            if node.attr in SCRATCH_AUDIT_MOVED or node.attr in SCRATCH_AUDIT_MOVED.values() or node.attr.startswith("SCRATCH_LIFETIME_"):
                return ast.Name(SCRATCH_AUDIT_MOVED.get(node.attr, node.attr), ast.Load())
            if node.value.id == "naming":
                return ast.Name(node.attr, ast.Load())
        return self.generic_visit(node)

    def visit_Call(self, node):
        if isinstance(node.func, ast.Attribute):
            if node.func.attr in SCRATCH_AUDIT_METHODS:
                node.func = ast.Name(SCRATCH_AUDIT_METHODS[node.func.attr], ast.Load())
            elif (isinstance(node.func.value, ast.Name) and node.func.value.id == "scratch_lifetime"
                  and node.func.attr in SCRATCH_AUDIT_METHODS.values()):
                name = node.func.attr
                node.func = ast.Name(name, ast.Load())
                node.args = node.args[1 if name in {"durable_owner", "_persist"} else 2:]
        return self.generic_visit(node)

    def visit_FunctionDef(self, node):
        node.name = SCRATCH_AUDIT_MOVED.get(node.name, node.name)
        if (node.body and isinstance(node.body[0], ast.Expr)
                and isinstance(node.body[0].value, ast.Constant)
                and isinstance(node.body[0].value.value, str)):
            node.body = node.body[1:]
        return self.generic_visit(node)


def scratch_audit_source(path, *, baseline=False):
    if baseline:
        return subprocess.check_output(
            ["git", "-C", str(SCRATCH_AUDIT_ROOT), "show", f"{SCRATCH_AUDIT_BASE}:{path}"], text=True)
    return (SCRATCH_AUDIT_ROOT / path).read_text()


def scratch_audit_dump(node):
    return ast.dump(ScratchOwnershipNormalizer().visit(node), include_attributes=False)


def scratch_audit_functions(raw):
    return {n.name: n for n in ast.parse(raw).body if isinstance(n, ast.FunctionDef)}


def main():
    profile = "src/prismabuild/local_scratch.py"
    owner = "src/prismabuild/scratch_lifetime.py"
    before = scratch_audit_functions(scratch_audit_source(profile, baseline=True))
    after = scratch_audit_functions(scratch_audit_source(profile))
    moved = scratch_audit_functions(scratch_audit_source(owner))
    failures = []
    for old, new in SCRATCH_AUDIT_MOVED.items():
        if old in after or scratch_audit_dump(before[old]) != scratch_audit_dump(moved[new]):
            failures.append(f"moved-body:{old}")
    for name in after:
        if scratch_audit_dump(before[name]) != scratch_audit_dump(after[name]):
            failures.append(f"profile-body:{name}")
    assert len(before) == len(after) + len(SCRATCH_AUDIT_MOVED)
    for node in ast.walk(ast.parse(scratch_audit_source(profile))):
        if isinstance(node, (ast.Import, ast.ImportFrom)) and "scratch_lifetime" in ast.unparse(node):
            failures.append("profile-imports-lifetime")
    queuepath = "src/prismabuild/pool.py"
    oldtree = ast.parse(scratch_audit_source(queuepath, baseline=True))
    newtree = ast.parse(scratch_audit_source(queuepath))
    oldqueue = next(n for n in oldtree.body if isinstance(n, ast.ClassDef) and n.name == "PoolQueue")
    newqueue = next(n for n in newtree.body if isinstance(n, ast.ClassDef) and n.name == "PoolQueue")
    prior = {n.name: n for n in oldqueue.body if isinstance(n, ast.FunctionDef)}
    current = {n.name: n for n in newqueue.body if isinstance(n, ast.FunctionDef)}
    for name in SCRATCH_AUDIT_METHODS:
        if name in current:
            failures.append(f"legacy-queue-api:{name}")
    for name, node in current.items():
        if scratch_audit_dump(node) != scratch_audit_dump(prior[name]):
            failures.append(f"queue-body:{name}")
    changed = subprocess.check_output([
        "git", "-C", str(SCRATCH_AUDIT_ROOT), "diff", "--name-only", SCRATCH_AUDIT_BASE], text=True).splitlines()
    assertion_count = decorator_count = 0
    for path in changed:
        if not path.startswith("tests/") or not path.endswith(".py"):
            continue
        if subprocess.run(["git", "-C", str(SCRATCH_AUDIT_ROOT), "cat-file", "-e",
                           f"{SCRATCH_AUDIT_BASE}:{path}"], capture_output=True).returncode:
            continue
        trees = [ast.parse(scratch_audit_source(path, baseline=b)) for b in (True, False)]
        assertions = [[scratch_audit_dump(n) for n in ast.walk(t) if isinstance(n, ast.Assert)] for t in trees]
        decorators = [[scratch_audit_dump(d) for n in ast.walk(t) if isinstance(n, ast.FunctionDef)
                       for d in n.decorator_list] for t in trees]
        assertion_count += len(assertions[0])
        decorator_count += len(decorators[0])
        if assertions[0] != assertions[1] or decorators[0] != decorators[1]:
            failures.append(f"assertions-or-decorators:{path}")
    print(json.dumps(dict(passed=not failures, failures=failures, moved_functions=len(SCRATCH_AUDIT_MOVED),
                         unchanged_profile_functions=len(after), queue_methods_preserved=len(current),
                         original_assertions=assertion_count, original_decorators=decorator_count), indent=2))
    return bool(failures)


if __name__ == "__main__":
    raise SystemExit(main())
