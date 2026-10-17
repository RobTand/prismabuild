"""Pin lifecycle source effects and preserve native test definitions using only stdlib.

Run ``python3 tests/lifecycle_source_audit.py --out DIRECTORY`` before
and after the move. The exact base is a Git object, never a mutable checkout.
Runtime collection and physical outcomes require a separate admitted PB run.
"""
from __future__ import annotations

import argparse
import ast
from collections import Counter
import copy
import hashlib
import io
import json
from pathlib import Path
import re
import subprocess
import tarfile
import tokenize

LIFECYCLE_AUDIT_BASE = "578e54d0788c0f3b138f5dcf217e980322bb7726"
LIFECYCLE_AUDIT_ROOT = Path(__file__).resolve().parents[1]
LIFECYCLE_AUDIT_RENAMES = {
    "_same_claim": "same_claim", "_check_claim_lease_identity": "check_lease_identity",
    "_CLAIM_IDENTITY": "CLAIM_IDENTITY_FIELDS",
    "_transition_locked": "action_transition_lock", "_scope_from_record": "scope_for_record",
}
LIFECYCLE_AUDIT_OWNERS = (
    "_entomb_claim", "finish", "_finish_late", "_retry_late_finish", "_archive_late_finish",
    "reap_stale", "quarantine_orphans", "sweep_finish_tombstones", "sweep_ready_transitions",
    "_file_unreadable", "reclaim_terminal_reservation", "_archive_earlier_terminal",
)
LIFECYCLE_AUDIT_SOURCES = {}


def lifecycle_audit_git(*args):
    return subprocess.check_output(["git", "-C", str(LIFECYCLE_AUDIT_ROOT), *args])


def lifecycle_audit_text(path, revision=None):
    if revision:
        if revision not in LIFECYCLE_AUDIT_SOURCES:
            raw = lifecycle_audit_git("archive", revision, "src", "tests", "tools")
            with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
                LIFECYCLE_AUDIT_SOURCES[revision] = {
                    member.name: archive.extractfile(member).read().decode()
                    for member in archive if member.isfile() and member.name.endswith(".py")}
        return LIFECYCLE_AUDIT_SOURCES[revision][path]
    return (LIFECYCLE_AUDIT_ROOT / path).read_text()


class LifecycleAuditNormalizer(ast.NodeTransformer):
    def visit_Name(self, node):
        node.id = LIFECYCLE_AUDIT_RENAMES.get(node.id, node.id)
        return node

    def visit_Attribute(self, node):
        node.attr = LIFECYCLE_AUDIT_RENAMES.get(node.attr, node.attr)
        if isinstance(node.value, ast.Name) and node.value.id in {"pool", "claim_identity"}:
            if node.attr in {"same_claim", "check_lease_identity", "CLAIM_IDENTITY_FIELDS"}:
                return ast.Name(id=node.attr, ctx=node.ctx)
        return self.generic_visit(node)

    def visit_Constant(self, node):
        if isinstance(node.value, str):
            for old, new in LIFECYCLE_AUDIT_RENAMES.items():
                node.value = re.sub(r"(?<![\w])" + re.escape(old) + r"(?![\w])", new, node.value)
        return node


def lifecycle_audit_dump(node):
    return ast.dump(LifecycleAuditNormalizer().visit(copy.deepcopy(node)), include_attributes=False)


def lifecycle_audit_definitions(raw):
    result = {}

    def walk(body, prefix=""):
        for node in body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                name = prefix + node.name
                if name in result:
                    occurrence = 2
                    while f"{name}@{occurrence}" in result:
                        occurrence += 1
                    name = f"{name}@{occurrence}"
                result[name] = node
                walk(node.body, name + ".")
    walk(ast.parse(raw).body)
    return result


def lifecycle_audit_effects(node):
    rows = []

    def walk(value, context=()):
        if isinstance(value, ast.Call):
            rows.append({"line": value.lineno, "call": ast.unparse(value), "context": list(context)})
        if isinstance(value, ast.AST):
            for field, child in ast.iter_fields(value):
                nested = context
                if isinstance(value, ast.If) and field in {"body", "orelse"}:
                    nested += (("if " if field == "body" else "else ") + ast.unparse(value.test),)
                if isinstance(value, (ast.With, ast.AsyncWith)) and field == "body":
                    nested += tuple("with " + ast.unparse(item.context_expr) for item in value.items)
                walk(child, nested)
        elif isinstance(value, list):
            for child in value:
                walk(child, context)
    walk(node)
    return rows


def lifecycle_audit_inventory(revision=None):
    args = ("ls-tree", "-r", "--name-only", revision) if revision else ("ls-files",)
    paths = lifecycle_audit_git(*args).decode().splitlines()
    functions, tests, uses, hashes = {}, {}, [], {}
    for path in paths:
        if not path.endswith(".py") or not path.startswith(("src/", "tests/", "tools/")):
            continue
        raw = lifecycle_audit_text(path, revision)
        hashes[path] = hashlib.sha256(raw.encode()).hexdigest()
        definitions = lifecycle_audit_definitions(raw)
        for name, node in definitions.items():
            key = f"{path}::{name}"
            functions[key] = {"line": node.lineno, "end": node.end_lineno,
                              "ast_sha256": hashlib.sha256(lifecycle_audit_dump(node).encode()).hexdigest()}
            if path.startswith("tests/") and isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                tests[key] = {
                    "decorators": [lifecycle_audit_dump(d) for d in node.decorator_list],
                    "assertions": [lifecycle_audit_dump(n) for n in ast.walk(node) if isinstance(n, ast.Assert)],
                    "native_definition": name.startswith("test_") or ".test_" in name,
                }
        for token in tokenize.generate_tokens(io.StringIO(raw).readline):
            if any(re.search(r"(?<![\w])" + re.escape(old) + r"(?![\w])", token.string)
                   for old in LIFECYCLE_AUDIT_RENAMES):
                uses.append({"path": path, "line": token.start[0], "kind": tokenize.tok_name[token.type],
                             "text": token.string})
    pool = lifecycle_audit_definitions(lifecycle_audit_text("src/prismabuild/pool.py", revision))
    return {"revision": revision, "hashes": hashes, "functions": functions, "tests": tests,
            "seam_uses": uses, "effects": {name: lifecycle_audit_effects(pool["PoolQueue." + name])
            for name in LIFECYCLE_AUDIT_OWNERS if "PoolQueue." + name in pool}}


LIFECYCLE_AUDIT_MOVES = {
    name: name for name in ("_finish_late", "_retry_late_finish", "_archive_late_finish",
                            "_archive_earlier_terminal", "_file_unreadable", "_sweep_orphan_requeue_staging")
} | {name: "_" + name for name in ("quarantine_orphans", "sweep_ready_transitions", "sweep_widowed_leases")}


class LifecycleBodyNormalizer(LifecycleAuditNormalizer):
    def visit_Expr(self, node):
        if isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name) and node.value.func.id == "_legal_destination":
            return None
        return self.generic_visit(node)

    def visit_Name(self, node):
        if node.id == "self":
            node.id = "queue"
        elif node.id == "invalid":
            node.id = "AmbiguousClaimHolder"
        return super().visit_Name(node)

    def visit_Attribute(self, node):
        if isinstance(node.value, ast.Name) and node.value.id == "pool" and node.attr in {
                "_now", "DEFAULT_MAX_ATTEMPTS", "POOL_OUTCOME_SCHEMA_V1", "UNREADABLE_HEAD_BYTES"}:
            return ast.Name(node.attr, ast.Load())
        return super().visit_Attribute(node)

    def visit_ImportFrom(self, node):
        if node.level == 1 and (node.module == "pool" or
                                node.module is None and [a.name for a in node.names] == ["pool"]):
            return None
        return node

    def visit_Call(self, node):
        if isinstance(node.func, ast.Attribute) and node.func.attr in LIFECYCLE_AUDIT_MOVES:
            node.args.insert(0, node.func.value)
            node.func = ast.Name(LIFECYCLE_AUDIT_MOVES[node.func.attr], ast.Load())
        node = self.generic_visit(node)
        if isinstance(node.func, ast.Name) and node.func.id == "check_lease_identity":
            node.keywords = [k for k in node.keywords if k.arg != "invalid"]
        return node


def lifecycle_audit_body(node):
    body = copy.deepcopy(node.body)
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
        body = body[1:]
    return ast.dump(LifecycleBodyNormalizer().visit(ast.Module(body=body, type_ignores=[])),
                    include_attributes=False)


def lifecycle_audit_owner_equivalence():
    old = lifecycle_audit_definitions(lifecycle_audit_text("src/prismabuild/pool.py", LIFECYCLE_AUDIT_BASE))
    current = lifecycle_audit_definitions(lifecycle_audit_text("src/prismabuild/pool.py"))
    owner_path = LIFECYCLE_AUDIT_ROOT / "src/prismabuild/lifecycle.py"
    owner = lifecycle_audit_definitions(owner_path.read_text()) if owner_path.exists() else {}
    checks = {}
    for old_name, new_name in LIFECYCLE_AUDIT_MOVES.items():
        if new_name in owner:
            checks["moved-body:" + old_name] = lifecycle_audit_body(old["PoolQueue." + old_name]) == lifecycle_audit_body(owner[new_name])
    for old_name, new_name in (("_entomb_claim", "_entomb_claim"),
                              ("_transition_locked", "action_transition_lock"),
                              ("_scope_from_record", "scope_for_record")):
        checks["boundary-body:" + old_name] = lifecycle_audit_body(old["PoolQueue." + old_name]) == lifecycle_audit_body(current["PoolQueue." + new_name])
    checks["ResourceLedger-full-AST"] = ast.dump(old["ResourceLedger"], include_attributes=False) == ast.dump(current["ResourceLedger"], include_attributes=False)
    before_store = lifecycle_audit_definitions(lifecycle_audit_text("src/prismabuild/record_store.py", LIFECYCLE_AUDIT_BASE))
    after_store = lifecycle_audit_definitions(lifecycle_audit_text("src/prismabuild/record_store.py"))
    checks["RecordStore-full-body"] = lifecycle_audit_body(before_store["RecordStore"]) == lifecycle_audit_body(after_store["RecordStore"])
    checks["error-vocabulary"] = ast.dump(old["AmbiguousClaimHolder"], include_attributes=False) == ast.dump(after_store["AmbiguousClaimHolder"], include_attributes=False)
    identity = lifecycle_audit_definitions(lifecycle_audit_text("src/prismabuild/claim_identity.py"))
    for old_name, new_name in (("_same_claim", "same_claim"), ("_check_claim_lease_identity", "check_lease_identity")):
        checks["identity-body:" + old_name] = lifecycle_audit_body(old[old_name]) == lifecycle_audit_body(identity[new_name])
    for path, names in (("src/prismabuild/scratch_lifetime.py", ("register_attempt", "_persist", "finalize")),):
        before = lifecycle_audit_definitions(lifecycle_audit_text(path, LIFECYCLE_AUDIT_BASE))
        after = lifecycle_audit_definitions(lifecycle_audit_text(path))
        for name in names:
            checks["scratch-body:" + name] = lifecycle_audit_body(before[name]) == lifecycle_audit_body(after[name])
    public = ("finish", "reap_stale", "sweep_finish_tombstones", "sweep_ready_transitions",
              "sweep_widowed_leases", "quarantine_orphans", "reclaim_terminal_reservation", "_defer_unstarted_claim")
    for name in public:
        a, b = old["PoolQueue." + name], current["PoolQueue." + name]
        checks["public-signature:" + name] = (ast.dump(a.args) == ast.dump(b.args) and
            [ast.dump(d) for d in a.decorator_list] == [ast.dump(d) for d in b.decorator_list])
    # These are the original decisions before the delegated conclusion starts.
    before_loop = next(n for n in old["PoolQueue.reap_stale"].body if isinstance(n, ast.For))
    after_loop = next(n for n in current["PoolQueue.reap_stale"].body if isinstance(n, ast.For))
    before = before_loop.body[1].body if isinstance(before_loop.body[1], ast.With) else before_loop.body[0].body
    after = after_loop.body[1].body if isinstance(after_loop.body[1], ast.With) else after_loop.body[0].body
    cut = next(i for i, n in enumerate(before) if isinstance(n, ast.Assign) and any(
        isinstance(c, ast.Attribute) and c.attr == "cleanup_action_containers" for c in ast.walk(n)))
    checks["reaper-acted-from-prefix"] = lifecycle_audit_body(ast.Module(body=before[:cut])) == lifecycle_audit_body(ast.Module(body=after[:cut]))
    return {"checks": checks, "passed": all(checks.values()), "moved_owners": LIFECYCLE_AUDIT_MOVES}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    before = lifecycle_audit_inventory(LIFECYCLE_AUDIT_BASE)
    after = lifecycle_audit_inventory()
    equivalence = lifecycle_audit_owner_equivalence()
    (args.out / "owner-equivalence.json").write_text(json.dumps(equivalence, indent=2) + "\n")
    failures = []
    failures.extend(name for name, passed in equivalence["checks"].items() if not passed)
    for key, old in before["tests"].items():
        new = after["tests"].get(key)
        if new is None or old["decorators"] != new["decorators"]:
            failures.append("definition-or-decorators:" + key)
        if new is None or Counter(old["assertions"]) - Counter(new["assertions"]):
            failures.append("assertion-condition:" + key)
    for name, value in (("base", before), ("current", after)):
        path = args.out / (name + "-source.json")
        if path.exists() and name == "base":
            if json.loads(path.read_bytes()) != value:
                raise ValueError("refusing to replace a differing pinned baseline")
        else:
            path.write_text(json.dumps(value, indent=2) + "\n")
    summary = {"base": LIFECYCLE_AUDIT_BASE,
               "head": lifecycle_audit_git("rev-parse", "HEAD").decode().strip(),
               "base_functions": len(before["functions"]), "base_test_definitions": len(before["tests"]),
               "base_assertions": sum(len(t["assertions"]) for t in before["tests"].values()),
               "base_decorators": sum(len(t["decorators"]) for t in before["tests"].values()),
               "base_seam_uses": len(before["seam_uses"]), "failures": failures, "passed": not failures}
    (args.out / "source-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary))
    return bool(failures)


if __name__ == "__main__":
    raise SystemExit(main())
