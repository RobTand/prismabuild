"""Pin lifecycle source effects and preserve native test definitions using only stdlib.

Run ``python3 tools/maintenance/audit_lifecycle.py --out DIRECTORY`` before
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
LIFECYCLE_AUDIT_ROOT = Path(__file__).resolve().parents[2]
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    before = lifecycle_audit_inventory(LIFECYCLE_AUDIT_BASE)
    after = lifecycle_audit_inventory()
    failures = []
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
