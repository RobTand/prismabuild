"""Check the final funding move against the accepted Root authority methods."""
from __future__ import annotations

import argparse
import ast
import builtins
import copy
import hashlib
import inspect
import json
from pathlib import Path
import subprocess
import symtable


ROOT = "bda51d769a86df3368366ef94a93d5a2e064988b"
MOVES = (
    ("src/prismabuild/pool.py", "PoolQueue", "_output_batch_authority",
     "_output_batch_authority"),
    ("src/prismabuild/pool.py", "PoolQueue", "_output_precommit_authority",
     "_output_precommit_authority"),
    ("tools/fleet/tier_loop.py", None, "_stranded_producer_dead",
     "stranded_producer_dead"),
    ("tools/fleet/tier_loop.py", None, "_stranded_prewrite_paths_absent",
     "stranded_prewrite_paths_absent"),
)
PROTECTED = tuple("src/prismabuild/" + name + ".py" for name in (
    "core", "digest_primitives", "immutable_files", "local_scratch")) + (
    "tools/fleet/local_scratch_profile.py", "src/prismabuild/kept_reads.py")


def definition(tree, name, owner=None):
    body = next(n.body for n in tree.body
                if isinstance(n, ast.ClassDef) and n.name == owner) if owner else tree.body
    return next(n for n in body if isinstance(n, ast.FunctionDef) and n.name == name)


class OwnerBindings(ast.NodeTransformer):
    def __init__(self, queue_method):
        self.queue_method = queue_method

    def visit_Name(self, node):
        if not self.queue_method and node.id == "queue":
            return ast.copy_location(ast.Attribute(
                ast.Name("self", ast.Load()), "queue", node.ctx), node)
        return node

    def visit_Attribute(self, node):
        if (not self.queue_method and isinstance(node.value, ast.Name)
                and node.value.id == "pool"
                and node.attr in ("record_store", "PoolContractError")):
            return ast.copy_location(ast.Name(node.attr, node.ctx), node)
        if (self.queue_method and isinstance(node.value, ast.Name)
                and node.value.id == "self" and node.attr == "root"):
            return ast.copy_location(ast.Attribute(ast.Attribute(
                ast.Name("self", ast.Load()), "queue", ast.Load()), "root", node.ctx), node)
        return self.generic_visit(node)


def unbound_globals(source, filename):
    module = symtable.symtable(source, filename, "exec")
    known = {s.get_name() for s in module.get_symbols()
             if s.is_assigned() or s.is_imported() or s.is_namespace()}
    known |= set(dir(builtins)) | {"__file__", "__name__", "__package__"}
    missing = []

    def walk(table):
        missing.extend((table.get_name(), symbol.get_name())
                       for symbol in table.get_symbols()
                       if symbol.is_referenced() and symbol.is_global()
                       and symbol.get_name() not in known)
        for child in table.get_children():
            walk(child)

    walk(module)
    return missing


def method_shape(node):
    node = copy.deepcopy(node)
    if (node.body and isinstance(node.body[0], ast.Expr)
            and isinstance(node.body[0].value, ast.Constant)
            and isinstance(node.body[0].value.value, str)):
        node.body[0].value.value = inspect.cleandoc(node.body[0].value.value)
    return ast.dump(node)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--ref", default="HEAD")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    def git(*words):
        return subprocess.check_output(["git", "-C", str(args.repo), *words])

    def blob(ref, path):
        return git("show", ref + ":" + path)

    source = blob(args.ref, "src/prismabuild/funding.py")
    target = ast.parse(source)
    mappings = []
    for path, owner, old, new in MOVES:
        original = definition(ast.parse(blob(ROOT, path)), old, owner)
        normalized = OwnerBindings(owner is not None).visit(copy.deepcopy(original))
        normalized.name = new
        if owner is None:
            normalized.args.args[0] = ast.arg("self")
        actual = definition(target, new, "FundingLedger")
        assert method_shape(normalized) == method_shape(actual), (path, old, new)
        mappings.append({"from": path + "::" + old,
                         "to": "funding.FundingLedger." + new})
    missing = unbound_globals(source.decode(), "funding.py")
    assert not missing, missing
    for path in PROTECTED:
        assert blob(ROOT, path) == blob(args.ref, path), path
    old_pool = ast.parse(blob(ROOT, "src/prismabuild/pool.py"))
    current_pool = ast.parse(blob(args.ref, "src/prismabuild/pool.py"))
    ledger = lambda tree: next(n for n in tree.body
                              if isinstance(n, ast.ClassDef) and n.name == "ResourceLedger")
    assert ast.dump(ledger(old_pool)) == ast.dump(ledger(current_pool))
    hooks = [n for n in ast.parse(blob(args.ref, "tests/conftest.py")).body
             if isinstance(n, ast.FunctionDef) and n.name == "pytest_terminal_summary"]
    assert len(hooks) == 1, "a later pytest hook would shadow the runtime evidence contract"
    assert b"prismabuild-test-runtime: " in blob(args.ref, "tests/conftest.py")
    report = dict(source=git("rev-parse", args.ref).decode().strip(), authority_source=ROOT,
                  full_method_equivalence=mappings, unbound_globals=missing,
                  documentation_owner_indentation_normalized=True,
                  protected_byte_exact=list(PROTECTED), generic_ledger_ast_exact=True,
                  one_standard_runtime_hook=True, source_sha256=hashlib.sha256(source).hexdigest(),
                  runtime_credit=False)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(dict(verified=True, source=report["source"], methods=len(mappings))))


if __name__ == "__main__":
    main()
