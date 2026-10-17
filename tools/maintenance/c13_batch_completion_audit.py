#!/usr/bin/env python3
"""Source-only C13 completion proof; never imports product code or runs tests."""
from __future__ import annotations
import argparse
import ast
import copy
import json
from pathlib import Path
import subprocess

from c13_contract_names import C13_LEAF_NAMES, C13_OWNER_NAMES
from c13_batch_capture_audit import (
    c13_capture_functions, c13_capture_source, c13_capture_identity,
)

C13_DAC = "dac2006656264b4bd55e4e3837331205a9740fd3"
C13_PUBLIC = {
    "batch_record": "bl.BatchLedger.record",
    "batch_records": "bl.BatchLedger.records",
    "BATCH_STATE_COMMITTED": "oc.BATCH_STATE_COMMITTED",
    "BATCH_STATE_RETIRING": "oc.BATCH_STATE_RETIRING",
    "BATCH_STATE_RECLAIMED": "oc.BATCH_STATE_RECLAIMED",
    "ProducedOutputError": "oc.ProducedOutputError",
}


class C13CompletionIdentity(ast.NodeTransformer):
    """Exact leaf definitions, module aliases and pure bound scope addresses."""
    def visit_Name(self, node):
        node.id = {v: k for k, v in C13_LEAF_NAMES.items()}.get(node.id, node.id)
        return node

    def visit_FunctionDef(self, node):
        node.name = {v: k for k, v in C13_LEAF_NAMES.items()}.get(node.name, node.name)
        node.name = {v: k for k, v in C13_OWNER_NAMES.items()}.get(node.name, node.name)
        return self.generic_visit(node)

    def visit_Attribute(self, node):
        node = self.generic_visit(node)
        if isinstance(node.value, ast.Name) and node.value.id == "bl":
            return ast.Name(id=node.attr, ctx=ast.Load())
        if (isinstance(node.value, ast.Name) and node.value.id == "queue"
                and node.attr == "root"):
            return ast.Name(id="queue_root", ctx=ast.Load())
        return node


def c13_completion_ast(nodes):
    if isinstance(nodes, list):
        nodes = ast.Module(body=nodes, type_ignores=[])
    return ast.dump(C13CompletionIdentity().visit(c13_capture_identity(nodes)))


def c13_completion_with(fn):
    return next(n for n in fn.body if isinstance(n, ast.With)).body


def c13_completion_assignment(nodes, variable):
    return next(i for i, node in enumerate(nodes) if isinstance(node, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == variable
                        for t in node.targets))


def c13_completion_main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", default=C13_DAC)
    parser.add_argument("--after", default="WORKTREE")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    names = ("batch_ledger", "produced_output", "output_contract", "pool",
             "action_edges", "stage_release", "produced_spool")
    paths = {n: "src/prismabuild/" + n + ".py" for n in names}
    sources = {n: c13_capture_source(args.after, p) for n, p in paths.items()}
    before_sources = {n: c13_capture_source(args.before, p) for n, p in paths.items()}
    trees = {n: ast.parse(s) for n, s in sources.items()}
    old = {n: c13_capture_functions(ast.parse(s)) for n, s in before_sources.items()}
    new = {n: c13_capture_functions(t) for n, t in trees.items()}
    # The entire pure contract differs only by definition/caller names.
    assert c13_completion_ast(ast.parse(before_sources["output_contract"])) == c13_completion_ast(trees["output_contract"])
    renamed = []
    for previous, current in C13_OWNER_NAMES.items():
        assert c13_completion_ast(old["batch_ledger"][previous]) == c13_completion_ast(new["batch_ledger"][current]), current
        renamed.append({"before": previous, "after": current,
                        "complete_ast_equal_after_exact_names": True})
    # Consumer preparation, queue index and publication retain their old order.
    declared = c13_completion_with(old["produced_output"]["declare_origin_consumer"])
    index = c13_completion_assignment(declared, "directory")
    new_declaration = new["batch_ledger"]["file_consumer_declaration"].body
    assert c13_completion_ast(declared[index:]) == c13_completion_ast(
        [n for n in new_declaration if not isinstance(n, ast.ImportFrom)])
    released = c13_completion_with(old["produced_output"]["_release_origin_consumer_locked"])
    index = c13_completion_assignment(released, "directory")
    prepare = new["batch_ledger"]["ConsumerReleaseFile.prepare"].body
    assert c13_completion_ast(released[index:index + 3]) == c13_completion_ast(prepare[:-1])
    file_method = new["batch_ledger"]["ConsumerReleaseFile.file"].body
    old_file = next(n for n in released[index + 3:] if isinstance(n, ast.Try))
    new_file = next(n for n in file_method if isinstance(n, ast.Try))
    assert c13_completion_ast(old_file) == c13_completion_ast(new_file)
    constructor = new["batch_ledger"]["ConsumerReleaseFile.__init__"]
    assert len(constructor.body) == 3 and all(isinstance(n, ast.Assign) for n in constructor.body)
    current_release = c13_completion_with(new["produced_output"]["_release_origin_consumer_locked"])
    effects = [ast.unparse(n.func) for stmt in current_release for n in ast.walk(stmt)
               if isinstance(n, ast.Call) and ast.unparse(n.func) in
               ("bl.ConsumerReleaseFile.prepare", "_index_release", "publication.file")]
    assert effects[-3:] == ["bl.ConsumerReleaseFile.prepare", "_index_release", "publication.file"]
    backfill = old["produced_output"]["_backfill_attempt"]
    current_backfill = copy.deepcopy(new["batch_ledger"]["backfill_attempt"])
    current_backfill.name = backfill.name
    assert c13_completion_ast(backfill) == c13_completion_ast(current_backfill)
    # Canonical prewrite intake remains the old validator and wire recipe.
    assert c13_completion_ast(old["batch_ledger"]["read_prewrite"]) == c13_completion_ast(new["batch_ledger"]["read_prewrite"])
    for helper in ("_funding_dir", "_read_funding", "_write_funding", "_delete_funding"):
        assert ast.dump(old["batch_ledger"][helper]) == ast.dump(new["batch_ledger"][helper]), helper
    # The public names are permanent object bindings, with no forwarding code.
    assignments = {t.id: n.value for n in trees["produced_output"].body
                   if isinstance(n, ast.Assign) for t in n.targets if isinstance(t, ast.Name)}
    exports = ast.literal_eval(assignments["__all__"])
    for name, target in C13_PUBLIC.items():
        assert name in exports and ast.unparse(assignments[name]) == target, name
    for retired in ("reservation_holder", "reserve_working_minimum"):
        assert retired not in new["produced_output"] and retired not in exports
    # Production crosses actual owners only; C12's four explicit seams remain.
    private = []
    raw = []
    for name, tree in trees.items():
        for node in ast.walk(tree):
            if (isinstance(node, ast.Subscript) and isinstance(node.value, ast.Call)
                    and isinstance(node.value.func, ast.Attribute)
                    and node.value.func.attr == "entries"
                    and isinstance(node.slice, ast.Constant)
                    and node.slice.value == "batches"):
                raise AssertionError((name, node.lineno, "captured entries are not the old wire document"))
            if (isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name)
                    and node.value.id in ("bl", "oc") and node.attr.startswith("_")):
                private.append({"module": name, "line": node.lineno,
                                "name": node.value.id + "." + node.attr})
                assert node.value.id == "bl" and node.attr in {
                    "_funding_dir", "_read_funding", "_write_funding", "_delete_funding"}, private[-1]
            if (name == "produced_output" and isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr in ("capture", "file")
                    and "BatchLedger" in ast.unparse(node.func)):
                raw.append(ast.unparse(node))
    assert not raw
    for name in ("batch_ledger", "output_contract"):
        for node in ast.walk(trees[name]):
            if isinstance(node, ast.ImportFrom):
                assert not (node.module in ("prismabuild.pool", "pool")
                            or node.module in (None, "prismabuild")
                            and any(a.name == "pool" for a in node.names))
    pool_source = c13_capture_source(args.before, paths["pool"])
    assert "from .record_store import AmbiguousClaimHolder, PoolError, PoolContractError" in pool_source
    out = {"before": args.before, "after": args.after,
           "whole_output_contract_ast_equal_after_exact_names": True,
           "renamed_domain_functions": renamed,
           "consumer_declaration_block_exact": True,
           "consumer_release_prepare_and_publish_blocks_exact": True,
           "consumer_release_order": effects[-3:], "release_constructor_has_no_effects": True,
           "backfill_complete_ast_equal": True, "prewrite_loader_complete_ast_equal": True,
           "legacy_funding_complete_ast_equal": True,
           "PoolContractError_is_existing_RecordStore_binding": True,
           "documented_public_bindings": C13_PUBLIC, "retired_wrappers_absent": True,
           "production_private_dependencies_only_C12": private,
           "production_raw_commitment_io_absent": True, "no_entries_old_document_subscripts": True, "lower_owners_import_no_Pool": True,
           "credit": "Read-only source/AST proof; no native, runtime, workload or compatibility pass credit"}
    with args.out.open("x") as stream:
        json.dump(out, stream, indent=2)
        stream.write("\n")


if __name__ == "__main__":
    c13_completion_main()
