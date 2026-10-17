#!/usr/bin/env python3
"""Rerunnable, source-only proof for C13's captured-entry extraction.

No PrismaBuild product imports or test execution. Compare ordered AST bodies,
canonical codecs, the eight C12 signatures and the deliberately unchanged
funding implementations. --after may be a commit or WORKTREE.
"""
from __future__ import annotations
import argparse
import ast
import copy
import json
from pathlib import Path
import subprocess


def c13_capture_source(pin, path):
    if pin == "WORKTREE":
        return Path(path).read_text()
    return subprocess.check_output(["git", "show", f"{pin}:{path}"], text=True)


def c13_capture_functions(tree):
    result = {}

    def visit(nodes, prefix=""):
        for node in nodes:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                result[prefix + node.name] = node
                visit(node.body, prefix + node.name + ".")
            elif isinstance(node, ast.ClassDef):
                visit(node.body, prefix + node.name + ".")
    visit(tree.body)
    return result


class C13CapturedBody(ast.NodeTransformer):
    def visit_Attribute(self, node):
        node = self.generic_visit(node)
        if isinstance(node.value, ast.Name) and node.value.id == "self":
            if node.attr == "_entry":
                node.attr = "entry"
        return node

    def visit_Call(self, node):
        node = self.generic_visit(node)
        if isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name):
            if node.func.value.id == "self" and node.func.attr in (
                    "retire_materialization", "retire_first_copy"):
                name = node.func.attr
                node.func.value = ast.Name(id="BatchLedger", ctx=ast.Load())
                index = 0 if name == "retire_materialization" else 3
                node.args.insert(index, ast.Name(id="batches", ctx=ast.Load()))
        return node


def c13_capture_body(fn, *, bound=False):
    body = copy.deepcopy(fn.body)
    if (body and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)):
        body.pop(0)
    if bound:
        bindings = {
            "batches = self._batches", "commitments = self._record",
            "commitments_path = self.ledger.path", "entry = entry._entry",
            "entry = self._entry", "entry = self._batches[batch_id]",
        }
        body = [node for node in body if ast.unparse(node) not in bindings]
        body = [C13CapturedBody().visit(node) for node in body]
    return ast.dump(ast.Module(body=body, type_ignores=[]))


def c13_capture_main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", default="24a063338446b71735d27162f08d43f5a9c0eac0")
    parser.add_argument("--after", default="WORKTREE")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    paths = ["src/prismabuild/" + name + ".py" for name in
             ("batch_ledger", "produced_output", "pool")]
    old = {p: c13_capture_functions(ast.parse(c13_capture_source(args.before, p)))
           for p in paths}
    new = {p: c13_capture_functions(ast.parse(c13_capture_source(args.after, p)))
           for p in paths}
    owner = paths[0]
    transitions = ["retire_materialization", "retire_first_copy",
                   "record_staged_paths", "file_retirement", "origin_gate"]
    decisions = ["begin_origin_retirement", "complete_origin_retirement",
                 "retirement_report", "quiet_retirement", "retirement_repins"]
    rows = []
    for name in transitions + decisions:
        before = old[owner]["BatchLedger." + name]
        target = ("BatchCapture." if name in transitions else "RetirementEntry.") + name
        after = new[owner][target]
        equal = c13_capture_body(before) == c13_capture_body(after, bound=True)
        assert equal, target
        assert "BatchLedger." + name not in new[owner], name
        rows.append({"before": "BatchLedger." + name, "after": target,
                     "ordered_body_equal": equal,
                     "decorator_change": "staticmethod to captured receiver",
                     "original_assertions": sum(isinstance(n, ast.Assert)
                                                for n in ast.walk(before))})
    codecs = {}
    for name in ("BatchLedger.capture", "BatchLedger.file"):
        codecs[name] = ast.dump(old[owner][name]) == ast.dump(new[owner][name])
        assert codecs[name], name
    # Do not normalize a stale receiver into an apparent equivalence proof.
    current_tree = ast.parse(c13_capture_source(args.after, owner))
    current_facets = next(n for n in current_tree.body
                          if isinstance(n, ast.ClassDef) and n.name == "BatchFacets")
    stale = [n.lineno for n in ast.walk(current_facets)
             if isinstance(n, ast.Attribute) and n.attr == "entry"
             and isinstance(n.value, ast.Name) and n.value.id == "self"]
    assert not stale, ("stale BatchFacets receiver", stale)
    facets = []
    for name, before in old[owner].items():
        if not name.startswith("BatchFacets."):
            continue
        after = new[owner][name]
        after = copy.deepcopy(after)
        for node in ast.walk(after):
            if isinstance(node, ast.Attribute) and node.attr == "_entry":
                node.attr = "entry"
        equal = ast.dump(before) == ast.dump(after)
        assert equal, name
        facets.append({"name": name, "ordered_body_equal": equal})
    stable = {}
    names = ["BatchLedger.precommit", "BatchLedger.restage_authority",
             "BatchLedger.publish_stage", "_funding_dir", "_read_funding",
             "_write_funding", "_delete_funding"]
    for name in names:
        a, b = old[owner][name], new[owner][name]
        equal = ast.dump(a) == ast.dump(b)
        assert equal, name
        stable[name] = {"signature": ast.unparse(b.args), "complete_ast_equal": True}
    a, b = old[paths[1]]["commit_batch"], new[paths[1]]["commit_batch"]
    assert ast.dump(a.args) == ast.dump(b.args)
    assert [ast.dump(x) for x in a.decorator_list] == [ast.dump(x) for x in b.decorator_list]
    stable["produced_output.commit_batch"] = {"signature": ast.unparse(b.args),
        "signature_and_decorators_equal": True,
        "change": "capture, named duplicate/charge questions, bound publication; funding order retained"}
    before = old[paths[2]]["PoolQueue._output_batch_authority"]
    index = next(i for i, n in enumerate(before.body)
                 if isinstance(n, ast.Try) and any(isinstance(x, ast.Attribute)
                     and x.attr == "capture" for x in ast.walk(n)))
    body = copy.deepcopy(before.body[index:])
    class PoolOwner(ast.NodeTransformer):
        def visit_Attribute(self, node):
            node = self.generic_visit(node)
            if isinstance(node.value, ast.Name) and node.value.id == "bl":
                return ast.Name(id=node.attr, ctx=ast.Load())
            if isinstance(node.value, ast.Name) and node.value.id == "self" and node.attr == "root":
                return ast.Name(id="queue_root", ctx=ast.Load())
            return node
    before = ast.dump(PoolOwner().visit(ast.Module(body=body, type_ignores=[])))
    after = c13_capture_body(new[owner]["BatchLedger.authorizes_mover"])
    assert before == after, "Pool ordered authority block"
    result = {"before": args.before, "after": args.after,
              "transitions": rows, "facets": facets, "canonical_codecs": codecs,
              "stable_C12": stable, "pool_authority_ordered_block_equal": True,
              "credit": "Source AST equivalence only; no runtime or test-pass credit"}
    with args.output.open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")


if __name__ == "__main__":
    c13_capture_main()
