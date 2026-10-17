#!/usr/bin/env python3
"""Replay the C10 owner extraction against the pinned source using only ASTs."""

import argparse
import ast
from pathlib import Path
import subprocess
import textwrap

BASE = "578e54d0788c0f3b138f5dcf217e980322bb7726"
MOVED = {
    "tier_mint_lock": "mutation_lock",
    "hold_tier_host_memory": "_hold_host",
    "release_tier_host_memory": "_release_host",
    "_ram_tier_host": "_ram_host",
    "take_tier_advance": "take_advance",
    "cancel_tier_fence": "cancel_fence",
    "release_tier_holder": "release_unspent_best_effort",
    "reconcile_ram_host_holds": "reconcile_mirror",
    "transfer_tier_reservation": "handover",
    "transfer_tier_reservation_count": "handover_count",
    "mint_tier_capacity": "mint",
    "_apply_tier_capacity": "_apply_mint",
    "_reclaim_dead_markers": "_reclaim_mint",
    "mint_tier_capacity_guarded": "mint_guarded",
    "release_tier_holder_for_egress": "settle_egress",
}


class OwnerMove(ast.NodeTransformer):
    def __init__(self, method):
        self.method = method

    def visit_Name(self, node):
        if node.id == "tier_id":
            return ast.copy_location(ast.Attribute(ast.Name("self", ast.Load()),
                                                   "tier_id", node.ctx), node)
        if node.id == "TIER_RATE_KINDS":
            node.id = "RATE_KINDS"
        if node.id == "host" and self.method in {"hold_tier_host_memory", "release_tier_host_memory"}:
            return ast.copy_location(ast.Call(
                ast.Attribute(ast.Name("self", ast.Load()), "_ram_host", ast.Load()), [], []), node)
        return node

    def visit_Attribute(self, node):
        node = self.generic_visit(node)
        if isinstance(node.value, ast.Name) and node.value.id == "self":
            if node.attr == "RAM_HOST_MEMORY_PREFIX":
                node.value.id = "storage_tiers"
            elif node.attr == "ledger":
                node.attr = "_host_ledgers"
            elif node.attr == "root":
                node.value = ast.Attribute(node.value, "records", ast.Load())
        return node

    def visit_Call(self, node):
        original = node.func.attr if isinstance(node.func, ast.Attribute) else ""
        self_call = (isinstance(node.func, ast.Attribute)
                     and isinstance(node.func.value, ast.Name)
                     and node.func.value.id == "self")
        node = self.generic_visit(node)
        if not self_call:
            return node
        if original == "tier_ledger":
            return ast.copy_location(ast.Attribute(ast.Name("self", ast.Load()),
                                                   "_ledger", ast.Load()), node)
        if original in MOVED:
            node.func.attr = MOVED[original]
            if original in {"tier_mint_lock", "_ram_tier_host", "_apply_tier_capacity",
                            "hold_tier_host_memory", "release_tier_host_memory"}:
                node.args = node.args[1:]
        return node


def extract_owner(root):
    source = subprocess.check_output(
        ["git", "show", f"{BASE}:src/prismabuild/pool.py"], cwd=root, text=True)
    tree = ast.parse(source)
    queue = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "PoolQueue")
    chunks = []
    for original, renamed in MOVED.items():
        method = next(n for n in queue.body if isinstance(n, ast.FunctionDef) and n.name == original)
        method.name = renamed
        removed = {"tier_id"}
        if original in {"hold_tier_host_memory", "release_tier_host_memory"}:
            removed.add("host")
        method.args.args = [n for n in method.args.args if n.arg not in removed]
        if (method.body and isinstance(method.body[0], ast.Expr)
                and isinstance(method.body[0].value, ast.Constant)
                and isinstance(method.body[0].value.value, str)):
            method.body = method.body[1:]
        method = OwnerMove(original).visit(method)
        if original == "cancel_tier_fence":
            method.body[0] = ast.parse(
                "try:\n outcome = {'released': int(self._ledger.release(str(holder)))}\n"
                "except (OSError, ValueError):\n outcome = {'released': 0}").body[0]
        if original == "_reclaim_dead_markers":
            method.body.insert(0, ast.ImportFrom("pool", [ast.alias("_holder_is_dir")], 1))
        if original == "mint_tier_capacity_guarded":
            for n in ast.walk(method):
                if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "wanted_fn":
                    n.args = [ast.Name("self", ast.Load())]
        ast.fix_missing_locations(method)
        chunks.append(textwrap.indent(ast.unparse(method), "    "))
    return "\n\n".join(chunks) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--extract", type=Path, required=True)
    args = parser.parse_args()
    args.extract.write_text(extract_owner(args.root))


if __name__ == "__main__":
    main()
