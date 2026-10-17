#!/usr/bin/env python3
"""Replay the C10 owner extraction against the pinned source using only ASTs."""

import argparse
import ast
import copy
import json
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

RAW = {
    "holder_tokens": "holdings", "held": "holdings", "held_keys": "holders",
    "capacity": "mint_supply", "capacity_census": "mint_census",
    "available": "free_supply", "acquire": "take", "begin_acquire": "begin_take",
    "commit_acquire": "commit_take", "abandon_acquire": "abandon_take",
    "acquisitions_of": "pending_takes", "unparsed_acquisitions_of": "unparsed_takes",
    "sweep_stale_acquisitions": "reap_pending_takes", "transfer": "handover",
    "transfer_count": "handover_count", "transfer_tokens": "handover_named",
    "ensure_capacity": "grow_supply", "retire_free_capacity": "trim_supply",
    "retire_held": "retire_duplicate", "holder_census": "holder_observations",
    "_mutation_locked": "mutation_lock", "last_token_shortage": "shortage",
    "_census_glob": "token_paths", "_census_scan": "entry_paths",
    "_strict_census": "uses_tier_census", "_minted_and_dead_names": "mint_history",
    "last_census_report": "mint_issues",
    "census_report_path": "census_report",
}
AGGREGATE = {"tier_ids": "ids", "tier_holdings": "for_action",
             "release_tier_rate_reservations": "release_rates_all",
             "release_tier_reservations": "release_unspent_all"}


def tier_call(node):
    return (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            and node.func.attr in {"tier_ledger", "tier"})


def caller_aliases(tree, seeds=None):
    parents, scopes = {}, {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parents[child] = node
    def scope(node):
        while node is not tree and not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            node = parents[node]
        return node
    functions = {n.name: n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    for node in ast.walk(tree):
        scopes[node] = scope(node)
    aliases = {n: set() for n in set(scopes.values())}
    for function in functions.values():
        aliases[function].update((seeds or {}).get(function.name, ()))
        for arg in (*function.args.args, *function.args.kwonlyargs):
            annotation = arg.annotation
            if ((isinstance(annotation, ast.Attribute) and annotation.attr == "TierLedger")
                    or isinstance(annotation, ast.Name) and annotation.id == "TierLedger"):
                aliases[function].add(arg.arg)
    returns, values = {}, {}
    def is_tier(node, enclosing):
        return tier_call(node) or (isinstance(node, ast.Name) and node.id in aliases[enclosing])
    def shape(node, enclosing):
        if is_tier(node, enclosing):
            return True
        if isinstance(node, (ast.Tuple, ast.List)):
            return tuple(shape(n, enclosing) for n in node.elts)
        if isinstance(node, ast.Name):
            return values.get((enclosing, node.id), returns.get(node.id, False))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            return returns.get(node.func.id, False)
        return False
    def bind(target, value, enclosing):
        if isinstance(target, ast.Name):
            values[enclosing, target.id] = value
            if value is True:
                aliases[enclosing].add(target.id)
        elif isinstance(target, (ast.Tuple, ast.List)) and isinstance(value, tuple):
            for name, item in zip(target.elts, value):
                bind(name, item, enclosing)
    for _ in range(12):
        before = (sum(map(len, aliases.values())), repr(returns), repr(values))
        for function in functions.values():
            parent = scope(parents[function])
            local = {arg.arg for arg in function.args.args}
            local.update(n.id for n in ast.walk(function)
                         if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store)
                         and scopes[n] is function)
            aliases[function].update(aliases[parent] - local)
        for node in ast.walk(tree):
            enclosing = scopes[node]
            if isinstance(node, ast.Return) and node.value is not None:
                value = shape(node.value, enclosing)
                if value and hasattr(enclosing, "name"):
                    returns[enclosing.name] = value
            if isinstance(node, (ast.Assign, ast.AnnAssign)) and node.value is not None:
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                for target in targets:
                    bind(target, shape(node.value, enclosing), enclosing)
            if isinstance(node, ast.Call):
                name = (node.func.id if isinstance(node.func, ast.Name) else
                        node.func.attr if isinstance(node.func, ast.Attribute) else "")
                called = functions.get(name)
                if called is None or name == "held_names_visible":
                    continue
                params = called.args.args
                if isinstance(node.func, ast.Attribute) and params and params[0].arg == "self":
                    params = params[1:]
                for param, arg in zip(params, node.args):
                    if is_tier(arg, enclosing):
                        aliases[called].add(param.arg)
        if (sum(map(len, aliases.values())), repr(returns), repr(values)) == before:
            break
    return scopes, aliases


class CallerMove(ast.NodeTransformer):
    def __init__(self, scopes, aliases):
        self.scopes, self.aliases = scopes, aliases
        self.scoped = set()
        self.need_import = False

    def visit(self, node):
        previous = self.scoped
        if node in self.scopes:
            self.scoped = self.aliases[self.scopes[node]]
        try:
            return super().visit(node)
        finally:
            self.scoped = previous

    def is_tier(self, node):
        return tier_call(node) or isinstance(node, ast.Name) and node.id in self.scoped

    def authority(self):
        self.need_import = True
        return ast.keyword("authority", ast.parse("tier_holdings.Unspent.NEVER_STARTED", mode="eval").body)

    def visit_Attribute(self, node):
        bound = self.is_tier(node.value)
        node = self.generic_visit(node)
        if node.attr == "tier_ledger":
            node.attr = "tier"
        elif node.attr == "TIER_RATE_KINDS":
            node.value = ast.Name("tier_holdings", ast.Load())
            node.attr = "RATE_KINDS"
            self.need_import = True
        elif bound:
            if node.attr in {"held_dir", "free_dir", "minted_dir", "base"}:
                node.value = ast.Attribute(node.value, "paths", ast.Load())
                node.attr = {"held_dir": "held", "free_dir": "free", "minted_dir": "minted"}.get(node.attr, node.attr)
            else:
                node.attr = RAW.get(node.attr, node.attr)
        return node

    def visit_Call(self, node):
        old = (node.func.attr if isinstance(node.func, ast.Attribute) else
               node.func.id if isinstance(node.func, ast.Name) else "")
        receiver = node.func.value if isinstance(node.func, ast.Attribute) else None
        bound = receiver is not None and self.is_tier(receiver)
        held = (old == "held_names_visible" and node.args and self.is_tier(node.args[0]))
        node = self.generic_visit(node)
        if old in AGGREGATE:
            self.need_import = True
            node.func = ast.Attribute(ast.Name("tier_holdings", ast.Load()), AGGREGATE[old], ast.Load())
            node.args.insert(0, ast.Attribute(receiver, "records", ast.Load()) if old == "tier_ids" else receiver)
            if old == "release_tier_reservations":
                node.keywords.append(self.authority())
        elif old in MOVED and not old.startswith("_") and node.args:
            if old in {"hold_tier_host_memory", "release_tier_host_memory"}:
                host, *remaining = node.args
                tier = ast.BinOp(ast.Constant("ram:"), ast.Add(), ast.Call(ast.Name("str", ast.Load()), [host], []))
                node.func = ast.Attribute(ast.Call(ast.Attribute(receiver, "tier", ast.Load()), [tier], []), MOVED[old], ast.Load())
                node.args = remaining
                return node
            tier, *remaining = node.args
            node.func = ast.Attribute(ast.Call(ast.Attribute(receiver, "tier", ast.Load()), [tier], []), MOVED[old], ast.Load())
            node.args = remaining
            if old == "release_tier_holder":
                node.keywords.append(self.authority())
        elif held:
            node.func = ast.Attribute(node.args.pop(0), "holder_state", ast.Load())
        elif old == "held_grants" and node.args and self.is_tier(node.args[0]):
            node.func = ast.Attribute(node.args.pop(0), "held_grants", ast.Load())
        elif bound and old in {"release", "release_except", "release_kinds", "release_count"}:
            node.func.attr = "release_rates" if old == "release_kinds" else "release_unspent"
            if old == "release_kinds":
                node.args = node.args[:1]
            else:
                if old == "release_except":
                    node.keywords.append(ast.keyword("keep", node.args.pop(1)))
                if old == "release_count":
                    node.keywords.append(ast.keyword("counts", node.args.pop(1)))
                node.keywords.append(self.authority())
        return node

    def visit_Constant(self, node):
        if isinstance(node.value, str):
            if node.value == "tier_ledger":
                node.value = "tier"
            elif node.value.endswith("PoolQueue.tier_ledger"):
                node.value = node.value.removesuffix("tier_ledger") + "tier"
        return node


def migrate_callers(root, path):
    source = path.read_text()
    original = ast.parse(source)
    tree = copy.deepcopy(original)
    scopes, aliases = caller_aliases(tree)
    mover = CallerMove(scopes, aliases)
    tree = mover.visit(tree)
    original_nodes = {(n.lineno, n.col_offset, n.end_lineno, n.end_col_offset, type(n)): n
                      for n in ast.walk(original) if isinstance(n, ast.expr)}
    replacements = []
    lines = source.splitlines(keepends=True)
    starts = [0]
    for line in lines:
        starts.append(starts[-1] + len(line.encode()))
    for node in ast.walk(tree):
        key = (getattr(node, "lineno", None), getattr(node, "col_offset", None),
               getattr(node, "end_lineno", None), getattr(node, "end_col_offset", None), type(node))
        old = original_nodes.get(key)
        if old is not None and ast.dump(old) != ast.dump(node):
            replacements.append((starts[old.lineno - 1] + old.col_offset,
                                 starts[old.end_lineno - 1] + old.end_col_offset,
                                 ast.unparse(node)))
    chosen, end = [], -1
    for start, stop, replacement in sorted(replacements, key=lambda r: (r[0], -r[1])):
        if start >= end:
            chosen.append((start, stop, replacement))
            end = stop
    raw = source.encode()
    for start, stop, replacement in reversed(chosen):
        raw = raw[:start] + replacement.encode() + raw[stop:]
    result = raw.decode()
    if mover.need_import and not any("tier_holdings" in ast.unparse(n) for n in ast.parse(result).body
                                     if isinstance(n, (ast.Import, ast.ImportFrom))):
        parsed = ast.parse(result)
        imports = [n for n in parsed.body if isinstance(n, (ast.Import, ast.ImportFrom))]
        if imports:
            after = imports[-1].end_lineno
            rows = result.splitlines(keepends=True)
            rows.insert(after, "from prismabuild import tier_holdings\n")
            result = "".join(rows)
    ast.parse(result)
    if result != source:
        path.write_text(result)
    return {"path": str(path.relative_to(root)), "expressions": len(chosen),
            "aliases": {getattr(s, "name", "module"): sorted(a) for s, a in aliases.items() if a}}


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
    parser.add_argument("--extract", type=Path)
    parser.add_argument("--migrate", nargs="*")
    args = parser.parse_args()
    if args.extract:
        args.extract.write_text(extract_owner(args.root))
    if args.migrate:
        for relative in args.migrate:
            print(json.dumps(migrate_callers(args.root, args.root / relative)))


if __name__ == "__main__":
    main()
