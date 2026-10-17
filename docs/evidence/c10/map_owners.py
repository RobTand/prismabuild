#!/usr/bin/env python3
"""Map each original tier owner and changed fixture signature to actual source."""
import argparse
import ast
import copy
import hashlib
import json
from pathlib import Path
import runpy
import subprocess


def index(source):
    found = {}
    def walk(node, prefix=""):
        for child in ast.iter_child_nodes(node):
            name = prefix
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                name = prefix + child.name
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    found[name] = child
                name += "."
            walk(child, name)
    walk(ast.parse(source))
    return found


def digest(node):
    return hashlib.sha256(ast.dump(node).encode()).hexdigest()


def describe(node):
    calls = sorted((n for n in ast.walk(node) if isinstance(n, ast.Call)),
                   key=lambda n: (n.lineno, n.col_offset))
    return dict(line=node.lineno, signature=ast.unparse(node.args), ast_sha256=digest(node),
                decorators=[ast.unparse(n) for n in node.decorator_list],
                ordered_calls=[ast.unparse(n.func) for n in calls])


def without_docs(node):
    result = copy.deepcopy(node)
    if (result.body and isinstance(result.body[0], ast.Expr)
            and isinstance(result.body[0].value, ast.Constant)
            and isinstance(result.body[0].value.value, str)):
        result.body = result.body[1:]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--base", default="578e54d0788c0f3b138f5dcf217e980322bb7726")
    parser.add_argument("--prerequisite", default="bfc6efce3dd700bffa08f70cc502fa7382dd4fb0")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    def git(*words):
        return subprocess.check_output(["git", *words], cwd=args.root, text=True).strip()
    def source(revision, path):
        return git("show", revision + ":" + path)
    move = runpy.run_path(str(args.root / "docs/evidence/c10/migrate_source.py"))
    pool_path, tier_path = "src/prismabuild/pool.py", "src/prismabuild/tier_holdings.py"
    original = index(source(args.base, pool_path))
    current = index(source("HEAD", tier_path))
    mappings = dict(move["MOVED"])
    mappings.update(move["AGGREGATE"])
    mappings["tier_ledger"] = "__init__"
    owners = []
    for old, new in mappings.items():
        destination = new if old in move["AGGREGATE"] else "TierLedger." + new
        assert "PoolQueue." + old not in index(source("HEAD", pool_path))
        owners.append(dict(original_owner="PoolQueue." + old, original_path=pool_path,
                           original=describe(original["PoolQueue." + old]),
                           current_owner=destination, current_path=tier_path,
                           current=describe(current[destination])))
    window = index(source(args.base, "src/prismabuild/window_credit.py"))
    for old, new in (("cancel", "cancel_fence"), ("held_grants", "held_grants")):
        owners.append(dict(original_owner=old, original_path="src/prismabuild/window_credit.py",
                           original=describe(window[old]), current_owner="TierLedger." + new,
                           current_path=tier_path, current=describe(current["TierLedger." + new])))
    prerequisite = index(source(args.prerequisite, pool_path))
    final_pool = index(source("HEAD", pool_path))
    generic = [name for name, before in prerequisite.items()
               if name.startswith("ResourceLedger.")
               and ast.dump(without_docs(before)) != ast.dump(without_docs(final_pool[name]))]
    assert set(generic) == {"ResourceLedger.commit_acquire", "ResourceLedger.transfer_tokens"}, generic
    assert not ({"reserve_fence", "fund_output_batch", "funded_cover", "output_funded_cover"}
                & {name.split(".")[-1] for name in current})
    signatures = []
    for path in git("diff", "--name-only", args.base, "HEAD", "--", "tests").splitlines():
        if not path.endswith(".py"):
            continue
        old_blob = git("ls-tree", args.base, path)
        if not old_blob:
            continue
        before, after = index(source(args.base, path)), index(source("HEAD", path))
        for name, old in before.items():
            if name in after and ast.dump(old.args) != ast.dump(after[name].args):
                signatures.append(dict(path=path, function=name, original=describe(old),
                                       current=describe(after[name]),
                                       reason="Actual moved owner or fault callback signature; original declaration and predicates audited separately."))
    report = dict(source_only=True, base=args.base, prerequisite=args.prerequisite,
                  head=git("rev-parse", "HEAD"), tree=git("rev-parse", "HEAD^{tree}"),
                  owner_mappings=owners, raw_operation_mappings=move["RAW"],
                  generic_mechanism_changes=generic, fixture_signature_mappings=signatures)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(dict(owners=len(owners), raw_operations=len(move["RAW"]),
                          fixture_signatures=len(signatures), generic_changes=generic)))


if __name__ == "__main__":
    main()
