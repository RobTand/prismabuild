"""Prove physical cleanup naming preserves complete callers and leaf effects."""
import argparse
import ast
import copy
import hashlib
import json
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--before", default="2ce0865ec169af44009da1bbc71d90af6dc886c7")
    parser.add_argument("--head", default="HEAD")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    def git(*words):
        return subprocess.check_output(["git", *words], cwd=args.root, text=True).strip()

    def method(revision, path, name):
        tree = ast.parse(git("show", revision + ":" + path))
        return next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == name)

    def body(node):
        statements = copy.deepcopy(node.body)
        if isinstance(statements[0], ast.Expr) and isinstance(statements[0].value, ast.Constant):
            statements = statements[1:]
        return statements

    def dump(node):
        return ast.dump(node, include_attributes=False)

    def statements(nodes):
        return dump(ast.Module(body=nodes, type_ignores=[]))

    def digest(value):
        return hashlib.sha256(value.encode()).hexdigest()

    authority = ast.parse("tier_holdings.Unspent.NEVER_STARTED", mode="eval").body

    class Restore(ast.NodeTransformer):
        def __init__(self, old, new, authority_value=authority):
            self.old, self.new, self.changed = old, new, 0
            self.authority_value = authority_value

        def visit_Call(self, node):
            self.generic_visit(node)
            if isinstance(node.func, ast.Attribute) and node.func.attr == self.new:
                self.changed += 1
                node.func.attr = self.old
                node.keywords.append(ast.keyword("authority", copy.deepcopy(self.authority_value)))
            return node

    proofs = []
    for path, name, old, new in (
        ("src/prismabuild/pool.py", "reclaim_terminal_reservation", "release_unspent_all", "egress_all"),
        ("src/prismabuild/produced_output.py", "safe_release_instance", "release_unspent", "egress"),
    ):
        before, after = method(args.before, path, name), method(args.head, path, name)
        restored = copy.deepcopy(after)
        move = Restore(old, new)
        move.visit(restored)
        assert move.changed == 1 and dump(restored) == dump(before)
        proofs.append(dict(path=path, function=name, exact_calls_changed=move.changed,
                           old_binding=old, new_binding=new,
                           before_sha256=digest(dump(before)),
                           after_sha256=digest(dump(after)),
                           restored_entire_method_ast_equal=True))

    path = "src/prismabuild/tier_holdings.py"
    unspent, egress = (method(args.head, path, name) for name in ("release_unspent", "egress"))
    specialized = body(unspent)
    assert ast.unparse(specialized[0].test) == "counts is not None"
    assert ast.unparse(specialized[0].orelse[0].test) == "keep is not None"
    specialized[:1] = specialized[0].orelse[0].orelse
    assert statements(specialized) == statements(body(egress))
    assert dump(unspent) == dump(method(args.before, path, "release_unspent"))
    assert dump(egress) == dump(method(args.before, path, "egress"))
    aggregate = method(args.head, path, "egress_all")
    restore = Restore("release_unspent", "egress", ast.Name("authority", ast.Load()))
    restore.visit(aggregate)
    assert restore.changed == 1
    assert statements(body(aggregate)) == statements(body(method(args.head, path, "release_unspent_all")))
    report = dict(source_only=True, before=git("rev-parse", args.before),
                  head=git("rev-parse", args.head), tree=git("rev-parse", args.head + "^{tree}"),
                  callers=proofs, no_options_leaf_body_equal=True,
                  aggregate_body_equal=True, existing_guards_defaults_decorators_errors_order_equal=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
