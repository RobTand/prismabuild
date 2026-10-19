"""Prove the data-plan owner move from the retained campaign source."""
from __future__ import annotations

import argparse
import ast
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import subprocess


BASE = "fe416690b87aba99ad30c7df4f95ac8da47055d5"
CAMPAIGN = "tools/fleet/pbcampaign.py"
OWNER = "src/prismabuild/decomposition.py"
PROTECTED = tuple("src/prismabuild/" + name + ".py" for name in (
    "core", "digest_primitives", "immutable_files", "local_scratch", "kept_reads",
)) + ("tools/fleet/local_scratch_profile.py", "src/prismabuild/client.py")


def shape(value):
    if isinstance(value, list):
        return [shape(node) for node in value]
    return ast.dump(value, include_attributes=False)


def function(body, name):
    return next(node for node in body if isinstance(node, ast.FunctionDef) and node.name == name)


class RecordBindings(ast.NodeTransformer):
    def visit_Name(self, node):
        if node.id == "ManifestError":
            return ast.copy_location(ast.Name("RecordError", node.ctx), node)
        return node


class OwnerBindings(ast.NodeTransformer):
    def visit_Name(self, node):
        if node.id in ("path", "plan"):
            return ast.copy_location(ast.Attribute(ast.Name("self", ast.Load()), node.id, node.ctx), node)
        return node

    def visit_Attribute(self, node):
        if isinstance(node.value, ast.Name) and node.value.id == "dc" and node.attr == "document_bytes":
            return ast.copy_location(ast.Name(node.attr, node.ctx), node)
        return self.generic_visit(node)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--ref", default="HEAD", help="A Git ref or WORKTREE for source-only review")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    def git(*words):
        return subprocess.check_output(["git", "-C", str(args.repo), *words])

    def blob(ref, path):
        return (args.repo / path).read_bytes() if ref == "WORKTREE" else git("show", ref + ":" + path)

    checks = {}

    def equal(name, before, after):
        assert shape(before) == shape(after), name
        checks[name] = True

    old_campaign = ast.parse(blob(BASE, CAMPAIGN))
    new_campaign = ast.parse(blob(args.ref, CAMPAIGN))
    old_owner = ast.parse(blob(BASE, OWNER))
    new_owner = ast.parse(blob(args.ref, OWNER))
    before = function(old_campaign.body, "frozen_child_data_plans")
    after = function(new_campaign.body, "frozen_child_data_plans")
    old_body = before.body[1:]
    assert isinstance(after.body[1], ast.Try) and len(after.body) == 2
    boundary = after.body[1]
    new_body = boundary.body
    equal("campaign_signature", before.args, after.args)
    equal("campaign_decorators", before.decorator_list, after.decorator_list)
    equal("campaign_documentation", before.body[0], after.body[0])
    assert len(boundary.handlers) == 1 and not boundary.orelse and not boundary.finalbody
    equal("public_error_adapter", boundary.handlers[0], ast.parse(
        "try:\n    pass\nexcept dc.RecordError as exc:\n    raise ManifestError(str(exc)) from None").body[0].handlers[0])

    original_reader = deepcopy(function(old_campaign.body, "_stored_document"))
    original_reader.name = "_stored_record"
    original_reader.body = original_reader.body[1:]
    original_reader = RecordBindings().visit(original_reader)
    equal("read_decode_error_protocol", original_reader, function(new_owner.body, "_stored_record"))
    equal("existing_record_reader_unchanged", function(old_owner.body, "_stored_record"),
          function(new_owner.body, "_stored_record"))

    assert isinstance(old_body[2], ast.If) and isinstance(new_body[2], ast.If)
    equal("initial_absence_question", old_body[2].test, new_body[2].test)
    equal("residency_preparation_read_effect_order", old_body[2].body[:-2], new_body[2].body[:-1])
    equal("campaign_record_capture_at_original_read", new_body[:2], ast.parse(
        "record = dc.DataPlans(cas.root, plan)\nstored = record.capture()").body)
    equal("campaign_owner_publication_at_original_write", new_body[2].body[-1],
          ast.parse("stored = record.freeze(rows)").body[0])

    validator = function(new_owner.body, "_validate_data_plans")
    equal("strict_header_predicate_and_error", RecordBindings().visit(deepcopy(old_body[3])), validator.body[0])
    old_loop, new_loop = old_body[5], new_body[4]
    record_loop = validator.body[1]
    equal("child_structural_iteration", old_loop.target, record_loop.target)
    equal("child_structural_population", old_loop.iter, record_loop.iter)
    equal("strict_child_identity_predicate_and_error",
          RecordBindings().visit(deepcopy(old_loop.body[0])), record_loop.body[0])
    equal("lazy_one_child_yield", record_loop.body[1:], ast.parse("yield child, row").body)
    assert not record_loop.orelse and len(validator.body) == 2
    equal("per_child_residency_guards_and_effect_order", old_loop.body[1:], new_loop.body)
    equal("campaign_child_owner_read", new_loop.iter,
          ast.parse("record.rows_for_children(stored, children)", mode="eval").body)
    equal("campaign_child_iteration_target", old_loop.target, new_loop.target)
    equal("campaign_result_lifetime", [old_body[4], old_body[6]], [new_body[3], new_body[5]])

    owner = next(node for node in new_owner.body if isinstance(node, ast.ClassDef) and node.name == "DataPlans")
    equal("pure_constructor", function(owner.body, "__init__").body, ast.parse(
        'self.path = record_dir(cas_root, plan["parent_key"]) / "data-plans.json"\nself.plan = plan').body)
    equal("initial_path_binding", old_body[0].value, ast.parse(
        'dc.record_dir(cas.root, plan["parent_key"]) / "data-plans.json"', mode="eval").body)
    equal("capture_uses_existing_reader", function(owner.body, "capture").body,
          ast.parse("return _stored_record(self.path)").body)
    freezer = function(owner.body, "freeze")
    equal("exact_first_writer_document", OwnerBindings().visit(deepcopy(old_body[2].body[-2])), freezer.body[0])
    old_publish = old_body[2].body[-1]
    equal("first_writer_publisher_and_serializer", OwnerBindings().visit(deepcopy(old_publish.test)),
          freezer.body[1].test)
    equal("first_writer_success_value", old_publish.body[0].value, freezer.body[1].body[0].value)
    original_readback = OwnerBindings().visit(deepcopy(old_publish.orelse[0].value))
    original_readback.func.id = "_stored_record"
    equal("lost_writer_readback", original_readback, freezer.body[2].value)
    assert isinstance(freezer.body[1].body[0], ast.Return) and isinstance(freezer.body[2], ast.Return)
    assert not freezer.body[1].orelse and len(freezer.body) == 3
    equal("lazy_record_validation_binding", function(owner.body, "rows_for_children").body,
          ast.parse("return _validate_data_plans(stored, self.plan, children)").body)

    def remaining(tree, names):
        return [node for node in tree.body if not (
            isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names)]

    campaign_before = remaining(old_campaign, {"_stored_document", "frozen_child_data_plans"})
    campaign_before = [node for node in campaign_before if not (
        isinstance(node, ast.ImportFrom) and node.module == "prismabuild"
        and [item.name for item in node.names] == ["immutable_files"])]
    equal("all_other_campaign_source_ast", campaign_before,
          remaining(new_campaign, {"frozen_child_data_plans"}))
    new_declarations = remaining(new_owner, {"DataPlans", "_validate_data_plans"})
    exports = next(node for node in new_declarations if isinstance(node, ast.Assign)
                   and any(isinstance(target, ast.Name) and target.id == "__all__" for target in node.targets))
    exports.value.elts = [item for item in exports.value.elts if item.value != "DataPlans"]
    equal("all_other_decomposition_source_ast", old_owner.body, new_declarations)
    protected = []
    for path in PROTECTED:
        assert blob(BASE, path) == blob(args.ref, path), path
        protected.append(path)
    old_tests = git("ls-tree", "-r", "--name-only", BASE, "tests").decode().splitlines()
    for path in old_tests:
        assert blob(BASE, path) == blob(args.ref, path), path
    report = dict(source=args.ref if args.ref == "WORKTREE" else git("rev-parse", args.ref).decode().strip(),
                  base=BASE, checks=checks, protected_byte_exact=protected,
                  original_test_paths_byte_exact=len(old_tests),
                  current_source_sha256={path: hashlib.sha256(blob(args.ref, path)).hexdigest()
                                         for path in (OWNER, CAMPAIGN)},
                  ownership={"capture": "decomposition.DataPlans.capture",
                             "write": "decomposition.DataPlans.freeze",
                             "structural_read": "decomposition.DataPlans.rows_for_children",
                             "residency": "pbcampaign.frozen_child_data_plans"},
                  runtime_credit=False)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(dict(verified=True, checks=len(checks), original_test_paths=len(old_tests))))


if __name__ == "__main__":
    main()
