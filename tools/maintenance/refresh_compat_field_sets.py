"""Review every source-policy delta before refreshing the complete field inventory.

Uses only the standard library, the source-only scanner and Git. No product
module is imported, so this inventory can run on the coordinator.
"""
from __future__ import annotations

import argparse
import ast
from collections import Counter
import json
from pathlib import Path
import subprocess
import sys


def fieldset_git(root, *arguments, raw=None):
    return subprocess.check_output(["git", "-C", str(root), *arguments], input=raw,
                                   stderr=subprocess.PIPE)


def fieldset_identity(root, raw):
    return fieldset_git(root, "hash-object", "--stdin", raw=raw).decode().strip()


def fieldset_key(row):
    return repr(sorted(row.items()))


def fieldset_witness(root, revision, row):
    relative = row["path"]
    try:
        raw = (root / relative).read_bytes() if revision is None else fieldset_git(
            root, "show", revision + ":" + relative)
    except (FileNotFoundError, subprocess.CalledProcessError):
        return {"path": relative, "source_absent": True}
    nodes = ast.parse(raw).body
    selected = None
    if row["owner"] == "<module>":
        for node in nodes:
            targets = node.targets if isinstance(node, ast.Assign) else (
                [node.target] if isinstance(node, ast.AnnAssign) else [])
            if any(isinstance(target, ast.Name) and target.id == row.get("binding")
                   for target in targets):
                selected = node
                break
    else:
        for component in row["owner"].split("."):
            selected = next((node for node in nodes
                             if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
                             and node.name == component), None)
            if selected is None:
                break
            nodes = selected.body
    witness = dict(path=relative, source_git_blob=fieldset_identity(root, raw),
                   owner=row["owner"], owner_present=selected is not None)
    if selected is not None:
        witness.update(line=selected.lineno, end_line=selected.end_lineno,
                       source=ast.get_source_segment(raw.decode(), selected))
    return witness


def refresh_fieldsets(args):
    root = args.root.resolve()
    relative = "tests/compat/field_sets.json"
    before_revision = fieldset_git(root, "rev-parse", "--verify",
                                  args.before_source + "^{commit}").decode().strip()
    before_raw = fieldset_git(root, "show", before_revision + ":" + relative)
    before = json.loads(before_raw)
    sys.path.insert(0, str(root / "tests/compat/support"))
    from wire_inventory import audit
    _, after = audit(root)
    indexed = {fieldset_key(row): row for row in before + after}
    old_counts = Counter(map(fieldset_key, before))
    new_counts = Counter(map(fieldset_key, after))
    changes = [dict(change=direction, row=indexed[key], count=count)
               for direction, delta in (("removed", old_counts - new_counts),
                                        ("added", new_counts - old_counts))
               for key, count in sorted(delta.items())]
    reviews = json.loads(args.review.read_bytes()) if args.review else []
    review_index = {}
    for review in reviews:
        key = (review["change"], fieldset_key(review["row"]), review["count"])
        if key in review_index or not review.get("reason", "").strip():
            raise ValueError('every policy review needs one unique exact delta and a reason')
        review_index[key] = review["reason"]
    actual = [(item["change"], fieldset_key(item["row"]), item["count"]) for item in changes]
    reviewed = Counter(review_index.keys()) == Counter(actual)
    if (args.apply or args.review) and not reviewed:
        raise ValueError('review must account for every added and removed policy, including multiplicity')
    target = root / relative
    present = json.loads(target.read_bytes())
    if present != before and present != after:
        raise ValueError('current metadata differs from both the declared baseline and complete scanner')
    after_raw = (json.dumps(after) + "\n").encode()
    for item in changes:
        item["reason"] = review_index.get((item["change"], fieldset_key(item["row"]), item["count"]))
        item["before"] = fieldset_witness(root, before_revision, item["row"])
        item["after"] = fieldset_witness(root, None, item["row"])
    report = dict(before_source=before_revision,
                  current_head=fieldset_git(root, "rev-parse", "HEAD").decode().strip(),
                  current_dirty=fieldset_git(root, "status", "--porcelain").decode().splitlines(),
                  before_metadata_git_blob=fieldset_identity(root, before_raw),
                  after_metadata_git_blob=fieldset_identity(root, after_raw),
                  before_count=len(before), after_count=len(after),
                  reviewed=reviewed, applied=args.apply, changes=changes,
                  scanner_git_blob=fieldset_identity(root, (root / "tests/compat/support/wire_inventory.py").read_bytes()))
    with args.report.open("x") as stream:
        stream.write(json.dumps(report, indent=2) + "\n")
    if args.apply:
        target.write_bytes(after_raw)
    print(json.dumps({key: report[key] for key in
                      ("before_count", "after_count", "reviewed", "applied")}))


def fieldset_main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--before-source", required=True)
    parser.add_argument("--review", type=Path)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    refresh_fieldsets(parser.parse_args())


if __name__ == "__main__":
    fieldset_main()
