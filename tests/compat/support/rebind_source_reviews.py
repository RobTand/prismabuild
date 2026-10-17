"""Rebind reviews when constructor ASTs survive resolved schema-owner moves.

This source-only tool imports no product code. It retains review counts,
classification reasons and every constructor occurrence, including duplicates.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
from pathlib import Path
import subprocess


def review_source_hash(raw):
    return hashlib.sha256(raw).hexdigest()


def reviewed_sites(raw, owner, members, wire=None):
    nodes = ast.parse(raw).body
    for component in owner.split("."):
        selected = next((node for node in nodes
                         if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
                         and node.name == component), None)
        if selected is None:
            raise ValueError(('reviewed owner is absent', owner))
        nodes = selected.body
    result = []
    for statement in nodes:
        for node in ast.walk(statement):
            if isinstance(node, ast.Dict):
                names = [key.value for key in node.keys if isinstance(key, ast.Constant)]
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "dict":
                names = [keyword.arg for keyword in node.keywords if keyword.arg]
            else:
                continue
            if "schema" in names and sorted(names) == members:
                if wire is not None:
                    if isinstance(node, ast.Dict):
                        for index, key in enumerate(node.keys):
                            if isinstance(key, ast.Constant) and key.value == "schema":
                                node.values[index] = ast.Constant(wire)
                    else:
                        for keyword in node.keywords:
                            if keyword.arg == "schema":
                                keyword.value = ast.Constant(wire)
                result.append((node.lineno, ast.dump(node, include_attributes=False)))
    return sorted(result)


def rebind_reviews(args):
    root = args.root.resolve()
    def old_file(relative):
        return subprocess.check_output(["git", "-C", str(root), "show", args.before_source + ":" + relative])

    inventory = [json.loads(line) for line in (args.scan / "inventory.jsonl").read_text().splitlines()]
    observation_path = args.scan / "source-audit.json"
    if not observation_path.is_file():
        observation_path = args.scan / "observations.json"
    observations = json.loads(observation_path.read_bytes())
    source_hashes = {row["path"]: row["sha256"] for row in observations["parsed_sources"]}
    constructors = [dict(wire_id=row["wire_id"], **site) for row in inventory
                    for site in row["schema_builders"] if site["scope"] == "production"]
    dynamic = [row for row in observations["unresolved_schema_builders"] if row["scope"] == "production"]
    changes, replacements = [], {}
    for filename, current, discriminator in (
            ("constructor_dispositions.jsonl", constructors, "wire_id"),
            ("dynamic_schema_dispositions.jsonl", dynamic, "schema")):
        relative = "tests/compat/" + filename
        before_raw = old_file(relative)
        before = [json.loads(line) for line in before_raw.decode().splitlines()]
        after = []
        for row in before:
            prior_source = old_file(row["path"])
            if review_source_hash(prior_source) != row["source_sha256"]:
                raise ValueError(('declared baseline does not own the original review', row))
            wire = row.get("wire_id")
            prior_sites = reviewed_sites(prior_source, row["owner"], row["fields"], wire)
            fingerprint = next(shape for line, shape in prior_sites if line == row["line"])
            rank = [line for line, shape in prior_sites if shape == fingerprint].index(row["line"])
            matched = []
            for candidate in current:
                if any(candidate[key] != row[key] for key in ("owner", "fields", discriminator)):
                    continue
                raw = (root / candidate["path"]).read_bytes()
                if review_source_hash(raw) != source_hashes.get(candidate["path"]):
                    raise ValueError(('source differs from the supplied scanner evidence', candidate["path"]))
                sites = reviewed_sites(raw, candidate["owner"], candidate["fields"], wire)
                equivalent = [line for line, shape in sites if shape == fingerprint]
                prior_equivalent = [line for line, shape in prior_sites if shape == fingerprint]
                if len(equivalent) == len(prior_equivalent) and equivalent[rank] == candidate["line"]:
                    matched.append((candidate, raw))
            if len(matched) != 1:
                raise ValueError(('reviewed constructor changed or moved ambiguously; explicit review required', row))
            candidate, raw = matched[0]
            updated = dict(row, path=candidate["path"], line=candidate["line"], source_sha256=review_source_hash(raw))
            changes.append(dict(file=relative, before=row, after=updated,
                                constructor_ast_equal_after_wire_identity=True, resolved_schema_wire=wire))
            after.append(updated)
        present = (root / relative).read_bytes()
        replacement = ("".join(json.dumps(row) + "\n" for row in after)).encode()
        if present not in (before_raw, replacement):
            raise ValueError(('metadata differs from both the baseline and verified replacement', relative))
        replacements[relative] = replacement
    report = dict(before_source=args.before_source, scan=str(args.scan), applied=args.apply,
                  review_count=len(changes), all_constructor_asts_equal_after_wire_identity=True, rows=changes)
    with args.report.open("x") as stream:
        stream.write(json.dumps(report, indent=2) + "\n")
    if args.apply:
        for relative, raw in replacements.items():
            (root / relative).write_bytes(raw)
    print(json.dumps(dict(review_count=len(changes), changed=sum(row["before"] != row["after"] for row in changes),
                          all_constructor_asts_equal_after_wire_identity=True, applied=args.apply)))


def source_review_main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--before-source", required=True)
    parser.add_argument("--scan", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    rebind_reviews(parser.parse_args())


if __name__ == "__main__":
    source_review_main()
