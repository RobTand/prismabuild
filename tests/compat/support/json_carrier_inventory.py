"""Expose schema-free state, named JSON carriers and every serialization owner."""
from __future__ import annotations

import argparse
import ast
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import re

from wire_inventory import catalogue, sources
from compat_references import module_paths


def reference_path(root, reference):
    module, qualified = reference.split(":", 1)
    names = module_paths(module)
    path = next((root / name for name in names if (root / name).is_file()), None)
    if path is None:
        return {"reference": reference, "present": False}
    nodes = ast.parse(path.read_bytes()).body
    for component in qualified.split("."):
        if component == "<locals>":
            continue
        node = next((n for n in nodes if isinstance(n, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
                     and n.name == component), None)
        if node is None:
            return {"reference": reference, "present": False, "path": str(path.relative_to(root))}
        nodes = node.body
    builders = []
    for child in ast.walk(node):
        if isinstance(child, ast.Dict):
            fields = [key.value for key in child.keys if isinstance(key, ast.Constant) and isinstance(key.value, str)]
            if fields and "schema" not in fields:
                builders.append({"line": child.lineno, "fields": sorted(fields), "expansion": None in child.keys})
        elif isinstance(child, ast.Call) and isinstance(child.func, ast.Name) and child.func.id == "dict":
            fields = [kw.arg for kw in child.keywords if kw.arg]
            if fields and "schema" not in fields:
                builders.append({"line": child.lineno, "fields": sorted(fields), "expansion": bool(child.args)})
    return {"reference": reference, "present": True, "path": str(path.relative_to(root)),
            "source_sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "constructors": builders}


def census(root, inventory, dispositions):
    kinds = list(catalogue(root).values())
    unversioned = [{"kind": kind["name"], "location": kind["location"], "variants": kind["variants"],
                    "reader": reference_path(root, kind["reader"]),
                    "writers": [reference_path(root, ref) for ref in kind["writers"]]}
                   for kind in kinds if kind["wire_id"] is None]
    rows = [json.loads(line) for line in inventory.read_text().splitlines()]
    owners = defaultdict(set)
    for row in rows:
        for category in ("schema_builders", "reader_checks"):
            for site in row[category]:
                if site["scope"] == "production":
                    owners[site["path"], site["owner"]].add(row["wire_id"] or "unversioned:" + row["catalogue"]["name"])
    for kind in kinds:
        for ref in (kind["reader"], *kind["writers"], *kind.get("legacy_writers", []), *kind.get("legacy_readers", [])):
            path = reference_path(root, ref)
            if path["present"]:
                owners[path["path"], ref.split(":", 1)[1]].add(kind["name"])
    observed = {"parsed_sources": [], "unparsed_sources": []}
    paths = defaultdict(list)
    for source, tree in sources(root, observed):
        if source["path"].startswith("tests/"):
            continue
        def visit(node, owner="<module>"):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                owner = (owner + "." + node.name).removeprefix("<module>.")
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and re.fullmatch(r"[.\w-]+\.json(?:l|\.gz)?", node.value):
                matching = [kind["name"] for kind in kinds if node.value in kind["location"] or
                            any(Path(name).name == node.value for name in kind.get("fixture_source", []))]
                paths[node.value].append({"path": source["path"], "owner": owner, "line": node.lineno,
                    "source_sha256": source["sha256"], "catalogue_location_matches": matching,
                    "schema_or_codec_owners": sorted(owners[source["path"], owner])})
            for child in ast.iter_child_nodes(node):
                visit(child, owner)
        visit(tree)
    unassigned = {name: sites for name, sites in sorted(paths.items())
                  if not any(site["catalogue_location_matches"] or site["schema_or_codec_owners"] for site in sites)}
    reviewed = {}
    for line in dispositions.read_text().splitlines() if dispositions.exists() else ():
        row = json.loads(line)
        assert row["name"] not in reviewed and row["reason"]
        assert row["disposition"] in ("catalogued_indirect_carrier", "operator_control", "qualification_artifact", "external_format")
        assert all(name in {kind["name"] for kind in kinds} for name in row.get("kinds", []))
        for site in row["sources"]:
            assert hashlib.sha256((root / site["path"]).read_bytes()).hexdigest() == site["source_sha256"], site
        if row["name"] in unassigned:
            assert row["sources"] == [{key: site[key] for key in ("path", "owner", "line", "source_sha256")}
                                      for site in unassigned[row["name"]]], row["name"]
            reviewed[row["name"]] = row
    return {"unversioned_kinds": unversioned, "named_json_carriers": dict(sorted(paths.items())),
            "carrier_dispositions": reviewed, "unassigned_named_carriers": {name: sites for name, sites in unassigned.items() if name not in reviewed},
            "note": "Filename matches are source discovery only, never runtime codec credit. Dynamic carriers remain in the complete serialization-site audit."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--dispositions", type=Path)
    parser.add_argument("--require-complete", action="store_true")
    args = parser.parse_args()
    report = census(args.root, args.inventory, args.dispositions or args.root / "tests/compat/carrier_dispositions.jsonl")
    with args.out.open("x") as stream:
        stream.write(json.dumps(report, sort_keys=True, indent=2) + "\n")
    args.out.chmod(0o444)
    print("unversioned kinds", len(report["unversioned_kinds"]), "named JSON carriers", len(report["named_json_carriers"]),
          "unassigned filenames", len(report["unassigned_named_carriers"]))
    if args.require_complete and report["unassigned_named_carriers"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
