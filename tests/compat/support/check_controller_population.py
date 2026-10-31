"""Compare executed controller populations, assertions and immutable corpus bytes."""
from __future__ import annotations

import argparse
import ast
from collections import Counter
import difflib
import json
from pathlib import Path
import subprocess


def collected_nodes(report):
    nodes = []
    for shard in json.loads(report.read_bytes()):
        for line in shard["output"].splitlines():
            if line.startswith("pbtest-completion: "):
                completion = json.loads(line.removeprefix("pbtest-completion: "))
                assert completion["collection_complete"] and not completion["collection_errors"]
                assert not completion["collect_only"]
                nodes.extend(completion["collected"])
    assert nodes, "no executed native collection evidence"
    return sorted(nodes)


def standalone_contracts(source):
    return {node.name: ast.dump(node, include_attributes=False)
            for node in ast.parse(source).body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}


def check_population():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[3]
    relative = "tests/test_record_compatibility_controller.py"
    original = subprocess.check_output(["git", "show", args.base + ":" + relative], cwd=root)
    paths = sorted((root / "tests").glob("test_record_compatibility_controller*.py"))
    expected_contracts = standalone_contracts(original)
    actual_contracts = {}
    for path in paths:
        for name, value in standalone_contracts(path.read_bytes()).items():
            assert name not in actual_contracts, ("duplicate test or fixture", name)
            actual_contracts[name] = value
    assert actual_contracts == expected_contracts, "standalone test/fixture bodies or assertions changed"
    contract = "tests/record_compat_contract.py"
    old_contract = ast.parse(subprocess.check_output(["git", "show", args.base + ":" + contract], cwd=root))
    new_contract = ast.parse((root / contract).read_bytes())
    before_class = next(node for node in old_contract.body if isinstance(node, ast.ClassDef) and node.name == "RecordContract")
    after_class = next(node for node in new_contract.body if isinstance(node, ast.ClassDef) and node.name == "RecordContract")
    assert ast.dump(before_class) == ast.dump(after_class), "inherited replay assertions changed"
    before = collected_nodes(args.before)
    after = [node for node in collected_nodes(args.after)
             if node.split("::", 1)[0] in {str(path.relative_to(root)) for path in paths}]
    normalize = lambda nodes: sorted(node.split("::", 1)[1] for node in nodes)
    old, new = normalize(before), normalize(after)
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "before-normalized.txt").write_text("".join(node + "\n" for node in old))
    (args.out / "after-normalized.txt").write_text("".join(node + "\n" for node in new))
    difference = list(difflib.unified_diff([node + "\n" for node in old], [node + "\n" for node in new],
                                         fromfile="before-normalized.txt", tofile="after-normalized.txt"))
    (args.out / "population.diff").write_text("".join(difference))
    assert Counter(old) == Counter(new), "controller node population changed; see population.diff"
    assert all(count == 1 for count in Counter(new).values()), "duplicate controller case"
    protected = subprocess.check_output(["git", "ls-tree", "-r", "--name-only", args.base, "tests/compat"], cwd=root, text=True).splitlines()
    protected = [name for name in protected if Path(name).name.startswith(("goldens", "historical", "field_sets"))]
    protected += ["AGENTS.md", "docs/contributing.md", "tests/conftest.py"]
    for name in protected:
        expected = subprocess.check_output(["git", "show", args.base + ":" + name], cwd=root)
        assert (root / name).read_bytes() == expected, ("protected bytes changed", name)
    result = {"before": len(old), "after": len(new), "duplicate_nodes": 0,
              "unchanged_standalone_contracts": len(expected_contracts), "unchanged_inherited_contract": True,
              "byte_identical_files": protected, "diff": "empty"}
    (args.out / "population.json").write_text(json.dumps(result, indent=2) + "\n")
    print(f"Population {len(old)} -> {len(new)}; empty normalized diff; {len(protected)} protected files byte-identical")


if __name__ == "__main__":
    check_population()
