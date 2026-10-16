"""Regenerate the immutable owner pin in Core without importing product code."""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
from pathlib import Path


def pin(root, *, write=False):
    owner = root / "src/prismabuild/immutable_files.py"
    path = root / "src/prismabuild/core.py"
    source = path.read_text()
    assignments = [node for node in ast.parse(source).body if isinstance(node, ast.Assign)
                   and any(isinstance(target, ast.Name) and target.id == "_IMMUTABLE_FILES_SOURCE"
                           for target in node.targets)]
    if len(assignments) != 1:
        raise ValueError("Core must contain exactly one immutable source pin")
    node = assignments[0]
    raw = owner.read_bytes()
    expected = {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
    if ast.literal_eval(node.value) != expected:
        if not write:
            raise ValueError("immutable source changed; regenerate Core's source pin")
        lines = source.splitlines(keepends=True)
        lines[node.lineno - 1:node.end_lineno] = ["_IMMUTABLE_FILES_SOURCE = " + json.dumps(expected) + "\n"]
        path.write_text("".join(lines))
    return expected


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    print(json.dumps(pin(args.root, write=args.write), sort_keys=True))
