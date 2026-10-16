#!/usr/bin/env python3
"""Inventory prewarm and stage-mark symbols without importing product code."""
from __future__ import annotations

import ast
import json
from pathlib import Path
import subprocess


def prewarm_stage_inventory(root: Path) -> dict:
    owners = ("tools/fleet/prewarm_loop.py", "src/prismabuild/stage_marks.py")
    definitions = {}
    for name in owners:
        tree = ast.parse((root / name).read_text())
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
                names = [node.name]
            elif isinstance(node, ast.Assign):
                names = [target.id for target in node.targets if isinstance(target, ast.Name)]
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                names = [node.target.id]
            else:
                continue
            for symbol in names:
                definitions.setdefault(symbol, []).append({"file": name, "line": node.lineno})
    references = {name: [] for name in definitions}
    paths = subprocess.check_output(["git", "-C", str(root), "ls-files", "--", "*.py"])
    for name in paths.decode().splitlines():
        tree = ast.parse((root / name).read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Name):
                symbol, spelling = node.id, node.id
            elif isinstance(node, ast.Attribute):
                symbol, spelling = node.attr, ast.unparse(node)
            elif isinstance(node, ast.alias):
                symbol, spelling = node.name, node.name
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                symbol, spelling = node.value, repr(node.value)
            else:
                continue
            if symbol in references:
                references[symbol].append({"file": name, "line": node.lineno,
                                           "kind": type(node).__name__, "spelling": spelling})
    return {"method": "All tracked Python AST names, attributes, imports and exact string references; same-named symbols can include unrelated owners.",
            "owners": owners, "definitions": definitions, "references": references}


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[2]
    print(json.dumps(prewarm_stage_inventory(root), indent=2))
