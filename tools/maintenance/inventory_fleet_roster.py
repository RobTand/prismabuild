#!/usr/bin/env python3
"""Print every roster reference, including embedded source and quoted patches."""
from __future__ import annotations

import argparse
import ast
from pathlib import Path
import re
import subprocess

PATTERN = re.compile(r"fleet_boxes|fleet_roster|generation_aliases|entry_for_host|recorder_hostnames|local_disk_free_floor_percent|gpu_interpreters|d38_enforce|FLEET_ROSTER_PATH|_roster_entry|_configured_boxes")


def inventory(root: Path):
    paths = subprocess.check_output(["git", "ls-files", "-co", "--exclude-standard", "-z"], cwd=root).decode().split("\0")
    for name in sorted(set(paths) - {"", "tools/maintenance/inventory_fleet_roster.py"}):
        path = root / name
        try:
            text = path.read_text()
        except (OSError, UnicodeError):
            continue
        owners = {}
        if path.suffix == ".py":
            try:
                tree = ast.parse(text)
            except SyntaxError:
                tree = ast.Module(body=[], type_ignores=[])
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    for line in range(node.lineno, node.end_lineno + 1):
                        owners[line] = node.name
        kind = "test" if name.startswith("tests/") else "source" if name.startswith(("src/", "tools/", "fleet/", ".githooks/")) else "historical-or-doc"
        for line, value in enumerate(text.splitlines(), 1):
            if PATTERN.search(value):
                yield name, str(line), kind, owners.get(line, "module-or-embedded"), value.strip().replace("\t", " ")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    args = parser.parse_args()
    print("path\tline\tkind\tsymbol\treference")
    for row in inventory(args.root):
        print("\t".join(row))
