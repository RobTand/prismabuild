#!/usr/bin/env python3
"""Replace only exact, four-field, valid receipt-writing recipes. Source-only."""
import argparse
import ast
import json
from pathlib import Path


SCHEMA = "prismaquant.prismabuild.runtime_version.v1"


def migrate(root, write):
    changes = []
    for path in sorted((root / "tests").glob("*.py")):
        if path.name in {"generation_fixture.py", "test_generation_receipt_capture.py",
                         "test_generation_receipt_source_contract.py"}:
            continue
        source = path.read_text()
        tree = ast.parse(source)
        parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
        starts, offset = [], 0
        for line in source.splitlines(keepends=True):
            starts.append(offset)
            offset += len(line)
        replacements = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Dict) or len(node.keys) != 4:
                continue
            if [key.value if isinstance(key, ast.Constant) else None for key in node.keys] != [
                    "schema", "generation", "commit", "files"]:
                continue
            schema = node.values[0]
            known_schema = isinstance(schema, ast.Constant) and schema.value == SCHEMA
            known_schema |= ast.unparse(schema) in {
                'publication.RUNTIME_SCHEMA', 'migration.supervise.RUNTIME_VERSION_SCHEMA', 'RECEIPT_SCHEMA'}
            if not known_schema:
                continue
            encoded = parents.get(node)
            writer = parents.get(encoded)
            direct_writer = (isinstance(encoded, ast.Call) and isinstance(encoded.func, ast.Attribute)
                             and ast.unparse(encoded.func) == "json.dumps"
                             and isinstance(writer, ast.Call) and isinstance(writer.func, ast.Attribute)
                             and writer.func.attr == "write_text")
            assigned_receipt = (isinstance(encoded, ast.Assign) and len(encoded.targets) == 1
                                and isinstance(encoded.targets[0], ast.Name)
                                and encoded.targets[0].id == 'receipt')
            if not direct_writer and not assigned_receipt:
                continue
            values = [ast.get_source_segment(source, value) for value in node.values[1:]]
            replacements.append((starts[node.lineno - 1] + node.col_offset,
                                 starts[node.end_lineno - 1] + node.end_col_offset,
                                 "generation_receipt(" + ", ".join(values) + ")"))
        if not replacements:
            continue
        if any(isinstance(node, ast.ImportFrom) and node.module == "prismabuild"
               and any(alias.name == "generation_receipt" for alias in node.names)
               for node in tree.body):
            raise ValueError("fixture name collides with product import: " + str(path))
        updated = source
        for start, end, replacement in sorted(replacements, reverse=True):
            updated = updated[:start] + replacement + updated[end:]
        imported = any(isinstance(node, ast.ImportFrom) and node.module == "generation_fixture"
                       for node in tree.body)
        if not imported:
            anchors = [node.end_lineno for node in tree.body
                       if isinstance(node, (ast.Import, ast.ImportFrom))]
            insertion = max(anchors) if anchors else 1
            lines = updated.splitlines(keepends=True)
            lines.insert(insertion, "from generation_fixture import generation_receipt\n")
            updated = "".join(lines)
        ast.parse(updated)
        changes.append({"path": str(path.relative_to(root)), "recipes": len(replacements)})
        if write:
            path.write_text(updated)
    return changes


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkout", type=Path, default=Path.cwd())
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    print(json.dumps(migrate(args.checkout.resolve(), args.write), indent=2))
