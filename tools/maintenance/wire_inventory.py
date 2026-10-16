"""Inventory wire literals, constructors, imports, JSON boundaries and closed fields."""
from __future__ import annotations

import argparse
import ast
from collections import Counter, defaultdict
import json
from pathlib import Path
import re

WIRE = re.compile(r"^[A-Za-z][\w.-]+\.v\d+$")


def module_name(path):
    if path.startswith("src/"):
        return path[4:-3].replace("/", ".")
    return Path(path).stem


def sources(root):
    result = {}
    for base in ("src", "tools"):
        for path in sorted((root / base).rglob("*")):
            if not path.is_file() or path.suffix not in (".py", ""):
                continue
            try:
                result[str(path.relative_to(root))] = ast.parse(path.read_bytes())
            except (SyntaxError, UnicodeError):
                continue
    return result


def catalogue(root):
    tree = ast.parse((root / "src/prismabuild/record_kinds.py").read_bytes())
    result = {}
    fields = ("name", "wire_id", "location", "field_policy", "reader", "writers", "variants", "legacy", "closed_sets")
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "RecordKind":
            value = {field: ast.literal_eval(arg) for field, arg in zip(fields, node.args)}
            value.update({kw.arg: ast.literal_eval(kw.value) for kw in node.keywords})
            result[value["wire_id"]] = value
    return result


def evaluate(node, values):
    if isinstance(node, ast.Name):
        return values[node.id]
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in ("set", "frozenset"):
        return set(evaluate(node.args[0], values)) if node.args else set()
    if isinstance(node, ast.BinOp):
        left, right = set(evaluate(node.left, values)), set(evaluate(node.right, values))
        if isinstance(node.op, ast.BitOr):
            return left | right
        if isinstance(node.op, ast.BitAnd):
            return left & right
        if isinstance(node.op, ast.Sub):
            return left - right
    return ast.literal_eval(node)


def audit(root):
    trees = sources(root)
    known = catalogue(root)
    wire_ids = {row["name"]: wire for wire, row in known.items()}
    constants, imports = {}, {}
    for path, tree in trees.items():
        module = module_name(path)
        local, aliases = {}, {}
        for node in tree.body:
            if isinstance(node, ast.Assign):
                try:
                    if (isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name)
                            and node.value.func.id == "wire_id"):
                        value = wire_ids[ast.literal_eval(node.value.args[0])]
                    else:
                        value = evaluate(node.value, local)
                except (ValueError, TypeError, KeyError, IndexError):
                    continue
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        local[target.id] = value
                        constants[module + "." + target.id] = value
            elif isinstance(node, ast.ImportFrom):
                origin = node.module or ""
                if node.level:
                    origin = ".".join(module.split(".")[:-node.level]) + ("." + origin if origin else "")
                for name in node.names:
                    aliases[name.asname or name.name] = origin + "." + name.name
            elif isinstance(node, ast.Import):
                for name in node.names:
                    aliases[name.asname or name.name] = name.name
        imports[path] = (local, aliases)
    rows = defaultdict(lambda: {"occurrences": [], "schema_builders": [], "json_boundaries": []})
    policies = []
    for path, tree in trees.items():
        local, aliases = imports[path]
        module = module_name(path)

        def resolve(node):
            if isinstance(node, ast.Name):
                return local.get(node.id, constants.get(aliases.get(node.id)))
            if isinstance(node, ast.Attribute):
                head, tail = node, []
                while isinstance(head, ast.Attribute):
                    tail.insert(0, head.attr)
                    head = head.value
                if isinstance(head, ast.Name):
                    return constants.get(aliases.get(head.id, head.id) + "." + ".".join(tail))
            if isinstance(node, ast.Constant):
                return node.value
            return None

        def walk(node, owner="<module>", embedded=False):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                owner = (owner + "." + node.name).removeprefix("<module>.")
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if WIRE.fullmatch(node.value):
                    rows[node.value]["occurrences"].append({"path": path, "owner": owner, "line": node.lineno, "embedded": embedded})
                elif "schema" in node.value and "\n" in node.value:
                    try:
                        walk(ast.parse(node.value), owner + ":embedded", True)
                    except SyntaxError:
                        pass
            schema, names, expansion = None, [], False
            if isinstance(node, ast.Dict):
                for key, value in zip(node.keys, node.values):
                    if key is None:
                        expansion = True
                    elif isinstance(key, ast.Constant) and isinstance(key.value, str):
                        names.append(key.value)
                        if key.value == "schema":
                            schema = resolve(value)
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name) and node.func.id == "dict":
                    names = [kw.arg for kw in node.keywords if kw.arg]
                    expansion = any(kw.arg is None for kw in node.keywords)
                    schema = next((resolve(kw.value) for kw in node.keywords if kw.arg == "schema"), None)
                target = ast.unparse(node.func)
                if target.endswith(("json.dump", "json.dumps", "write_json", "write_record", "write_document", "_write_json_atomic", "_write_atomic", "_publish_immutable", "publish_immutable")):
                    # Boundaries remain source witnesses until a production adapter covers them.
                    for wire in set(v for v in local.values() if isinstance(v, str) and WIRE.fullmatch(v)):
                        rows[wire]["json_boundaries"].append({"path": path, "owner": owner, "line": node.lineno, "call": target})
                if target.endswith("_exact_mapping"):
                    for kw in node.keywords:
                        if kw.arg == "keys":
                            pin(kw.value, owner)
            if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Sub) and isinstance(node.left, ast.Call) and isinstance(node.left.func, ast.Name) and node.left.func.id == "set":
                pin(node.right, owner)
            if isinstance(schema, str) and WIRE.fullmatch(schema):
                rows[schema]["schema_builders"].append({"path": path, "owner": owner, "line": node.lineno,
                                                        "fields": sorted(names), "expansion": expansion, "embedded": embedded})
            for child in ast.iter_child_nodes(node):
                walk(child, owner, embedded)

        def pin(node, owner):
            try:
                fields = evaluate(node, local)
            except (ValueError, TypeError, KeyError, IndexError):
                fields = None
            if isinstance(fields, (set, frozenset, list, tuple)) and all(isinstance(k, str) for k in fields):
                policies.append({"path": path, "owner": owner, "fields": sorted(fields)})
            else:
                policies.append({"path": path, "owner": owner, "unresolved": ast.unparse(node)})

        walk(tree)
    for base in ("src", "tools"):
        for path in sorted((root / base).rglob("*.json")):
            try:
                value = json.loads(path.read_bytes())
            except (ValueError, UnicodeError):
                continue
            def data(node):
                if isinstance(node, dict):
                    for item in node.values():
                        data(item)
                elif isinstance(node, list):
                    for item in node:
                        data(item)
                elif isinstance(node, str) and WIRE.fullmatch(node):
                    rows[node]["occurrences"].append({"path": str(path.relative_to(root)), "owner": "<data>", "embedded": False})
            data(value)
    for wire, kind in known.items():
        rows[wire]["catalogue"] = kind
    result = []
    for wire, value in sorted(rows.items()):
        if wire in known:
            disposition = "qualified_adapter"
        elif not wire.startswith(("prismabuild.", "prismaquant.prismabuild.", "fleet.")):
            disposition = "control_or_algorithm_requires_review"
        elif all(any(part in row["path"] for part in ("bench", "qualify", "diag_", "diag940", "pbcanary", "d38_gate")) for row in value["occurrences"]):
            disposition = "control_report"
        else:
            disposition = "active_requires_adapter"
        result.append({"wire_id": wire, "disposition": disposition, **value})
    return result, sorted(policies, key=lambda row: (row["path"], row["owner"], json.dumps(row, sort_keys=True)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--require-complete", action="store_true")
    args = parser.parse_args()
    rows, policies = audit(args.root)
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "inventory.jsonl").write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows))
    (args.out / "field_sets.json").write_text(json.dumps(policies, sort_keys=True) + "\n")
    summary = {"wire_ids": len(rows), "dispositions": dict(Counter(r["disposition"] for r in rows)),
               "resolved_closed_boundaries": sum("fields" in r for r in policies),
               "unresolved_closed_boundaries": sum("unresolved" in r for r in policies),
               "syntax_builders": sum(len(r["schema_builders"]) for r in rows)}
    (args.out / "inventory-summary.json").write_text(json.dumps(summary, sort_keys=True, indent=2) + "\n")
    print(json.dumps(summary, sort_keys=True))
    if args.require_complete and any("requires" in row["disposition"] for row in rows):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
