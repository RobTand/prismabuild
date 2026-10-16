"""Inventory wire literals, constructors, imports, JSON boundaries and closed fields."""
from __future__ import annotations

import argparse
import ast
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import re

WIRE = re.compile(r"^[A-Za-z][\w.-]+\.v\d+$")


def production(path):
    return not path.startswith("tests/")


def module_name(path):
    if path.startswith("src/"):
        return path[4:-3].replace("/", ".")
    if path.startswith("tests/"):
        return path[:-3].replace("/", ".")
    return Path(path).stem


def sources(root, observations):
    result = {}
    for base in ("src", "tools", "tests"):
        for path in sorted((root / base).rglob("*")):
            if not path.is_file() or path.suffix not in (".py", ""):
                continue
            try:
                raw = path.read_bytes()
                result[str(path.relative_to(root))] = ast.parse(raw)
                observations["parsed_sources"].append({"path": str(path.relative_to(root)),
                                                       "sha256": hashlib.sha256(raw).hexdigest()})
            except (SyntaxError, UnicodeError) as error:
                observations["unparsed_sources"].append({"path": str(path.relative_to(root)),
                                                         "reason": type(error).__name__,
                                                         "python_suffix": path.suffix == ".py"})
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
            if value["wire_id"] in result or any(row["name"] == value["name"] for row in result.values()):
                raise ValueError("record kinds must not alias an existing name or wire ID")
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


def audit(root, observations=None):
    observations = observations if observations is not None else {}
    observations.update({key: [] for key in ("parsed_sources", "unparsed_sources", "unresolved_schema_builders", "serialization_sites")})
    trees = sources(root, observations)
    known = catalogue(root)
    required_generations = set()
    for name in ("window.json", "prepared.json"):
        path = root / "tests/compat/generations" / name
        if path.exists():
            document = json.loads(path.read_bytes())
            required_generations.update(row["generation"] for row in
                                        (document["generations"] if isinstance(document, dict) else document))
    captured = {"current": set(), "historical": defaultdict(set)}
    for path in (root / "tests/compat").glob("*.jsonl"):
        if not path.name.startswith(("goldens", "historical")):
            continue
        for line in path.read_text().splitlines():
            item = json.loads(line)
            if path.name.startswith("goldens"):
                captured["current"].add(item["case"])
            else:
                captured["historical"][item["generation"]].add(item["case"])
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
    rows = defaultdict(lambda: {"occurrences": [], "schema_builders": [], "json_boundaries": [], "reader_checks": []})
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

        def scope_values(node, parent):
            values = dict(parent)
            for child in ast.walk(node):
                if not isinstance(child, (ast.Assign, ast.AnnAssign)) or child.value is None:
                    continue
                targets = child.targets if isinstance(child, ast.Assign) else [child.target]
                try:
                    value = evaluate(child.value, values)
                except (ValueError, TypeError, KeyError, IndexError):
                    continue
                for target in targets:
                    if isinstance(target, ast.Name):
                        values[target.id] = value
            return values

        def walk(node, owner="<module>", embedded=False, scope=None):
            scope = local if scope is None else scope
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                owner = (owner + "." + node.name).removeprefix("<module>.")
                scope = scope_values(node, scope)
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if WIRE.fullmatch(node.value):
                    rows[node.value]["occurrences"].append({"path": path, "owner": owner, "line": node.lineno, "embedded": embedded})
                elif "schema" in node.value and "\n" in node.value:
                    try:
                        walk(ast.parse(node.value), owner + ":embedded", True, scope)
                    except SyntaxError:
                        pass
            schema, names, expansion, schema_node = None, [], False, None
            if isinstance(node, ast.Dict):
                for key, value in zip(node.keys, node.values):
                    if key is None:
                        expansion = True
                    elif isinstance(key, ast.Constant) and isinstance(key.value, str):
                        names.append(key.value)
                        if key.value == "schema":
                            schema_node = value
                            schema = resolve(value)
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name) and node.func.id == "dict":
                    names = [kw.arg for kw in node.keywords if kw.arg]
                    expansion = any(kw.arg is None for kw in node.keywords)
                    schema_node = next((kw.value for kw in node.keywords if kw.arg == "schema"), None)
                    schema = resolve(schema_node)
                target = ast.unparse(node.func)
                if target.endswith(("json.dump", "json.dumps", "write_json", "write_record", "write_document", "_write_json_atomic", "_write_atomic", "_publish_immutable", "publish_immutable")):
                    observations["serialization_sites"].append({"path": path, "owner": owner, "line": node.lineno,
                                                                "call": target, "embedded": embedded})
                    # Boundaries remain source witnesses until a production adapter covers them.
                    for wire in set(v for v in local.values() if isinstance(v, str) and WIRE.fullmatch(v)):
                        rows[wire]["json_boundaries"].append({"path": path, "owner": owner, "line": node.lineno, "call": target})
                if target.endswith("_exact_mapping"):
                    for kw in node.keywords:
                        if kw.arg == "keys":
                            pin(kw.value, owner, scope)
            if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Sub) and isinstance(node.left, ast.Call) and isinstance(node.left.func, ast.Name) and node.left.func.id == "set":
                pin(node.right, owner, scope)
            if isinstance(node, (ast.Assign, ast.AnnAssign)) and node.value is not None:
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                for target in targets:
                    if isinstance(target, ast.Name) and re.fullmatch(r"_?(?:allowed|optional|required|keys|fields)|.*_(?:KEYS|FIELDS)", target.id):
                        pin(node.value, owner, scope, binding=target.id)
            if isinstance(node, ast.Compare):
                pairs = zip((node.left, *node.comparators[:-1]), node.comparators)
                for left, right in pairs:
                    is_keys = lambda item: (isinstance(item, ast.Call) and isinstance(item.func, ast.Name)
                                            and item.func.id == "set")
                    if is_keys(left) or is_keys(right):
                        fields = right if is_keys(left) else left
                        if isinstance(fields, (ast.Tuple, ast.List)) and any(isinstance(part, ast.Set) for part in fields.elts):
                            for part in fields.elts:
                                pin(part, owner, scope)
                        elif not is_keys(fields):
                            pin(fields, owner, scope)
                for expression in ast.walk(node):
                    wire = resolve(expression)
                    if isinstance(wire, str) and WIRE.fullmatch(wire) and "schema" in ast.unparse(node).lower():
                        rows[wire]["reader_checks"].append({"path": path, "owner": owner, "line": node.lineno,
                                                           "expression": ast.unparse(node), "embedded": embedded})
            if isinstance(schema, str) and WIRE.fullmatch(schema):
                rows[schema]["schema_builders"].append({"path": path, "owner": owner, "line": node.lineno,
                                                        "fields": sorted(names), "expansion": expansion, "embedded": embedded})
            elif schema_node is not None:
                observations["unresolved_schema_builders"].append({"path": path, "owner": owner, "line": node.lineno,
                                                                   "schema": ast.unparse(schema_node), "fields": sorted(names),
                                                                   "embedded": embedded})
            for child in ast.iter_child_nodes(node):
                walk(child, owner, embedded, scope)

        def pin(node, owner, scope, **extra):
            try:
                fields = evaluate(node, scope)
            except (ValueError, TypeError, KeyError, IndexError):
                fields = None
            if isinstance(fields, (set, frozenset, list, tuple)) and all(isinstance(k, str) for k in fields):
                policies.append({"path": path, "owner": owner, "fields": sorted(fields), **extra})
            else:
                policies.append({"path": path, "owner": owner, "unresolved": ast.unparse(node), **extra})

        walk(tree)
    for base in ("src", "tools", "tests"):
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
        witnesses = [row for row in value["occurrences"] if production(row["path"])]
        for category in ("occurrences", "schema_builders", "json_boundaries", "reader_checks"):
            for row in value[category]:
                row["scope"] = "production" if production(row["path"]) else "test_or_corpus_control"
        if wire in known:
            kind = known[wire]
            scenarios = {kind["name"] + "/" + variant for variant in kind["variants"]}
            value["missing_current_captures"] = sorted(scenarios - captured["current"])
            value["historical_capture_counts"] = {generation: len(scenarios & found)
                                                   for generation, found in captured["historical"].items()}
            value["missing_historical_captures"] = {generation: sorted(scenarios - captured["historical"][generation])
                                                     for generation in sorted(required_generations)
                                                     if scenarios - captured["historical"][generation]}
            disposition = ("adapter_requires_capture" if value["missing_current_captures"] or value["missing_historical_captures"]
                           else "catalogued_adapter")
        elif wire in ("buffered-sequential-write-fdatasync-read.v1", "contiguous-residency-exact-cover.v1",
                      "repeated-shake256-block.v1"):
            disposition = "algorithm_identifier_not_record"
            assert not any(row["scope"] == "production" for row in value["schema_builders"])
        elif wire == "prismaquant.prismabuild.action.v1":
            disposition = "retired_rejected_schema"
            value["disposition_reason"] = "core.validate_action accepts action.v2 only; no current v1 constructor"
            assert not any(row["scope"] == "production" for row in value["schema_builders"])
        elif not witnesses:
            disposition = "test_or_history_reference"
        elif not wire.startswith(("prismabuild.", "prismaquant.prismabuild.", "fleet.")):
            disposition = "control_or_algorithm_requires_review"
        elif all(any(part in row["path"] for part in ("bench", "qualify", "diag_", "diag940", "pbcanary", "d38_gate")) for row in witnesses):
            disposition = "control_report"
        else:
            disposition = "active_requires_adapter"
        result.append({"wire_id": wire, "disposition": disposition, **value})
    for category in observations.values():
        for row in category:
            row["scope"] = "production" if production(row["path"]) else "test_or_corpus_control"
    policies = [row for row in policies if production(row["path"])]
    return result, sorted(policies, key=lambda row: (row["path"], row["owner"], json.dumps(row, sort_keys=True)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--require-complete", action="store_true")
    args = parser.parse_args()
    observations = {}
    rows, policies = audit(args.root, observations)
    args.out.mkdir(parents=True, exist_ok=False)
    (args.out / "inventory.jsonl").write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows))
    (args.out / "field_sets.json").write_text(json.dumps(policies, sort_keys=True) + "\n")
    (args.out / "source-audit.json").write_text(json.dumps(observations, sort_keys=True) + "\n")
    summary = {"wire_ids": len(rows), "dispositions": dict(Counter(r["disposition"] for r in rows)),
               "production_wire_ids": sum(r["disposition"] != "test_or_history_reference" for r in rows),
               "resolved_field_sets": sum("fields" in r for r in policies),
               "unresolved_field_expressions": sum("unresolved" in r for r in policies),
               "field_policy_modules": len({r["path"] for r in policies if "fields" in r}),
               "syntax_builders": sum(len(r["schema_builders"]) for r in rows),
               "production_syntax_builders": sum(sum(w["scope"] == "production" for w in r["schema_builders"]) for r in rows)}
    summary.update({key: len(value) for key, value in observations.items()})
    (args.out / "inventory-summary.json").write_text(json.dumps(summary, sort_keys=True, indent=2) + "\n")
    for path in args.out.iterdir():
        path.chmod(0o444)
    print(json.dumps(summary, sort_keys=True))
    if args.require_complete and (any("requires" in row["disposition"] for row in rows)
                                  or any(row["python_suffix"] for row in observations["unparsed_sources"])):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
