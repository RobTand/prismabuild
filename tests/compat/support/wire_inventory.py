"""Inventory wire literals, constructors, imports, JSON boundaries and closed fields."""
from __future__ import annotations

import argparse
import ast
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import re
import tarfile
import tempfile

WIRE = re.compile(r"^[A-Za-z][\w.-]+\.v\d+$")


def production(path):
    return not path.startswith("tests/")


def module_name(path):
    if path.startswith("src/"):
        return path[4:-3].replace("/", ".")
    if path.startswith("tests/"):
        return path[:-3].replace("/", ".")
    return Path(path).stem


def source_units(path, raw):
    if path.suffix != ".sh":
        yield None, raw, 1
        return
    text = raw.decode()
    pattern = r"<<-?\s*(['\"]?)([A-Za-z_]\w*)\1[^\n]*\n(.*?)^\2[ \t]*$"
    for match in re.finditer(pattern, text, re.MULTILINE | re.DOTALL):
        line = text.count("\n", 0, match.start(3)) + 1
        yield str(line), match[3].encode(), line


def syntax_tree(path, snippet):
    try:
        return ast.parse(snippet), False
    except SyntaxError:
        if path.suffix != ".sh" or not snippet.lstrip().startswith(b"{") or b'"schema"' not in snippet:
            raise
        # Inspect keys and literal IDs of shell-expanded JSON; never execute
        # substitutions or describe the placeholder values as wire bytes.
        previous, sanitized = None, snippet
        while previous != sanitized:
            previous = sanitized
            sanitized = re.sub(rb"\$\([^()\n]*\)", b"0", sanitized)
        sanitized = re.sub(rb"\$\{[^}\n]*\}|\$[A-Za-z_][A-Za-z_0-9]*", b"shell_value", sanitized)
        return ast.parse(sanitized), True


def sources(root, observations):
    for base in ("src", "tools", "tests", "fleet"):
        for path in sorted((root / base).rglob("*")):
            if not path.is_file() or path.suffix not in (".py", "", ".sh"):
                continue
            raw = path.read_bytes()
            for block, snippet, line in source_units(path, raw):
                source = {"path": str(path.relative_to(root)), "sha256": hashlib.sha256(raw).hexdigest(),
                          "block": block, "line_start": line}
                try:
                    tree, template = syntax_tree(path, snippet)
                except (SyntaxError, UnicodeError) as error:
                    observations["unparsed_sources"].append({**source, "reason": type(error).__name__,
                        "python_suffix": path.suffix == ".py", "contains_wire_schema": b"schema" in snippet
                            and bool(re.search(rb"[A-Za-z][\w.-]+\.v\d+", snippet))})
                    continue
                observations["parsed_sources"].append(source)
                source["shell_template"] = template
                yield source, tree


def catalogue(root):
    path = root / "src/prismabuild/record_kinds.py"
    if not path.is_file():
        return {}
    tree = ast.parse(path.read_bytes())
    result = {}
    fields = ("name", "wire_id", "location", "field_policy", "reader", "writers", "variants", "legacy", "closed_sets")
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "RecordKind":
            value = {field: ast.literal_eval(arg) for field, arg in zip(fields, node.args)}
            value.update({kw.arg: ast.literal_eval(kw.value) for kw in node.keywords})
            if value["wire_id"] is None and value.get("fixture_source"):
                source = next(root / path for path in value["fixture_source"] if (root / path).is_file())
                value["wire_id"] = json.loads(source.read_bytes())["schema"]
                value["wire_identity_owner"] = str(source.relative_to(root))
            key = value["wire_id"] or "unversioned:" + value["name"]
            if key in result or any(row["name"] == value["name"] for row in result.values()):
                raise ValueError("record kinds must not alias an existing name or wire ID")
            result[key] = value
    return result


def classifications(root):
    path = root / "tests/compat/dispositions.jsonl"
    result = {}
    for line in path.read_text().splitlines() if path.exists() else ():
        row = json.loads(line)
        assert row["wire_id"] not in result and row["reason"]
        assert row["disposition"] in {"operator_control", "identity_domain", "derived_projection", "retired_api"}
        result[row["wire_id"]] = row
    return result


def retained_inventory(root):
    for archive in sorted((root / "tests/compat/generations").glob("*.tar.gz")):
        manifest = json.loads(archive.with_suffix("").with_suffix(".json").read_bytes())
        assert hashlib.sha256(archive.read_bytes()).hexdigest() == manifest["archive_sha256"]
        with tempfile.TemporaryDirectory(prefix="compat-source-inventory-") as directory:
            destination = Path(directory)
            receipt_name = manifest.get("provenance_file", "RUNTIME_VERSION.json")
            with tarfile.open(archive) as contents:
                names = set()
                for member in contents:
                    path = Path(member.name)
                    assert member.isfile() and not path.is_absolute() and ".." not in path.parts and member.name not in names
                    names.add(member.name)
                    raw = contents.extractfile(member).read()
                    assert member.name in manifest["files"] or member.name == receipt_name
                    expected = manifest["receipt_sha256"] if member.name == receipt_name else manifest["files"][member.name]
                    assert hashlib.sha256(raw).hexdigest() == expected
                    path = destination / path
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(raw)
                assert set(manifest["files"]) < names and len(names) == len(manifest["files"]) + 1
            rows, _ = audit(destination)
            yield manifest["generation"], manifest["archive_sha256"], rows


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
    known = catalogue(root)
    reviewed = classifications(root)
    assert not known.keys() & reviewed.keys(), "a kind cannot be qualified and excluded from the window"
    required_generations = set()
    for name in ("window.json", "prepared.json"):
        path = root / "tests/compat/generations" / name
        if path.exists():
            document = json.loads(path.read_bytes())
            required_generations.update(row["generation"] for row in
                                        (document["generations"] if isinstance(document, dict) else document))
    captured = {"current": set(), "historical": defaultdict(set), "absent_inputs": defaultdict(set)}
    for path in (root / "tests/compat").glob("*.jsonl"):
        if not path.name.startswith(("goldens", "historical", "absent-inputs", "absent-participants")):
            continue
        for line in path.read_text().splitlines():
            item = json.loads(line)
            if path.name.startswith("goldens"):
                captured["current"].add(item["case"])
            elif path.name.startswith(("absent-inputs", "absent-participants")):
                kind = next(row for row in known.values() if row["name"] == item["case"].split("/")[0])
                assert kind.get("older_participant_absence") or (kind.get("caller_input") and kind.get("older_input_absence") and not kind["writers"])
                captured["absent_inputs"][item["generation"]].add(item["case"])
            else:
                captured["historical"][item["generation"]].add(item["case"])
    wire_ids = {row["name"]: wire for wire, row in known.items()}
    constants, imports, exports = {}, {}, {}
    for unit, tree in sources(root, observations):
        path = unit["path"]
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
        imports[path, unit["block"]] = (local, aliases)
        exports.update({module + "." + name: origin for name, origin in aliases.items()})

    def imported_constant(name):
        visited = set()
        while name not in visited:
            if name in constants:
                return constants[name]
            visited.add(name)
            prefix = name
            while prefix:
                if prefix in exports:
                    name = exports[prefix] + name[len(prefix):]
                    break
                prefix = prefix.rsplit(".", 1)[0] if "." in prefix else ""
            else:
                return None
        return None
    rows = defaultdict(lambda: {"occurrences": [], "schema_builders": [], "json_boundaries": [], "reader_checks": []})
    policies = []
    for source_file in observations["parsed_sources"]:
        path = source_file["path"]
        raw = (root / path).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == source_file["sha256"], "source changed during inventory"
        snippet = next(snippet for block, snippet, _ in source_units(Path(path), raw) if block == source_file["block"])
        tree, _ = syntax_tree(Path(path), snippet)
        local, aliases = imports[path, source_file["block"]]
        module = module_name(path)

        def resolve(node, scope):
            imported = scope.get("__wire_imports__", aliases)
            if isinstance(node, ast.Name):
                return scope.get(node.id, local.get(node.id, imported_constant(imported.get(node.id, node.id))))
            if isinstance(node, ast.Attribute):
                head, tail = node, []
                while isinstance(head, ast.Attribute):
                    tail.insert(0, head.attr)
                    head = head.value
                if isinstance(head, ast.Name):
                    return imported_constant(imported.get(head.id, head.id) + "." + ".".join(tail))
            if isinstance(node, ast.Constant):
                return node.value
            return None

        def scope_values(node, parent):
            values = dict(parent)
            imported = dict(parent.get("__wire_imports__", aliases))
            for child in ast.walk(node):
                if isinstance(child, ast.ImportFrom):
                    origin = child.module or ""
                    if child.level:
                        origin = ".".join(module.split(".")[:-child.level]) + ("." + origin if origin else "")
                    imported.update({name.asname or name.name: origin + "." + name.name for name in child.names})
                elif isinstance(child, ast.Import):
                    imported.update({name.asname or name.name: name.name for name in child.names})
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
            values["__wire_imports__"] = imported
            return values

        def walk(node, owner="<module>", embedded=False, scope=None):
            scope = local if scope is None else scope
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                owner = (owner + "." + node.name).removeprefix("<module>.")
                scope = scope_values(node, scope)
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if WIRE.fullmatch(node.value):
                    rows[node.value]["occurrences"].append({"path": path, "owner": owner, "line": node.lineno, "embedded": embedded})
                elif "schema" in node.value:
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
                            schema = resolve(value, scope)
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name) and node.func.id == "dict":
                    names = [kw.arg for kw in node.keywords if kw.arg]
                    expansion = any(kw.arg is None for kw in node.keywords)
                    schema_node = next((kw.value for kw in node.keywords if kw.arg == "schema"), None)
                    schema = resolve(schema_node, scope)
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
                    wire = resolve(expression, scope)
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

        walk(tree, "<module>" if source_file["block"] is None else
             ("<shell-json@" if source_file["shell_template"] else "<shell-python@") + source_file["block"] + ">")
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
    for base in ("src", "tools", "tests", "fleet"):
        for path in sorted((root / base).rglob("*")):
            if not path.is_file() or path.suffix not in (".sh", ".yml", ".yaml", ".toml", ".txt"):
                continue
            try:
                source = path.read_text()
            except UnicodeError:
                continue
            for wire in sorted(set(re.findall(r"(?<![\w.-])([A-Za-z][\w.-]+\.v\d+)(?![\w.-])", source))):
                rows[wire]["occurrences"].append({"path": str(path.relative_to(root)), "owner": "<text-reference>", "embedded": False})
    for generation, archive_hash, historical in retained_inventory(root):
        for historical_row in historical:
            wire = historical_row["wire_id"]
            rows[wire].setdefault("retained", {})[generation] = {
                "archive_sha256": archive_hash, **{key: historical_row[key] for key in
                    ("occurrences", "schema_builders", "reader_checks")}}
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
            value["historical_input_absence_counts"] = {generation: len(scenarios & found)
                                                        for generation, found in captured["absent_inputs"].items()}
            value["missing_historical_captures"] = {generation: sorted(scenarios - captured["historical"][generation]
                                                                        - captured["absent_inputs"][generation])
                                                     for generation in sorted(required_generations)
                                                     if scenarios - captured["historical"][generation]
                                                        - captured["absent_inputs"][generation]}
            disposition = ("adapter_requires_capture" if value["missing_current_captures"] or value["missing_historical_captures"]
                           else "catalogued_adapter")
        elif wire in reviewed:
            declaration = reviewed[wire]
            value["classification"] = declaration
            observed = {category: sorted({row["path"] + ":" + row["owner"] for row in value[category]
                                          if row["scope"] == "production"})
                        for category in ("occurrences", "schema_builders", "reader_checks")}
            mismatch = {category: values for category, values in observed.items()
                        if values != declaration["witnesses"][category]}
            value["classification_changed"] = mismatch
            disposition = "classification_requires_review" if mismatch else declaration["disposition"]
        elif wire in ("buffered-sequential-write-fdatasync-read.v1", "contiguous-residency-exact-cover.v1",
                      "repeated-shake256-block.v1"):
            disposition = "algorithm_identifier_not_record"
            assert not any(row["scope"] == "production" for row in value["schema_builders"])
        elif wire == "prismaquant.prismabuild.action.v1":
            disposition = "retired_rejected_schema"
            value["disposition_reason"] = "core.validate_action accepts action.v2 only; no current v1 constructor"
            assert not any(row["scope"] == "production" for row in value["schema_builders"])
        elif not witnesses and value.get("retained"):
            disposition = "retained_only_requires_review"
        elif not witnesses:
            disposition = "test_or_history_reference"
        elif not wire.startswith(("prismabuild.", "prismaquant.prismabuild.", "fleet.")):
            disposition = "control_or_algorithm_requires_review"
        else:
            disposition = "active_requires_adapter"
        result.append({"wire_id": known[wire]["wire_id"] if wire in known else wire,
                       "disposition": disposition, **value})
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
    dynamic = [row for row in observations["unresolved_schema_builders"] if row["scope"] == "production"]
    review_path = args.root / "tests/compat/dynamic_schema_dispositions.jsonl"
    reviews = [json.loads(line) for line in review_path.read_text().splitlines()] if review_path.exists() else []
    reviewed = set()
    review_problems = []
    for review in reviews:
        source_path = args.root / review["path"]
        if not source_path.is_file() or hashlib.sha256(source_path.read_bytes()).hexdigest() != review["source_sha256"]:
            review_problems.append(dict(problem="reviewed source changed or moved", review=review))
            continue
        matches = [row for row in dynamic if all(row[key] == review[key] for key in ("path", "owner", "line", "schema", "fields"))]
        if len(matches) != 1 or not review["reason"]:
            review_problems.append(dict(problem="stale or ambiguous dynamic-schema review", review=review))
            continue
        identity = review["path"], review["owner"], review["line"]
        if identity in reviewed:
            review_problems.append(dict(problem="duplicate dynamic-schema review", review=review))
            continue
        reviewed.add(identity)
    observations["dynamic_schema_reviews"] = reviews
    observations["source_review_problems"] = review_problems
    observations["unreviewed_dynamic_schema_builders"] = [row for row in dynamic
        if (row["path"], row["owner"], row["line"]) not in reviewed]
    args.out.mkdir(parents=True, exist_ok=False)
    (args.out / "inventory.jsonl").write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows))
    (args.out / "field_sets.json").write_text(json.dumps(policies, sort_keys=True) + "\n")
    (args.out / "source-audit.json").write_text(json.dumps(observations, sort_keys=True) + "\n")
    summary = {"wire_ids": sum(r["wire_id"] is not None for r in rows),
               "unversioned_catalogued_records": sum(r["wire_id"] is None for r in rows),
               "dispositions": dict(Counter(r["disposition"] for r in rows)),
               "production_wire_ids": sum(r["wire_id"] is not None and r["disposition"] != "test_or_history_reference" for r in rows),
               "resolved_field_sets": sum("fields" in r for r in policies),
               "unresolved_field_expressions": sum("unresolved" in r for r in policies),
               "field_policy_modules": len({r["path"] for r in policies if "fields" in r}),
               "syntax_builders": sum(len(r["schema_builders"]) for r in rows),
               "production_syntax_builders": sum(sum(w["scope"] == "production" for w in r["schema_builders"]) for r in rows)}
    summary.update({key: len(value) for key, value in observations.items()})
    summary["production_unresolved_schema_builders"] = sum(
        row["scope"] == "production" for row in observations["unresolved_schema_builders"])
    (args.out / "inventory-summary.json").write_text(json.dumps(summary, sort_keys=True, indent=2) + "\n")
    for path in args.out.iterdir():
        path.chmod(0o444)
    print(json.dumps(summary, sort_keys=True))
    if review_problems or args.require_complete and (any("requires" in row["disposition"] for row in rows)
                                  or observations["unreviewed_dynamic_schema_builders"]
                                  or any(row["python_suffix"] or row["contains_wire_schema"]
                                         for row in observations["unparsed_sources"])):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
