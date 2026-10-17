"""Audit C14 ownership and preserve the exact base's tests and assertions."""
from __future__ import annotations
import argparse
import ast
from collections import Counter
import sys
import io
import json
from pathlib import Path
import subprocess
import tarfile

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from prismabuild.digest_primitives import raw_sha256

BASE = "f0bcb21e195378e7fb6e85e1bbfde24c45672e6b"
PROTECTED = ("action_client", "action_wait", "client_channel", "client_recovery",
             "capability_eligibility", "contracts", "record_store")
NEW_OWNERS = {"submission", "submission_types", "d38_gate", "target_evidence"}
CLI = {"main", "parse_args", "configured_store", "render_submission", "__file__",
       "RUNTIME_ROOT", "fleet_roster", "request_from_args", "_profile_mode"}
RENAMES = {"build_git_checkout_snapshot": "snapshot_checkout", "freeze_action_template": "freeze_template",
    "prepare_submission": "prepare", "seal_action_from_template": "seal",
    "seal_decomposed_child": "seal_child", "submit_and_publish": "_prepare_and_publish",
    "submit_deferred": "register_deferred", "template_from_record": "restore_template",
    "template_record": "record_template", "seal_movement_action": "seal_movement",
    "check_supersession_or_exit": "validate_supersession_request",
    "file_supersession_or_exit": "record_supersession_request", "release_origin_consumer_cli": "release_declared_consumer"}
ROOTS = {"SH": "root", "FLEET_ROSTER_PATH": "roster_path",
         "CONTAINER_WRAPPER_DIR": "wrapper_dir", "RUNTIME_VERSION": "runtime_version"}


def submission_source_digest(raw: bytes) -> str:
    return raw_sha256(raw)


def base_sources(root: Path) -> dict[str, str]:
    raw = subprocess.check_output(["git", "archive", BASE, "tests", "tools/fleet/pbrun.py"], cwd=root)
    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
        return {member.name: archive.extractfile(member).read().decode()
                for member in archive.getmembers() if member.isfile() and member.name.endswith(".py")}


class Owner(ast.NodeTransformer):
    """Ignore owner, explicit context and checked-refusal representation changes."""
    def visit_Attribute(self, node):
        if isinstance(node.value, ast.Name) and node.value.id == "pbrun" and node.attr not in CLI:
            if node.attr in ROOTS:
                return ast.Name("store_" + ROOTS[node.attr], ast.Load())
            node = ast.Attribute(ast.Name("submit_core", ast.Load()), RENAMES.get(node.attr, node.attr), ast.Load())
        if isinstance(node.value, ast.Call) and ast.unparse(node.value).endswith("configured_store()"):
            return ast.Name("store_" + node.attr, ast.Load())
        node = self.generic_visit(node)
        if node.attr in {"reason", "returncode"} and isinstance(node.value, ast.Attribute) and node.value.attr == "value":
            node.attr = "code"
        return node

    def visit_Name(self, node):
        if node.id == "submission":
            node.id = "submit_core"
        return node

    def visit_Call(self, node):
        if isinstance(node.func, ast.Name) and node.func.id == "refusal_value":
            return ast.Attribute(self.visit(node.args[0]), "code", ast.Load())
        node = self.generic_visit(node)
        if isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name) and node.func.value.id == "submit_core":
            node.keywords = [key for key in node.keywords if key.arg not in {"store", "display"}]
        return node


def assertion_owner_form(node) -> str:
    return ast.dump(Owner().visit(ast.parse(ast.unparse(node))), include_attributes=False)


def test_facts(source: str):
    tree = ast.parse(source)
    definitions = Counter(node.name for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test"))
    assertions = [node for node in ast.walk(tree) if isinstance(node, ast.Assert)]
    return definitions, assertions


def assertion_delta_rows(conditions, delta):
    remaining, result = delta.copy(), []
    for node in conditions:
        form = assertion_owner_form(node)
        if remaining[form]:
            result.append({"line": node.lineno, "assertion": ast.unparse(node),
                           "normalized_sha256": submission_source_digest(form.encode())})
            remaining[form] -= 1
    return result


def legacy_cli_references(tree):
    aliases = {"pbrun", "pbrun_mod"}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            aliases.update(alias.asname or alias.name for alias in node.names if alias.name == "pbrun")
    references = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id in aliases:
            if node.attr not in CLI:
                references.append([node.lineno, node.attr])
        if not isinstance(node, ast.Call):
            continue
        patch = isinstance(node.func, ast.Attribute) and node.func.attr in {"setattr", "object"}
        read = isinstance(node.func, ast.Name) and node.func.id in {"getattr", "hasattr", "setattr"}
        if not (patch or read) or len(node.args) < 2:
            continue
        if isinstance(node.args[0], ast.Name) and node.args[0].id in aliases:
            names = [part.value for part in ast.walk(node.args[1])
                     if isinstance(part, ast.Constant) and isinstance(part.value, str)]
            references.extend([node.lineno, "dynamic:" + name] for name in names if name not in CLI)
            if not names:
                references.append([node.lineno, "unresolved dynamic CLI attribute"])
        if patch and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
            owner, _, name = node.args[0].value.partition(".")
            if owner in aliases and name not in CLI:
                references.append([node.lineno, "patch:" + name])
    return references


def embedded_programs(tree):
    parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
    for node in ast.walk(tree):
        if isinstance(node, ast.JoinedStr):
            source = "".join(part.value if isinstance(part, ast.Constant) else repr("C14_EXPRESSION")
                             for part in node.values)
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            if isinstance(parents.get(node), ast.JoinedStr):
                continue
            source = node.value
        else:
            continue
        if "import " in source and "pbrun" in source:
            try:
                yield node.lineno, ast.parse(source)
            except SyntaxError:
                # Textual fragments and documentation are not executable programs.
                continue


def inspect(root: Path) -> dict:
    errors, legacy, forbidden, embedded = [], [], [], []
    tests, assertions, changed = {}, {}, {}
    baseline = base_sources(root)
    missing_definitions, missing_assertions = {}, {}
    base_assertions = base_definitions = 0
    for folder in ("src", "tools", "tests"):
        for path in sorted((root / folder).rglob("*.py")):
            relative = path.relative_to(root).as_posix()
            source = path.read_text()
            try:
                tree = ast.parse(source)
            except SyntaxError as exc:
                errors.append({"path": relative, "error": str(exc)})
                continue
            if folder == "tests":
                definitions, conditions = test_facts(source)
                tests[relative], assertions[relative] = sum(definitions.values()), len(conditions)
                if relative in baseline:
                    old_definitions, old_conditions = test_facts(baseline[relative])
                    base_definitions += sum(old_definitions.values())
                    base_assertions += len(old_conditions)
                    if missing := old_definitions - definitions:
                        missing_definitions[relative] = dict(missing)
                    if len(conditions) < len(old_conditions):
                        missing_assertions[relative] = [len(old_conditions), len(conditions)]
                    old = Counter(assertion_owner_form(node) for node in old_conditions)
                    new = Counter(assertion_owner_form(node) for node in conditions)
                    removed = old - new
                    if removed:
                        changed[relative] = {"old": assertion_delta_rows(old_conditions, removed),
                                             "new": assertion_delta_rows(conditions, new - old)}
            legacy.extend([relative, *row] for row in legacy_cli_references(tree))
            for line, program in embedded_programs(tree):
                refs = legacy_cli_references(program)
                if refs:
                    embedded.append([relative, line, refs])
            for node in ast.walk(tree):
                if folder == "src" and path.stem in NEW_OWNERS:
                    if isinstance(node, (ast.Import, ast.ImportFrom)):
                        names = [alias.name for alias in node.names]
                        if getattr(node, "module", "") in {"pbrun", "argparse", "runpy"} or any(name in {"pbrun", "argparse", "runpy"} for name in names):
                            forbidden.append([relative, node.lineno, ast.unparse(node)])
                    if isinstance(node, ast.Attribute) and ast.unparse(node) == "sys.argv":
                        forbidden.append([relative, node.lineno, "sys.argv"])
    protected = {}
    for name in PROTECTED:
        relative = f"src/prismabuild/{name}.py"
        actual = (root / relative).read_bytes()
        old = subprocess.check_output(["git", "show", f"{BASE}:{relative}"], cwd=root)
        protected[relative] = {"sha256": submission_source_digest(actual), "unchanged": actual == old}
    old_api = {node.name for node in ast.parse(baseline["tools/fleet/pbrun.py"]).body
               if isinstance(node, (ast.FunctionDef, ast.ClassDef))} - CLI
    current_api = {node.name for node in ast.parse((root / "tools/fleet/pbrun.py").read_text()).body
                   if isinstance(node, (ast.FunctionDef, ast.ClassDef))}
    registry_path = root / "tools" / "maintenance" / "submission_assertions.json"
    registered = json.loads(registry_path.read_text()) if registry_path.exists() else {}
    actual = {path: {side: sorted(row["normalized_sha256"] for row in rows[side])
                     for side in ("old", "new")} for path, rows in changed.items()}
    unreviewed = sorted(path for path in actual.keys() | registered.keys()
        if path not in registered or path not in actual
        or {side: registered[path].get(side) for side in ("old", "new")} != actual.get(path)
        or len(registered[path].get("reason", "").split()) < 8)
    return {"base": BASE, "syntax_errors": errors, "legacy_references": legacy,
        "embedded_legacy": embedded, "forbidden": forbidden, "protected": protected,
        "deleted_pbrun_api": sorted(old_api), "retained_legacy_api": sorted(old_api & current_api),
        "missing_definitions": missing_definitions, "missing_assertions": missing_assertions,
        "changed_assertions": changed, "unreviewed_assertion_changes": unreviewed,
        "tests": tests, "assertions": assertions,
        "base_test_definitions": base_definitions, "base_assertions": base_assertions}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = inspect(args.root)
    if args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: result[key] for key in ("syntax_errors", "legacy_references", "embedded_legacy",
        "forbidden", "retained_legacy_api", "missing_definitions", "missing_assertions",
        "unreviewed_assertion_changes")} | {
        "protected": all(row["unchanged"] for row in result["protected"].values()),
        "deleted_api_count": len(result["deleted_pbrun_api"]), "test_files": len(result["tests"]),
        "test_definitions": sum(result["tests"].values()), "assertions": sum(result["assertions"].values()),
        "changed_assertion_files": len(result["changed_assertions"])}))
    return bool(any(result[key] for key in ("syntax_errors", "legacy_references", "embedded_legacy",
        "forbidden", "retained_legacy_api", "missing_definitions", "missing_assertions",
        "unreviewed_assertion_changes")) or
        not all(row["unchanged"] for row in result["protected"].values()))


if __name__ == "__main__":
    raise SystemExit(main())
