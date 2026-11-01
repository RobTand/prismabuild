"""Pin actual closed boundaries and find new writers outside the catalogue."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests/compat/support"))
from wire_inventory import audit, field_policies
from rebind_source_reviews import reviewed_sites


def test_closed_field_sets_remain_pinned():
    policies = field_policies(ROOT)
    expected = json.loads((ROOT / "tests/compat/field_sets.json").read_bytes())
    assert policies == expected


def test_symbolic_fstring_policies_retain_selected_source_bytes(tmp_path, monkeypatch):
    import ast
    original = ast.unparse

    def version_specific_unparse(node):
        if any(isinstance(part, ast.JoinedStr) for part in ast.walk(node)):
            raise AssertionError('interpolated source must not depend on interpreter rendering')
        return original(node)

    monkeypatch.setattr(ast, 'unparse', version_specific_unparse)
    expression = '''{'schema', value['host'], f"runtime-generation:{value['generation']}"}'''
    source = 'def check(value):\n    required = ' + expression + '\n    if set(value) != required: raise ValueError("fields")\n'
    for name in ('direct', 'embedded'):
        root = tmp_path / name
        package = root / 'src/prismabuild'
        package.mkdir(parents=True)
        (package / 'reader.py').write_text(source if name == 'direct' else 'SCRIPT = ' + repr(source) + '\n')
        _, policies = audit(root)
        selected = [row for row in policies if row.get('binding') == 'required']
        assert len(selected) == 1
        assert selected[0]['unresolved'] == expression


def test_inventory_finds_unregistered_imported_embedded_and_data_wires(tmp_path):
    package = tmp_path / "src/prismabuild"
    package.mkdir(parents=True)
    (package / "record_kinds.py").write_text("KINDS = ()\n")
    (package / "owner.py").write_text('SCHEMA = "example.actual_record.v7"\n')
    (package / "caller.py").write_text('from . import owner\ndef write():\n    return dict(schema=owner.SCHEMA, payload=1)\ndef read(row):\n    allowed = frozenset({"schema", "payload"})\n    if set(row) - allowed: raise ValueError("unknown")\n    if row.get("schema") != owner.SCHEMA: raise ValueError("schema")\n    return row["payload"]\n')
    (package / "lazy.py").write_text('def publish():\n    from . import owner as selected\n    return {"schema": selected.SCHEMA, "payload": 2}\n')
    tools = tmp_path / "tools"
    tools.mkdir()
    (tools / "embedded.py").write_text('SCRIPT = \'schema = {"schema": "example.embedded.v3"}\\n\'\n')
    (tools / "config.json").write_text('{"schema": "example.configuration.v2"}')
    (tools / "once.py").write_text('SCRIPT = \'{"schema": "example.one_line.v4"}\'\n')
    (tools / "embedded.sh").write_text('python3 -c \'print({"schema": "example.shell.v2"})\'\n')
    (tools / "record.sh").write_text("python3 <<'PY'\ndef publish():\n    return {'schema': 'example.heredoc.v5'}\nPY\n")
    (tools / "marker.sh").write_text('cat <<EOF\n{"schema": "example.marker.v2", "unix": $(date +%s), "host": "$HOST"}\nEOF\n')
    rows, policies = audit(tmp_path)
    found = {row["wire_id"]: row for row in rows}
    assert set(found) == {"example.actual_record.v7", "example.embedded.v3", "example.configuration.v2",
                          "example.one_line.v4", "example.shell.v2", "example.heredoc.v5", "example.marker.v2"}
    writer = found["example.actual_record.v7"]["schema_builders"]
    assert writer[0]["owner"] == "write" and writer[0]["fields"] == ["payload", "schema"]
    assert any(row["owner"] == "publish" and row["path"].endswith("lazy.py") for row in writer)
    assert found["example.embedded.v3"]["occurrences"][0]["embedded"]
    assert found["example.heredoc.v5"]["schema_builders"][0]["owner"].endswith(".publish")
    assert found["example.marker.v2"]["schema_builders"][0]["fields"] == ["host", "schema", "unix"]
    assert found["example.actual_record.v7"]["reader_checks"][0]["owner"] == "read"
    assert any(row["owner"] == "read" and row.get("fields") == ["payload", "schema"] for row in policies)


def test_control_classification_cannot_hide_a_new_durable_reader(tmp_path):
    package = tmp_path / "src/prismabuild"
    package.mkdir(parents=True)
    source = package / "report.py"
    source.write_text('def report():\n    return {"schema": "example.status.v1", "rows": []}\n')
    rows, _ = audit(tmp_path)
    row = rows[0]
    captured = {key: sorted({item["path"] + ":" + item["owner"] for item in row[key]})
                for key in ("occurrences", "schema_builders", "reader_checks")}
    destination = tmp_path / "tests/compat/dispositions.jsonl"
    destination.parent.mkdir(parents=True)
    destination.write_text(json.dumps({"wire_id": row["wire_id"], "disposition": "operator_control",
                                      "reason": "stdout report with no durable reader", "witnesses": captured}) + "\n")
    assert audit(tmp_path)[0][0]["disposition"] == "operator_control"
    with source.open("a") as stream:
        stream.write('def recover(row):\n    if row.get("schema") != "example.status.v1": raise ValueError("wrong generation")\n    return row["rows"]\n')
    changed = audit(tmp_path)[0][0]
    assert changed["disposition"] == "classification_requires_review"
    assert changed["classification_changed"]["reader_checks"] == ["src/prismabuild/report.py:recover"]


def test_inventory_follows_transitive_module_owners_without_importing_them(tmp_path):
    package = tmp_path / "src/prismabuild"
    package.mkdir(parents=True)
    (package / "owner.py").write_text('SCHEMA = "example.transitive.v1"\nraise RuntimeError("never import source")\n')
    (package / "facade.py").write_text('from . import owner as selected\nfrom .owner import SCHEMA as REEXPORTED\n')
    (package / "cycle_a.py").write_text('from .cycle_b import SCHEMA\n')
    (package / "cycle_b.py").write_text('from .cycle_a import SCHEMA\n')
    (package / "caller.py").write_text('from . import facade, cycle_a\n'
        'def read(row):\n    if row.get("schema") != facade.selected.SCHEMA: raise ValueError("identity")\n'
        '    return row\n'
        'def read_export(row):\n    if row.get("schema") != facade.REEXPORTED: raise ValueError("identity")\n'
        '    return row\n'
        'def unrelated(row):\n    return row.get("schema") == cycle_a.SCHEMA\n')
    rows, _ = audit(tmp_path)
    record = next(row for row in rows if row["wire_id"] == "example.transitive.v1")
    assert [(row["path"], row["owner"]) for row in record["reader_checks"]] == [
        ("src/prismabuild/caller.py", "read"), ("src/prismabuild/caller.py", "read_export")]


def test_source_review_rebinding_compares_every_non_schema_expression():
    old = b"def read():\n    return {'schema': old_owner.SCHEMA, 'body': record}\n"
    moved = old.replace(b"old_owner", b"new_owner")
    changed = moved.replace(b"'body': record", b"'body': other")
    members = ["body", "schema"]
    assert reviewed_sites(old, "read", members, "resolved") == reviewed_sites(moved, "read", members, "resolved")
    assert reviewed_sites(old, "read", members, "resolved") != reviewed_sites(changed, "read", members, "resolved")


@pytest.mark.parametrize("stale", ["source", "line"])
def test_stale_source_review_retains_discovery_and_still_refuses(tmp_path, stale):
    import hashlib
    package = tmp_path / "src/prismabuild"
    package.mkdir(parents=True)
    source = package / "owner.py"
    source.write_text("def normalize(schema):\n    return {'schema': schema, 'body': True}\n")
    review = dict(path="src/prismabuild/owner.py", owner="normalize", line=2,
                  schema="schema", fields=["body", "schema"], reason="Private source control",
                  source_sha256=hashlib.sha256(source.read_bytes()).hexdigest())
    if stale == "source":
        review["source_sha256"] = "0" * 64
    else:
        review["line"] = 99
    metadata = tmp_path / "tests/compat/dynamic_schema_dispositions.jsonl"
    metadata.parent.mkdir(parents=True)
    metadata.write_text(json.dumps(review) + "\n")
    original = metadata.read_bytes()
    output = tmp_path / "audit"
    result = subprocess.run([sys.executable, str(ROOT / "tests/compat/support/wire_inventory.py"),
                             "--root", str(tmp_path), "--out", str(output), "--require-complete"],
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 2, result.stderr
    report = json.loads((output / "source-audit.json").read_bytes())
    assert len(report["source_review_problems"]) == 1
    assert len(report["unreviewed_dynamic_schema_builders"]) == 1
    assert (output / "inventory.jsonl").is_file()
    assert (output / "field_sets.json").is_file()
    assert metadata.read_bytes() == original

def _field_policy_tree(root):
    sources = {
        'src/prismabuild/plain.py': "ALLOWED_FIELDS = {'schema', 'value'}\ndef check(row):\n    required = {'schema'}\n    if set(row) != required: raise ValueError('fields')\n",
        'tools/embedded.py': "SCRIPT = \"def check(row):\\n    fields = {'embedded'}\\n    if set(row) != fields: raise ValueError('fields')\\n# schema\\n\"\n",
        'tools/extensionless': "def check(row):\n    optional = {'extra'}\n    return row\n",
        'tools/status.py': "def check(row):\n    return row.get('schema') == 'example.status.v1'\n",
        'fleet/check.sh': "python3 <<'PYTHON'\ndef check(row):\n    allowed = {'shell'}\n    return row\nPYTHON\n",
        'tests/ignored.py': "TEST_FIELDS = {'control'}\n",
    }
    for name, source in sources.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source)
    return [
        {'path': 'fleet/check.sh', 'owner': '<shell-python@2>.check', 'fields': ['shell'], 'binding': 'allowed'},
        {'path': 'src/prismabuild/plain.py', 'owner': '<module>', 'fields': ['schema', 'value'], 'binding': 'ALLOWED_FIELDS'},
        {'path': 'src/prismabuild/plain.py', 'owner': 'check', 'fields': ['schema'], 'binding': 'required'},
        {'path': 'src/prismabuild/plain.py', 'owner': 'check', 'fields': ['schema']},
        {'path': 'tools/embedded.py', 'owner': '<module>:embedded.check', 'fields': ['embedded'], 'binding': 'fields'},
        {'path': 'tools/embedded.py', 'owner': '<module>:embedded.check', 'fields': ['embedded']},
        {'path': 'tools/extensionless', 'owner': 'check', 'fields': ['extra'], 'binding': 'optional'},
    ]


def test_field_policy_findings_are_characterized(tmp_path):
    expected = _field_policy_tree(tmp_path)
    _, policies = audit(tmp_path)
    assert policies == expected
    assert field_policies(tmp_path) == expected


def test_pinned_field_guard_does_not_scan_corpus_or_retained_records(tmp_path, monkeypatch):
    import wire_inventory

    expected = _field_policy_tree(tmp_path)
    pin = tmp_path / 'tests/compat/field_sets.json'
    pin.parent.mkdir(parents=True)
    pin.write_text(json.dumps(expected))
    original = wire_inventory.syntax_tree

    def policy_source(path, snippet):
        assert b'TEST_FIELDS' not in snippet, "field guard must not parse test-only source"
        return original(path, snippet)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("field guard must not audit retained record generations")

    monkeypatch.setattr(wire_inventory, 'syntax_tree', policy_source)
    monkeypatch.setattr(wire_inventory, 'retained_inventory', forbidden)
    monkeypatch.setattr(wire_inventory, 'source_expression', forbidden)
    monkeypatch.setitem(globals(), 'ROOT', tmp_path)
    test_closed_field_sets_remain_pinned()
