"""Pin actual closed boundaries and find new writers outside the catalogue."""
from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests/compat/support"))
from wire_inventory import audit
from rebind_source_reviews import reviewed_sites


def test_closed_field_sets_remain_pinned():
    _, policies = audit(ROOT)
    expected = json.loads((ROOT / "tests/compat/field_sets.json").read_bytes())
    assert policies == expected


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
