"""Pin actual closed boundaries and find new writers outside the catalogue."""
from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests/compat/support"))
from wire_inventory import audit


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
    tools = tmp_path / "tools"
    tools.mkdir()
    (tools / "embedded.py").write_text('SCRIPT = \'schema = {"schema": "example.embedded.v3"}\\n\'\n')
    (tools / "config.json").write_text('{"schema": "example.configuration.v2"}')
    rows, policies = audit(tmp_path)
    found = {row["wire_id"]: row for row in rows}
    assert set(found) == {"example.actual_record.v7", "example.embedded.v3", "example.configuration.v2"}
    writer = found["example.actual_record.v7"]["schema_builders"]
    assert writer[0]["owner"] == "write" and writer[0]["fields"] == ["payload", "schema"]
    assert found["example.embedded.v3"]["occurrences"][0]["embedded"]
    assert found["example.actual_record.v7"]["reader_checks"][0]["owner"] == "read"
    assert any(row["owner"] == "read" and row.get("fields") == ["payload", "schema"] for row in policies)
