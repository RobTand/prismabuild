"""Runtime declarations and offer liveness are distinct evidence sources."""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbmcp
import pbmcp_fixture as fx
from prismabuild import fleet_roster


def test_runtime_reports_declared_absence_independently_of_offer_loss(tmp_path):
    fleet = fx.build(tmp_path)
    path = fleet.repo_link.resolve() / "tools" / "fleet" / "fleet_boxes.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"boxes": {
        "partner": {"_alias": "partner-alias", "status": "offline", "status_by": "operator",
                    "status_reason": "maintenance", "status_unix": 1}, "present": {}}}))
    session = pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                            repo_link=fleet.repo_link)
    body = session.call("pb_runtime")
    assert body["loops"] == []
    hosts = body["host_presence"]["hosts"]
    assert hosts["partner"]["state"] == "absent"
    assert hosts["partner-alias"]["canonical_host"] == "partner"
    assert hosts["present"]["state"] == "present"


@pytest.mark.parametrize("raw", [None, "{broken", '{"boxes":{"partner":{"status":"offline"}}}'])
def test_missing_or_invalid_runtime_declaration_is_unknown(tmp_path, raw):
    if raw is not None:
        path = tmp_path / "tools" / "fleet" / "fleet_boxes.json"
        path.parent.mkdir(parents=True)
        path.write_text(raw)
    observed = fleet_roster.runtime_presence(tmp_path)
    assert observed["state"] == "unknown"
    assert observed["hosts"] is None
