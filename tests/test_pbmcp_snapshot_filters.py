"""Snapshot submissions can be found by their sealed Git identities."""

import json
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))
sys.path.insert(0, str(REPOSITORY / "tests"))

import pbmcp
import pbmcp_fixture as fx


@pytest.mark.parametrize("field", ["parent", "commit"])
def test_snapshot_filter_finds_only_matching_git_identity(tmp_path, field):
    fleet = fx.build(tmp_path)
    for key, value in [(fx.READY_KEY, "1" * 40), (fx.TWIN_KEY, "2" * 40)]:
        record = fleet.record("ready", fx.READY_KEY)
        record["action_key"] = key
        record["checkout_root"] = None
        record["checkout_snapshot"] = {"parent": value, "commit": value}
        fleet.queue.item_path("ready", key).write_text(json.dumps(record))
    session = pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                            repo_link=fleet.repo_link)
    body = session.call("pb_actions", {"snapshot_" + field: "1" * 40,
                                        "limit": 50})
    assert body["complete"] is True
    assert [row["action_key"] for row in body["actions"]] == [fx.READY_KEY]
    assert body["filter"]["snapshot_" + field] == "1" * 40
    none = session.call("pb_actions", {"snapshot_" + field: "3" * 40,
                                        "keys": [fx.READY_KEY]})
    assert none["actions"] == [] and none["scanned"] == 1
