"""The reader must derive the same checkout identity as the claim producer."""

import json
from pathlib import Path
import sys

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))
sys.path.insert(0, str(REPOSITORY / "tests"))

import pbmcp
import pbmcp_fixture as fx


def test_symlinked_checkout_finds_the_producers_claim(tmp_path):
    fleet = fx.build(tmp_path)
    link = tmp_path / "checkout-alias"
    link.symlink_to(fleet.checkout, target_is_directory=True)
    path = fleet.queue.item_path("done", fx.DONE_KEY)
    record = json.loads(path.read_text())
    record["checkout_root"] = str(link)
    path.write_text(json.dumps(record))
    session = pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                            repo_link=fleet.repo_link)
    body = session.call("pb_action", {"key_prefix": fx.DONE_KEY})
    producer = pbmcp.pb._local_result_claim_body(fx.action_manifest(fleet), link)
    assert body["complete"] is True
    assert body["local_result_claim"]["sha256"] == pbmcp.pb.canonical_sha256(producer)
    assert body["local_result_claim"]["present"] is True
    assert body["local_result_claim"]["derived_from"] == producer
