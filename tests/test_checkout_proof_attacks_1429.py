"""Caller outcome dictionaries cannot replace descriptor-backed unlink journals.

A genuine journal settles the exact registration even if the caller's outcome
proof is missing or edited. Interrupted removal still retains the original
inode until recovery proves its unlink.
"""
import os
from pathlib import Path

import pytest

from prismabuild import materialize, pool, record_store
from lifetime_fixtures_1429 import claim, fenced_snapshot_queue
from test_lifetime_contract_1429 import _execute, _finish, broker, Skew


def _strip_durable_proof(queue, key):
    for path in (queue.records.lease_path(key), queue.records.item_path(pool.CLAIMED, key)):
        record = record_store.read_document(path)
        record.pop("checkout_cleanup", None)
        record["checkout_registrations"] = [
            {k: v for k, v in entry.items() if k not in ("cleanup", "completion_claim")}
            for entry in record["checkout_registrations"]]
        queue.records.replace_record_at(path, record)


@pytest.mark.parametrize("field", ["genuine", "action_key", "published_unix", "claimed_by",
                                   "claimed_unix", "claimed_host", "nonce", "scope_id",
                                   "execution_id", "inode"])
def test_caller_outcome_cannot_replace_authoritative_unlink(tmp_path, broker, monkeypatch, field):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    key = action["action_key"]
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    item = claim(queue)
    outcome = _execute(queue, item)
    proof = dict(outcome["checkout_cleanup"])
    assert proof["complete"] is True and proof["unlinked"] is True
    _strip_durable_proof(queue, key)
    if field == "execution_id":
        proof["registration"] = {**proof["registration"], "execution_id": "0" * 32}
    elif field == "inode":
        proof["inode"] = proof["inode"] + 1
    elif field != "genuine":
        proof["claim"] = {**proof["claim"], field: "foreign"}
    outcome["checkout_cleanup"] = proof
    destination = _finish(queue, item, outcome)
    assert destination == queue.records.item_path(pool.DONE, key)
    assert queue.ledger().held_keys() == []
    # None of the caller's edited evidence was needed. The authority retained
    # the original descriptor across unlink and journalled that exact inode.
    receipt = record_store.read_document(queue.root / "checkout-unlinks" / (
        item["checkout_registrations"][0]["execution_id"] + ".json"))
    assert receipt["registration"]["inode"] == outcome["checkout_cleanup"]["registration"]["inode"]


def test_restart_mid_removal_returns_tokens_only_after_exact_unlink(tmp_path, monkeypatch):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    key = action["action_key"]
    root = tmp_path / "materialized"
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", root)
    item = claim(queue)
    child = os.fork()
    if child == 0:
        def prepared(*args, **kwargs):
            return {"status": "executed", "returncode": 0, "stdout": "", "stderr": ""}
        queue._execute_in_checkout = prepared
        original_empty = materialize.owned_tree._empty_tree

        def interrupt(fd, *args, **kwargs):
            path = Path(os.readlink(f"/proc/self/fd/{fd}"))
            if path.parent == root:
                (path / "checkout" / "task.py").unlink()
                os._exit(91)
            return original_empty(fd, *args, **kwargs)
        materialize.owned_tree._empty_tree = interrupt
        try:
            queue.execute(item, containment=False)
        except BaseException:
            os._exit(93)
        os._exit(94)
    _, status = os.waitpid(child, 0)
    assert os.waitstatus_to_exitcode(status) == 91
    registered = [p for p in root.iterdir() if materialize.CHECKOUT_TREE.fullmatch(p.name)]
    assert len(registered) == 1
    fresh = pool.PoolQueue(queue.root)
    Skew(monkeypatch).to(item["lifetime_deadline_unix"] + pool.LEASE_TIMEOUT_S + 1)
    fresh.reap_stale()
    held = fresh.ledger().held_keys()
    trees = [p for p in root.iterdir() if materialize.CHECKOUT_TREE.fullmatch(p.name)]
    records = [record_store.read_document(fresh.records.item_path(state, key))
               for state in (pool.CLAIMED, pool.DONE, pool.FAILED, pool.READY)]
    entries = [e for r in records if r for e in r.get("checkout_registrations") or []]
    print("restart", "held", held, "trees", [p.name for p in trees],
          "unlinked", [bool((e.get("cleanup") or {}).get("unlinked")) for e in entries])
    if held:
        assert trees
    else:
        assert not trees
        assert entries and all((e.get("cleanup") or {}).get("unlinked") is True
                               and e.get("path") == str(registered[0]) for e in entries)
