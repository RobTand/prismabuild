"""A failed snapshot removal cannot certify cleanup or return its reservation."""
import os
from pathlib import Path

import pytest

from prismabuild import _measurement_reservation as reservation
from prismabuild import materialize, pool, record_store
from lifetime_fixtures_1429 import claim, fenced_snapshot_queue
from test_lifetime_contract_1429 import _execute, _finish, broker, Skew


@pytest.mark.parametrize("late", [False, True])
def test_snapshot_removal_failure_retains_the_successful_attempt_until_retry(
        tmp_path, broker, monkeypatch, late):
    queue, action, cas = fenced_snapshot_queue(tmp_path)
    key = action["action_key"]
    checkout_root = tmp_path / "materialized"
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", checkout_root)
    original = materialize.owned_tree.remove_tree
    refused = []

    def refuse_removal(parent, name, **kwargs):
        refused.append(Path(os.readlink(f"/proc/self/fd/{parent}")) / name)
        raise PermissionError("injected checkout removal failure")

    monkeypatch.setattr(materialize.owned_tree, "remove_tree", refuse_removal)
    item = claim(queue)
    outcome = _execute(queue, item)
    assert outcome["status"] == "executed"
    assert outcome["returncode"] == 0
    assert refused and refused[0].is_dir()
    assert list((checkout_root / "cleanup-failures").glob("*.json"))
    pending = _finish(queue, item, outcome)
    assert pending == queue.records.item_path(pool.CLAIMED, key)
    live = record_store.read_document(pending)
    assert live["finish_pending"]["detail"]["status"] == "executed"
    assert live["finish_pending"]["detail"]["returncode"] == 0
    assert reservation.attempt_release_audit(queue.queue, live) is None
    assert queue.ledger().held_keys() == [key]
    assert queue.records.lease_path(key).exists()
    assert not queue.records.item_path(pool.DONE, key).exists()
    monkeypatch.setattr(materialize.owned_tree, "remove_tree", original)
    if late:
        Skew(monkeypatch).to(item["lifetime_deadline_unix"] + 1)
    assert queue.reap_stale() == []
    done = queue.records.item_path(pool.DONE, key)
    terminal = record_store.read_document(done)
    assert terminal["status"] == "executed"
    assert queue.ledger().held_keys() == []
    assert not refused[0].exists()
    assert list((checkout_root / "cleanup-failures").glob("*.json"))
    attempt = queue.attempt_outcomes(terminal)[0]
    assert attempt["detail"]["returncode"] == 0
    for name in ("stdout", "stderr"):
        assert (queue.root / attempt["logs"][name]["path"]).read_text() == outcome[name]
    assert terminal["lifetime_evidence"]["phases"]["cleanup"]["enforced"] is (not late)
    assert reservation.attempt_release_audit(queue.queue, terminal) == (
        None if late else item["lifetime_deadline_unix"])


@pytest.mark.parametrize("field", [None, "claimed_by", "published_unix", "nonce", "ended_unix"])
def test_cleanup_proof_from_another_attempt_never_returns_tokens(
        tmp_path, broker, monkeypatch, field):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    item = claim(queue)
    outcome = _execute(queue, item)
    assert outcome["status"] == "executed"
    valid = outcome["checkout_cleanup"]
    journal = queue.root / "checkout-unlinks" / (valid["registration"]["execution_id"] + ".json")
    retained_journal = tmp_path / "original-unlink-journal.json"
    journal.rename(retained_journal)
    # Simulate loss of the durable proof too: a real stored completion remains
    # authoritative even when caller detail is missing or belongs elsewhere.
    path = queue.records.item_path(pool.CLAIMED, item["action_key"])
    for owner_path in (path, queue.records.lease_path(item["action_key"])):
        durable = record_store.read_document(owner_path)
        durable.pop("checkout_cleanup")
        for registration in durable["checkout_registrations"]:
            registration.pop("cleanup", None)
        queue.records.replace_record_at(owner_path, durable)
    if field is None:
        outcome.pop("checkout_cleanup")
    elif field == "ended_unix":
        outcome["checkout_cleanup"] = {**valid, "ended_unix": None}
    else:
        outcome["checkout_cleanup"] = {**valid, "claim": {**valid["claim"], field: "foreign"}}
    pending = _finish(queue, item, outcome)
    assert pending == queue.records.item_path(pool.CLAIMED, item["action_key"])
    assert queue.ledger().held_keys() == [item["action_key"]]
    assert reservation.attempt_release_audit(queue.queue, record_store.read_document(pending)) is None
    outcome["checkout_cleanup"] = valid
    # A caller cannot restore authority, even with the former exact dictionary.
    assert _finish(queue, item, outcome) == path
    assert queue.ledger().held_keys() == [item["action_key"]]
    retained_journal.rename(journal)
    done = _finish(queue, item, outcome)
    assert done == queue.records.item_path(pool.DONE, item["action_key"])
    assert queue.ledger().held_keys() == []


def test_late_snapshot_completion_keeps_its_cleanup_proof_and_successor_isolation(
        tmp_path, broker, monkeypatch):
    queue, action, _ = fenced_snapshot_queue(tmp_path, max_attempts=3)
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    first = claim(queue)
    outcome = _execute(queue, first)
    assert outcome["status"] == "executed"
    # A collector can fail after the payload published. Its completed attempt
    # remains immutable even if its old worker later reports success.
    ready = _finish(queue, first, {**outcome, "status": "failed", "returncode": 1})
    original = queue.attempt_outcomes(record_store.read_document(ready))[0]
    successor = claim(queue)
    key = first["action_key"]
    assert successor["claimed_by"] != first["claimed_by"]
    path = queue.records.item_path(pool.CLAIMED, key)
    live, lease = path.read_bytes(), queue.records.lease_path(key).read_bytes()
    held = queue.ledger().held()
    _finish(queue, first, outcome)
    assert path.read_bytes() == live
    assert queue.records.lease_path(key).read_bytes() == lease
    assert queue.ledger().held() == held
    assert not queue.records.late_finish_path(first).exists()
    assert queue.attempt_outcomes(record_store.read_document(path))[0] == original


def test_empty_removal_error_never_certifies_a_vanished_checkout(
        tmp_path, broker, monkeypatch):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    original = materialize.owned_tree.remove_tree

    def remove_then_fail(*args, **kwargs):
        original(*args, **kwargs)
        raise PermissionError()

    monkeypatch.setattr(materialize.owned_tree, "remove_tree", remove_then_fail)
    item = claim(queue)
    outcome = _execute(queue, item)
    assert outcome["status"] == "executed"
    assert outcome["checkout_cleanup"]["complete"] is False
    assert "PermissionError" in outcome["checkout_cleanup"]["error"]
    pending = _finish(queue, item, outcome)
    key = item["action_key"]
    assert pending == queue.records.item_path(pool.CLAIMED, key)
    assert queue.ledger().held_keys() == [key]
    assert queue.records.lease_path(key).exists()
    assert reservation.attempt_release_audit(queue.queue, record_store.read_document(pending)) is None
    assert queue.reap_stale() == []
    assert queue.ledger().held_keys() == [key]
