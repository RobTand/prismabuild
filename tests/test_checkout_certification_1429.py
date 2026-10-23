"""Checkout certification regressions adopted from the r2 review (#1429)."""
import os
from pathlib import Path

import pytest

from prismabuild import _measurement_reservation as reservation
from prismabuild import materialize, pool, record_store, resource_scope
from lifetime_fixtures_1429 import claim, fenced_snapshot_queue
from test_lifetime_contract_1429 import _execute, _finish, broker, Skew


@pytest.mark.parametrize("entry", ["finish", "withdraw", "reap"])
def test_root_renamed_during_removal_cannot_certify_registered_inode(
        tmp_path, broker, monkeypatch, entry):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    key = action["action_key"]
    root = tmp_path / "materialized"
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", root)
    original = materialize.owned_tree._empty_tree
    retained = []

    def empty_then_replace(fd, *args, **kwargs):
        result = original(fd, *args, **kwargs)
        path = Path(os.readlink(f"/proc/self/fd/{fd}"))
        if path.parent == root and materialize.CHECKOUT_TREE.fullmatch(path.name) and not retained:
            moved = path.with_name(path.name + ".retained")
            path.rename(moved)
            path.mkdir()
            (moved / "unremoved").write_text("registered checkout still exists")
            retained.append(moved)
        return result

    monkeypatch.setattr(materialize.owned_tree, "_empty_tree", empty_then_replace)
    item = claim(queue)
    outcome = _execute(queue, item)
    assert retained and (retained[0] / "unremoved").is_file()
    assert (retained[0].stat().st_dev, retained[0].stat().st_ino) == (
        outcome["checkout_cleanup"]["device"], outcome["checkout_cleanup"]["inode"])
    if entry == "withdraw":
        queue.withdraw(key, reason="round 2 inode custody")
    if entry == "reap":
        Skew(monkeypatch).to(item["lifetime_deadline_unix"] + pool.LEASE_TIMEOUT_S + 1)
        queue.reap_stale()
    else:
        destination = _finish(queue, item, outcome)
        terminal = record_store.read_document(destination)
        print("mid-removal", entry, "proof_complete", outcome["checkout_cleanup"]["complete"],
              "finite_audit", isinstance(reservation.attempt_release_audit(queue.queue, terminal), float))
    assert queue.ledger().held_keys() == [key], "registered inode survived, but its tokens were returned"


def test_same_attempt_old_checkout_proof_cannot_replace_new_cleanup_debt(
        tmp_path, broker, monkeypatch):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    key = action["action_key"]
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    item = claim(queue)
    original_outcome = _execute(queue, item)
    old_proof = original_outcome["checkout_cleanup"]
    assert old_proof["complete"] is True

    def refuse(*args, **kwargs):
        raise PermissionError("keep second registered checkout")

    monkeypatch.setattr(materialize.owned_tree, "remove_tree", refuse)
    # Reentry is refused by the existing scope guard, after a new checkout
    # has been materialized. Its cleanup debt must still remain authoritative.
    with pytest.raises(pool.PoolContractError, match="already owns a resource scope"):
        _execute(queue, item)
    path = queue.records.item_path(pool.CLAIMED, key)
    durable = record_store.read_document(path)
    new_proof = durable["checkout_cleanup"]
    assert new_proof["complete"] is False
    assert new_proof["claim"] == old_proof["claim"]
    assert new_proof["path"] != old_proof["path"]
    assert Path(new_proof["path"]).is_dir()
    destination = _finish(queue, item, original_outcome)
    terminal = record_store.read_document(destination)
    print("proof-replay", "new_checkout_survives", Path(new_proof["path"]).is_dir(),
          "finite_audit", isinstance(reservation.attempt_release_audit(queue.queue, terminal), float))
    assert queue.ledger().held_keys() == [key], "old same-claim proof erased a different checkout's cleanup debt"


def test_widowed_lease_cannot_release_unproved_snapshot_cleanup(tmp_path, broker, monkeypatch):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    key = action["action_key"]
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    def refuse(*args, **kwargs):
        raise PermissionError("registered checkout retained")
    monkeypatch.setattr(materialize.owned_tree, "remove_tree", refuse)
    item = claim(queue)
    outcome = _execute(queue, item)
    assert outcome["checkout_cleanup"]["complete"] is False
    assert Path(outcome["checkout_cleanup"]["path"]).is_dir()
    # The recovery API explicitly handles a missing claim beside its lease.
    # Preserve the original bytes as evidence instead of discarding them.
    path = queue.records.item_path(pool.CLAIMED, key)
    path.rename(tmp_path / "lost-claim-evidence.json")
    Skew(monkeypatch).to(item["lifetime_deadline_unix"] + pool.LEASE_TIMEOUT_S + 1)
    fresh = pool.PoolQueue(queue.root)
    fresh.sweep_widowed_leases(timeout_s=pool.LEASE_TIMEOUT_S)
    assert Path(outcome["checkout_cleanup"]["path"]).is_dir()
    assert fresh.ledger().held_keys() == [key], "widowed lease omitted the fenced checkout and returned its tokens"


@pytest.mark.parametrize("entry", ["finish", "withdraw", "reap", "deferral", "widow", "direct", "late"])
def test_every_checkout_release_path_requires_the_single_certifier(tmp_path, broker, monkeypatch, entry):
    queue, action, _ = fenced_snapshot_queue(tmp_path, max_attempts=3)
    key = action["action_key"]
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    item = claim(queue)
    if entry == "deferral":
        def refuse_scope(*args, **kwargs):
            raise resource_scope.ResourceUnavailable("scope creation maintenance")
        monkeypatch.setattr(queue.queue, "_start_resource_scope", refuse_scope)
        with pytest.raises(resource_scope.ResourceUnavailable):
            _execute(queue, item)
        outcome = None
    else:
        outcome = _execute(queue, item)
    if entry == "late":
        _finish(queue, item, {**outcome, "status": "failed", "returncode": 1})
        claim(queue)
    seen, released = [], []
    original_release = pool.ResourceLedger.release

    def deny_checkout(queue_owner, record, **kwargs):
        seen.append(record["action_key"])
        return {"complete": False, "used": True, "error": "certification denied by regression"}

    def observe_return(ledger, action_key):
        released.append(action_key)
        return original_release(ledger, action_key)

    monkeypatch.setattr(materialize, "certify_checkout_cleanup", deny_checkout)
    monkeypatch.setattr(pool.ResourceLedger, "release", observe_return)
    if entry in ("finish", "late"):
        _finish(queue, item, outcome)
    elif entry == "withdraw":
        queue.withdraw(key, reason="certification boundary")
        _finish(queue, item, outcome)
    elif entry == "deferral":
        queue.queue._defer_unstarted_claim(item)
    elif entry == "direct":
        with pytest.raises(pool.PoolContractError, match="not certified"):
            queue.queue._release_reservation(key, host=item["claimed_host"])
    else:
        if entry == "widow":
            queue.records.item_path(pool.CLAIMED, key).rename(tmp_path / "lost-claim.json")
        Skew(monkeypatch).to(item["lifetime_deadline_unix"] + pool.LEASE_TIMEOUT_S + 1)
        if entry == "widow":
            queue.sweep_widowed_leases(timeout_s=pool.LEASE_TIMEOUT_S)
        else:
            queue.reap_stale()
    assert seen and set(seen) == {key}
    assert released == [], "a checkout-bearing conclusion bypassed certification"
    assert queue.ledger().held_keys() == [key]
    assert queue.records.lease_path(key).exists()


def test_scope_token_returns_have_one_conclusion_boundary():
    import ast
    root = Path(__file__).resolve().parents[1]
    releases = set()
    for module in ("pool", "lifecycle"):
        tree = ast.parse((root / "src/prismabuild" / (module + ".py")).read_bytes())

        def walk(node, owners=()):
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                owners = (*owners, node.name)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                receiver = ast.unparse(node.func.value)
                if node.func.attr == "release" and receiver.startswith("self.ledger"):
                    releases.add((module, *owners))
                if (receiver == "tier_holdings" and node.func.attr == "egress_all"):
                    releases.add((module, *owners))
            for child in ast.iter_child_nodes(node):
                walk(child, owners)

        walk(tree)
    # Acquisition rollback precedes execution and cannot own a checkout.
    # Every concluding host/tier return has the guarded common boundary.
    assert releases == {("pool", "_ClaimReservation", "abandon"),
                        ("pool", "PoolQueue", "_release_reservation")}


def test_widowed_checkout_debt_releases_only_after_exact_removal(tmp_path, broker, monkeypatch):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    key = action["action_key"]
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    original = materialize.owned_tree.remove_tree

    def refuse(*args, **kwargs):
        raise PermissionError("retain registered inode")

    monkeypatch.setattr(materialize.owned_tree, "remove_tree", refuse)
    item = claim(queue)
    outcome = _execute(queue, item)
    registration = item["checkout_registrations"][0]
    lease = record_store.read_document(queue.records.lease_path(key))
    assert lease["checkout_registrations"] == item["checkout_registrations"]
    assert registration["execution_id"] == outcome["checkout_cleanup"]["registration"]["execution_id"]
    queue.records.item_path(pool.CLAIMED, key).rename(tmp_path / "lost-claim.json")
    Skew(monkeypatch).to(item["lifetime_deadline_unix"] + pool.LEASE_TIMEOUT_S + 1)
    queue.sweep_widowed_leases(timeout_s=pool.LEASE_TIMEOUT_S)
    assert queue.ledger().held_keys() == [key]
    monkeypatch.setattr(materialize.owned_tree, "remove_tree", original)
    queue.sweep_widowed_leases(timeout_s=pool.LEASE_TIMEOUT_S)
    assert not Path(registration["path"]).exists()
    assert queue.ledger().held_keys() == []
    assert not queue.records.lease_path(key).exists()


def test_every_execute_retains_its_own_debt_until_recovery(tmp_path, broker, monkeypatch):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    key = action["action_key"]
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    item = claim(queue)
    first = _execute(queue, item)
    original = materialize.owned_tree.remove_tree

    def refuse(*args, **kwargs):
        raise PermissionError("second execute cleanup debt")

    monkeypatch.setattr(materialize.owned_tree, "remove_tree", refuse)
    with pytest.raises(pool.PoolContractError, match="already owns a resource scope"):
        _execute(queue, item)
    lease = record_store.read_document(queue.records.lease_path(key))
    entries = lease["checkout_registrations"]
    assert len(entries) == 2 and len({e["execution_id"] for e in entries}) == 2
    assert entries[0]["cleanup"]["complete"] and not entries[1]["cleanup"]["complete"]
    assert _finish(queue, item, first) == queue.records.item_path(pool.CLAIMED, key)
    monkeypatch.setattr(materialize.owned_tree, "remove_tree", original)
    queue.reap_stale()
    assert queue.ledger().held_keys() == []
    assert not Path(entries[1]["path"]).exists()
    terminal = record_store.read_document(queue.records.item_path(pool.DONE, key))
    assert all(e["cleanup"]["unlinked"] for e in terminal["checkout_registrations"])


@pytest.mark.parametrize("entry", ["finish", "widow"])
def test_unfenced_snapshot_also_retains_exact_checkout_custody(tmp_path, broker, monkeypatch, entry):
    queue, action, _ = fenced_snapshot_queue(tmp_path, fence_s=None)
    key = action["action_key"]
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    original = materialize.owned_tree.remove_tree

    def refuse(*args, **kwargs):
        raise PermissionError("retain unfenced checkout")

    monkeypatch.setattr(materialize.owned_tree, "remove_tree", refuse)
    item = claim(queue)
    assert item.get("lifetime_fence_s") is None
    outcome = _execute(queue, item)
    checkout = Path(outcome["checkout_cleanup"]["path"])
    if entry == "widow":
        queue.records.item_path(pool.CLAIMED, key).rename(tmp_path / "lost-claim.json")
        Skew(monkeypatch).to(item["claimed_unix"] + pool.LEASE_TIMEOUT_S + 1)
        queue.sweep_widowed_leases(timeout_s=pool.LEASE_TIMEOUT_S)
    else:
        assert _finish(queue, item, outcome) == queue.records.item_path(pool.CLAIMED, key)
    assert checkout.is_dir()
    assert queue.ledger().held_keys() == [key]
    lease = record_store.read_document(queue.records.lease_path(key))
    durable = lease["checkout_registrations"]
    assert [{k: v for k, v in e.items() if k != "cleanup"} for e in durable] == [
        {k: v for k, v in e.items() if k != "cleanup"} for e in item["checkout_registrations"]]
    assert all(e["cleanup"]["complete"] is False for e in durable)
    monkeypatch.setattr(materialize.owned_tree, "remove_tree", original)
    if entry == "widow":
        queue.sweep_widowed_leases(timeout_s=pool.LEASE_TIMEOUT_S)
    else:
        queue.reap_stale()
    assert not checkout.exists()
    assert queue.ledger().held_keys() == []
    assert not queue.records.lease_path(key).exists()


def test_new_name_after_proven_unlink_does_not_erase_exact_cleanup(tmp_path, broker, monkeypatch):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    key = action["action_key"]
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    original = materialize.owned_tree.remove_tree
    replacements = []

    def replace_after_unlink(parent, name, **kwargs):
        original(parent, name, **kwargs)
        path = Path(os.readlink(f"/proc/self/fd/{parent}")) / name
        path.mkdir()
        replacements.append(path)

    monkeypatch.setattr(materialize.owned_tree, "remove_tree", replace_after_unlink)
    item = claim(queue)
    outcome = _execute(queue, item)
    assert replacements and replacements[0].is_dir()
    assert outcome["checkout_cleanup"]["unlinked"] is True
    assert _finish(queue, item, outcome) == queue.records.item_path(pool.DONE, key)
    assert queue.ledger().held_keys() == []


@pytest.mark.parametrize("fence_s", [300.0, None])
def test_proven_checkout_without_lease_finishes_from_its_exact_claim(
        tmp_path, broker, monkeypatch, fence_s):
    queue, action, _ = fenced_snapshot_queue(tmp_path, fence_s=fence_s)
    key = action["action_key"]
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    item = claim(queue)
    outcome = _execute(queue, item)
    assert outcome["checkout_cleanup"]["unlinked"] is True
    queue.records.lease_path(key).unlink()
    assert _finish(queue, item, outcome) == queue.records.item_path(pool.DONE, key)
    assert not queue.records.item_path(pool.CLAIMED, key).exists()
    assert queue.ledger().held_keys() == []
