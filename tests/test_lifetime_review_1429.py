"""Exact checkout custody on deferral, replacement and exceptional exits (#1429)."""
from pathlib import Path

import pytest

from prismabuild import _measurement_reservation as reservation
from prismabuild import lifetime_fence, materialize, pool, record_store, resource_scope
from lifetime_fixtures_1429 import accept_assumption, claim, fenced_snapshot_queue
from test_lifetime_contract_1429 import _execute, _finish, broker


@pytest.mark.parametrize("phase", ["materialize", "prelaunch"])
def test_exception_keeps_successful_checkout_removal_proof(tmp_path, monkeypatch, phase):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    key = action["action_key"]
    checkout_root = tmp_path / "materialized"
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", checkout_root)
    accept_assumption(queue)
    ready = record_store.read_document(queue.records.item_path(pool.READY, key))
    assert isinstance(reservation.candidate_release_bound(queue.queue, ready), float)
    removals = []
    original_cleanup = materialize._cleanup_execution_checkout

    def cleanup(*args, **kwargs):
        verdict = original_cleanup(*args, **kwargs)
        removals.append(verdict["complete"])
        return verdict

    def fail(*args, **kwargs):
        raise materialize.MaterializationError("injected prompt failure")

    monkeypatch.setattr(materialize, "_cleanup_execution_checkout", cleanup)
    if phase == "materialize":
        monkeypatch.setattr(materialize, "_run_materializer_git", fail)
    else:
        monkeypatch.setattr(queue.queue, "_execute_in_checkout", fail)
    with pytest.raises(materialize.MaterializationError, match="injected prompt failure"):
        queue.serve_once(tags=[lifetime_fence.LIFETIME_TAG])
    assert removals == [True]
    terminal = record_store.read_document(queue.records.item_path(pool.FAILED, key))
    assert "injected prompt failure" in terminal["detail"]["exception"]
    assert terminal["checkout_cleanup"]["complete"] is True
    assert terminal["checkout_cleanup"]["claim"] == lifetime_fence.attempt_identity(terminal)
    assert not any(path.is_dir() for path in checkout_root.iterdir())
    queue.reap_stale()
    assert queue.ledger().held_keys() == [], "proved removal was lost on the exception path"


def test_replacement_tree_cannot_certify_the_registered_checkout(tmp_path, broker, monkeypatch):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    key = action["action_key"]
    checkout_root = tmp_path / "materialized"
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", checkout_root)
    original = materialize._remove_tree_before
    moved = []

    def swap_then_remove(path: Path, deadline_unix: float, **kwargs):
        retained = path.with_name(path.name + ".retained")
        path.rename(retained)
        moved.append(retained)
        path.mkdir()
        (path / "foreign").write_text("replacement tree")
        original(path, deadline_unix, **kwargs)

    monkeypatch.setattr(materialize, "_remove_tree_before", swap_then_remove)
    item = claim(queue)
    outcome = _execute(queue, item)
    assert outcome["status"] == "executed"
    assert moved[0].is_dir()
    assert (moved[0] / "checkout" / "task.py").exists()
    destination = _finish(queue, item, outcome)
    terminal = record_store.read_document(destination)
    audited = reservation.attempt_release_audit(queue.queue, terminal)
    assert audited is None, "replacement was removed while the registered tree survived"
    assert destination == queue.records.item_path(pool.CLAIMED, key)
    assert queue.ledger().held_keys() == [key]
    assert (Path(outcome["checkout_cleanup"]["path"]) / "foreign").read_text() == "replacement tree"
    queue.reap_stale()
    assert queue.ledger().held_keys() == [key]


def test_scope_maintenance_refusal_cannot_release_failed_checkout_removal(tmp_path, monkeypatch):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    key = action["action_key"]
    checkout_root = tmp_path / "materialized"
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", checkout_root)
    attempted = []

    def fail_removal(parent, name, **kwargs):
        attempted.append(name)
        raise PermissionError("injected checkout removal failure")

    def refuse_scope(*args, **kwargs):
        raise resource_scope.ResourceUnavailable("injected maintenance refusal")

    monkeypatch.setattr(materialize.owned_tree, "remove_tree", fail_removal)
    monkeypatch.setattr(queue.queue, "_start_resource_scope", refuse_scope)
    assert queue.serve_once(tags=[lifetime_fence.LIFETIME_TAG], containment=True) is None
    assert attempted
    assert (checkout_root / attempted[0] / "checkout" / "task.py").exists()
    assert queue.ledger().held_keys() == [key], "maintenance refusal returned tokens despite failed removal"


@pytest.mark.parametrize("entry", ["finish", "withdraw", "reap"])
@pytest.mark.parametrize("fault", ["permission", "partial", "empty_after_remove"])
def test_removal_fault_retains_every_release_path(tmp_path, broker, monkeypatch, entry, fault):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    key = action["action_key"]
    checkout_root = tmp_path / "materialized"
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", checkout_root)
    original = materialize.owned_tree.remove_tree

    def fail(parent, name, **kwargs):
        if fault == "empty_after_remove":
            original(parent, name, **kwargs)
            raise PermissionError()
        if fault == "partial":
            tree = Path(__import__("os").readlink(f"/proc/self/fd/{parent}")) / name
            for file in (tree / "checkout").glob("task.py"):
                file.unlink()
        raise PermissionError("injected partial or refused removal")

    monkeypatch.setattr(materialize.owned_tree, "remove_tree", fail)
    item = claim(queue)
    outcome = _execute(queue, item)
    assert outcome["checkout_cleanup"]["complete"] is False
    _finish(queue, item, outcome)
    if entry == "withdraw":
        queue.withdraw(key, reason="independent cleanup review")
    elif entry == "reap":
        queue.reap_stale()
    assert queue.ledger().held_keys() == [key]
    assert queue.records.lease_path(key).exists()
    assert not queue.records.item_path(pool.DONE, key).exists()
    assert reservation.attempt_release_audit(
        queue.queue, record_store.read_document(queue.records.item_path(pool.CLAIMED, key))) is None


@pytest.mark.parametrize("recovery", ["reap", "drain", "withdraw"])
def test_deferred_checkout_debt_recovers_without_charging_an_execution(
        tmp_path, monkeypatch, recovery):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    key = action["action_key"]
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    original = materialize.owned_tree.remove_tree

    def fail_removal(*args, **kwargs):
        raise PermissionError("checkout retained during maintenance")

    def refuse_scope(*args, **kwargs):
        raise resource_scope.ResourceUnavailable("maintenance")

    monkeypatch.setattr(materialize.owned_tree, "remove_tree", fail_removal)
    monkeypatch.setattr(queue.queue, "_start_resource_scope", refuse_scope)
    assert queue.serve_once(tags=[lifetime_fence.LIFETIME_TAG], containment=True) is None
    pending = record_store.read_document(queue.records.item_path(pool.CLAIMED, key))
    assert pending["attempts"] == 0
    assert pending["finish_pending"]["status"] == "defer"
    assert pending["checkout_cleanup"]["claim"] == lifetime_fence.attempt_identity(pending)
    assert pending["checkout_cleanup"]["complete"] is False
    assert queue.records.lease_path(key).exists()
    if recovery == "withdraw":
        queue.withdraw(key, reason="cancel deferred work")
        assert queue.ledger().held_keys() == [key]
    monkeypatch.setattr(materialize.owned_tree, "remove_tree", original)
    if recovery == "drain":
        monkeypatch.setattr(queue.queue, "_sweep_due", lambda: True)
        assert queue.retry_own_pending_finishes() == [key]
    else:
        queue.reap_stale()
    assert queue.ledger().held_keys() == []
    assert not queue.records.item_path(pool.CLAIMED, key).exists()
    assert not queue.records.lease_path(key).exists()
    if recovery == "withdraw":
        assert not queue.records.item_path(pool.READY, key).exists()
        assert queue.records.item_path(pool.WITHDRAWN, key).exists()
    else:
        ready = record_store.read_document(queue.records.item_path(pool.READY, key))
        assert ready["attempts"] == 0
        assert queue.attempt_outcomes(ready) == []
        assert "checkout_cleanup" not in ready and "lifetime_evidence" not in ready


def test_direct_exception_persists_checkout_proof_for_a_later_finisher(tmp_path, monkeypatch):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    key = action["action_key"]
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")

    def fail(*args, **kwargs):
        raise materialize.MaterializationError("ordinary git failure")

    monkeypatch.setattr(materialize, "_run_materializer_git", fail)
    item = claim(queue)
    with pytest.raises(materialize.MaterializationError, match="ordinary git failure"):
        _execute(queue, item)
    durable = record_store.read_document(queue.records.item_path(pool.CLAIMED, key))
    assert durable["checkout_cleanup"]["complete"] is True
    assert durable["checkout_cleanup"] == item["checkout_cleanup"]
    assert durable["checkout_cleanup"]["claim"] == lifetime_fence.attempt_identity(durable)
    assert queue.ledger().held_keys() == [key]
    queue.finish(key, status="failed", detail={"exception": "ordinary git failure"},
                 claim_snapshot=durable)
    assert queue.ledger().held_keys() == []


def test_prelaunch_exception_after_scope_creation_releases_on_exact_proof(
        tmp_path, broker, monkeypatch):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    key = action["action_key"]
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")

    def fail(*args, **kwargs):
        raise RuntimeError("prelaunch environment failed")

    monkeypatch.setattr(queue.queue, "launch_environment", fail)
    with pytest.raises(RuntimeError, match="prelaunch environment failed"):
        queue.serve_once(tags=[lifetime_fence.LIFETIME_TAG], containment=True)
    terminal = record_store.read_document(queue.records.item_path(pool.FAILED, key))
    assert "prelaunch environment failed" in terminal["detail"]["exception"]
    assert terminal["checkout_cleanup"]["complete"] is True
    assert terminal["checkout_cleanup"]["claim"] == lifetime_fence.attempt_identity(terminal)
    assert terminal["checkout_cleanup"]["claim"]["nonce"]
    assert queue.ledger().held_keys() == []
    assert not broker.kernel.groups


@pytest.mark.parametrize("fault", ["missing", "foreign", "incomplete"])
def test_proven_durable_cleanup_survives_stale_caller_detail(tmp_path, broker, monkeypatch, fault):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    key = action["action_key"]
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    item = claim(queue)
    outcome = _execute(queue, item)
    proven = dict(outcome["checkout_cleanup"])
    if fault == "missing":
        outcome.pop("checkout_cleanup")
    elif fault == "foreign":
        outcome["checkout_cleanup"] = {**proven, "claim": {**proven["claim"], "nonce": "foreign"}}
    else:
        outcome["checkout_cleanup"] = {**proven, "complete": False, "ended_unix": None}
    destination = _finish(queue, item, outcome)
    assert destination == queue.records.item_path(pool.DONE, key)
    assert queue.ledger().held_keys() == []
    assert record_store.read_document(destination)["checkout_cleanup"] == proven
