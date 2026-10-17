"""Qualify conclusion recovery against disposable real broker scopes under PB."""
from contextlib import ExitStack
import json
import time

import pytest

from prismabuild import core, pool, resource_scope
from qualify_claim_recovery import start_scope
from test_lifecycle_recovery_crashes import (
    KEY, _assert_endpoint, _history, _interrupt_after, _publication, _recover,
)


pytestmark = pytest.mark.skipif(
    not resource_scope.BROKER_SOCKET.exists(), reason="real PB resource broker socket unavailable")


def _owned_scope(queue, item, stack):
    # Published workers carry a container owner even for a native command.
    # Its real daemon census is what commits the broker's settlement proof.
    item["container_owner"] = core.canonical_sha256({"queue": str(queue.root), "key": KEY})
    queue.records.replace_record_at(queue.records.item_path(pool.record_store.CLAIMED, KEY), item)
    return start_scope(queue, item, stack)


def _closed(record, scope, process, queue, request, capsys):
    cleanup = record["resource_scope_cleanup"]
    assert cleanup["complete"] is True
    assert cleanup["nonce"] == scope.nonce
    assert cleanup["export"]["scope_id"] == scope.unit
    assert all(cleanup["export"][field] is True
               for field in ("empty", "ok", "settled", "released", "stopped"))
    assert cleanup["export"]["tickets_pending"] is False
    process.communicate(timeout=10)
    assert process.returncode != 0
    assert not scope.cgroup_path.exists()
    with capsys.disabled():
        print("lifecycle-scope-proof: " + json.dumps({
            "nodeid": request.node.nodeid, "action_key": record["action_key"],
            "published_unix": record["published_unix"], "nonce": cleanup["nonce"],
            "scope_id": scope.unit, "export": cleanup["export"],
            "cleanup_complete": cleanup["complete"], "cgroup_absent": not scope.cgroup_path.exists(),
            "process_returncode": process.returncode, "reservations": queue.ledger().held(),
        }), flush=True)


@pytest.mark.parametrize("cancelled,phase", [
    (False, "cleanup"), (False, "attempt"), (False, "lease"), (False, "publication"),
    (True, "cleanup"), (True, "attempt"), (True, "decision_proof"), (True, "reservation"),
])
def test_durable_crash_retry_retains_real_stopped_scope_proof(tmp_path, cancelled, phase, request, capsys):
    queue, item, decision = _history(tmp_path, cancelled)
    with ExitStack() as stack:
        scope, process, started = _owned_scope(queue, item, stack)
        assert len(resource_scope.scope_pids(scope.cgroup_path)) >= 2
        assert process.poll() is None
        _interrupt_after(queue, item, tmp_path, phase, cancelled)
        restarted = _recover(tmp_path, item, cancelled)
        _assert_endpoint(restarted, item, cancelled, decision)
        state = pool.record_store.WITHDRAWN if cancelled else pool.record_store.DONE
        record = pool.record_store.read_document(restarted.records.item_path(state, KEY))
        _closed(record, scope, process, restarted, request, capsys)
        if cancelled:
            _closed(record["withdrawn_attempt"], scope, process, restarted, request, capsys)
        assert started["scope_id"] == scope.unit


@pytest.mark.parametrize("cancelled", [False, True])
def test_late_finish_stops_only_its_saved_scope_beside_live_successor(tmp_path, cancelled, request, capsys):
    queue, item, decision = _history(tmp_path, cancelled)
    with ExitStack() as stack:
        old_scope, old_process, _ = _owned_scope(queue, item, stack)
        _interrupt_after(queue, item, tmp_path, "reservation", cancelled)
        restarted = _recover(tmp_path, item, cancelled)
        _assert_endpoint(restarted, item, cancelled, decision)
        state = pool.record_store.WITHDRAWN if cancelled else pool.record_store.DONE
        old_record = pool.record_store.read_document(restarted.records.item_path(state, KEY))
        _closed(old_record, old_scope, old_process, restarted, request, capsys)
        _publication(restarted, tmp_path)
        successor = restarted.claim(owner="successor", capacity={"cpu": 1})
        assert successor is not None
        scope, process, started = _owned_scope(restarted, successor, stack)
        paths = (restarted.records.item_path(pool.record_store.CLAIMED, KEY),
                 restarted.records.lease_path(KEY))
        saved = [path.read_bytes() for path in paths]
        restarted.finish(KEY, status="withdrawn" if cancelled else "executed",
                         detail={"stdout": "late report"}, claim_snapshot=item)
        assert [path.read_bytes() for path in paths] == saved
        assert process.poll() is None
        assert {started["pid"], started["child_pid"]} <= set(resource_scope.scope_pids(scope.cgroup_path))
        assert restarted.ledger().held() == {"cpu": 1}
        with capsys.disabled():
            print("lifecycle-live-successor-proof: " + json.dumps({
                "nodeid": request.node.nodeid, "old_scope_id": old_scope.unit,
                "successor_scope_id": scope.unit, "successor_alive": process.poll() is None,
                "claim_and_lease_unchanged": [path.read_bytes() for path in paths] == saved,
                "members": resource_scope.scope_pids(scope.cgroup_path),
                "reservations": restarted.ledger().held(), "affinity": started["affinity"],
            }), flush=True)
        result = restarted.finish(KEY, status="executed", detail={"stdout": "successor"},
                                  claim_snapshot=successor)
        assert result == restarted.records.item_path(pool.record_store.DONE, KEY)
        record = pool.record_store.read_document(result)
        _closed(record, scope, process, restarted, request, capsys)
        assert restarted.ledger().held() == {}
        old_process.communicate(timeout=10)
        assert old_process.returncode != 0 and not old_scope.cgroup_path.exists()


@pytest.mark.parametrize("cancelled", [False, True])
def test_late_finish_stops_a_live_predecessor_beside_a_live_successor(tmp_path, cancelled, request, capsys):
    queue, item, decision = _history(tmp_path, cancelled)
    with ExitStack() as stack:
        old_scope, old_process, old_started = _owned_scope(queue, item, stack)
        # Reproduce the retained legacy/operator handoff covered by the original
        # late-scope tests: claim ownership moved, but the old exact scope lives.
        queue.records.item_path(pool.record_store.CLAIMED, KEY).unlink()
        queue.records.lease_path(KEY).unlink()
        queue.ledger().release(KEY)
        _publication(queue, tmp_path)
        successor = queue.claim(owner="successor", capacity={"cpu": 1})
        assert successor is not None
        scope, process, started = _owned_scope(queue, successor, stack)
        assert old_scope.nonce != scope.nonce
        assert old_process.poll() is None and process.poll() is None
        assert {old_started["pid"], old_started["child_pid"]} <= set(resource_scope.scope_pids(old_scope.cgroup_path))
        assert {started["pid"], started["child_pid"]} <= set(resource_scope.scope_pids(scope.cgroup_path))
        paths = (queue.records.item_path(pool.record_store.CLAIMED, KEY), queue.records.lease_path(KEY))
        saved = [path.read_bytes() for path in paths]
        with capsys.disabled():
            print("lifecycle-live-predecessor-proof: " + json.dumps({
                "nodeid": request.node.nodeid, "old_scope_id": old_scope.unit,
                "successor_scope_id": scope.unit, "both_alive": old_process.poll() is None and process.poll() is None,
                "predecessor_members": resource_scope.scope_pids(old_scope.cgroup_path),
                "successor_members": resource_scope.scope_pids(scope.cgroup_path),
                "affinity": started["affinity"],
            }), flush=True)
        deadline = time.monotonic() + 20
        while True:
            result = queue.finish(KEY, status="withdrawn" if cancelled else "executed",
                                  detail={"stdout": "late live payload"}, claim_snapshot=item)
            if not result.name.endswith(pool.record_store.LATE_FINISH_SUFFIX):
                break
            assert time.monotonic() < deadline, "late exact scope never completed cleanup"
            time.sleep(.05)
        assert [path.read_bytes() for path in paths] == saved
        assert process.poll() is None and queue.ledger().held() == {"cpu": 1}
        assert {started["pid"], started["child_pid"]} <= set(resource_scope.scope_pids(scope.cgroup_path))
        with capsys.disabled():
            print("lifecycle-live-successor-proof: " + json.dumps({
                "nodeid": request.node.nodeid, "old_scope_id": old_scope.unit,
                "successor_scope_id": scope.unit, "successor_alive": process.poll() is None,
                "claim_and_lease_unchanged": [path.read_bytes() for path in paths] == saved,
                "members": resource_scope.scope_pids(scope.cgroup_path),
                "reservations": queue.ledger().held(), "affinity": started["affinity"],
            }), flush=True)
        old_record = next(record for path, record in queue.records.superseded_records(KEY)
                          if path.name.endswith(".late-finish.json")
                          and record["resource_scope"]["nonce"] == old_scope.nonce)
        attempt = queue.records.attempt_document(queue.records.attempt_path(item, 1))
        assert attempt["detail"]["resource_scope_cleanup"] == old_record["resource_scope_cleanup"]
        _closed(old_record, old_scope, old_process, queue, request, capsys)
        if decision:
            assert decision[0].read_bytes() == decision[1]
        result = queue.finish(KEY, status="executed", detail={"stdout": "successor"}, claim_snapshot=successor)
        assert result == queue.records.item_path(pool.record_store.DONE, KEY)
        _closed(pool.record_store.read_document(result), scope, process, queue, request, capsys)
        assert queue.ledger().held() == {}
