"""Regression witnesses for independent declaration-record review findings.

Admitted CPU fixtures only; no live broker, directory cleanup or GPU work.
"""
from __future__ import annotations

import hashlib
import json
import threading

import pytest

from prismabuild import client, pool, resource_scope
from test_scratch_declaration_record import FIELD, context, live_record, process, record, runtime

from admitted_queue_fixture import AdmittedQueueFixture  # noqa: E402


def other_actor_takes_key(queue, key, on_acquired=None):
    result = []

    def contender():
        with queue._transition_locked(key, blocking=False) as acquired:
            result.append(acquired)
            if acquired and on_acquired is not None:
                on_acquired()

    thread = threading.Thread(target=contender)
    thread.start()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert len(result) == 1
    return result[0]


def test_scope_creation_keeps_its_original_transition_exclusion(runtime, monkeypatch):
    create, calls = runtime
    queue, item, variables = create()
    original = resource_scope.ResourceScope.create

    def checked(scope):
        assert not other_actor_takes_key(queue, item["action_key"]), (
            "scope startup lost transition exclusion")
        return original(scope)

    monkeypatch.setattr(resource_scope.ResourceScope, "create", checked)
    queue._start_resource_scope(item)


def test_preintent_boundary_cannot_overwrite_a_successor(runtime, monkeypatch):
    create, calls = runtime
    queue, item, variables = create()
    original = pool._write_json_atomic
    transitions = []

    def guarded(path, value):
        if path == queue.item_path(pool.CLAIMED, item["action_key"]) and "resource_scope_intent" in value:
            def successor():
                replacement = dict(live_record(queue, item))
                replacement["published_unix"] += 10
                original(path, replacement)
            transitions.append(other_actor_takes_key(queue, item["action_key"], successor))
        return original(path, value)

    monkeypatch.setattr(pool, "_write_json_atomic", guarded)
    queue._start_resource_scope(item)
    assert transitions and not any(transitions), "startup allowed a successor transition before intent write"
    assert live_record(queue, item)["published_unix"] == item["published_unix"]


def test_request_absent_custom_uncontained_launcher_remains_supported(tmp_path, monkeypatch):
    queue = AdmittedQueueFixture(
        pool.PoolQueue(tmp_path / "queue"), capacity={"cpu": 8, "mem_gb": 16},
        default_demand={"cpu": 1, "mem_gb": 1})
    key = "a" * 64
    queue.publish(action_key=key, cas_root=tmp_path / "absent-cas", checkout_root=tmp_path,
                  worker_script="/custom-worker.py")
    item = queue.claim()
    assert item is not None
    launched = []
    process(monkeypatch, lambda *_: launched.append(True))
    assert queue.execute(item, containment=False)["status"] == "executed"
    assert launched == [True] and FIELD not in live_record(queue, item)


@pytest.mark.parametrize("field", ["resource_scope", "resource_scope_intent"])
def test_scope_contradiction_in_lease_is_retained_not_overwritten(runtime, monkeypatch, field):
    create, calls = runtime
    queue, item, variables = create()
    process(monkeypatch)
    queue.execute(item, containment=True)
    lease = json.loads(queue.lease_path(item["action_key"]).read_text())
    nonce = "2" * 32
    lease[field]["nonce"] = nonce
    if field == "resource_scope":
        lease[field]["scope_id"] = "prismabuild-job" + hashlib.sha256(
            (item["action_key"] + nonce).encode()).hexdigest()[:32] + ".slice"
    pool._write_json_atomic(queue.lease_path(item["action_key"]), lease)
    claim_before = queue.item_path(pool.CLAIMED, item["action_key"]).read_bytes()
    lease_before = queue.lease_path(item["action_key"]).read_bytes()
    held = queue.ledger().held()
    with pytest.raises((client.LocalScratchError, pool.PoolContractError)):
        record(queue, item)
    assert queue.item_path(pool.CLAIMED, item["action_key"]).read_bytes() == claim_before
    assert queue.lease_path(item["action_key"]).read_bytes() == lease_before
    assert queue.ledger().held() == held


@pytest.mark.parametrize("needs_repair", [False, True])
def test_idempotent_recording_preserves_worker_heartbeat_authority(runtime, monkeypatch, needs_repair):
    create, calls = runtime
    queue, item, variables = create()
    process(monkeypatch)
    queue.execute(item, containment=True)
    lease = json.loads(queue.lease_path(item["action_key"]).read_text())
    lease.update(pid=1234567, child_pid=2345678, heartbeat_unix=lease["heartbeat_unix"] - 1,
                 execution_observation={"launcher_alive": True, "stdout_bytes": 123},
                 progress_observation={"phase": "payload", "quiet_s": 0.25})
    if needs_repair:
        lease.pop(FIELD)
    pool._write_json_atomic(queue.lease_path(item["action_key"]), lease)
    before = queue.lease_path(item["action_key"]).read_bytes()
    evidence = record(queue, item)
    after = json.loads(queue.lease_path(item["action_key"]).read_text())
    assert after[FIELD] == evidence
    assert {key: value for key, value in after.items() if key != FIELD} == {
        key: value for key, value in lease.items() if key != FIELD}
    if not needs_repair:
        assert queue.lease_path(item["action_key"]).read_bytes() == before


def test_cancelled_before_recording_reap_keeps_later_declaration_evidence(runtime):
    create, calls = runtime
    queue, item, variables = create()
    queue._start_resource_scope(item)
    queue.withdraw(item["action_key"], signal_child=False)
    decision = pool._read_json(queue.item_path(pool.WITHDRAWN, item["action_key"]))
    assert decision is not None and FIELD not in decision
    evidence = record(queue, item)
    # Cancellation concludes a claim; unlike a retry, it is not returned in
    # reap_stale's list of keys requeued. Check the actual removal/evidence.
    assert queue.reap_stale(timeout_s=-1) == []
    assert not queue.item_path(pool.CLAIMED, item["action_key"]).exists()
    archived = [json.loads(path.read_text()) for path in queue.superseded_dir().glob("*.json")]
    assert any(value.get(FIELD) == evidence for value in archived), (
        "cancelled reaping discarded later declaration evidence")
    assert not queue.item_path(pool.CLAIMED, item["action_key"]).exists()
    assert not queue.lease_path(item["action_key"]).exists()
    assert queue.ledger().held() == {}
