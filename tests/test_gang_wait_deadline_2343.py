"""A submitter deadline can end only its own unstarted publications."""
from __future__ import annotations

from prismabuild import _gang, pool
from test_gang_reservation_1517 import gang_fleet  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401


def deadline_group(gang_fleet, name="deadline", timeout=60):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys = members(name)
    path = _gang.group_path(queue, group)
    record = _gang.read_group(queue, group)
    record["queue_wait_timeout_s"] = timeout
    pool.immutable_files.write_json_atomic(path, record)
    return group, keys


def test_wait_deadline_cancels_only_own_unstarted_gang(gang_fleet):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys = deadline_group(gang_fleet)
    other, other_keys = members("other-submitter")
    originals = [pool.record_store.value_or_raise(queue.records.item(pool.record_store.READY, key))
                 for key in keys]
    clock.now += 61
    queue.sweep_gangs()
    assert all(queue.withdrawal_covers(row) is not None for row in originals), (
        "opted-in unstarted gang was not cancelled")
    assert all(queue.records.item_path(pool.record_store.READY, key).exists() for key in other_keys)
    assert _gang.teardown(queue, other) is None


def test_first_claim_permanently_disarms_queue_wait(gang_fleet):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys = deadline_group(gang_fleet)
    assert claim("sparklina") is None
    assert claim("sparky") == keys[1]
    clock.now += 61
    queue.sweep_gangs()
    assert _gang.teardown(queue, group) is None
    assert not any(queue.records.withdrawal_decisions(key) for key in keys)
    assert queue.ledger("sparky").held_keys() == [keys[1]]
    assert claim("sparklina") == keys[0]


def test_replacement_publication_is_never_cancelled(gang_fleet):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys = deadline_group(gang_fleet)
    path = queue.records.item_path(pool.record_store.READY, keys[1])
    replacement = pool.record_store.read_document(path)
    replacement["published_unix"] += 0.5
    pool.immutable_files.write_json_atomic(path, replacement)
    before = path.read_bytes()
    clock.now += 61
    queue.sweep_gangs()
    assert path.read_bytes() == before
    assert not any(queue.records.withdrawal_decisions(key) for key in keys)
    assert _gang.teardown(queue, group) is None


def test_busy_member_transition_defers_entire_cancellation(gang_fleet):
    import threading
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys = deadline_group(gang_fleet)
    acquired, release = threading.Event(), threading.Event()
    def hold_member():
        with queue.action_transition_lock(keys[1]):
            acquired.set()
            release.wait(60)
    worker = threading.Thread(target=hold_member)
    worker.start()
    try:
        assert acquired.wait(60)
        clock.now += 61
        queue.sweep_gangs()
        assert _gang.teardown(queue, group) is None
        assert not any(queue.records.withdrawal_decisions(key) for key in keys)
    finally:
        release.set()
        worker.join(60)
    assert not worker.is_alive()
    queue.sweep_gangs()
    assert all(queue.records.withdrawal_decisions(key) for key in keys)


def test_group_registration_roundtrips_opt_in_and_rejects_invalid_deadlines(gang_fleet):
    import pytest
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys = members("registration", file_group=False)
    rows = [pool.record_store.value_or_raise(queue.records.item(pool.record_store.READY, key))
            for key in keys]
    for invalid in [True, 0, -1, float("inf"), "60"]:
        with pytest.raises(_gang.GangContractError, match="queue_wait_timeout_s"):
            _gang.publish_group(queue, group, rows, queue_wait_timeout_s=invalid)
    _gang.publish_group(queue, group, rows, queue_wait_timeout_s=60)
    clock.now += 61
    queue.sweep_gangs()
    assert all(queue.records.withdrawal_decisions(key) for key in keys)


def test_pbgang_manifest_accepts_only_valid_wait_timeout(tmp_path):
    import json
    import sys
    from pathlib import Path
    import pytest
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
    import pbgang
    path = tmp_path / "manifest.json"
    members = [{"tag": host, "argv": ["python3", "task.py"]}
               for host in ["sparklina", "sparky"]]
    path.write_text(json.dumps({"members": members, "queue_wait_timeout_s": 60}))
    loaded = pbgang._pbgang_load_manifest(path)
    assert loaded["queue_wait_timeout_s"] == 60
    path.write_text(json.dumps({"members": members, "queue_wait_timeout_s": -1}))
    with pytest.raises(SystemExit, match="queue_wait_timeout_s"):
        pbgang._pbgang_load_manifest(path)


def test_deadline_sweep_preserves_successor_published_after_cancellation(gang_fleet, monkeypatch):
    from prismabuild import gang_wait
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys = deadline_group(gang_fleet)
    path = queue.records.item_path(pool.record_store.READY, keys[0])
    successor = pool.record_store.read_document(path)
    successor["published_unix"] += 1
    cancel = gang_wait.cancel_expired
    inserted = False
    def replace_after_cancel(*args, **kwargs):
        nonlocal inserted
        result = cancel(*args, **kwargs)
        if result and not inserted:
            inserted = True
            pool.immutable_files.write_json_atomic(path, successor)
        return result
    monkeypatch.setattr(gang_wait, "cancel_expired", replace_after_cancel)
    clock.now += 61
    queue.sweep_gangs()
    assert path.exists(), "deadline cleanup cancelled a successor after releasing member locks"
    assert pool.record_store.read_document(path)["published_unix"] == successor["published_unix"]
    assert queue.withdrawal_covers(successor) is None
