"""Group registration and orphan withdrawal share member transition ownership."""
from __future__ import annotations

import threading

import pytest

from prismabuild import _gang, pool
from test_gang_reservation_1517 import gang_fleet  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401


def _rows(queue, keys):
    return [pool.record_store.value_or_raise(queue.records.item(pool.READY, key))
            for key in keys]


def test_late_publisher_cannot_register_withdrawn_orphans(gang_fleet):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys = members("late-publisher", file_group=False)
    original = _rows(queue, keys)
    clock.now += 601
    queue.sweep_gangs()
    assert _rows(queue, keys) == [None, None]

    with pytest.raises(_gang.GangContractError, match="member.*publication"):
        _gang.publish_group(queue, group, original)

    assert _gang.read_group(queue, group) is None


def test_registration_cannot_cross_the_orphan_sweeps_last_check(gang_fleet, monkeypatch):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys = members("registration-race", file_group=False)
    original = _rows(queue, keys)
    clock.now += 601
    checked, release = threading.Event(), threading.Event()
    reads = []
    failures = []
    read_group = _gang.read_group

    def paused_read(current_queue, current_group):
        result = read_group(current_queue, current_group)
        if threading.current_thread() is sweeper and current_group == group:
            reads.append(result)
            if len(reads) == 2:
                checked.set()
                if not release.wait(10):
                    raise AssertionError("registration did not release the paused sweep")
        return result

    def sweep():
        try:
            queue.sweep_gangs()
        except BaseException as exc:
            failures.append(exc)

    monkeypatch.setattr(_gang, "read_group", paused_read)
    sweeper = threading.Thread(target=sweep)
    sweeper.start()
    try:
        assert checked.wait(10), "sweep did not reach its final locked registration check"
        with pytest.raises(_gang.GangContractError, match="registration.*busy"):
            _gang.publish_group(queue, group, original)
        assert read_group(queue, group) is None
    finally:
        release.set()
        sweeper.join(10)

    assert not sweeper.is_alive()
    assert failures == []
    assert _rows(queue, keys) == [None, None]
    assert read_group(queue, group) is None


def test_identical_registration_is_idempotent_after_members_end(gang_fleet):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys = members("existing-registration")
    original = _rows(queue, keys)
    registered = _gang.read_group(queue, group)
    queue.withdraw(keys[0], reason="member ended after registration")

    assert _gang.publish_group(queue, group, original) == registered
