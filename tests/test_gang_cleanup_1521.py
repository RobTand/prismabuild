"""Exact-publication gang cleanup with a controlled clock and real queue."""
from __future__ import annotations

from contextlib import contextmanager

import pytest

from test_gang_reservation_1517 import gang_fleet  # noqa: F401
from test_gang_teardown_1517 import _both_claimed
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401
from prismabuild import _gang, pool


def _row(queue, state, key):
    return pool.record_store.value_or_raise(queue.records.item(state, key))


def _republish(queue, row):
    return queue.publish(
        action_key=row['action_key'], cas_root=row['cas_root'],
        checkout_root=row['checkout_root'], worker_script=row['worker_script'],
        resources=row['resources'], needs_gpu=row['needs_gpu'], tags=row['tags'],
        priority=row['priority'], gang=row['gang'], max_attempts=1,
    )


def test_unregistered_members_expire_but_fresh_and_ordinary_rows_survive(gang_fleet):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys = members('unregistered', file_group=False)
    ordinary = publish('ordinary', priority=0, gpu=0, mem_gb=1)
    latest = max(_row(queue, pool.READY, key)['published_unix'] for key in keys)
    clock.now = latest + 599
    queue.sweep_gangs()
    assert all(_row(queue, pool.READY, key) is not None for key in keys)
    clock.now = latest + 601
    queue.sweep_gangs()
    assert all(_row(queue, pool.WITHDRAWN, key) is not None for key in keys)
    assert all(_row(queue, pool.READY, key) is None for key in keys)
    assert _row(queue, pool.READY, ordinary) is not None
    assert _gang.read_group(queue, group) is None


def test_registered_slow_members_are_not_orphans(gang_fleet):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys = members('registered')
    clock.now += 6000
    queue.sweep_gangs()
    assert all(_row(queue, pool.READY, key) is not None for key in keys)
    assert _gang.read_group(queue, group) is not None


def test_registration_is_rechecked_under_the_member_transition_lock(gang_fleet, monkeypatch):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys = members('late-registration', file_group=False)
    rows = [_row(queue, pool.READY, key) for key in keys]
    clock.now += 601
    original = queue.action_transition_lock
    registered = []

    @contextmanager
    def register_before_lock(key, **kwargs):
        if key in keys and not registered:
            registered.append(group)
            _gang.publish_group(queue, group, rows)
        with original(key, **kwargs) as acquired:
            yield acquired

    monkeypatch.setattr(queue, 'action_transition_lock', register_before_lock)
    queue.sweep_gangs()
    assert registered == [group]
    assert all(_row(queue, pool.READY, key) is not None for key in keys)


def test_sweep_retries_running_member_withdrawal_until_it_is_covered(gang_fleet, monkeypatch):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, first, second = _both_claimed(queue, publish, finish, claim, members)
    original = queue.withdraw
    failures = []

    def interrupted(key, **kwargs):
        if key == first and len(failures) < 2:
            failures.append(key)
            raise OSError('injected interrupted withdrawal')
        return original(key, **kwargs)

    monkeypatch.setattr(queue, 'withdraw', interrupted)
    monkeypatch.setattr(pool.socket, 'gethostname', lambda: 'sparky')
    queue.finish(second, status='failed', detail={'returncode': 1})
    assert _gang.teardown(queue, group) is not None
    assert queue.withdrawal_covers(_row(queue, pool.CLAIMED, first)) is None
    queue.sweep_gangs()
    assert len(failures) == 2
    assert queue.withdrawal_covers(_row(queue, pool.CLAIMED, first)) is None
    queue.sweep_gangs()
    assert queue.withdrawal_covers(_row(queue, pool.CLAIMED, first)) is not None
    assert queue.ledger('sparklina').held_keys() == [first]
    finish(first, 'sparklina')
    assert queue.sweep_gangs() == [group]


@pytest.mark.parametrize('cleanup', ['sweep', 'immediate'])
def test_old_teardown_does_not_cancel_a_successor_publication(gang_fleet, cleanup):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys = members('successor')
    original = _row(queue, pool.READY, keys[0])
    queue.withdraw(keys[0], reason='end original publication')
    clock.now += 1
    _republish(queue, original)
    successor = _row(queue, pool.READY, keys[0])
    assert successor['published_unix'] != original['published_unix']
    if cleanup == 'sweep':
        queue.sweep_gangs()
    else:
        queue._gang_teardown_withdraw(_row(queue, pool.WITHDRAWN, keys[1]),
                                     reason='retry original teardown', by=keys[1])
    assert _row(queue, pool.READY, keys[0]) == successor
    assert queue.withdrawal_covers(successor) is None


def test_pruning_requires_the_groups_exact_terminal_publications(gang_fleet):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys = members('exact-ending')
    original = _row(queue, pool.READY, keys[0])
    queue.withdraw(keys[0], reason='original ended')
    clock.now += 1
    _republish(queue, original)
    queue.withdraw(keys[0], reason='successor ended')
    assert queue.sweep_gangs() == []
    assert _gang.read_group(queue, group) is not None
