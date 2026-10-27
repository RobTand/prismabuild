"""A retained or malformed gang cannot stop cleanup of later groups."""
from __future__ import annotations

import json

import pytest

from prismabuild import _gang, pool
from test_gang_reservation_1517 import gang_fleet  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401


def _row(queue, state, key):
    return pool.record_store.value_or_raise(queue.records.item(state, key))


@pytest.mark.parametrize('publication', [None, 'not-a-number'])
def test_malformed_ending_does_not_abort_another_gangs_cleanup(gang_fleet, publication):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    damaged, damaged_keys = members('damaged-ending')
    healthy, healthy_keys = members('healthy-ending')
    queue.withdraw(damaged_keys[0], reason='end damaged gang')
    queue.withdraw(healthy_keys[0], reason='end healthy gang')
    path = queue.records.item_path(pool.WITHDRAWN, damaged_keys[0])
    row = json.loads(path.read_text())
    row['published_unix'] = publication
    _gang.immutable_files.write_json_atomic(path, row)

    assert healthy in queue.sweep_gangs()
    assert _gang.read_group(queue, damaged) is not None
    assert _gang.read_group(queue, healthy) is None


def test_retained_first_group_does_not_starve_a_later_teardown(gang_fleet, monkeypatch):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    (retained, retained_keys), (torn, torn_keys) = sorted([
        members('first-group'), members('second-group')])
    original = queue.withdraw
    failures = []

    def interrupted(key, **kwargs):
        if key == torn_keys[1] and not failures:
            failures.append(key)
            raise OSError('injected interrupted sibling withdrawal')
        return original(key, **kwargs)

    monkeypatch.setattr(queue, 'withdraw', interrupted)
    queue.withdraw(torn_keys[0], reason='tear down later group')
    assert failures == [torn_keys[1]]
    assert _row(queue, pool.READY, torn_keys[1]) is not None

    queue.sweep_gangs(limit=1)
    queue.sweep_gangs(limit=1)

    assert _row(queue, pool.READY, torn_keys[1]) is None
    assert _row(queue, pool.WITHDRAWN, torn_keys[1]) is not None
    assert all(_row(queue, pool.READY, key) is not None for key in retained_keys)
