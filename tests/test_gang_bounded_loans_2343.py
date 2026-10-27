"""Dual-signal gang loans in private queues, driven by a fake clock."""
from __future__ import annotations

import json

import pytest

from prismabuild import _gang, fleet_roster, pool
from test_gang_reservation_1517 import gang_fleet  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401
from test_gang_fence_backfill import waiting


@pytest.fixture
def absent_partner(gang_fleet, monkeypatch):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys, incumbent = waiting(gang_fleet)
    document = {"boxes": {
        "sparklina": {},
        "sparky": {"status": "offline", "status_reason": "test operator declaration",
                   "status_by": "fixture", "status_unix": clock.now}}}
    monkeypatch.setattr(fleet_roster.Roster, "at", classmethod(
        lambda cls, *args, **kwargs: fleet_roster.Roster.capture(json.dumps(document).encode())))
    return group, keys, incumbent, document


def observe(gang_fleet, seconds):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    for _ in range(seconds // 10):
        clock.now += 10
        assert claim("sparklina") is None


def test_sustained_dual_absence_admits_one_declared_bounded_loan(
        gang_fleet, absent_partner):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys, incumbent, document = absent_partner
    observe(gang_fleet, 620)
    key = publish("bounded-dead-partner-loan", priority=-10, mem_gb=24,
                  tags=["sparklina"], timeout_s=1800, retry_safe=False, max_attempts=1)
    assert claim("sparklina") == key, "sustained dual absence refused the bounded loan"
    claimed = pool.record_store.value_or_raise(queue.records.item(pool.record_store.CLAIMED, key))
    assert claimed["gang_backfill"][0]["group"] == group
    assert queue.ledger("sparklina").held_keys() == [key]
    assert _gang.teardown(queue, group) is None
