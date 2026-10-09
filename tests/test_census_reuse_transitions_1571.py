"""Reused census transitions stay conservative under concurrent change (#1571).

The claim pass reuses one locked census for later candidates and refreshes
only the small election sources between them. Three transitions must stay
safe. A replacement measurement election under a retired action key must
fence lower priority work: the refresh compares the whole selection, not
the key alone. A slow shared read must refuse the reuse inside its budget
and release host admission, never stall sibling loops. A completed gang
whose members all left READY/CLAIMED must fence nothing: the refresh
proves live membership exactly as the full census does.
"""
from __future__ import annotations

import os
import sys
import time

import pytest

from prismabuild import _gang, _measurement_reservation as reservation
from prismabuild import adaptive_cpu, adaptive_gpu, core as pb, pool

from test_census_tmpfs_state_1451 import (  # noqa: F401  (fixtures)
    tmpfs_mount, tmpfs_state)
from test_gang_reservation_1517 import gang_fleet  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401
from test_measurement_reservation_backfill_1419 import (
    _bounded_measurement_wait, _observe_real_sharing_permission)

CAPACITY = {"cpu": 20, "gpu": 1, "mem_gb": 120}
TIERS = {"preferred": list(range(20)), "fallback": []}


def _chosen(queue, key):
    record = pool._read_json(queue.passes_path(key))
    assert isinstance(record, dict)
    chosen = reservation.selection(record)
    assert chosen is not None
    return chosen


def _claim_row(queue, key, host="sparklina"):
    row = pool._read_json(queue.item_path(pool.READY, key))
    assert isinstance(row, dict)
    result = queue.claim(capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True,
                         has_gpu=True, tags=["gb10", host], ready=[row])
    return None if result is None else result["action_key"]


def test_a_replacement_election_between_candidates_still_fences_lower_priority(
        fleet, monkeypatch):
    """A new generation under a retired key fences the next candidate."""
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder, measurement, opportunity, snapshot = _bounded_measurement_wait(fleet)
    first = _chosen(queue, measurement)
    assert first["host"] == "sparklina"

    controller = adaptive_cpu.Controller(queue.ledger(), TIERS)
    pass_census = reservation.PassCensus(queue, queue.ledger(), controller)
    with reservation.locked_census(queue, queue.ledger(), controller) as census:
        pass_census.store(census)
    assert measurement in (pass_census.census or {}).get("elections", {})

    # Between the pass's candidates the measurement runs, finishes, is
    # published again, and elects a new generation: the old key stays
    # present in the stored selections, but its bytes change.
    queue.finish(holder, status="executed")
    tick(11)
    assert claim() == measurement
    queue.finish(measurement, status="executed")
    tick(11)
    assert queue.item_path(pool.DONE, measurement).exists()
    blocker = publish("replacement-blocker", priority=-10, timeout_s=4200,
                      cpu=2, gpu=1, mem_gb=8)
    tick(11)
    assert claim() == blocker
    tick(1)
    republished = publish("priority-zero-measurement", measurement=True,
                          priority=0, cpu=8, gpu=1, mem_gb=48)
    assert republished == measurement
    successor = pool._read_json(queue.item_path(pool.READY, republished))
    assert isinstance(successor, dict)
    assert queue.attempt_generation(successor) != first["generation"]
    tick(11)
    assert claim() is None
    replacement = _chosen(queue, republished)
    assert replacement["generation"] != first["generation"]
    assert replacement != first

    stacked = pass_census.acquire()
    assert stacked is not None
    with stacked:
        reused = pass_census.reused()
    assert reused["elections"][measurement] == replacement, reused["elections"][measurement]
    lower = publish("fenced-by-replacement", timeout_s=5)
    row = pool._read_json(queue.item_path(pool.READY, lower))
    assert isinstance(row, dict)
    blocked = reservation.blocking_selection(
        reused, row, host="sparklina", funded_by=None)
    assert blocked == replacement, blocked


def test_a_slow_refresh_refuses_reuse_and_releases_host_admission(
        fleet, tmpfs_state, monkeypatch):
    """A stalled shared read ends the reuse inside its budget."""
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", tmpfs_state / "box-state")
    holder = publish("slow-refresh-holder", priority=10, cpu=2, gpu=0, mem_gb=112)
    assert claim() == holder
    first = publish("slow-refresh-first", priority=-10, timeout_s=600,
                    cpu=2, gpu=0, mem_gb=16)
    second = publish("slow-refresh-second", priority=-10, timeout_s=600,
                     cpu=2, gpu=0, mem_gb=16)
    rows = [pool._read_json(queue.item_path(pool.READY, key))
            for key in (first, second)]
    real_read = reservation._read
    refreshes = []

    def slow_read(path, **kwargs):
        if str(path).endswith(".json") and "/passes/" in str(path):
            time.sleep(0.05)
        return real_read(path, **kwargs)

    real_refresh = reservation.CensusReader.refresh_elections

    def counted_refresh(self):
        refreshes.append(1)
        return real_refresh(self)

    monkeypatch.setattr(reservation, "_read", slow_read)
    monkeypatch.setattr(reservation.CensusReader, "refresh_elections", counted_refresh)
    monkeypatch.setattr(reservation, "REFRESH_BUDGET_S", 0.2)
    started = time.monotonic()
    result = queue.claim(capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True,
                         has_gpu=True, tags=["gb10", "sparklina"], ready=rows)
    elapsed = time.monotonic() - started
    assert result is None
    # The first candidate stores the census; the slow refresh refuses the
    # reuse inside its budget, the pass reads fresh, and both gated rows
    # stay READY -- never admitted on a partial read, never wedged.
    assert len(refreshes) >= 1, "one refresh ran and timed out"
    assert elapsed < 0.2 + 2 * reservation.READ_BUDGET_S + 15, (
        f"a stalled refresh must not hold the pass: {elapsed:.1f}s")
    controller = adaptive_cpu.Controller(queue.ledger("sparklina"), TIERS)
    with controller.locked():
        pass
    for key in (first, second):
        assert queue.item_path(pool.READY, key).exists(), key


def test_a_completed_gang_fences_nothing_on_reuse(gang_fleet, monkeypatch):
    """Elections without a live READY/CLAIMED member are not fences."""
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents = {}
    for host in ("sparklina", "sparky"):
        incumbents[host] = publish(f"incumbent-{host}", priority=-10, timeout_s=None,
                                   cpu=2, gpu=1, mem_gb=48)
        assert gclaim(host) == incumbents[host]
    group, (first, second) = members("completed", priority=10, mem_gb=100)
    assert gclaim("sparklina") is None
    assert gclaim("sparky") is None
    elections = _gang.elections(queue, group, 2)
    assert len(elections) == 2
    finish(incumbents["sparklina"], "sparklina")
    assert gclaim("sparklina") is None
    assert denial(first, "sparklina")["reason"] == "gang_waiting_for_peers"
    finish(incumbents["sparky"], "sparky")
    # Both members claim and finish: success files no teardown, so the
    # election files stay -- but no member row is READY or CLAIMED.
    assert gclaim("sparky") == second, denial(second, "sparky")
    assert gclaim("sparklina") == first, denial(first, "sparklina")
    finish(first, "sparklina")
    finish(second, "sparky")
    assert queue.item_path(pool.DONE, first).exists()
    assert queue.item_path(pool.DONE, second).exists()
    assert _gang.teardown(queue, group) is None
    assert len(_gang.elections(queue, group, 2)) == 2

    monkeypatch.setattr(pool.socket, "gethostname", lambda: "sparklina")
    probe = publish("completed-gang-probe", priority=-10, timeout_s=600,
                    cpu=1, gpu=0, mem_gb=1, tags=["sparklina"])
    follower = publish("completed-gang-follower", priority=-10, timeout_s=600,
                       cpu=1, gpu=0, mem_gb=1, tags=["sparklina"])
    rows = [pool._read_json(queue.item_path(pool.READY, key))
            for key in (probe, follower)]
    result = queue.claim(capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True,
                         has_gpu=True, tags=["gb10", "sparklina", _gang.TAG],
                         ready=rows)
    assert result is not None, denial(probe, "sparklina")
    assert result["action_key"] == probe, denial(probe, "sparklina")
    assert queue.ledger("sparklina").held_keys() == [probe]
