"""#1498: a slow NFS census over concluded sidecars stalled ordinary admission.

Observed 2026-10-04 on both GB10 workers: every census read every one of
~2,940 ``passes/`` sidecars, 2,923 of them for withdrawn/done/failed keys and
none carrying an election. The published ``_capture`` took 3.2-5.1 s of its
5 s budget on a Spark NFS client, so it timed out outright or held the host's
single reader fence while every sibling loop's census was refused "reader
busy" -- ordinary CPU and GPU rows included.

Reproduced here with the real fence, the real bounded census child and the
real claim path: per-record latency is injected into the census child's reads
(the NFS round trip), nothing in the census, fence, fork or lock is mocked.
"""
from __future__ import annotations

import fcntl
import json
import math
import os
import sys
import threading
import time

import pytest

from prismabuild import _measurement_reservation as reservation, adaptive_cpu, core, pool

from test_census_tmpfs_state_1451 import (  # noqa: F401  (fixtures)
    tmpfs_mount, tmpfs_state)
from test_measurement_reservation_backfill_1419 import (
    _bounded_measurement_wait, _observe_real_sharing_permission, fleet as fleet_fixture)

fleet = fleet_fixture

CAPACITY = {"cpu": 20, "gpu": 1, "mem_gb": 120}
TIERS = {"preferred": list(range(20)), "fallback": []}


def _conclude(queue, index, *, state=pool.WITHDRAWN, sidecar=None):
    """A concluded key with the leftover pass sidecar the fleet carries."""
    key = f"{0xC0DE0000 + index:064x}"
    queue.passes_path(key).parent.mkdir(exist_ok=True)
    queue.item_path(state, key).write_text(
        f'{{"action_key": "{key}", "status": "withdrawn"}}', encoding="utf-8")
    record = sidecar if sidecar is not None else (
        f'{{"action_key": "{key}", "passes": 3, "first_unix": 1.0, "updated_unix": 2.0}}')
    queue.passes_path(key).write_text(record.replace("KEY", key), encoding="utf-8")
    return key


def _slow_reads(monkeypatch, seconds):
    """NFS round-trip latency for every census record read (forked child too)."""
    real = reservation._read

    def slow(path, **kwargs):
        time.sleep(seconds)
        return real(path, **kwargs)

    monkeypatch.setattr(reservation, "_read", slow)


def _claim(queue, key):
    candidate = pool._read_json(queue.item_path(pool.READY, key))
    return queue.claim(capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True,
                       has_gpu=True, tags=["gb10", "sparklina"], ready=[candidate])


def test_slow_census_over_concluded_sidecars_refuses_ordinary_work_until_swept(
        fleet, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    ordinary = publish("ordinary-cpu-row", priority=-10, gpu=0)
    orphans = [_conclude(queue, index) for index in range(300)]
    # 300 records x 20 ms > READ_BUDGET_S: the production shape, scaled down.
    _slow_reads(monkeypatch, 0.02)

    started = time.monotonic()
    assert _claim(queue, ordinary) is None
    assert time.monotonic() - started < reservation.READ_BUDGET_S + 5
    refusal = denial(ordinary)
    assert refusal["reason"] == "measurement_census_unavailable"
    assert "timed_out" in refusal["evidence"]["unavailable"], refusal
    assert not queue.ledger().held_keys()

    # Bounded per call: the backlog drains over successive sweeps.
    first = queue.sweep_orphan_passes()
    assert len(first) == pool.ORPHAN_PASSES_SWEEP_LIMIT
    rest = queue.sweep_orphan_passes()
    assert sorted(first + rest) == sorted(orphans)
    assert queue.sweep_orphan_passes() == []
    assert not any(queue.passes_path(key).exists() for key in orphans)
    # Terminal/withdrawal records are untouched: only the sidecar retires.
    assert all(queue.item_path(pool.WITHDRAWN, key).exists() for key in orphans)

    tick(11)
    claimed = _claim(queue, ordinary)
    assert claimed is not None and claimed["action_key"] == ordinary


def test_sweep_keeps_every_sidecar_that_could_be_authority(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    live = publish("live-row")
    queue.record_pass(live)
    prunable = _conclude(queue, 1, state=pool.DONE)
    elected = _conclude(queue, 2, sidecar=(
        '{"action_key": "KEY", "passes": 1, "measurement_reservation": {"schema": "x"}}'))
    foreign = _conclude(queue, 3, sidecar=(
        '{"action_key": "' + "f" * 64 + '", "passes": 1}'))
    unreadable = _conclude(queue, 4, sidecar="{not json")
    locked = _conclude(queue, 5)
    unconcluded = f"{0xC0DE0006:064x}"
    queue.passes_path(unconcluded).parent.mkdir(exist_ok=True)
    queue.passes_path(unconcluded).write_text(
        f'{{"action_key": "{unconcluded}", "passes": 1}}', encoding="utf-8")

    holding, release = threading.Event(), threading.Event()

    def hold():
        with queue._transition_locked(locked):
            holding.set()
            release.wait(timeout=60)

    holder = threading.Thread(target=hold)
    holder.start()
    try:
        assert holding.wait(timeout=60)
        assert queue.sweep_orphan_passes() == [prunable]
    finally:
        release.set()
        holder.join(timeout=60)
    for key in (live, elected, foreign, unreadable, locked, unconcluded):
        assert queue.passes_path(key).exists(), key
    # Released, the concluded counter-only sidecar goes on the next sweep.
    assert queue.sweep_orphan_passes() == [locked]


@pytest.mark.parametrize("limit", [1, 7, pool.ORPHAN_PASSES_SWEEP_LIMIT])
def test_sweep_rotates_past_257_kept_sidecars_across_loop_restarts(
        fleet, monkeypatch, limit):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    retained = [
        '{"action_key": "KEY", "measurement_reservation": {"schema": "x"}}',
        '{"action_key": "' + "f" * 64 + '", "passes": 1}',
        "{not json",
    ]
    kept = [_conclude(queue, index, sidecar=retained[index % len(retained)])
            for index in range(257)]
    before = {key: queue.passes_path(key).read_bytes() for key in kept}
    orphan = _conclude(queue, 300)
    real_read = pool._read_json
    inspected = []

    def read(path, **kwargs):
        if path.parent == queue.root / pool.PASSES:
            inspected.append(path.name)
        return real_read(path, **kwargs)

    monkeypatch.setattr(pool, "_read_json", read)
    pruned = []
    for _ in range(math.ceil((len(kept) + 1) / limit)):
        # Different loop instances share the existing host-local sweep owner.
        restarted = pool.PoolQueue(queue.root)
        assert restarted._sweep_due()
        inspected.clear()
        pruned += restarted.sweep_orphan_passes(limit=limit)
        assert len(inspected) <= limit
        assert not restarted._sweep_due(), "cursor writes must not change the schedule"
        tick(pool.HEARTBEAT_S + 1)
    assert pruned == [orphan], "the retained sorted prefix starved the later orphan"
    assert not queue.passes_path(orphan).exists()
    assert {key: queue.passes_path(key).read_bytes() for key in kept} == before

    # A new earlier key is reached after wrapping, even when the last name
    # inspected was deleted; the cursor is a name, not a surviving-file index.
    earlier = _conclude(queue, -1)
    pruned = []
    for _ in range(math.ceil((len(kept) + 1) / limit)):
        inspected.clear()
        pruned += pool.PoolQueue(queue.root).sweep_orphan_passes(limit=limit)
        assert len(inspected) <= limit
    assert pruned == [earlier]
    assert {key: queue.passes_path(key).read_bytes() for key in kept} == before


def test_fair_sweep_recovers_capped_census_without_retiring_elections(
        fleet, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    ordinary = publish("ordinary-after-fair-sweep", gpu=0)
    queue.record_pass(ordinary)
    kept = []
    for index in range(257):
        key = _conclude(queue, index, state=pool.DONE)
        publication = {"action_key": key, "published_unix": 1.0}
        chosen = {"schema": reservation.SCHEMA, **publication,
                  "generation": core.canonical_sha256(publication),
                  "host": "sparklina", "priority": 0,
                  "epoch_unix": 2.0, "opportunity_unix": 3.0}
        queue.passes_path(key).write_text(json.dumps(
            {"action_key": key, reservation.FIELD: chosen}), encoding="utf-8")
        queue.item_path(pool.DONE, key).write_text(json.dumps(
            {"schema": pool.POOL_OUTCOME_SCHEMA_V1, **publication,
             "status": "executed", "finished_unix": 4.0}), encoding="utf-8")
        kept.append(key)
    before = {key: queue.passes_path(key).read_bytes() for key in kept}
    orphan = _conclude(queue, 300)
    # Scale only the record cap: real bounded census, retirement proof, reader
    # fence, transition locks, admission gate and capacity claim all execute.
    monkeypatch.setattr(reservation, "MAX_RECORDS", len(kept) + 2)
    assert _claim(queue, ordinary) is None
    refusal = denial(ordinary)
    assert refusal["reason"] == "measurement_census_unavailable"
    assert "record cap exceeded" in refusal["evidence"]["unavailable"]
    assert not queue.ledger().held_keys()

    assert queue.sweep_orphan_passes() == []
    assert pool.PoolQueue(queue.root).sweep_orphan_passes() == [orphan]
    assert {key: queue.passes_path(key).read_bytes() for key in kept} == before
    census = reservation.CensusReader(queue, queue.ledger()).capture()
    assert set(census["selections"]) == set(kept)
    assert census["elections"] == {}  # exact endings, not missing authority
    tick(11)
    claimed = _claim(queue, ordinary)
    assert claimed is not None and claimed["action_key"] == ordinary


def test_swept_census_still_refuses_lower_work_behind_a_live_election(fleet, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder, measurement, opportunity, snapshot = _bounded_measurement_wait(fleet)
    election = queue.passes_path(measurement).read_bytes()
    orphans = [_conclude(queue, index) for index in range(40)]
    assert sorted(queue.sweep_orphan_passes()) == sorted(orphans)
    assert queue.passes_path(measurement).read_bytes() == election

    _slow_reads(monkeypatch, 0.002)
    lower = publish("lower-behind-election", timeout_s=5)
    _observe_real_sharing_permission(fleet, lower)
    assert _claim(queue, lower) is None
    refusal = denial(lower)
    assert refusal["reason"] == "deferred_for_measurement_reservation", refusal
    assert refusal["evidence"]["selection"] == reservation.selection(
        pool._read_json(queue.passes_path(measurement)))
    assert queue.ledger().held_keys() == [holder]


def _hold_fence(census, seconds, started):
    guard = census.directory / (census.name + ".guard")
    descriptor = os.open(guard, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    started.set()
    try:
        time.sleep(seconds)
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def test_a_sibling_census_is_waited_for_not_turned_into_a_denial(
        fleet, tmpfs_state, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", tmpfs_state / "box-state")
    ordinary = publish("ordinary-behind-sibling", gpu=0)
    census = reservation.CensusReader(queue, queue.ledger())
    started = threading.Event()
    sibling = threading.Thread(target=_hold_fence, args=(census, 0.5, started))
    sibling.start()
    try:
        assert started.wait(timeout=60)
        claimed = _claim(queue, ordinary)
    finally:
        sibling.join(timeout=60)
    assert claimed is not None and claimed["action_key"] == ordinary


def test_a_fence_held_past_the_wait_still_refuses_nonblocking(
        fleet, tmpfs_state, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", tmpfs_state / "box-state")
    census = reservation.CensusReader(queue, queue.ledger())
    started = threading.Event()
    sibling = threading.Thread(
        target=_hold_fence, args=(census, reservation.FENCE_WAIT_S + 2, started))
    sibling.start()
    try:
        assert started.wait(timeout=60)
        began = time.monotonic()
        try:
            reservation.CensusReader(queue, queue.ledger()).capture()
            raise AssertionError("a held fence granted a census")
        except reservation.CensusUnavailable as exc:
            assert "reader busy" in str(exc)
        waited = time.monotonic() - began
        assert reservation.FENCE_WAIT_S <= waited < reservation.FENCE_WAIT_S + 1, waited
    finally:
        sibling.join(timeout=60)


def test_serve_once_runs_the_orphan_sweep_on_the_reaper_schedule(fleet, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    orphan = _conclude(queue, 7)
    monkeypatch.setattr(queue, "_sweep_due", lambda **kwargs: False)
    monkeypatch.setattr(queue, "claim", lambda **kwargs: None)
    assert queue.serve_once(python=sys.executable) is None
    assert queue.passes_path(orphan).exists()
    monkeypatch.setattr(queue, "_sweep_due", lambda **kwargs: True)
    monkeypatch.setattr(queue, "reap_stale", lambda **kwargs: [])
    assert queue.serve_once(python=sys.executable) is None
    assert not queue.passes_path(orphan).exists()
