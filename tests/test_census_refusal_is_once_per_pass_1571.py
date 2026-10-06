"""A census refusal is item-independent, so a pass takes it once (#1571).

The census reads the whole queue and asks nothing about the candidate.  When
the reader fence is busy, every candidate in the same pass met the same
refusal -- but each one still waited ``FENCE_WAIT_S`` for the fence, so an
80-row pass behind a busy fence cost 80 waits per loop, ten loops hammered the
one fence at once, and dependents of a running measurement sat READY for 42
minutes with 'measurement census reader busy' (the 2026-10-06 forward
6bfa929e, starved of its staged inputs and ended no_progress).

The repair under test: one refusal ends the census for the rest of that pass;
every remaining candidate gets the same recorded denial without taking the
fence again.  Refusing never authorizes anything, so this only removes
repeated waiting.  A publication that disappears while the census scans
(claimed, finished or withdrawn between listing and reading) is a race with
the queue, not an unreadable census: it is rescanned.
"""
from __future__ import annotations

import fcntl
import os

import pytest

from prismabuild import _measurement_reservation as reservation, adaptive_cpu, pool

from test_census_tmpfs_state_1451 import (  # noqa: F401  (fixtures)
    tmpfs_mount, tmpfs_state)
from test_measurement_drains_gpu_backfill import fleet as fleet_fixture

fleet = fleet_fixture

CAPACITY = {"cpu": 20, "gpu": 1, "mem_gb": 120}
TIERS = {"preferred": list(range(20)), "fallback": []}
CANDIDATES = 6


def _denials(queue):
    base = adaptive_cpu.local_state_base(queue.ledger().base)
    records = adaptive_cpu.read_json(base / pool.CLAIM_DENIALS).get("records", {})
    return list(records.values())


def test_a_busy_fence_is_waited_for_once_per_pass_not_once_per_candidate(
        fleet, tmpfs_state, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", tmpfs_state / "box-state")
    monkeypatch.setattr(reservation, "FENCE_WAIT_S", 0.05)
    keys = [publish(f"census-candidate-{index}", priority=-10)
            for index in range(CANDIDATES)]
    records = [pool._read_json(queue.item_path(pool.READY, key)) for key in keys]

    attempts: list[int] = []
    real_held = reservation.CensusReader.held

    def counted(self):
        attempts.append(1)
        return real_held(self)

    monkeypatch.setattr(reservation.CensusReader, "held", counted)

    # Another loop owns the fence for the whole pass.
    other = reservation.CensusReader(queue, queue.ledger())
    descriptor = os.open(other.directory / (other.name + ".guard"),
                         os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        claimed = queue.claim(
            capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True,
            has_gpu=True, tags=["gb10", "sparklina"], ready=records)
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)

    assert claimed is None
    refused = [row for row in _denials(queue)
               if row["reason"] == "measurement_census_unavailable"
               and row["action_key"] in keys]
    assert len(refused) == CANDIDATES, (
        "every candidate must still get the recorded denial", refused)
    assert len(attempts) == 1, (
        f"a refused census must end the census for the pass, not be retried "
        f"for each of {CANDIDATES} candidates: {len(attempts)} fence attempts")


def test_the_next_pass_takes_the_fence_again(fleet, tmpfs_state, monkeypatch):
    """The refusal lives for one pass only: a free fence admits the next pass."""
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", tmpfs_state / "box-state")
    monkeypatch.setattr(reservation, "FENCE_WAIT_S", 0.05)
    key = publish("census-next-pass", priority=-10)
    record = pool._read_json(queue.item_path(pool.READY, key))

    other = reservation.CensusReader(queue, queue.ledger())
    descriptor = os.open(other.directory / (other.name + ".guard"),
                         os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        assert queue.claim(
            capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True,
            has_gpu=True, tags=["gb10", "sparklina"], ready=[record]) is None
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)

    claimed = queue.claim(
        capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True,
        has_gpu=True, tags=["gb10", "sparklina"], ready=[record])
    assert claimed is not None and claimed["action_key"] == key


def test_a_publication_that_vanishes_during_the_scan_is_rescanned(
        fleet, tmpfs_state, monkeypatch):
    """A row claimed or finished between listing and reading is not an outage."""
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", tmpfs_state / "box-state")
    keep = publish("census-kept", measurement=True, priority=-100)
    gone = publish("census-vanishing", measurement=True, priority=-100)
    gone_path = queue.item_path(pool.READY, gone)

    real_read = reservation._read

    def racing_read(path, *args, **kwargs):
        # The census reads in a forked child, so the race is recorded on disk,
        # not in this process.  The row was claimed and finished after the
        # scan listed it: gone on first read, absent on every rescan.
        if str(path) == str(gone_path) and gone_path.exists():
            os.unlink(gone_path)
        return real_read(path, *args, **kwargs)

    monkeypatch.setattr(reservation, "_read", racing_read)
    census = reservation.CensusReader(queue, queue.ledger()).capture()
    assert not gone_path.exists(), "the race was not exercised"
    assert keep in census["keys"]
    assert gone not in census["keys"], "a vanished row is simply gone"


def test_a_publication_that_keeps_vanishing_still_refuses_after_bounded_rescans(
        fleet, tmpfs_state, monkeypatch, tmp_path):
    """Rescanning is bounded: a queue that keeps changing is still a refused census.

    The retryable exception is the one the census raises for a vanished
    publication, so every attempt is retried; the count is recorded on disk
    because the census reads in a forked child (#1571 review note).
    """
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", tmpfs_state / "box-state")
    publish("census-always-gone", priority=-10)
    attempts = tmp_path / "attempts"

    def always_missing(path, *args, **kwargs):
        with attempts.open("a", encoding="utf-8") as handle:
            handle.write("x")
        raise reservation.PublicationDisappeared(
            f"publication disappeared during census: {path}")

    monkeypatch.setattr(reservation, "_read", always_missing)
    with pytest.raises(reservation.CensusUnavailable):
        reservation.CensusReader(queue, queue.ledger()).capture()
    # One read per scan reaches the first listed publication before it raises.
    assert len(attempts.read_text(encoding="utf-8")) == reservation.CAPTURE_RESCANS
