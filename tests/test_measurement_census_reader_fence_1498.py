"""#1498: the census reader fence must hold across both locked_census phases.

``locked_census`` reads the census once outside H for discovery and again
under H for its refresh. Pre-fix, ``CensusReader.capture`` released the
host-local fence between those phases, so a concurrent queue observer could
take it in the gap and deny the refresh -- after the caller had already taken
the measurement transition keys and host admission -- with
``measurement census reader busy``. That is the deterministic mechanism
behind the observed EXL3 preflight denial storm (b00a00aea99a/e04435487de0),
and it is reproduced here through the real fence, the real bounded child and
the real claim path: no census guard, fork, marker or lock is mocked.

The repair under test: one fence acquisition per ``locked_census``. The
single-reader fence contract (ownership marker persisted per acquisition,
retained-reader liveness, at most one bounded census child at a time) is
unchanged, and a refused fence still denies the pass nonblocking before any
M/H investment.
"""
from __future__ import annotations

import contextlib
import fcntl
import os
import threading
import time

from prismabuild import _measurement_reservation as reservation, adaptive_cpu, pool

from test_census_tmpfs_state_1451 import (  # noqa: F401  (fixtures)
    tmpfs_mount, tmpfs_state)
from test_measurement_drains_gpu_backfill import fleet as fleet_fixture

fleet = fleet_fixture

CAPACITY = {"cpu": 20, "gpu": 1, "mem_gb": 120}
TIERS = {"preferred": list(range(20)), "fallback": []}


def _denial(queue, key):
    base = adaptive_cpu.local_state_base(queue.ledger().base)
    records = adaptive_cpu.read_json(base / pool.CLAIM_DENIALS).get("records", {})
    return next((value for value in records.values()
                 if value["action_key"] == key), None)


def _fill_ready(queue, count):
    """Real scan cost for the bounded census child: legacy publications.

    Each row passes the strict census record checks (hex64 name, matching
    action_key, strict JSON, finite published_unix) with no sealed request,
    the supported legacy direct-publication path.
    """
    directory = queue.root / pool.READY
    for index in range(count):
        key = f"{index:064x}"
        (directory / f"{key}.json").write_text(
            f'{{"action_key": "{key}", "published_unix": {1000.0 + index}}}',
            encoding="utf-8")


def test_the_reader_fence_holds_across_discovery_and_refresh(
        fleet, tmpfs_state, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", tmpfs_state / "box-state")
    _fill_ready(queue, 2500)
    other = pool.PoolQueue(queue.root)

    publish("fence-measurement", measurement=True, priority=-100)
    candidate = publish("fence-candidate", priority=-10)
    candidate_record = pool._read_json(queue.item_path(pool.READY, candidate))

    # Park the admission pass between the discovery read and the under-H
    # refresh, holding the measurement transition key and host admission,
    # exactly the window whose fence release the fleet denial exposed. The
    # first host-admission acquisition belongs to claim's own capacity
    # setup before the census gate; the census phase is the second.
    parked, resume = threading.Event(), threading.Event()
    real_lock = pool.PoolQueue._admission_lock
    entered: list[int] = []

    def parked_admission_lock(controller):
        @contextlib.contextmanager
        def held():
            with real_lock(controller):
                entered.append(1)
                if len(entered) == 2:
                    parked.set()
                    assert resume.wait(timeout=60), "parked pass never resumed"
                yield
        return held()

    monkeypatch.setattr(queue, "_admission_lock", parked_admission_lock)

    outcome: dict = {}

    def admit():
        outcome["claimed"] = queue.claim(
            capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True,
            has_gpu=True, tags=["gb10", "sparklina"], ready=[candidate_record])

    def observe():
        census = reservation.CensusReader(other, other.ledger())
        try:
            outcome["census"] = census.capture()
        except reservation.CensusUnavailable as exc:
            outcome["unavailable"] = str(exc)

    def fence_busy(census):
        guard = census.directory / (census.name + ".guard")
        descriptor = os.open(guard, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            return False
        finally:
            os.close(descriptor)

    pass_thread = threading.Thread(target=admit)
    pass_thread.start()
    assert parked.wait(timeout=60), "admission never reached its refresh"

    observer = reservation.CensusReader(other, other.ledger())
    # While parked between its phases, the pass owns the fence: the later
    # observer must be the one refused, never the parked pass's refresh.
    assert fence_busy(observer), "refresh park does not hold the reader fence"

    census_thread = threading.Thread(target=observe)
    census_thread.start()
    deadline = time.monotonic() + 60
    while not fence_busy(observer):
        assert time.monotonic() < deadline, "observer never took the fence"
        time.sleep(0.001)

    resume.set()
    pass_thread.join(timeout=120)
    census_thread.join(timeout=120)
    assert not pass_thread.is_alive() and not census_thread.is_alive()

    # The pass that took M and H keeps the fence through its refresh: a later
    # observer's discovery read is refused instead of stealing the phase.
    claimed = outcome.get("claimed")
    assert isinstance(claimed, dict) and claimed.get("action_key") == candidate, outcome
    assert _denial(queue, candidate) is None, _denial(queue, candidate)
    # The observer waits a bounded FENCE_WAIT_S for the fence (#1498): it
    # either censuses after the pass let go, or is refused naming the busy
    # fence and nothing else. Fail-closed denial is preserved, not bypassed.
    assert ("census" in outcome
            or "reader busy" in outcome.get("unavailable", "")), outcome
    # A settled fence stays reusable: recovery through a fresh acquisition.
    again = reservation.CensusReader(queue, queue.ledger()).capture()
    repeat = reservation.CensusReader(queue, queue.ledger()).capture()
    assert repeat["keys"] == again["keys"]


def test_a_busy_fence_refuses_before_any_transition_or_host_lock(
        fleet, tmpfs_state, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", tmpfs_state / "box-state")
    census = reservation.CensusReader(queue, queue.ledger())
    guard = census.directory / (census.name + ".guard")
    descriptor = os.open(guard, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        publish("busy-fence-measurement", measurement=True, priority=-100)
        candidate = publish("busy-fence-candidate", priority=-10)
        candidate_record = pool._read_json(queue.item_path(pool.READY, candidate))
        started = time.monotonic()
        result = queue.claim(capacity=CAPACITY, cpu_tiers=TIERS,
                             adaptive_cpu=True, has_gpu=True,
                             tags=["gb10", "sparklina"], ready=[candidate_record])
        elapsed = time.monotonic() - started
        assert result is None
        row = _denial(queue, candidate)
        assert row is not None and row["reason"] == "measurement_census_unavailable"
        assert "reader busy" in row["evidence"]["unavailable"]
        assert elapsed < 5.0, elapsed
        assert queue.ledger().held_keys() == []
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def test_a_body_exception_under_the_held_fence_keeps_its_type(fleet, tmpfs_state, monkeypatch):
    """#1506: only acquiring the fence is a census failure. An exception the
    caller's own body raises while it holds the fence (a GPU sample write in
    ``reserve_probe``) is not relabelled CensusUnavailable, the fence is
    released, and a busy fence is still refused as CensusUnavailable."""
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", tmpfs_state / "box-state")
    census = reservation.CensusReader(queue, queue.ledger())
    for error in (OSError("sample persistence unavailable"), ValueError("body value")):
        try:
            with census.held():
                raise error
        except reservation.CensusUnavailable as exc:
            raise AssertionError(f"body {type(error).__name__} relabelled: {exc}") from exc
        except type(error) as exc:
            assert exc is error
        assert census._held is None
    guard = census.directory / (census.name + ".guard")
    descriptor = os.open(guard, os.O_RDWR | os.O_NOFOLLOW)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)  # released by the body exits
        monkeypatch.setattr(reservation, "FENCE_WAIT_S", 0.0)
        try:
            with census.held():
                raise AssertionError("acquired a fence another holder owns")
        except reservation.CensusUnavailable as exc:
            assert "reader busy" in str(exc)
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)
