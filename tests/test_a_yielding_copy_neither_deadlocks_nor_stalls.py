"""Standing aside for a waited copy can neither deadlock nor kill (#1091 review 1).

Three defects in the first cut of the reader plan:

* A running copy stood aside whenever any waited copy was listed, ``ready``
  or ``claimed``.  A ``ready`` waited copy that cannot be claimed, because the
  stage GiB the standing-aside copies hold is what it needs, never lands, and
  the copies holding that GiB wait on it forever.  A copy now stands aside
  only while a waited copy is ``claimed``, so reading.
* A copy standing aside lands nothing, and under a progress policy the
  worker's ``no_progress`` rung ended it at its grace.  The worker now
  credits that quiet on its own evidence: a fresh tier record that lists a
  ``claimed`` waited copy, not this one, whose claim it reads itself.
* Once the cap bound, every denied mover row re-listed ``claimed/`` and read
  each claimed mover's receipt.  A pass now reads each tier's reading set
  once.

Everything runs on ``tmp_path`` queues (#628).
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import json
from pathlib import Path
import subprocess
import sys
import threading
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import pool, storage_tiers  # noqa: E402
import stage_move  # noqa: E402
from test_progress_keeps_a_working_action_alive import _claimed, _policy  # noqa: E402
from test_the_reader_plan_measures_and_fails_open import (  # noqa: E402
    GIB, TIER, _claim, _queue)

WAITED = "a" * 64
OTHER = "b" * 64


class _Clock:
    def __init__(self) -> None:
        self.seconds = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.seconds

    def time(self) -> float:
        return 10_000.0 + self.seconds

    def perf_counter(self) -> float:
        return self.seconds

    def advance(self, seconds: float) -> None:
        assert seconds >= 0
        self.seconds += seconds

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        pytest.fail("unexpected sleep in a finite reader-plan script")


@pytest.fixture
def clock(monkeypatch) -> _Clock:
    controlled = _Clock()
    for module in (pool, pool.pb, pool.materialize):
        monkeypatch.setattr(module, "time", controlled)
    monkeypatch.setattr(pool, "_now", controlled.time)
    return controlled


def _publish_mover(queue: pool.PoolQueue, key: str, ordinal: int) -> None:
    queue.publish(
        action_key=key, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
        tags=["dl380g10"], max_attempts=1, retry_safe=False,
        resources={"cpu": 1, "mem_gb": 1,
                   f"stage_gib{storage_tiers.TIER_DEMAND_SEPARATOR}{TIER}": 2},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": "9" * 64, "manifest_bytes": 1 << 40,
                   "range_start_bytes": ordinal * 2 * GIB,
                   "range_end_bytes": (ordinal + 1) * 2 * GIB})


def _announce(queue: pool.PoolQueue, rows: list[tuple[str, str]], *,
              cap: int | None = None, announced: float | None = None) -> None:
    """The tier record with a reader plan listing ``(mover, state)`` rows."""

    queue.announce_tier({
        "schema": storage_tiers.TIER_RECORD_SCHEMA_V1, "tier_id": TIER,
        "host": "dl380g10", "tier": "stage", "capacity_bytes": 16 * GIB,
        pool.READER_PLAN_FIELD: {
            "declared_wait": [{"mover_action_key": key, "state": state,
                               "consumers": ["c" * 64], "since_unix": 1.0}
                              for key, state in rows],
            "cap": {"movers": cap, "basis": "measured" if cap else "unmeasured"},
            "stale_after_s": pool.OFFER_TIMEOUT_S}},
        now=announced)


# ------------------------------------------ 1: no stand-aside for a ready row


def _plan(queue: pool.PoolQueue, mover: str, clock: _Clock
          ) -> stage_move._ReaderPlan:
    return stage_move._ReaderPlan(
        queue, TIER, mover, hold_s=0.25, clock=clock.monotonic,
        wall=clock.time, sleep=clock.sleep)


def test_a_running_copy_does_not_stand_aside_for_a_waited_copy_still_ready(
        tmp_path: Path, clock: _Clock) -> None:
    """The waited copy is ``ready``: it reads nothing, so nothing yields to it.

    If it cannot be claimed -- the stage GiB it needs is held by the copies
    standing aside for it -- yielding to it is a deadlock.  The waited copy
    still exempts itself, and the claim pass still defers other rows, which
    holds nothing.
    """

    queue = _queue(tmp_path, fill=0)
    _announce(queue, [(WAITED, "ready")])
    other, waited = _plan(queue, OTHER, clock), _plan(queue, WAITED, clock)
    stop = threading.Event()
    started = clock.monotonic()
    other.stand_aside(stop)

    assert not other.yields(), (
        "a running copy stands aside for a waited copy that is only ready")
    assert clock.monotonic() == started
    assert clock.sleeps == []
    assert not stop.is_set()
    assert other.report()["yields"] == 0
    assert other.report()["yielded_seconds"] == 0
    assert waited.exempt()


def test_a_running_copy_stands_aside_once_the_waited_copy_is_claimed(
        tmp_path: Path, clock: _Clock) -> None:
    queue = _queue(tmp_path, fill=0)
    _announce(queue, [(WAITED, "ready")])
    other = _plan(queue, OTHER, clock)
    assert not other.yields()

    _announce(queue, [(WAITED, "claimed")])
    clock.advance(other.hold_s + 0.125)

    assert other.yields()
    assert not _plan(queue, WAITED, clock).yields()


def test_a_reader_plan_refreshes_at_its_injected_hold_boundary(
        tmp_path: Path, clock: _Clock) -> None:
    queue = _queue(tmp_path, fill=0)
    _announce(queue, [(WAITED, "ready")])
    other, waited = _plan(queue, OTHER, clock), _plan(queue, WAITED, clock)
    assert not other.yields()
    assert waited.exempt()
    path = queue.tier_record_path(TIER)
    original_inode = path.stat().st_ino

    clock.advance(0.125)
    _announce(queue, [(WAITED, "claimed")])
    assert path.stat().st_ino != original_inode
    assert not other.yields()
    assert not waited.yields()
    clock.advance(0.125)
    assert clock.monotonic() == other.hold_s
    assert other.yields()
    assert not waited.yields()

    _announce(queue, [])
    assert other.yields()
    assert waited.exempt()
    clock.advance(other.hold_s)
    assert not other.yields()
    assert not waited.exempt()

    _announce(queue, [(WAITED, "claimed")])
    clock.advance(other.hold_s)
    assert other.yields()
    assert waited.exempt()
    clock.advance(pool.OFFER_TIMEOUT_S + 1.0)
    assert not other.yields()
    assert not waited.exempt()
    assert clock.sleeps == []


# ------------------------------ 2: the worker credits a verified stand-aside


def _standing_mover(tmp_path: Path):
    """A claimed stage mover on ``TIER`` under a 0.4 s progress grace."""

    queue, item = _claimed(tmp_path, mode="scripted", seconds=0,
                           policy=_policy(0.4, 0.4, 0.4),
                           source='raise AssertionError("use the process script")\n')
    queue.mint_tier_capacity(TIER, {"stage_gib": 16})
    item["residency"] = {"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                         "manifest_sha256": "9" * 64, "manifest_bytes": 4 * GIB,
                         "range_start_bytes": 2 * GIB,
                         "range_end_bytes": 4 * GIB}
    queue.item_path(pool.CLAIMED, str(item["action_key"])).write_text(
        json.dumps(item))
    return queue, item


def _waited_claimed(queue: pool.PoolQueue, clock: _Clock) -> None:
    """``WAITED`` claimed on ``TIER``, as the worker will read it."""

    _publish_mover(queue, WAITED, 0)
    source = queue.item_path(pool.READY, WAITED)
    record = json.loads(source.read_text())
    source.unlink()
    record.update({"claimed_unix": clock.time() - 5.0,
                   "claimed_by": "waited-fixture", "claimed_host": "dl380g10"})
    queue.item_path(pool.CLAIMED, WAITED).write_text(json.dumps(record))


def _publish_progress(path: Path, token: str, clock: _Clock) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps({
        "schema": pool.pb.PROGRESS_RECORD_SCHEMA_V1, "token": token,
        "phase": "run", "units_completed": 1, "reported_unix": clock.time()}))
    temporary.replace(path)


@dataclass(frozen=True)
class _Step:
    at: float | None = None
    commit: bool = False
    before: Callable[[], None] | None = None


class _Process:
    pid = 99_999_999
    stdout = stderr = None

    def __init__(self, clock: _Clock, steps: list[_Step]) -> None:
        self.clock = clock
        self.steps = iter(steps)
        self.events: list[tuple[str, float]] = []
        self.returncode: int | None = None
        self.environment: dict[str, str] | None = None

    def launch(self, argv, *, env, **kwargs):
        assert self.environment is None, "unexpected second process launch"
        self.environment = env
        return self

    def poll(self):
        return self.returncode

    def terminate(self, process, *, grace_s) -> None:
        assert process is self
        self.events.append(("terminated", self.clock.monotonic()))
        self.returncode = -15

    def communicate(self, *, timeout):
        if self.returncode is not None:
            return "", ""
        step = next(self.steps, None)
        assert step is not None, "process observations exhausted before an outcome"
        if step.before is not None:
            step.before()
        remaining = (timeout if step.at is None
                     else step.at - self.clock.monotonic())
        assert remaining > 0
        assert remaining <= timeout, (
            f"scripted observation at {step.at} exceeds poll allowance {timeout}")
        self.clock.advance(remaining)
        self.events.append(("committed" if step.commit else "quiet",
                            self.clock.monotonic()))
        if step.commit:
            assert self.environment is not None
            _publish_progress(
                Path(self.environment[pool.pb.ACTION_PROGRESS_PATH_ENV]),
                self.environment[pool.pb.ACTION_PROGRESS_TOKEN_ENV], self.clock)
            self.returncode = 0
            return "", ""
        raise subprocess.TimeoutExpired("scripted mover", timeout)


def _quiet_steps() -> list[_Step]:
    return [_Step() for _ in range(8)] + [_Step(1.5, commit=True)]


def _execute(queue: pool.PoolQueue, item, process: _Process, monkeypatch):
    subprocess_adapter = SimpleNamespace(**vars(subprocess))
    subprocess_adapter.Popen = process.launch
    monkeypatch.setattr(pool, "subprocess", subprocess_adapter)
    monkeypatch.setattr(pool.pb, "_terminate_process_group", process.terminate)
    return queue.execute(item, timeout_s=None, heartbeat_s=0.2,
                         timeout_grace_s=0.2)


def test_a_copy_standing_aside_for_a_claimed_waited_copy_is_not_killed(
        tmp_path: Path, clock: _Clock, monkeypatch) -> None:
    """Quiet for 1.5 s against a 0.4 s grace, all of it standing aside.

    The waited copy is claimed and listed in a fresh plan, and this copy is
    not.  Before the fix the rung ended it at its grace, which fails the
    waited copy's consumer next.
    """

    queue, item = _standing_mover(tmp_path)
    _waited_claimed(queue, clock)
    _announce(queue, [(WAITED, "claimed")])

    process = _Process(clock, _quiet_steps())
    outcome = _execute(queue, item, process, monkeypatch)

    assert outcome["status"] == "executed", (
        f"a copy standing aside for a claimed waited copy was ended "
        f"{outcome.get('termination_reason')!r}")
    observed = outcome["progress_observation"]
    assert observed["reader_plan_exempt_s"] > 0.4
    assert observed["reader_plan"]["exempt"] is True
    assert observed["reader_plan"]["waited"] == [WAITED]
    assert observed["last_accepted"]["units_completed"] == 1
    assert observed["accepted_count"] == 1
    assert process.events[-1] == ("committed", 1.5)
    assert outcome["elapsed_s"] == 1.5


@pytest.mark.parametrize("case", [
    "no-wait", "ready-only", "stale-plan", "not-claimed", "self-listed"])
def test_without_a_verified_stand_aside_the_grace_applies(
        tmp_path: Path, case: str, clock: _Clock, monkeypatch) -> None:
    """Each leg of the evidence, missing: the rung ends the copy as before.

    ``not-claimed`` is a plan that says ``claimed`` while the queue holds no
    such claim: the worker reads the claim itself and does not take the
    record's word for it.  ``self-listed`` is this copy as the waited one,
    which reads rather than stands aside.
    """

    queue, item = _standing_mover(tmp_path)
    me = str(item["action_key"])
    if case != "not-claimed":
        _waited_claimed(queue, clock)
    rows = {"no-wait": [], "ready-only": [(WAITED, "ready")],
            "stale-plan": [(WAITED, "claimed")],
            "not-claimed": [(WAITED, "claimed")],
            "self-listed": [(me, "claimed")]}[case]
    _announce(queue, rows, announced=(clock.time() - pool.OFFER_TIMEOUT_S - 1
                                      if case == "stale-plan" else None))

    process = _Process(clock, _quiet_steps())
    outcome = _execute(queue, item, process, monkeypatch)

    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "no_progress"
    assert outcome["progress_observation"].get("reader_plan_exempt_s", 0) == 0
    assert outcome["stall"]["credited_s"].get("reader_plan", 0) == 0
    assert process.events == [("quiet", 0.2), ("quiet", 0.4), ("terminated", 0.4)]
    assert outcome["progress_observation"]["accepted_count"] == 0


def test_the_grace_applies_again_once_the_waited_copy_leaves_the_plan(
        tmp_path: Path, clock: _Clock, monkeypatch) -> None:
    """Credited while the waited copy is listed, then charged again."""

    queue, item = _standing_mover(tmp_path)
    _waited_claimed(queue, clock)
    _announce(queue, [(WAITED, "claimed")])
    prior_credit = []

    def remove_waited() -> None:
        observation = json.loads(queue.lease_path(str(item["action_key"])).read_text())[
            "progress_observation"]
        credit = observation["reader_plan_exempt_s"]
        assert credit > 0
        assert observation["reader_plan"]["exempt"] is True
        assert observation["reader_plan"]["waited"] == [WAITED]
        prior_credit.append(credit)
        process.events.append(("credit_verified", clock.monotonic()))
        _announce(queue, [])
        process.events.append(("plan_removed", clock.monotonic()))

    steps = [_Step(), _Step(), _Step(), _Step(before=remove_waited)]
    steps.extend(_Step() for _ in range(10))
    steps.append(_Step(3.0, commit=True))
    process = _Process(clock, steps)
    outcome = _execute(queue, item, process, monkeypatch)

    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "no_progress"
    credited = outcome["stall"]["credited_s"].get("reader_plan", 0)
    assert credited > 0, "the stand-aside while the waited copy was listed was charged"
    assert prior_credit == [credited]
    assert credited == pytest.approx(0.4)
    observed = outcome["progress_observation"]
    assert observed["reader_plan_exempt_s"] == credited
    assert observed["reader_plan"]["exempt"] is False
    assert observed["accepted_count"] == 0
    assert outcome["stall"]["quiet_s"] == pytest.approx(0.4)
    assert clock.monotonic() == credited + outcome["stall"]["allowance_s"]
    assert outcome["elapsed_s"] == pytest.approx(clock.monotonic())
    assert process.events == [
        ("quiet", 0.2), ("quiet", 0.4), ("quiet", 0.4 + 0.2),
        ("credit_verified", 0.4 + 0.2), ("plan_removed", 0.4 + 0.2),
        ("quiet", 0.8), ("terminated", 0.8)]
    assert clock.monotonic() < 3.0


def test_a_reader_plan_credit_is_applied_once_per_virtual_interval(
        tmp_path: Path, clock: _Clock) -> None:
    queue, item = _standing_mover(tmp_path)
    _waited_claimed(queue, clock)
    _announce(queue, [(WAITED, "claimed")])
    policy = pool.progress_policy(item, None)
    assert policy is not None
    path = queue.action_progress_path(str(item["action_key"]))
    token = "virtual-launch"
    watch = pool.ProgressWatch(path, token, policy, started=clock.monotonic())
    verdict = queue.reader_plan_stand_aside(item)
    assert verdict is not None and verdict["exempt"] is True
    since = clock.monotonic() - (clock.time() - verdict["since_unix"])

    def credit() -> float:
        return watch.exempt_reader_plan(
            verdict, now=clock.monotonic(), since_monotonic=since)

    clock.advance(0.25)
    assert credit() == 0.25
    assert credit() == 0
    clock.advance(0.25)
    assert credit() == 0.25
    assert watch.exempt_staged_wait(
        {"exempt": True}, now=clock.monotonic(), since_monotonic=0.25) == 0
    clock.advance(0.25)
    assert watch.exempt_reader_plan(
        verdict, now=clock.monotonic(), since_monotonic=0.125) == 0.25
    assert watch.reader_plan_exempt_s == 0.75
    assert watch.stall_deadline() == 0.75 + 0.4
    assert watch.accepted == 0

    _publish_progress(path, token, clock)
    assert watch.sample(now=clock.monotonic())
    clock.advance(0.25)
    assert credit() == 0.25
    assert credit() == 0
    assert watch.reader_plan_exempt_s == 1.0
    assert watch.stall_deadline() == 1.0 + 0.4
    _announce(queue, [])
    denied = queue.reader_plan_stand_aside(item)
    assert denied is not None and denied["exempt"] is False
    clock.advance(0.125)
    assert watch.exempt_reader_plan(
        denied, now=clock.monotonic(), since_monotonic=since) == 0
    assert watch.reader_plan_exempt_s == 1.0
    assert watch.stall_deadline() == 1.0 + 0.4
    assert watch.as_record(now=clock.monotonic())["quiet_s"] == 0.125
    clock.advance(watch.stall_deadline() - clock.monotonic())
    assert clock.monotonic() == watch.stall_deadline()
    assert watch.as_record(now=clock.monotonic())["quiet_s"] == pytest.approx(0.4)


# ------------------------------------------- 3: one reading set per claim pass


def test_a_pass_over_denied_rows_reads_the_tiers_claims_once(
        tmp_path: Path, monkeypatch, clock: _Clock) -> None:
    """Twenty ready copies behind a cap of two: one pass, one listing.

    Before the fix every denied row listed ``claimed/`` again and read every
    claimed copy's receipt: twenty listings and forty receipt reads.
    """

    queue = _queue(tmp_path, fill=0)
    for ordinal in range(20):
        _publish_mover(queue, f"{ordinal:02d}".ljust(64, "e"), ordinal)
        clock.advance(0.01)
    _announce(queue, [], cap=2)
    assert _claim(queue) is not None and _claim(queue) is not None

    listings, receipts = [], []
    list_claimed = queue.movers_claimed_on_tier
    read_receipt = queue.move_record
    monkeypatch.setattr(queue, "movers_claimed_on_tier",
                        lambda tier: (listings.append(tier), list_claimed(tier))[1])
    monkeypatch.setattr(queue, "move_record",
                        lambda key: (receipts.append(key), read_receipt(key))[1])

    assert _claim(queue) is None

    assert len(listings) <= 1, (
        f"one claim pass listed the tier's claims {len(listings)} times")
    assert len(receipts) <= 2
