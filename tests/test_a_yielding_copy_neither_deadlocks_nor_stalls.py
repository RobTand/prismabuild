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
from prismabuild import core as pb, pool, storage_tiers  # noqa: E402
import stage_move  # noqa: E402
import test_the_reader_plan_measures_and_fails_open as reader_fixtures  # noqa: E402
from test_progress_keeps_a_working_action_alive import _claimed, _policy  # noqa: E402
from test_the_reader_plan_measures_and_fails_open import (  # noqa: E402
    GIB, TIER, _claim, _mover, _queue)

WAITED = "a" * 64
OTHER = "b" * 64


class _Clock:
    def __init__(self) -> None:
        self.seconds = 0.0

    def monotonic(self) -> float:
        return self.seconds

    def wall(self) -> float:
        return 2_000_000_000.0 + self.seconds

    def advance(self, seconds: float) -> None:
        self.seconds += seconds


@pytest.fixture
def clock(monkeypatch):
    clock = _Clock()
    simulated_time = SimpleNamespace(
        monotonic=clock.monotonic, time=clock.wall, sleep=clock.advance)
    with monkeypatch.context() as patch:
        patch.setattr(pool, "time", simulated_time)
        patch.setattr(pool, "_now", clock.wall)
        patch.setattr(reader_fixtures, "time", simulated_time)
        yield clock


GRACE = 0.4
QUIET_S = 1.5
POLL_S = 0.05


COMMIT = '''
import json, os
path = os.environ["PRISMABUILD_ACTION_PROGRESS_PATH"]
token = os.environ["PRISMABUILD_ACTION_PROGRESS_TOKEN"]
with open("result", "w") as handle:
    handle.write("ok")
report = {"schema": "prismabuild.action_progress.v1", "token": token,
          "phase": "run", "units_completed": 1}
with open(path + ".tmp", "w") as handle:
    json.dump(report, handle)
os.replace(path + ".tmp", path)
'''


class _Process:
    """One scripted payload transition per nonblocking worker poll."""

    def __init__(self, clock: _Clock, script) -> None:
        self.clock = clock
        self.script = iter(script(self))
        self.pid = 2_147_483_647
        self.returncode = None
        self.events = []
        self.credits = []
        self.committed = False

    def launch(self, argv, **kwargs):
        self.argv = argv
        self.environment = kwargs["env"]
        self.checkout = Path(argv[argv.index("--checkout-root") + 1])
        return self

    def poll(self):
        return self.returncode

    def communicate(self, timeout=None):
        if self.returncode is None:
            try:
                next(self.script)
            except StopIteration:
                self.returncode = 0
            else:
                raise subprocess.TimeoutExpired(self.argv, timeout)
        return "", ""

    def quiet(self, seconds: float) -> None:
        self.events.append("quiet")
        self.clock.advance(seconds)

    def commit(self) -> None:
        with pytest.MonkeyPatch.context() as patch:
            patch.chdir(self.checkout)
            for name in (pb.ACTION_PROGRESS_PATH_ENV, pb.ACTION_PROGRESS_TOKEN_ENV):
                patch.setenv(name, self.environment[name])
            source = (self.checkout / "task.py").read_text()
            exec(compile(source, "task.py", "exec"), {"__name__": "__main__"})
        assert (self.checkout / "result").read_text() == "ok"
        self.committed = True
        self.events.append("commit")

    def terminate(self) -> None:
        self.events.append("terminate")
        self.returncode = -15


def _quiet_then_commit(process):
    process.quiet(QUIET_S)
    yield
    process.commit()
    process.clock.advance(POLL_S)
    yield


def _execute(queue: pool.PoolQueue, item, process: _Process, monkeypatch):
    exempt_reader_plan = pool.ProgressWatch.exempt_reader_plan

    def observe_credit(watch, verdict, **kwargs):
        credit = exempt_reader_plan(watch, verdict, **kwargs)
        if credit > 0:
            process.credits.append(watch.reader_plan_exempt_s)
            process.events.append("credit")
        return credit

    def terminate(child, *, grace_s):
        assert child is process
        process.terminate()

    simulated_subprocess = SimpleNamespace(**vars(subprocess))
    simulated_subprocess.Popen = process.launch
    with monkeypatch.context() as patch:
        patch.setattr(pool, "subprocess", simulated_subprocess)
        patch.setattr(pb, "_terminate_process_group", terminate)
        patch.setattr(pool.ProgressWatch, "exempt_reader_plan", observe_credit)
        return queue.execute(item, timeout_s=None, heartbeat_s=POLL_S,
                             timeout_grace_s=0.2)


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


def _plan(queue: pool.PoolQueue, mover: str, clock: _Clock, sleep
          ) -> stage_move._ReaderPlan:
    return stage_move._ReaderPlan(
        queue, TIER, mover, hold_s=0.25, clock=clock.monotonic,
        wall=clock.wall, sleep=sleep)


def test_a_running_copy_does_not_stand_aside_for_a_waited_copy_still_ready(
        tmp_path: Path, clock) -> None:
    """The waited copy is ``ready``: it reads nothing, so nothing yields to it.

    If it cannot be claimed -- the stage GiB it needs is held by the copies
    standing aside for it -- yielding to it is a deadlock.  The waited copy
    still exempts itself, and the claim pass still defers other rows, which
    holds nothing.
    """

    queue = _queue(tmp_path, fill=0)
    _announce(queue, [(WAITED, "ready")])

    def unexpected_wait(seconds):
        pytest.fail("a running copy waited for a ready copy")

    other = _plan(queue, OTHER, clock, unexpected_wait)
    waited = _plan(queue, WAITED, clock, unexpected_wait)
    other.stand_aside(threading.Event())

    assert not other.yields(), (
        "a running copy stands aside for a waited copy that is only ready")
    assert other.report()["yields"] == 0
    assert other.report()["yielded_seconds"] == 0
    assert waited.exempt()


def test_a_running_copy_stands_aside_once_the_waited_copy_is_claimed(
        tmp_path: Path, clock) -> None:
    queue = _queue(tmp_path, fill=0)
    _announce(queue, [(WAITED, "ready")])
    stop = threading.Event()
    waits = []

    def wait(seconds):
        waits.append(seconds)
        clock.advance(seconds)
        stop.set()

    other = _plan(queue, OTHER, clock, wait)
    waited = _plan(queue, WAITED, clock, wait)
    assert not other.yields()
    assert waited.exempt()

    _announce(queue, [(WAITED, "claimed")])
    assert not other.yields()
    clock.advance(other.hold_s)

    assert other.yields()
    assert waited.exempt()
    assert not waited.yields()
    other.stand_aside(stop)
    assert waits == [other.hold_s]
    assert other.report()["yields"] == 1
    assert other.report()["yielded_seconds"] == other.hold_s
    waited.stand_aside(threading.Event())
    assert waits == [other.hold_s]
    assert waited.report()["yields"] == 0


def _standing_mover(tmp_path: Path):
    """A claimed stage mover on ``TIER`` under a 0.4 s progress grace."""

    queue, item = _claimed(tmp_path, mode="quiet", seconds=0,
                           policy=_policy(GRACE, GRACE, GRACE), source=COMMIT)
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

    _mover(queue, WAITED, 0)
    source = queue.item_path(pool.READY, WAITED)
    record = json.loads(source.read_text())
    source.unlink()
    record.update({"claimed_unix": clock.wall() - 5.0,
                   "claimed_by": "waited-fixture", "claimed_host": "dl380g10"})
    queue.item_path(pool.CLAIMED, WAITED).write_text(json.dumps(record))


def test_a_copy_standing_aside_for_a_claimed_waited_copy_is_not_killed(
        tmp_path: Path, clock, monkeypatch) -> None:
    """Quiet for 1.5 s against a 0.4 s grace, all of it standing aside.

    The waited copy is claimed and listed in a fresh plan, and this copy is
    not.  Before the fix the rung ended it at its grace, which fails the
    waited copy's consumer next.
    """

    queue, item = _standing_mover(tmp_path)
    _waited_claimed(queue, clock)
    _announce(queue, [(WAITED, "claimed")])

    process = _Process(clock, _quiet_then_commit)
    outcome = _execute(queue, item, process, monkeypatch)

    assert outcome["status"] == "executed", (
        f"a copy standing aside for a claimed waited copy was ended "
        f"{outcome.get('termination_reason')!r}")
    observed = outcome["progress_observation"]
    assert observed["reader_plan_exempt_s"] == pytest.approx(QUIET_S)
    assert observed["reader_plan_exempt_s"] > GRACE
    assert observed["reader_plan"]["exempt"] is True
    assert observed["reader_plan"]["waited"] == [WAITED]
    assert observed["accepted_count"] == 1
    assert observed["last_accepted"]["phase"] == "run"
    assert observed["last_accepted"]["units_completed"] == 1
    assert process.events == ["quiet", "credit", "commit"]


@pytest.mark.parametrize("case", [
    "no-wait", "ready-only", "stale-plan", "not-claimed", "self-listed"])
def test_without_a_verified_stand_aside_the_grace_applies(
        tmp_path: Path, case: str, clock, monkeypatch) -> None:
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
    _announce(queue, rows, announced=(clock.wall() - pool.OFFER_TIMEOUT_S - 1
                                      if case == "stale-plan" else None))

    process = _Process(clock, _quiet_then_commit)
    outcome = _execute(queue, item, process, monkeypatch)

    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "no_progress"
    assert outcome["progress_observation"]["reader_plan_exempt_s"] == 0
    assert outcome["stall"]["credited_s"]["reader_plan"] == 0
    assert outcome["stall"]["quiet_s"] == pytest.approx(QUIET_S)
    assert outcome["progress_observation"]["accepted_count"] == 0
    assert process.credits == []
    assert process.events == ["quiet", "terminate"]
    assert not process.committed


def test_the_grace_applies_again_once_the_waited_copy_leaves_the_plan(
        tmp_path: Path, clock, monkeypatch) -> None:
    """Credited while the waited copy is listed, then charged again."""

    queue, item = _standing_mover(tmp_path)
    _waited_claimed(queue, clock)
    _announce(queue, [(WAITED, "claimed")])

    def leave_after_credit(process):
        process.quiet(QUIET_S)
        yield
        assert process.credits == [pytest.approx(QUIET_S)]
        assert process.events == ["quiet", "credit"]
        _announce(queue, [])
        process.events.append("remove")
        process.quiet(QUIET_S)
        yield
        pytest.fail("the worker allowed uncredited quiet past its grace")

    process = _Process(clock, leave_after_credit)
    outcome = _execute(queue, item, process, monkeypatch)

    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "no_progress"
    credited = outcome["stall"]["credited_s"].get("reader_plan", 0)
    assert credited == pytest.approx(QUIET_S)
    assert outcome["progress_observation"]["reader_plan_exempt_s"] == credited
    assert outcome["progress_observation"]["reader_plan"]["exempt"] is False
    assert outcome["stall"]["quiet_s"] == pytest.approx(QUIET_S)
    assert process.events == ["quiet", "credit", "remove", "quiet", "terminate"]


def test_a_pass_over_denied_rows_reads_the_tiers_claims_once(
        tmp_path: Path, clock, monkeypatch) -> None:
    """Twenty ready copies behind a cap of two: one pass, one listing.

    Before the fix every denied row listed ``claimed/`` again and read every
    claimed copy's receipt: twenty listings and forty receipt reads.
    """

    queue = _queue(tmp_path, fill=0)
    for ordinal in range(20):
        _mover(queue, f"{ordinal:02d}".ljust(64, "e"), ordinal)
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
