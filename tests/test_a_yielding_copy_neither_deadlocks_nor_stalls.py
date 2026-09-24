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
import sys
import threading
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import pool, storage_tiers  # noqa: E402
import stage_move  # noqa: E402
from test_progress_keeps_a_working_action_alive import _claimed, _policy  # noqa: E402
from test_the_reader_plan_measures_and_fails_open import (  # noqa: E402
    GIB, TIER, _claim, _mover, _queue)

WAITED = "a" * 64
OTHER = "b" * 64


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


def _plan(queue: pool.PoolQueue, mover: str) -> stage_move._ReaderPlan:
    return stage_move._ReaderPlan(queue, TIER, mover, hold_s=0.001)


def test_a_running_copy_does_not_stand_aside_for_a_waited_copy_still_ready(
        tmp_path: Path) -> None:
    """The waited copy is ``ready``: it reads nothing, so nothing yields to it.

    If it cannot be claimed -- the stage GiB it needs is held by the copies
    standing aside for it -- yielding to it is a deadlock.  The waited copy
    still exempts itself, and the claim pass still defers other rows, which
    holds nothing.
    """

    queue = _queue(tmp_path, fill=0)
    _announce(queue, [(WAITED, "ready")])
    other, waited = _plan(queue, OTHER), _plan(queue, WAITED)
    stop = threading.Event()
    stop_timer = threading.Timer(0.5, stop.set)
    stop_timer.start()
    try:
        started = time.monotonic()
        other.stand_aside(stop)
        stood = time.monotonic() - started
    finally:
        stop_timer.cancel()

    assert not other.yields(), (
        "a running copy stands aside for a waited copy that is only ready")
    assert stood < 0.4 and other.report()["yields"] == 0
    assert waited.exempt()


def test_a_running_copy_stands_aside_once_the_waited_copy_is_claimed(
        tmp_path: Path) -> None:
    queue = _queue(tmp_path, fill=0)
    _announce(queue, [(WAITED, "ready")])
    other = _plan(queue, OTHER)
    assert not other.yields()

    time.sleep(0.01)
    _announce(queue, [(WAITED, "claimed")])
    time.sleep(0.01)

    assert other.yields()
    assert not _plan(queue, WAITED).yields()


# ------------------------------ 2: the worker credits a verified stand-aside


#: Quiet for ``SECONDS`` past a 0.4 s grace, then one committed unit.
QUIET = '''
import json, os, time
path = os.environ["PRISMABUILD_ACTION_PROGRESS_PATH"]
token = os.environ["PRISMABUILD_ACTION_PROGRESS_TOKEN"]
time.sleep(SECONDS)
report = {"schema": "prismabuild.action_progress.v1", "token": token,
          "phase": "run", "units_completed": 1, "reported_unix": time.time()}
with open(path + ".tmp", "w") as handle:
    json.dump(report, handle)
os.replace(path + ".tmp", path)
open("result", "w").write("ok")
'''


def _standing_mover(tmp_path: Path, *, seconds: float):
    """A claimed stage mover on ``TIER`` under a 0.4 s progress grace."""

    queue, item = _claimed(tmp_path, mode="quiet", seconds=seconds,
                           policy=_policy(0.4, 0.4, 0.4),
                           source=QUIET.replace("SECONDS", repr(seconds)))
    queue.mint_tier_capacity(TIER, {"stage_gib": 16})
    item["residency"] = {"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                         "manifest_sha256": "9" * 64, "manifest_bytes": 4 * GIB,
                         "range_start_bytes": 2 * GIB,
                         "range_end_bytes": 4 * GIB}
    queue.item_path(pool.CLAIMED, str(item["action_key"])).write_text(
        json.dumps(item))
    return queue, item


def _waited_claimed(queue: pool.PoolQueue) -> None:
    """``WAITED`` claimed on ``TIER``, as the worker will read it."""

    _mover(queue, WAITED, 0)
    source = queue.item_path(pool.READY, WAITED)
    record = json.loads(source.read_text())
    source.unlink()
    record.update({"claimed_unix": time.time() - 5.0,
                   "claimed_by": "waited-fixture", "claimed_host": "dl380g10"})
    queue.item_path(pool.CLAIMED, WAITED).write_text(json.dumps(record))


def _execute(queue: pool.PoolQueue, item):
    return queue.execute(item, timeout_s=None, heartbeat_s=0.05,
                         timeout_grace_s=0.2)


def test_a_copy_standing_aside_for_a_claimed_waited_copy_is_not_killed(
        tmp_path: Path) -> None:
    """Quiet for 1.5 s against a 0.4 s grace, all of it standing aside.

    The waited copy is claimed and listed in a fresh plan, and this copy is
    not.  Before the fix the rung ended it at its grace, which fails the
    waited copy's consumer next.
    """

    queue, item = _standing_mover(tmp_path, seconds=1.5)
    _waited_claimed(queue)
    _announce(queue, [(WAITED, "claimed")])

    outcome = _execute(queue, item)

    assert outcome["status"] == "executed", (
        f"a copy standing aside for a claimed waited copy was ended "
        f"{outcome.get('termination_reason')!r}")
    observed = outcome["progress_observation"]
    assert observed["reader_plan_exempt_s"] > 0.4
    assert observed["reader_plan"]["exempt"] is True
    assert observed["reader_plan"]["waited"] == [WAITED]


@pytest.mark.parametrize("case", [
    "no-wait", "ready-only", "stale-plan", "not-claimed", "self-listed"])
def test_without_a_verified_stand_aside_the_grace_applies(
        tmp_path: Path, case: str) -> None:
    """Each leg of the evidence, missing: the rung ends the copy as before.

    ``not-claimed`` is a plan that says ``claimed`` while the queue holds no
    such claim: the worker reads the claim itself and does not take the
    record's word for it.  ``self-listed`` is this copy as the waited one,
    which reads rather than stands aside.
    """

    queue, item = _standing_mover(tmp_path, seconds=1.5)
    me = str(item["action_key"])
    if case != "not-claimed":
        _waited_claimed(queue)
    rows = {"no-wait": [], "ready-only": [(WAITED, "ready")],
            "stale-plan": [(WAITED, "claimed")],
            "not-claimed": [(WAITED, "claimed")],
            "self-listed": [(me, "claimed")]}[case]
    _announce(queue, rows, announced=(time.time() - pool.OFFER_TIMEOUT_S - 1
                                      if case == "stale-plan" else None))

    outcome = _execute(queue, item)

    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "no_progress"
    assert outcome["progress_observation"].get("reader_plan_exempt_s", 0) == 0
    assert outcome["stall"]["credited_s"].get("reader_plan", 0) == 0


def test_the_grace_applies_again_once_the_waited_copy_leaves_the_plan(
        tmp_path: Path) -> None:
    """Credited while the waited copy is listed, then charged again."""

    queue, item = _standing_mover(tmp_path, seconds=3.0)
    _waited_claimed(queue)
    _announce(queue, [(WAITED, "claimed")])
    ended = threading.Timer(0.6, lambda: _announce(queue, []))
    ended.start()
    try:
        outcome = _execute(queue, item)
    finally:
        ended.cancel()

    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "no_progress"
    credited = outcome["stall"]["credited_s"].get("reader_plan", 0)
    assert credited > 0, "the stand-aside while the waited copy was listed was charged"
    assert outcome["elapsed_s"] < 2.5


# ------------------------------------------- 3: one reading set per claim pass


def test_a_pass_over_denied_rows_reads_the_tiers_claims_once(
        tmp_path: Path, monkeypatch) -> None:
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
