"""Claimed consumers drain an over-committed stage tier in admission order (#1011).

The #907 joint commitment gates newcomers only.  A consumer that is already
claimed is never re-checked, yet its commitment can still outgrow the tier:
its refill horizon lengthens when the landing rate falls, and the tier's room
shrinks under adoption conflicts or foreign files.  Once no claimed
consumer's next range fits and nothing past any horizon is left to evict,
each waits on the others' reading and none can read.  Before #1009 the
reader's constant wait or the ``no_progress`` rung killed one of them, and a
kill throws away a GPU consumer's work.

The fixture is R12's plan and receipts (``tests/fixtures/r12_stage_20260922.json``),
once per consumer, each under its own manifest.  Every consumer:

* was claimed ``CLAIM_GAP_S`` (5 s) after the one before it, and reports
  ``chain-043`` the same 3919 s after its claim as R12 did, so every one
  reads at R12's 20.7 MB/s and has R12's horizon;
* has read ``head`` to ``chain-044``, whose egress rows are in ``done/``;
* has no copy of ``chain-043``, the range it is reading: none landed and
  none published;
* holds its next ranges, landed at the fixture's own copy rates:
  ``chain-042`` to ``chain-034`` (198 GiB) with two consumers, ``chain-042``
  to ``chain-040`` (66 GiB) with eight.

By ``test_r12_and_the_capture_replay_under_the_refill_horizon``'s arithmetic
each consumer can be reading anything that starts before 274.5 GB, which is
``chain-042`` to ``chain-034``; ``chain-033`` completes the horizon and
``chain-032`` is the advance.  So every range a consumer holds is inside its
horizon, and nothing on the tier is past one.

The tier then shrinks to what the consumers hold plus 21 GiB.  Every
``chain-043`` is a 22 GiB range, so none fits, and the joint commitment --
each consumer's 242 GiB horizon -- is far past the tier.

The bound.  A range the window publishes lands within one heartbeat, one
cycle and the slowest copy of the consumer's plan: ``HEARTBEAT_S +
CYCLE_INTERVAL_S + landing_s``, the latency ``residency_plan.refill_horizon``
prices a horizon with.  Here that is 30 + 60 + 169.6 s with two consumers
(the slowest receipt they hold is ``chain-037``'s 138.1 MB/s, and the largest
range still to read is 23.42 GB), and 30 + 60 + 150.2 s with eight
(``chain-040``, 155.9 MB/s).  The oldest consumer's ``chain-043`` must be
published within that many seconds of cycles: ``ceil(259.6 / 60)`` and
``ceil(240.2 / 60)``, 5 cycles either way.  On main the state is a fixpoint
of ``cycle()``: nothing publishes, nothing is evicted, and no term depends
on the time.  So the bound is a ceiling, not a race.

Everything runs on ``tmp_path`` queues and stage roots (#628).
"""
from __future__ import annotations

import json
import math
from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import pool, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402
from test_a_consumer_stages_only_to_its_refill_horizon import (  # noqa: E402
    _cycle, _fixture_queue, _land)
from test_a_consumer_stages_only_to_its_refill_horizon import (  # noqa: E402
    _plan as _small_plan, _publish_consumer)
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    PHASE_GIB, TIER, _hexkey, _tier_record)
from test_r12_and_the_capture_replay_under_the_refill_horizon import (  # noqa: E402
    DATA, SAMPLE_UNIX, _claim, _consumer, _plan)

CLAIM_GAP_S = 5.0
READING = "chain-043"
PASSED = ("head", "forward-044", "chain-044")
PHASES = {str(phase["name"]): phase for phase in DATA["r12"]["phases"]}
RATES = {entry["phase"]: entry["bytes_staged"] / entry["seconds"]
         for entry in DATA["r12"]["landed"]}
TWO_HOLD = tuple(f"chain-{n:03d}" for n in range(42, 33, -1))
EIGHT_HOLD = tuple(f"chain-{n:03d}" for n in range(42, 39, -1))


def _key(n: int) -> str:
    return _hexkey(f"order{n}")


def _manifest(n: int) -> str:
    # Distinct in its first eight characters: ``_land`` files a range under
    # ``manifest[:8]``, and two consumers must not share a staged path here.
    return _hexkey(f"{n}m-order")


def _mover(n: int, phase: str) -> str:
    return _hexkey(f"order{n}mover{phase}")


def _blocked(queue: pool.PoolQueue, stage: Path, n: int, *, shift: float,
             holding: tuple[str, ...]) -> dict[str, object]:
    """Consumer ``n``: claimed, reading ``chain-043``, with ``holding`` landed."""

    r12 = DATA["r12"]
    key, manifest = _key(n), _manifest(n)
    plan = _plan(queue, key, label=f"order{n}", manifest=manifest,
                 phases=r12["phases"], fill=r12["sealed_fill_mb_s"],
                 stage_root=str(stage))
    _consumer(queue, key, plan, manifest=manifest,
              mem_gb=r12["resources"]["mem_gb"])
    for name in holding:
        start, end = int(PHASES[name]["start_bytes"]), int(PHASES[name]["end_bytes"])
        _land(queue, stage, consumer=key, manifest=manifest,
              mover=_mover(n, name), name=name, start=start, end=end,
              seconds=(end - start) / RATES[name])
    # The ranges it has read are given back: their egress rows ran.
    by_name = {str(phase["name"]): phase for phase in plan["phases"]}  # type: ignore[union-attr]
    for name in PASSED:
        egress = dict(by_name[name]["egress_row"])
        path = queue.item_path(pool.DONE, str(egress["action_key"]))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({**egress, "status": "done"}))
    offset = n * CLAIM_GAP_S
    _claim(queue, key, phase=READING,
           claimed_unix=r12["claimed_unix"] + shift + offset,
           reported_unix=r12["accepted"]["reported_unix"] + shift + offset,
           gpu_budget=r12["gpu_memory_budget_bytes"])
    return plan


def _fixture(tmp_path: Path, count: int, holding: tuple[str, ...]):
    shift = time.time() - SAMPLE_UNIX
    queue, stage = _fixture_queue(tmp_path, DATA["tier"]["capacity_gib"])
    plans = [_blocked(queue, stage, n, shift=shift, holding=holding)
             for n in range(count)]
    held = count * sum(int(PHASES[name]["stage_gib"]) for name in holding)
    shrunk = held + int(PHASES[READING]["stage_gib"]) - 1
    return queue, stage, plans, shrunk


def _bound(queue: pool.PoolQueue, stage: Path, plan, *, gib: int
           ) -> tuple[int, dict[str, object]]:
    """The module docstring's bound, in cycles, and the horizon it came from."""

    key = str(plan["consumer_action_key"])
    consumer = next(entry for entry in tier_loop.live_consumers(queue)
                    if entry["action_key"] == key)
    horizon = tier_loop._stage_horizon(queue, consumer, plan,
                                       _tier_record(stage, gib=gib))
    assert horizon is not None
    latency = (pool.HEARTBEAT_S + tier_loop.CYCLE_INTERVAL_S
               + float(horizon["landing_s"]))              # type: ignore[arg-type]
    return math.ceil(latency / tier_loop.CYCLE_INTERVAL_S), horizon


def _published(queue: pool.PoolQueue, mover: str) -> bool:
    return (queue.item_path(pool.READY, mover).exists()
            or queue.item_path(pool.CLAIMED, mover).exists()
            or bool(queue.tier_ledger(TIER).holder_tokens(mover)))


def _events(capsys: pytest.CaptureFixture[str]) -> list[dict[str, object]]:
    return [json.loads(line) for line in capsys.readouterr().out.splitlines()
            if line.startswith("{")]


_TOLD = ("window-gated", "window-stalled", "window-unknown", "window-unfunded",
         "tier-over-committed", "beyond-horizon-evicted",
         "beyond-horizon-eviction-futile", "beyond-horizon-eviction-declined",
         "claim-order-evicted", "claim-order-eviction-declined",
         "mover-published", "advance-deferred-unknown-evidence")


def _diagnosis(queue: pool.PoolQueue, events, count: int) -> str:
    """What the cycles said, trimmed to the events that decide publication."""

    told = []
    for event in events:
        if event.get("event") not in _TOLD:
            continue
        told.append({field: value for field, value in event.items()
                     if field not in ("terms", "commitment", "unix", "host")})
    record = queue.tier_commitment(TIER) or {}
    ledger = queue.tier_ledger(TIER)
    return json.dumps({
        "events": told[-40:],
        "commitment": {field: record.get(field) for field in (
            "capacity_gib", "committed_gib", "over_committed_gib",
            "held_gib", "evictable_gib", "claim_order")},
        "free_gib": ledger.available().get(storage_tiers.STAGE_CAPACITY_KIND),
        "reading_published": {n: _published(queue, _mover(n, READING))
                              for n in range(count)},
    }, indent=1, default=str)


@pytest.mark.parametrize("count,holding", [(2, TWO_HOLD), (8, EIGHT_HOLD)],
                         ids=["two", "eight"])
def test_the_oldest_claimed_consumers_range_publishes_within_the_landing_bound(
        tmp_path: Path, capsys: pytest.CaptureFixture[str], count: int,
        holding: tuple[str, ...]) -> None:
    """The acceptance fixture: ``count`` claimed R12s on a shrunk stage.

    On main no consumer's ``chain-043`` is published within the bound, or at
    all: every consumer waits on the others' reading, and only a kill would
    end the wait.  After the fix the oldest consumer's range is published
    within the bound, by evicting what the youngest holds farthest ahead.
    """

    queue, stage, plans, shrunk = _fixture(tmp_path, count, holding)
    bound, horizon = _bound(queue, stage, plans[0], gib=shrunk)
    # Every held range is inside the horizon, so nothing is past one.
    by_start = {int(phase["start_bytes"]): str(phase["name"])
                for phase in plans[0]["phases"]}          # type: ignore[union-attr]
    assert by_start[int(horizon["horizon_end_bytes"])] == "chain-032"  # type: ignore[arg-type]
    assert not {leg["phase"] for leg in horizon["beyond"]} & set(holding)  # type: ignore[union-attr]
    assert bound == 5

    published_at = None
    for cycle in range(1, bound + 1):
        _cycle(queue, stage, gib=shrunk)
        if _published(queue, _mover(0, READING)):
            published_at = cycle
            break
    events = _events(capsys)

    assert published_at is not None, _diagnosis(queue, events, count)


# ------------------------------------------------ N consumers of one range


def test_a_range_two_claimed_consumers_read_is_charged_to_each_of_them(
        tmp_path: Path) -> None:
    """#1011 point 3: how joint commitment charges a range two consumers read.

    Two claimed consumers read the same manifest, so their ``phase-0`` legs
    name the same bytes (``tier_loop._descriptor``) and, by
    ``stage_move.stage_relative``, the same staged name: the second copy
    adopts the first's file.  The ledger is not told.  Each consumer's mover
    is its own action key (the key hashes ``--consumer-action-key``), and the
    second mover's claim takes the range's full tokens from free
    (``PoolQueue._begin_tier_acquire``).  The census then counts the range
    once per consumer.  This test records that per-consumer charge; it is
    evidence for the gap the PR reports, not a behaviour to keep.
    """

    queue, stage = _fixture_queue(tmp_path, 20)
    manifest = _hexkey("7shared")
    first, second = _hexkey("sharedfirst"), _hexkey("sharedsecond")
    plans = {}
    for key, label in ((first, "sharedfirst"), (second, "sharedsecond")):
        plans[key] = _small_plan(queue, key, label=label, manifest=manifest,
                                 phases=4)
        _publish_consumer(queue, key, plans[key], manifest=manifest)
    lead = plans[first]["phases"][0]                       # type: ignore[index]
    first_mover = str(lead["mover_row"]["action_key"])
    _land(queue, stage, consumer=first, manifest=manifest, mover=first_mover,
          name="phase-0", start=int(lead["start_bytes"]),
          end=int(lead["end_bytes"]))
    now = time.time()
    for key in (first, second):
        _claim(queue, key, phase="phase-0", claimed_unix=now - 1000.0,
               reported_unix=now - 10.0)
    row = dict(plans[second]["phases"][0]["mover_row"])    # type: ignore[index]
    second_mover = str(row["action_key"])
    queue.publish(**row)
    received = queue.staged_range_of(first_mover)
    assert received is not None
    residency = row["residency"]
    assert tier_loop._descriptor(
        str(received["manifest_sha256"]), TIER,
        int(received["range_start_bytes"]), int(received["range_end_bytes"])
    ) == tier_loop._descriptor(
        str(residency["manifest_sha256"]), TIER,
        int(residency["range_start_bytes"]), int(residency["range_end_bytes"]))
    assert first_mover != second_mover
    ledger = queue.tier_ledger(TIER)
    before = int(ledger.held().get("stage_gib", 0))

    handles: dict[str, str] = {}
    funded: dict[str, dict[str, object]] = {}
    shortage = queue._begin_tier_acquire(
        second_mover, {TIER: {"stage_gib": PHASE_GIB}}, handles, funded)

    assert shortage is None
    # The claim wins its rename and files the tokens under the mover.
    assert queue._commit_tier_acquire(second_mover, handles) == PHASE_GIB
    # One range of PHASE_GIB bytes, charged twice.
    assert int(ledger.held().get("stage_gib", 0)) == before + PHASE_GIB
    assert before == PHASE_GIB
    tiers = {TIER: _tier_record(stage, gib=20)}
    unknown: list[dict[str, object]] = []
    consumers = tier_loop._planned_consumers(queue, tiers, unknown=unknown)
    census = tier_loop._commitment_census(queue, tiers, consumers=consumers,
                                          unknown=unknown, remember=False)[TIER]
    charged = {entry["consumer"]: entry["gib"] for entry in census["holders"]
               if entry["key"] in (first_mover, second_mover)}
    assert charged == {first: PHASE_GIB, second: PHASE_GIB}, census["holders"]
