"""An unmeasured reader is priced from what it declares, never the tier's offer (#909).

#907 charges a newcomer's read footprint when it is admitted.  Until the
newcomer reports, nothing has measured its reading, and three numbers stood in
for the missing measurements:

* its consumption rate was the tier's announced fill supply, so the verdict
  moved as the loop probed the pool: on main at 81d95cba an R12-shaped R13
  beside R12 was refused at 413 MB/s (footprint 242 GiB) and admitted at 144
  (176) -- PB ff200b3dacf1;
* its landing rate fell back to the same supply when its rows were sealed with
  no fill;
* its read-ahead was its memory reservation, ``mem_gb`` plus the GPU budget
  admission gives it, which prices R12 at 180 GiB of read-ahead and a 242 GiB
  footprint where a reader that holds one or two 22 GiB layers ahead needs 88.

A consumer now declares its reading on its plan (``reader``): how many bytes
it holds ahead of the phase it reads, and how fast it reads.  Beside a
measured rate the larger of the two prices it: each is a lower bound on how
fast the consumer reads, and the footprint is a promise that must not come out
short.  Where there is neither, the price is the window's #633 run-ahead
bound, which does not read the tier's supply.

Everything runs on ``tmp_path`` queues and stage roots (#628).
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    GIB, TIER, _hexkey)
from test_a_consumer_stages_only_to_its_refill_horizon import (  # noqa: E402
    _fixture_queue)
import test_r12_and_the_capture_replay_under_the_refill_horizon as replay  # noqa: E402

R13 = _hexkey("r13consumer")
R13_MANIFEST = _hexkey("r13manifest")
#: R12's reader as PQ's stage-fed reader would declare it: one 22 GiB layer
#: held ahead, read at the 20.7 MB/s R12 measured, in whole MB/s rounded up.
DECLARED = {"prefetch_depth_bytes": 22 * GIB, "read_mb_s": 21}
#: The same reader declaring a rate just under R12's measured 20.72 MB/s.
SLOWER = {**DECLARED, "read_mb_s": 20}
#: By ``residency_plan.read_footprint`` over R12's plan with the declaration
#: above and R12's sealed 144 MB/s landing: the phase being read, one layer of
#: read-ahead and the refill legs, 88 GiB.  With the memory reservation as
#: read-ahead (100 GiB host plus the 80 GiB GPU budget) it is 242.
DECLARED_FOOTPRINT = 88
MEMORY_FOOTPRINT = 242


def _with_reader(plan: dict[str, object], reader: dict[str, int] | None
                 ) -> dict[str, object]:
    if reader is None:
        return plan
    return residency_plan.validate_plan({**plan, "reader": dict(reader)})


def _tiers(stage: Path, supply: int) -> dict[str, dict[str, object]]:
    return {TIER: {**replay._tier_record(stage, gib=replay.CAPACITY),
                   "tokens": {storage_tiers.FILL_KIND: supply}}}


def _r12(tmp_path: Path, monkeypatch, *, reader: dict[str, int] | None = None
         ) -> tuple[pool.PoolQueue, Path]:
    """R12 at 22:30:26Z, as the #903 replay builds it, optionally declared."""

    original = replay._plan

    def plan(queue, consumer, **kwargs):
        built = original(queue, consumer, **kwargs)
        return _with_reader(built, reader) if consumer == replay.R12 else built

    monkeypatch.setattr(replay, "_plan", plan)
    queue, stage = _fixture_queue(tmp_path, replay.CAPACITY)
    replay._r12(queue, stage, time.time() - replay.SAMPLE_UNIX)
    monkeypatch.setattr(replay, "_plan", original)
    return queue, stage


def _r13(queue: pool.PoolQueue, *, reader: dict[str, int] | None) -> None:
    """An R13 shaped like R12, queued and not yet claimed."""

    r12 = replay.DATA["r12"]
    plan = _with_reader(replay._plan(
        queue, R13, label="r13", manifest=R13_MANIFEST, phases=r12["phases"],
        fill=r12["sealed_fill_mb_s"]), reader)
    replay._consumer(queue, R13, plan, manifest=R13_MANIFEST,
                     mem_gb=r12["resources"]["mem_gb"])


def _census(queue: pool.PoolQueue, tiers) -> dict[str, object]:
    unknown: list[dict[str, object]] = []
    consumers = tier_loop._planned_consumers(queue, tiers, unknown=unknown)
    census = tier_loop._commitment_census(queue, tiers, consumers=consumers,
                                          unknown=unknown)
    return census[TIER]


def _verdict(tmp_path: Path, monkeypatch, supply: int, *,
             reader: dict[str, int] | None) -> dict[str, object]:
    """R13's commitment terms and verdict beside R12 on a tier announcing ``supply``."""

    queue, stage = _r12(tmp_path / str(supply), monkeypatch)
    _r13(queue, reader=reader)
    tiers = _tiers(stage, supply)
    census = _census(queue, tiers)
    decision = tier_loop._commitment_decision(census, R13, admitted=set())
    assert decision is not None, census
    window = census["windows"][R13]                      # type: ignore[index]
    return {"admit": decision["admit"], "reason": decision.get("reason"),
            "footprint_gib": window["footprint_gib"],
            "consumption_basis": window["consumption_basis"],
            "readahead_basis": window.get("readahead_basis")}


# ------------------------------------------------- the verdict does not move


@pytest.mark.parametrize("reader", [None, DECLARED], ids=["undeclared", "declared"])
def test_a_newcomers_verdict_does_not_move_with_the_announced_supply(
        tmp_path: Path, monkeypatch, reader) -> None:
    """R13 beside R12 is decided the same at 413 MB/s as at 144.

    Before the fix its consumption was the announced supply: a 242 GiB
    footprint at 413, refused (``joint-commitment-stall``), and 176 at 144,
    admitted.  The loop probes its supply, so the same queue flipped between
    the two.  Undeclared, R13 is now priced at the run-ahead bound and
    refused at both; declared, at its declaration and admitted at both.
    """

    fast = _verdict(tmp_path, monkeypatch, 413, reader=reader)
    slow = _verdict(tmp_path, monkeypatch, 144, reader=reader)
    assert fast == slow
    assert fast["consumption_basis"] == ("declared" if reader else "undeclared")
    assert fast["admit"] is bool(reader)


def test_a_declared_newcomer_is_priced_at_its_declaration(
        tmp_path: Path, monkeypatch) -> None:
    """Declared, R13's footprint is R12's declared footprint, and it fits."""

    verdict = _verdict(tmp_path, monkeypatch, 413, reader=DECLARED)
    assert verdict["footprint_gib"] == DECLARED_FOOTPRINT
    assert verdict["consumption_basis"] == "declared"
    assert verdict["readahead_basis"] == "declared"
    assert verdict["admit"] is True


# ------------------------------------------------------ read-ahead is declared


@pytest.mark.parametrize("reader,basis", [(SLOWER, "measured"), (DECLARED, "declared")],
                         ids=["declared-under-measured", "declared-over-measured"])
def test_r12s_read_ahead_is_its_declared_depth(tmp_path: Path, monkeypatch,
                                               reader, basis) -> None:
    """R12 declared: priced at one layer of read-ahead, not 180 GiB of memory.

    Its rate is the larger of its measured 20.72 MB/s and its declaration:
    20 leaves the measurement standing, 21 raises it.
    """

    queue, stage = _r12(tmp_path, monkeypatch, reader=reader)
    window = _census(queue, _tiers(stage, 413))["windows"][replay.R12]  # type: ignore[index]
    assert window["footprint_gib"] == DECLARED_FOOTPRINT
    assert window["readahead_basis"] == "declared"
    assert window["consumption_basis"] == basis


def test_an_undeclared_reader_keeps_its_memory_reservation(
        tmp_path: Path, monkeypatch) -> None:
    """The live R12 declares nothing, and its footprint does not move on publish."""

    queue, stage = _r12(tmp_path, monkeypatch)
    window = _census(queue, _tiers(stage, 413))["windows"][replay.R12]  # type: ignore[index]
    assert window["footprint_gib"] == MEMORY_FOOTPRINT
    assert window["readahead_basis"] == "memory-reservation"


# ------------------------------------------------------------ the declaration


def test_a_plan_carries_its_readers_declaration(tmp_path: Path) -> None:
    queue, _stage = _fixture_queue(tmp_path, replay.CAPACITY)
    plan = replay._plan(queue, R13, label="r13", manifest=R13_MANIFEST,
                        phases=replay.DATA["capture"]["phases"], fill=144)
    declared = _with_reader(plan, DECLARED)
    assert declared["reader"] == DECLARED
    assert "reader" not in plan      # an undeclared plan is byte-identical


@pytest.mark.parametrize("reader", [
    {}, {"prefetch_depth_bytes": -1}, {"read_mb_s": 0},
    {"read_mb_s": 1.5}, {"prefetch_depth_bytes": True},
    {"read_mb_s": 21, "stray": 1}, "21",
])
def test_a_malformed_declaration_is_refused(tmp_path: Path, reader) -> None:
    queue, _stage = _fixture_queue(tmp_path, replay.CAPACITY)
    plan = replay._plan(queue, R13, label="r13", manifest=R13_MANIFEST,
                        phases=replay.DATA["capture"]["phases"], fill=144)
    with pytest.raises(residency_plan.ResidencyPlanError):
        residency_plan.validate_plan({**plan, "reader": reader})
