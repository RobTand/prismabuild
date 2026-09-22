"""A ready consumer's claim-time tier demand is pressure (#901).

On 2026-09-22 GLM Stage A R12 (``683cb3caa5ea``) waited about 25 minutes in
``ready/``.  Its claim needed ``stage_gib@prismabuild-stage:dl380g10: 48`` and
was denied ``tier_reservation_unavailable`` (available 35).  The tier was held
by 22 done stage movers of the withdrawn consumer R11, 484 GiB of orphans, and
the tier loop evicted none of them: ``window_pressure`` counted consumers'
input windows and a newcomer's unpublished lead, never a ready consumer's own
claim.  An operator cleared it with one manual egress per mover.

These cases pin the three rules the fix keeps:

* a ready consumer blocked only by tier tokens that orphans hold produces
  relief, and the sweep evicts oldest-first just enough for the claim;
* a demand that cannot fit even after every orphan returns asks for nothing
  (#632), and neither does a demand larger than the tier;
* a withdrawn ready key asks for nothing (#708).

Everything runs on a ``tmp_path`` queue and stage root; nothing touches a live
queue or a real stage mountpoint (#628).
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import pool, residency_plan  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    MANIFEST, PHASE_GIB, STAGE_KIND, TIER, _hexkey, _plan, _row,
    _stage_range, _tier_record, assert_ledger_matches_the_stage)

#: Ten GiB of stage: the successor's landed lead (2) and three orphans of the
#: withdrawn consumer (3 x 2) leave 2 free.
CAPACITY_GIB = 10
WITHDRAWN = "1" * 64    # R11's shape: withdrawn, its done movers still held
SUCCESSOR = "2" * 64    # R12's shape: ready, its lead landed, claim short
ORPHANS = 3


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    q.mint_tier_capacity(TIER, {"stage_gib": CAPACITY_GIB})
    return q


@pytest.fixture()
def stage(tmp_path: Path, queue: pool.PoolQueue) -> Path:
    path = tmp_path / "stage"
    path.mkdir()
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=path)
    return path


def _orphan(ordinal: int) -> str:
    return _hexkey(f"withdrawnmover{ordinal}")


def _setup(queue: pool.PoolQueue, stage: Path, *,
           claim_gib: int) -> dict[int, list[Path]]:
    """The #901 state: a withdrawn consumer's landed movers, a ready successor.

    The withdrawn consumer's ranges are of another manifest, so the
    successor's window cannot adopt them: after #890 no reader could use
    R11's bare-named copies either.  The successor's plan is one phase whose
    lead has landed, so its input window publishes nothing and has no
    pressure of its own; only its claim-time demand is short.
    """

    queue.publish(**_row(queue, WITHDRAWN, {"cpu": 1, "mem_gb": 1}))
    queue.withdraw(WITHDRAWN, reason="#901 fixture", by="test")
    orphans = {
        ordinal: _stage_range(queue, mover=_orphan(ordinal),
                              consumer=WITHDRAWN, stage=stage,
                              ordinal=ordinal, manifest="e" * 64)
        for ordinal in range(ORPHANS)}
    plan = _plan(queue, SUCCESSOR, phases=1, label="successor")
    _stage_range(queue, mover=_hexkey("successormover0"),
                 consumer=SUCCESSOR, stage=stage, ordinal=0)
    residency_plan.freeze(queue, plan)
    queue.publish(**_row(queue, SUCCESSOR,
                         {"cpu": 1, "mem_gb": 1, STAGE_KIND: claim_gib}),
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                             "tier_id": TIER, "manifest_sha256": MANIFEST,
                             "manifest_bytes": 1 << 30,
                             "leads": residency_plan.leads_for(plan)})
    assert queue.tier_ledger(TIER).available()["stage_gib"] == (
        CAPACITY_GIB - PHASE_GIB * (ORPHANS + 1))
    return orphans


def _claim_tier_shortage(queue: pool.PoolQueue,
                         claim_gib: int) -> dict[str, object] | None:
    """The claim's own tier gate, run and rolled back: ``None`` means it fits."""

    handles: dict[str, str] = {}
    funded: dict[str, dict[str, object]] = {}
    shortage = queue._begin_tier_acquire(
        SUCCESSOR, {TIER: {"stage_gib": claim_gib}}, handles, funded)
    queue._abandon_tier_acquire(handles)
    return shortage


def _held(queue: pool.PoolQueue, ordinal: int) -> bool:
    return bool(queue.tier_ledger(TIER).holder_tokens(_orphan(ordinal)))


def test_a_ready_claim_short_on_tier_tokens_evicts_oldest_orphans_just_enough(
        queue: pool.PoolQueue, stage: Path) -> None:
    """Free 2, claim 4: the oldest orphan goes, the other two stay."""

    orphans = _setup(queue, stage, claim_gib=4)
    tiers = {TIER: _tier_record(stage, gib=CAPACITY_GIB)}
    # The incident's denial, reproduced on the claim's own gate.
    shortage = _claim_tier_shortage(queue, 4)
    assert shortage is not None
    assert shortage["reason"] == "tier_reservation_unavailable"

    pressure = tier_loop.window_pressure(queue, tiers=tiers)
    # Free (2) plus the claim's shortfall (2).
    assert pressure.get(TIER) == 4, pressure
    swept = tier_loop.sweep_orphans(queue, tiers, pressure=pressure)

    evicted = [record["action_key"] for record in swept
               if record.get("reason") == "orphan-sweep"]
    assert evicted == [_orphan(0)], swept
    assert not _held(queue, 0)
    assert not any(path.exists() for path in orphans[0])
    for ordinal in (1, 2):
        assert _held(queue, ordinal)
        assert all(path.exists() for path in orphans[ordinal])
    # The successor's own lead is never relief.
    assert queue.tier_ledger(TIER).holder_tokens(
        _hexkey("successormover0")) == {"stage_gib": PHASE_GIB}
    assert _claim_tier_shortage(queue, 4) is None
    assert_ledger_matches_the_stage(queue)


@pytest.mark.parametrize("claim_gib", [9, CAPACITY_GIB + 1])
def test_a_claim_no_eviction_can_seat_asks_for_nothing(
        queue: pool.PoolQueue, stage: Path, claim_gib: int) -> None:
    """#632: 9 needs 7 more than free, the orphans hold 6; 11 exceeds the tier.

    The orphans are evictable -- a sweep with no pressure named takes every
    one -- so the only thing withheld is futile pressure.
    """

    _setup(queue, stage, claim_gib=claim_gib)
    tiers = {TIER: _tier_record(stage, gib=CAPACITY_GIB)}

    pressure = tier_loop.window_pressure(queue, tiers=tiers)
    assert TIER not in pressure, pressure
    tier_loop.sweep_orphans(queue, tiers, pressure=pressure)
    assert all(_held(queue, ordinal) for ordinal in range(ORPHANS))

    stage_release.sweep(queue, stage_roots={TIER: str(stage)})
    assert not any(_held(queue, ordinal) for ordinal in range(ORPHANS))
    assert_ledger_matches_the_stage(queue)


def test_a_withdrawn_ready_claim_asks_for_nothing(
        queue: pool.PoolQueue, stage: Path) -> None:
    """#708: a key with a live withdrawal marker will not claim."""

    _setup(queue, stage, claim_gib=4)
    tiers = {TIER: _tier_record(stage, gib=CAPACITY_GIB)}

    pressure = tier_loop.window_pressure(
        queue, tiers=tiers, withdrawn=frozenset({SUCCESSOR}))
    assert TIER not in pressure, pressure
    tier_loop.sweep_orphans(queue, tiers, pressure=pressure)
    assert all(_held(queue, ordinal) for ordinal in range(ORPHANS))
    assert_ledger_matches_the_stage(queue)
