"""The ram window publishes only what the tmpfs can hold, in the plan's read
order, behind the consumer's accepted progress.

Promotion scheduling is the stage window's own semantics, pointed at the ram
ledger: admission needs free ``ram_gib`` -- Rob's instinct, "empty space in
tmpfs", made exact through the ledger -- and the #633 run-ahead bound still
applies, because a consumer that accepts nothing has given no evidence it
consumes at all.  One more bound is the ram tier's own, and it is a
dependency rather than a size: a promotion is published only for a phase
whose stage range has landed, because its source is the stage and nothing
else -- pool -> ram directly is the one road this refuses (#640).
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402

import residency_publication  # noqa: E402
import tier_loop  # noqa: E402

CONSUMER = "c" * 64
MANIFEST = "9" * 64
STAGE_TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:dl380g10"
RAM_KIND = f"ram_gib@{RAM_TIER}"
STAGE_KIND = f"stage_gib@{STAGE_TIER}"
GIB = storage_tiers.GIB
PHASE_GIB = [2, 2, 2]
EPOCH = "1695052800-1a2b3c4d5e6f7a8b"


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _row(key: str, resources: dict[str, int], queue: pool.PoolQueue) -> dict:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


def _plan(queue: pool.PoolQueue) -> dict[str, object]:
    phases = []
    start = 0
    for ordinal, gib in enumerate(PHASE_GIB):
        end = start + gib * GIB
        phases.append({
            "name": f"phase-{ordinal:04d}",
            "start_bytes": start, "end_bytes": end, "stage_gib": gib,
            "mover_row": {
                **_row(_hexkey(f"mover{ordinal}"),
                       {STAGE_KIND: gib, "mem_gb": 1}, queue),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE_TIER,
                    "manifest_sha256": MANIFEST, "manifest_bytes": end,
                    "range_start_bytes": start, "range_end_bytes": end}},
            "egress_row": _row(_hexkey(f"egress{ordinal}"), {"mem_gb": 1},
                               queue),
            "ram_mover_row": {
                **_row(_hexkey(f"rampromote{ordinal}"),
                       {RAM_KIND: gib, "mem_gb": 1}, queue),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": RAM_TIER,
                    "manifest_sha256": MANIFEST, "manifest_bytes": end,
                    "range_start_bytes": start, "range_end_bytes": end}},
            "ram_egress_row": _row(_hexkey(f"ramrelease{ordinal}"),
                                   {"mem_gb": 1}, queue),
        })
        start = end
    return residency_plan.build_plan(
        consumer_action_key=CONSUMER, tier_id=STAGE_TIER,
        stage_root="/stage/prewarm", manifest_sha256=MANIFEST,
        manifest_bytes=start, phases=phases, ram_tier_id=RAM_TIER)


def _tiers(tmp_path: Path) -> dict[str, dict[str, object]]:
    return {
        STAGE_TIER: {"tier": "stage", "tier_id": STAGE_TIER,
                     "host": "dl380g10", "mountpoint": "/stage/prewarm"},
        RAM_TIER: {"tier": "ram", "tier_id": RAM_TIER, "host": "dl380g10",
                   "mountpoint": str(tmp_path / "ram"), "epoch": EPOCH,
                   "capacity_bytes": 112 * GIB, "window_gib": 112},
    }


def _fixture(tmp_path: Path, *, ram_capacity_gib: int,
             landed: int) -> pool.PoolQueue:
    """A consumer, its frozen plan, and the first ``landed`` stage ranges resident.

    Landed means landed: tokens held AND the finished fragment plus the
    mover's complete receipt filed (issue #759).  Reservations alone are
    accounting, never bytes.
    """

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    plan = _plan(queue)
    residency_plan.freeze(queue, plan)
    queue.publish(**_row(CONSUMER, {"mem_gb": 1}, queue), residency={
        "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE_TIER,
        "manifest_sha256": MANIFEST, "manifest_bytes": 6 * GIB,
        "leads": residency_plan.leads_for(plan)})
    queue.mint_tier_capacity(RAM_TIER, {"ram_gib": ram_capacity_gib})
    queue.mint_tier_capacity(STAGE_TIER, {"stage_gib": 64})
    start = 0
    for ordinal in range(landed):
        stage_lead = _hexkey(f"mover{ordinal}")
        assert queue.tier_ledger(STAGE_TIER).acquire(
            stage_lead, {"stage_gib": PHASE_GIB[ordinal]})
        end = start + PHASE_GIB[ordinal] * GIB
        residency_publication.vouch_landed(
            queue, consumer_action_key=CONSUMER, mover_action_key=stage_lead,
            tier_id=STAGE_TIER, stage_root="/stage/prewarm",
            manifest_sha256=MANIFEST, range_start_bytes=start,
            range_end_bytes=end, name=f"phase{ordinal}",
            unix=1000.0 + ordinal)
        start = end
    return queue


def test_the_first_phase_is_promoted_when_its_stage_range_has_landed(
        tmp_path: Path) -> None:
    queue = _fixture(tmp_path, ram_capacity_gib=2, landed=1)

    events = tier_loop.ram_residency_window(queue, tiers=_tiers(tmp_path))

    published = [event for event in events
                 if event["event"] == "ram-mover-published"]
    assert [event["phase"] for event in published] == ["phase-0000"]
    # In the plan's read order, and actually queued: the tokens bound it.
    assert queue.item_path(pool.READY, _hexkey("rampromote0")).exists()


def test_nothing_is_promoted_onto_a_range_the_stage_does_not_hold(
        tmp_path: Path) -> None:
    """The promotion's source is the stage; without it there is no promotion."""

    queue = _fixture(tmp_path, ram_capacity_gib=8, landed=0)

    events = tier_loop.ram_residency_window(queue, tiers=_tiers(tmp_path))

    assert [event for event in events
            if event["event"] == "ram-mover-published"] == []
    assert not queue.item_path(pool.READY, _hexkey("rampromote0")).exists()


def test_runahead_is_bounded_by_the_consumers_accepted_progress(
        tmp_path: Path) -> None:
    """A consumer that has accepted nothing gets one phase of run-ahead, and
    the rest is a reported stall, exactly as the stage window decides it.

    Two stage ranges have landed, so the stage-landed precondition is
    satisfied for both; the bound is the thing declining the third.
    """

    queue = _fixture(tmp_path, ram_capacity_gib=64, landed=2)

    events = tier_loop.ram_residency_window(queue, tiers=_tiers(tmp_path))

    published = [event["phase"] for event in events
                 if event["event"] == "ram-mover-published"]
    assert published == ["phase-0000", "phase-0001"]
    stalls = [event for event in events
              if event["event"] == "ram-window-stalled"]
    assert stalls and stalls[0]["reason"] == "no_accepted_progress"
    assert stalls[0]["tier_id"] == RAM_TIER
    assert stalls[0]["blocked_phase"] == "phase-0002"


def test_a_window_with_no_free_tokens_publishes_nothing(tmp_path: Path) -> None:
    """Free ``ram_gib`` is the bound: empty space in tmpfs, made exact through
    the ledger rather than guessed off statvfs by the publisher."""

    queue = _fixture(tmp_path, ram_capacity_gib=1, landed=1)

    events = tier_loop.ram_residency_window(queue, tiers=_tiers(tmp_path))

    assert [event for event in events
            if event["event"] == "ram-mover-published"] == []
    assert not queue.item_path(pool.READY, _hexkey("rampromote0")).exists()


def test_nothing_is_promoted_against_a_root_that_refuses_admission(
        tmp_path: Path) -> None:
    """The stage window's refusal, pointed at the tmpfs (#631).

    A ram root that is present but unregistered keeps its minted supply and
    admits no new promotions; the deferral names the refusal, and the ram
    egress path below is untouched by it.
    """

    queue = _fixture(tmp_path, ram_capacity_gib=2, landed=1)
    tiers = _tiers(tmp_path)
    tiers[RAM_TIER] = {**tiers[RAM_TIER],
                       "stage_root_owner": "stage_root_belongs_to_another_queue: /x",
                       "stage_root_admits": False}

    events = tier_loop.ram_residency_window(queue, tiers=tiers)

    assert [event for event in events
            if event["event"] == "ram-mover-published"] == []
    assert not queue.item_path(pool.READY, _hexkey("rampromote0")).exists()
    deferred = [event for event in events
                if event["event"] == "ram-mover-publish-deferred-unregistered-root"]
    assert len(deferred) == 1
    assert deferred[0]["tier_id"] == RAM_TIER
    assert deferred[0]["phases"] == ["phase-0000"]
