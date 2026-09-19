"""The ram leg of ``window_pressure`` asks the window, not the plan (#642).

The stage leg reports pressure from the window's own decision run with
unbounded room (#632): a phase the run-ahead bound declined is not something
the tier needs tokens for.  The ram leg instead read the plan directly -- the
first phase whose stage range had landed and whose promotion was unpublished
-- so a live consumer that had accepted nothing could register ``ram_gib``
pressure for a promotion the run-ahead bound would never publish, and the
orphan sweep treated that as "the tier needs the tokens": an orphan evicted
to make room nothing is going to fill.  Same deadlock shape #632 closed on
the stage side, one tier up, bounded to eviction-for-pressure (publication
itself was already bounded correctly -- ``ram_residency_window`` runs the
real decision).

The shape below is the issue's acceptance: the ram ledger holds nothing, the
consumer has accepted nothing, two stage ranges have landed, and the first
ram candidate (phase-0001, 8 GiB) sits beyond the run-ahead budget
(``prefill_depth`` 4).  Before the fix the ram tier is named with 8 GiB; after
it, only the stage tier's own 2 GiB head remains.  The companion test pins the
other direction: when the bound allows the promotion, the ram tier is still
named, so the fix cannot silence genuine pressure.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402

import tier_loop  # noqa: E402

CONSUMER = "c" * 64
MANIFEST = "9" * 64
STAGE_TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:dl380g10"
RAM_KIND = f"ram_gib@{RAM_TIER}"
STAGE_KIND = f"stage_gib@{STAGE_TIER}"
GIB = storage_tiers.GIB
#: The head fits any budget; the 8 GiB tail phases do not fit ``prefill_depth``
#: 4 once the bound applies past the phase being read.
PHASE_GIB = [2, 8, 8]
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


def _fixture(tmp_path: Path, *, landed: tuple[int, ...]) -> pool.PoolQueue:
    """A consumer that accepted nothing, with ``landed`` stage ranges resident.

    Nothing holds ``ram_gib``: no promotion is queued and none is pinned, so
    the ram ledger is empty and the only question is what the window would
    publish next.
    """

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    plan = _plan(queue)
    residency_plan.freeze(queue, plan)
    queue.publish(**_row(CONSUMER, {"mem_gb": 1}, queue), residency={
        "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE_TIER,
        "manifest_sha256": MANIFEST, "manifest_bytes": sum(PHASE_GIB) * GIB,
        "leads": residency_plan.leads_for(plan)})
    queue.mint_tier_capacity(RAM_TIER, {"ram_gib": 112})
    queue.mint_tier_capacity(STAGE_TIER, {"stage_gib": 64})
    phases = list(plan["phases"])
    assert isinstance(phases, list)
    for ordinal in landed:
        phase = phases[ordinal]
        assert isinstance(phase, dict)
        stage_lead = _hexkey(f"mover{ordinal}")
        assert queue.tier_ledger(STAGE_TIER).acquire(
            stage_lead, {"stage_gib": PHASE_GIB[ordinal]})
        queue.record_move(stage_lead, {
            "consumer_action_key": CONSUMER, "tier_id": STAGE_TIER,
            "stage_root": "/stage/prewarm", "manifest_sha256": MANIFEST,
            "range_start_bytes": int(phase["start_bytes"]),
            "range_end_bytes": int(phase["end_bytes"]),
            "bytes_staged": PHASE_GIB[ordinal] * GIB, "complete": True,
            "seconds": 1.0, "unix": 1000.0 + ordinal})
    return queue


def test_a_promotion_the_bound_declines_is_not_ram_pressure(
        tmp_path: Path, monkeypatch) -> None:
    """#642: the run-ahead bound declines phase-0001, so the sweep must not
    evict for it.

    ``prefill_depth`` 4 against 8 GiB tail phases: the ram window would
    publish the head (the work, never run-ahead) and stall on phase-0001 with
    ``runahead_budget``.  The head's stage range has not landed, so no
    publishable promotion remains and the ram tier is absent -- while the
    stage tier still names its own 2 GiB head, which its window really would
    publish.
    """

    queue = _fixture(tmp_path, landed=(1, 2))
    monkeypatch.setattr(tier_loop, "load_ram_policy",
                        lambda: {"prefill_depth": 4})

    assert tier_loop.window_pressure(
        queue, tiers=_tiers(tmp_path)) == {STAGE_TIER: 2}


def test_a_promotion_the_bound_covers_is_still_ram_pressure(
        tmp_path: Path, monkeypatch) -> None:
    """The fix must not silence genuine pressure: with no prefill cap the
    step budget covers phase-0001, its stage range has landed, and the ram
    tier names its 8 GiB alongside the stage tier's head."""

    queue = _fixture(tmp_path, landed=(1, 2))
    monkeypatch.setattr(tier_loop, "load_ram_policy",
                        lambda: {"prefill_depth": None})

    assert tier_loop.window_pressure(
        queue, tiers=_tiers(tmp_path)) == {STAGE_TIER: 2, RAM_TIER: 8}
