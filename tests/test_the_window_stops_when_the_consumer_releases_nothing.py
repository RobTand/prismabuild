"""Mover run-ahead is bounded by the consumer's accepted progress (#632).

The window's own contract is that "staging for phase k+N overlaps compute on
phase k and the stage never overfills".  Both halves rest on the release side
firing, and until #632 nothing said what happens when it does not.

On 2026-09-18 the GLM-5.3-Flash run `ad8803aa` carried a well-formed plan of
46 phases and 3.60 TB against a 744 GB stage.  Its consumer published no
progress-v1 records at all, so `accepted_phase` was `None` on every cycle, no
phase was ever evicted, and the only brake on the admit side was the tier's
free capacity.  The window published movers in read order until the free
tokens ran out: **19 movers done, 1 egress done**, 19 phases of 81-134 GB,
`prismabuild-stage/prewarm` at 0 B available.  The #628 ownership marker needs
one block to rewrite and could not get it, so from then on every sweep and
every egress refused with `stage_root_unregistered` (#631).  A stall would
have been recoverable; a full dataset that cannot write its own marker is not.

So the shape below is the incident's, not a restatement of the fix: a plan
several times the tier, a consumer that accepts nothing, and the question of
how much of the tier the window is allowed to spend on it.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402

CONSUMER = "c" * 64
TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
MANIFEST = "9" * 64
GIB = storage_tiers.GIB

#: The live plan, to scale: 46 phases of 81-134 GiB against a 744 GiB stage.
PHASE_GIB = [81 + (ordinal * 53) % 54 for ordinal in range(46)]
STAGE_CAPACITY_GIB = 744


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _row(key: str, resources: dict[str, int], queue: pool.PoolQueue) -> dict[str, object]:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


def _plan(queue: pool.PoolQueue) -> dict[str, object]:
    built = []
    start = 0
    for ordinal, gib in enumerate(PHASE_GIB):
        end = start + gib * GIB
        built.append({
            "name": f"phase-{ordinal:04d}",
            "start_bytes": start, "end_bytes": end, "stage_gib": gib,
            "mover_row": {
                **_row(_hexkey(f"mover{ordinal}"),
                       {STAGE_KIND: gib, "mem_gb": 1}, queue),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                    "manifest_sha256": MANIFEST, "manifest_bytes": end,
                    "range_start_bytes": start, "range_end_bytes": end},
            },
            "egress_row": _row(_hexkey(f"egress{ordinal}"), {"mem_gb": 1}, queue),
        })
        start = end
    return residency_plan.build_plan(
        consumer_action_key=CONSUMER, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=MANIFEST, manifest_bytes=start, phases=built)


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    q.mint_tier_capacity(TIER, {"stage_gib": STAGE_CAPACITY_GIB})
    return q


def test_a_consumer_that_accepts_nothing_does_not_get_the_whole_tier(queue) -> None:
    """The incident: 19 movers, ~721 GiB, 0 B free, and no egress to relieve it."""

    decision = residency_plan.window(
        _plan(queue), accepted_phase=None, free_gib=STAGE_CAPACITY_GIB)

    spent = sum(int(phase["stage_gib"]) for phase in decision["publish"])
    # The head phase plus the one the overlap contract needs, and no more.
    assert [phase["phase"] for phase in decision["publish"]] == [
        "phase-0000", "phase-0001"]
    assert spent < STAGE_CAPACITY_GIB // 2
    # ...and the stall says so, rather than looking like the end of the plan.
    stall = decision["stall"]
    assert stall is not None
    assert stall["reason"] == "no_accepted_progress"
    assert stall["blocked_phase"] == "phase-0002"
    assert stall["accepted_phase"] is None


def test_the_tier_keeps_room_for_one_more_phase_while_a_window_rolls(queue) -> None:
    """A reporting consumer still may not take the tier to 0 B.

    The #628 marker is rewritten under the stage root and needs one block; a
    tier at 0 B cannot give it one, and every deleter then refuses (#631).
    """

    plan = _plan(queue)
    decision = residency_plan.window(
        plan, accepted_phase="phase-0000", free_gib=STAGE_CAPACITY_GIB,
        capacity_gib=STAGE_CAPACITY_GIB)

    runahead = sum(int(phase["stage_gib"]) for phase in decision["publish"]
                   if phase["phase"] != "phase-0000")
    assert runahead <= STAGE_CAPACITY_GIB - max(PHASE_GIB[1:])
    assert runahead > max(PHASE_GIB[1:])      # a rolling window is not a pair
    assert decision["stall"] is not None
    assert decision["stall"]["reason"] == "runahead_budget"


def test_a_consumer_quiet_after_some_progress_is_bounded_the_same_way(queue) -> None:
    """Quiet-after-some is the harder case and gets the rolling bound, not a clock."""

    plan = _plan(queue)
    staged = [_hexkey(f"mover{n}") for n in range(0, 6)]

    decision = residency_plan.window(
        plan, accepted_phase="phase-0003", free_gib=STAGE_CAPACITY_GIB,
        capacity_gib=STAGE_CAPACITY_GIB, published=staged, staged=staged)

    ahead = sum(PHASE_GIB[4:6]) + sum(
        int(phase["stage_gib"]) for phase in decision["publish"])
    assert ahead <= STAGE_CAPACITY_GIB - max(PHASE_GIB[4:])


def test_the_loop_reports_a_stalled_window_rather_than_going_quiet(queue, tmp_path) -> None:
    """A silent stall reproduces the bug that caused this."""

    plan = _plan(queue)
    residency_plan.freeze(queue, plan)
    queue.publish(
        action_key=CONSUMER, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
        resources={"cpu": 1, "mem_gb": 1},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 40,
                   "leads": residency_plan.leads_for(plan)})

    events = tier_loop.residency_window(
        queue, tiers={TIER: {"tier_id": TIER, "tier": "stage",
                             "mountpoint": str(tmp_path / "stage")}})

    published = [event["phase"] for event in events
                 if event["event"] == "mover-published"]
    assert published == ["phase-0000", "phase-0001"]
    stalled = [event for event in events if event["event"] == "window-stalled"]
    assert len(stalled) == 1
    assert stalled[0]["consumer"] == CONSUMER
    assert stalled[0]["reason"] == "no_accepted_progress"
    assert stalled[0]["blocked_phase"] == "phase-0002"
    assert stalled[0]["runahead_budget_gib"] >= 1


def test_a_stalled_window_does_not_ask_the_sweep_for_room_it_will_not_use(
        queue, tmp_path) -> None:
    """#598: an orphan is evicted when a tier needs the tokens, never on a clock.

    A run-ahead stall is not the tier needing tokens.  Reporting it as pressure
    would evict a resident range for a phase the window has already declined to
    publish -- a stall no eviction can relieve, which is a deadlock of a
    different shape.
    """

    plan = _plan(queue)
    residency_plan.freeze(queue, plan)
    queue.publish(
        action_key=CONSUMER, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
        resources={"cpu": 1, "mem_gb": 1},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 40,
                   "leads": residency_plan.leads_for(plan)})
    tiers = {TIER: {"tier_id": TIER, "tier": "stage",
                    "mountpoint": str(tmp_path / "stage")}}
    tier_loop.residency_window(queue, tiers=tiers)
    ledger = queue.tier_ledger(TIER)
    for ordinal in (0, 1):
        assert ledger.acquire(_hexkey(f"mover{ordinal}"),
                              {"stage_gib": PHASE_GIB[ordinal]})

    assert tier_loop.window_pressure(queue, tiers=tiers) == {}


def test_a_phase_name_this_plan_does_not_carry_is_not_progress(queue) -> None:
    """`remaining` reads a stale name as the beginning; the bound must agree.

    Otherwise another plan's phase name, or one renamed by a resubmission,
    buys the deeper budget that only demonstrated progress earns.
    """

    decision = residency_plan.window(
        _plan(queue), accepted_phase="phase-from-another-plan",
        free_gib=STAGE_CAPACITY_GIB, capacity_gib=STAGE_CAPACITY_GIB)

    assert [phase["phase"] for phase in decision["publish"]] == [
        "phase-0000", "phase-0001"]
    assert decision["stall"]["reason"] == "no_accepted_progress"
