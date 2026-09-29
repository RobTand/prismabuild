"""#1332: a row the #1247 planner filed a plan for is staged like a sealed one.

The planner (``tools/fleet/manifest_promotion.py``) files a residency plan for
a bare READY manifest row and never rewrites the row.  Its docstring says the
tier loop discovers such a consumer "by the filed plan, not by the consumer
row's block".  Before #1332 nothing did: ``tier_loop.live_consumers`` and
``stage_release.live_claims`` / ``shared_interest`` read only
``item["residency"]["leads"]``, so the plan was filed and staged by nobody.
Measured on sparklina row-0063 (PQ #1654, 2026-09-29): prewarm receipt
``planned`` for the life of the row, no mover, no map, 51 GB read cold before
the first encode and about 203 s of idle GPU at the row start.

The second half is the phase contract.  The plan's phases are the manifest's
read phases; a PACT row reports ``startup``/``pricing``/``finalize``.  A
reported phase the plan does not carry reads as "the beginning", so a row that
reported its read phases and then ``pricing`` would have every evicted range
republished.  The row's sealed progress order places it instead.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))

from prismabuild import pool, residency_plan  # noqa: E402
from prewarm_fixture import Fleet  # noqa: E402
import manifest_promotion  # noqa: E402
import prewarm_loop  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
MANIFEST = "9" * 64
GIB = 1 << 30
READ_PHASES = ("phase-0", "phase-1", "phase-2")
MIB = 1024 * 1024


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _row(key: str, resources: dict[str, int], queue: pool.PoolQueue) -> dict[str, object]:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


def _plan(queue: pool.PoolQueue, consumer: str) -> dict[str, object]:
    """Three 2 GiB read phases, one stage mover and egress each."""

    built = []
    for ordinal, name in enumerate(READ_PHASES):
        start, end = ordinal * 2 * GIB, (ordinal + 1) * 2 * GIB
        built.append({
            "name": name, "start_bytes": start, "end_bytes": end,
            "stage_gib": 2,
            "mover_row": {**_row(_hexkey(f"mover{ordinal}"),
                                 {STAGE_KIND: 2, "mem_gb": 1}, queue),
                          "residency": {"schema": pool.RESIDENCY_SCHEMA_V1,
                                        "tier_id": TIER,
                                        "manifest_sha256": MANIFEST,
                                        "manifest_bytes": 6 * GIB,
                                        "range_start_bytes": start,
                                        "range_end_bytes": end}},
            "egress_row": _row(_hexkey(f"egress{ordinal}"), {"mem_gb": 1}, queue),
        })
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=MANIFEST, manifest_bytes=6 * GIB, phases=built)


def _tiers(tmp_path: Path) -> dict[str, dict[str, object]]:
    return {TIER: {"tier_id": TIER, "tier": "stage",
                   "mountpoint": str(tmp_path / "stage")}}


def _planner_row(fleet: Fleet, seed: str, *,
                 progress_phases: list[str] | None = None) -> tuple[str, dict]:
    """A bare READY row with a filed plan: exactly what the planner leaves.

    The row is sealed and published the way any submitter publishes it --
    no ``residency`` block -- and the plan is frozen under its key, which is
    the planner's whole output (``manifest_promotion`` never rewrites a row).
    """

    files = [fleet.file(f"{seed}.safetensors", 4 * MIB)]
    key = fleet.action(seed, files, progress_phases=progress_phases)
    fleet.queue.mint_tier_capacity(TIER, {"stage_gib": 16})
    plan = _plan(fleet.queue, key)
    residency_plan.freeze(fleet.queue, plan)
    item = json.loads(fleet.queue.item_path(pool.READY, key).read_text())
    assert "residency" not in item      # the planner's row stays bare
    return key, plan


def _claim(fleet: Fleet, key: str, *, claimed_unix: float = 1000.0) -> None:
    """Move the ready record into ``claimed/`` as a claim leaves it."""

    ready = fleet.queue.item_path(pool.READY, key)
    item = json.loads(ready.read_text())
    item.update({"claimed_by": "sparklina", "claimed_unix": claimed_unix})
    claimed = fleet.queue.item_path(pool.CLAIMED, key)
    claimed.parent.mkdir(parents=True, exist_ok=True)
    claimed.write_text(json.dumps(item))
    ready.unlink()


def test_a_planner_row_is_a_live_consumer_with_its_plans_leads(tmp_path):
    fleet = Fleet(tmp_path)
    key, plan = _planner_row(fleet, "row-live")

    consumers = {c["action_key"]: c for c in tier_loop.live_consumers(fleet.queue)}

    assert key in consumers, "a planner-filed row must be a live consumer"
    consumer = consumers[key]
    assert consumer["residency_source"] == "filed_plan"
    residency = consumer["residency"]
    assert residency["leads"] == residency_plan.leads_for(plan)
    assert residency["tier_id"] == TIER
    assert residency["manifest_sha256"] == MANIFEST


def test_the_window_stages_a_planner_rows_first_phase_before_its_claim(tmp_path):
    """The next row's lead is published while the row is still READY."""

    fleet = Fleet(tmp_path)
    key, plan = _planner_row(fleet, "row-window")

    events = tier_loop.residency_window(fleet.queue, tiers=_tiers(tmp_path))

    published = [e["action_key"] for e in events
                 if e["event"] == "mover-published" and e["consumer"] == key]
    lead = residency_plan.leads_for(plan)[0]
    assert lead in published, events
    assert fleet.queue.item_path(pool.READY, lead).exists()
    # Still ready: staging happened ahead of the claim, not because of it.
    assert fleet.queue.item_path(pool.READY, key).exists()


def test_the_sweep_counts_a_planner_rows_movers_as_wanted(tmp_path):
    """Published movers must not read as orphans the next sweep evicts."""

    fleet = Fleet(tmp_path)
    key, plan = _planner_row(fleet, "row-sweep")

    wanted, owners = stage_release.live_claims(fleet.queue)

    assert set(residency_plan.mover_keys(plan)) <= wanted
    assert owners.get(key) == key


def test_a_bare_row_without_a_plan_is_still_nobodys(tmp_path):
    """The ordinary row: no plan, no block, not a consumer (unchanged)."""

    fleet = Fleet(tmp_path)
    key = fleet.action("row-plain", [fleet.file("plain.safetensors", MIB)])

    assert key not in {c["action_key"] for c in tier_loop.live_consumers(fleet.queue)}
    wanted, owners = stage_release.live_claims(fleet.queue)
    assert key not in owners


def test_a_post_read_progress_phase_places_the_consumer_in_its_last_read_phase(
        tmp_path, monkeypatch):
    """``pricing`` after the read phases is the last read phase, not the start."""

    fleet = Fleet(tmp_path)
    order = ["startup", *READ_PHASES, "pricing", "finalize"]
    key, _plan_ = _planner_row(fleet, "row-linear", progress_phases=order)
    _claim(fleet, key)
    monkeypatch.setattr(prewarm_loop, "progress_phase", lambda _q, k, _c: (
        {"phase": "pricing", "reported_unix": 1100.0} if k == key else None))

    consumer = {c["action_key"]: c for c in tier_loop.live_consumers(fleet.queue)}[key]

    assert consumer["reported_phase"] == "pricing"
    assert consumer["accepted_phase"] == "phase-2"


def test_without_the_contract_a_report_reads_as_it_always_did(tmp_path, monkeypatch):
    """Today's PACT phases name no read phase: unchanged, the beginning."""

    fleet = Fleet(tmp_path)
    key, _plan_ = _planner_row(fleet, "row-pact",
                               progress_phases=["startup", "pricing", "finalize"])
    _claim(fleet, key)
    monkeypatch.setattr(prewarm_loop, "progress_phase", lambda _q, k, _c: (
        {"phase": "pricing", "reported_unix": 1100.0} if k == key else None))

    consumer = {c["action_key"]: c for c in tier_loop.live_consumers(fleet.queue)}[key]

    assert consumer["accepted_phase"] == "pricing"


def test_the_placement_rule():
    """``plan_phase`` and ``progress_contract``, the one rule both paths use."""

    order = ["startup", "phase-0", "phase-1", "phase-2", "pricing", "finalize"]
    assert residency_plan.progress_contract(READ_PHASES, order) == "linear"
    assert residency_plan.plan_phase(READ_PHASES, "pricing", order) == "phase-2"
    assert residency_plan.plan_phase(READ_PHASES, "finalize", order) == "phase-2"
    assert residency_plan.plan_phase(READ_PHASES, "phase-1", order) == "phase-1"
    # Before any read phase: unchanged, which ``remaining`` reads as the start.
    assert residency_plan.plan_phase(READ_PHASES, "startup", order) == "startup"
    assert residency_plan.plan_phase(READ_PHASES, None, order) is None
    # No contract, no placement.
    assert residency_plan.progress_contract(READ_PHASES, None) == "undeclared"
    assert residency_plan.progress_contract(
        READ_PHASES, ["startup", "pricing"]) == "unnamed"
    misordered = ["phase-1", "phase-0", "phase-2", "pricing"]
    assert residency_plan.progress_contract(READ_PHASES, misordered) == "misordered"
    assert residency_plan.plan_phase(READ_PHASES, "pricing", misordered) == "pricing"


def test_a_placed_report_evicts_behind_and_republishes_nothing(tmp_path):
    """Why the placement must exist before a producer names its read phases.

    Unplaced, ``pricing`` reads as the beginning: every range the window
    evicted after ``phase-2`` was reported is ahead and unpublished again,
    and is copied a second time.  Placed, it is the last read phase.
    """

    fleet = Fleet(tmp_path)
    _key, plan = _planner_row(fleet, "row-churn")
    unplaced = residency_plan.window(plan, accepted_phase="pricing", free_gib=16,
                                     capacity_gib=16, published=(), staged=())
    placed = residency_plan.window(plan, accepted_phase="phase-2", free_gib=16,
                                   capacity_gib=16, published=(), staged=())

    assert [entry["phase"] for entry in unplaced["publish"]][:1] == ["phase-0"]
    assert [entry["phase"] for entry in placed["publish"]] == ["phase-2"]


def test_the_receipt_says_landed_when_the_verdict_reads_resident(tmp_path, monkeypatch):
    """``planned`` alone cannot certify staging; the loop writes the landing."""

    fleet = Fleet(tmp_path)
    key, _plan_ = _planner_row(fleet, "row-landed")
    fleet.queue.record_prewarm(key, {"tier": {
        "destination": manifest_promotion.TIER_RECEIPT_DESTINATION,
        "status": "planned", "phases": 3}})
    asked: list[dict] = []

    def verdict(item):
        asked.append(dict(item["residency"]))
        return {"state": "resident", "leads": list(item["residency"]["leads"])}

    monkeypatch.setattr(fleet.queue, "residency_verdict", verdict)
    tier_loop.residency_window(fleet.queue, tiers=_tiers(tmp_path), now=5000.0)

    tier = fleet.queue.prewarm(key)["tier"]
    assert tier["status"] == "landed"
    assert tier["consumer_state"] == pool.READY
    assert tier["observed_unix"] == 5000.0
    assert tier["phases"] == 3                  # the planner's fields survive
    assert asked and asked[0]["leads"] == residency_plan.leads_for(_plan_)


def test_a_claim_before_the_landing_is_said_so(tmp_path, monkeypatch):
    fleet = Fleet(tmp_path)
    key, _plan_ = _planner_row(fleet, "row-cold")
    fleet.queue.record_prewarm(key, {"tier": {
        "destination": manifest_promotion.TIER_RECEIPT_DESTINATION,
        "status": "planned", "phases": 3}})
    _claim(fleet, key, claimed_unix=4000.0)
    monkeypatch.setattr(prewarm_loop, "progress_phase", lambda *_a: None)
    monkeypatch.setattr(fleet.queue, "residency_verdict",
                        lambda item: {"state": "lead_not_resident"})

    tier_loop.residency_window(fleet.queue, tiers=_tiers(tmp_path), now=5000.0)

    tier = fleet.queue.prewarm(key)["tier"]
    assert tier["status"] == "claimed_before_landing"
    assert tier["claimed_unix"] == 4000.0
    assert tier["verdict"] == "lead_not_resident"
