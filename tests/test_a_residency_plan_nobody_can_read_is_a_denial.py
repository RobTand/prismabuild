"""A plan the coordinator cannot read must say so, not skip the consumer (#615).

The GLM-5.3-Flash run stage sat in ``ready/`` for 25 minutes with a staged head
window, an idle GPU at 4.8 W and a tier loop whose log showed only
``tier-cycle``.  Its plan carried ``demand_source`` -- a key #609 added -- and
the loop's ``validate_plan`` predated it, so ``residency_plan.read`` answered
``None`` and ``residency_window`` did ``if plan is None: continue``.

``None`` was covering two different answers.  *No plan* is the ordinary case:
almost no action is staged, and a consumer without one is nobody's work.  *A
plan that will not validate* is a denial -- this consumer will never be staged
for and will never be admitted -- and a denial with no record is the silence
the whole of #583's accounting exists to end.

Three places have to say it, because three different people read them: the
loop's own log for whoever is watching the box, a claim denial for whoever
runs ``pbstatus``, and the consumer's own verdict for whoever asks why the
item will not claim.

Nothing here touches the live queue, a real pool or a real device.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import adaptive_cpu, pool, residency_map  # noqa: E402
from prismabuild import residency_plan, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402

CONSUMER = "c" * 64
TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
MANIFEST = "9" * 64
GIB = storage_tiers.GIB


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _row(key: str, resources: dict[str, int], queue: pool.PoolQueue) -> dict[str, object]:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


def _plan(queue: pool.PoolQueue, *, gib_per_phase: int = 2,
          phases: int = 2) -> dict[str, object]:
    built = []
    for ordinal in range(phases):
        start = ordinal * gib_per_phase * GIB
        end = start + gib_per_phase * GIB
        built.append({
            "name": f"phase-{ordinal}",
            "start_bytes": start, "end_bytes": end,
            "stage_gib": gib_per_phase,
            "mover_row": {
                **_row(_hexkey(f"mover{ordinal}"),
                       {STAGE_KIND: gib_per_phase, "mem_gb": 1}, queue),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                    "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 30,
                    "range_start_bytes": start, "range_end_bytes": end},
            },
            "egress_row": _row(_hexkey(f"egress{ordinal}"), {"mem_gb": 1}, queue),
        })
    return residency_plan.build_plan(
        consumer_action_key=CONSUMER, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=MANIFEST, manifest_bytes=1 << 30, phases=built)


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    q.mint_tier_capacity(TIER, {"stage_gib": 5})
    return q


def _publish_consumer(queue: pool.PoolQueue, plan: dict[str, object], *,
                      pinned_tier: bool = True) -> None:
    """The consumer row, as the submitter seals it.

    ``pinned_tier`` is off in the claim tests only: with a tier named, a lead
    counts as resident while it still *holds* that tier's tokens, and driving
    a real pin would mean staging real bytes.  The gate under test here is the
    one after that, so the leads are made resident the way
    ``test_residency_is_derived_and_gates_admission`` makes them resident.
    """

    block: dict[str, object] = {
        "schema": pool.RESIDENCY_SCHEMA_V1,
        "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 30,
        "leads": residency_plan.leads_for(plan)}
    if pinned_tier:
        block["tier_id"] = TIER
    queue.publish(
        action_key=CONSUMER, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
        resources={"cpu": 1, "mem_gb": 1}, residency=block)


def _make_the_lead_resident(queue: pool.PoolQueue, plan: dict[str, object]) -> None:
    """Run the first phase's mover to ``executed``, holding no tier tokens."""

    lead = residency_plan.leads_for(plan)[0]
    queue.publish(action_key=lead, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py",
                  resources={"cpu": 1})
    claimed = queue.claim(owner="mover", capacity={"cpu": 4, "mem_gb": 4})
    assert claimed is not None and claimed["action_key"] == lead
    queue.finish(lead, status="executed", claim_snapshot=claimed)


def _write_plan_from_a_newer_writer(queue: pool.PoolQueue,
                                    plan: dict[str, object]) -> None:
    """The incident's own shape: a key the reader's key set does not hold.

    Written straight to the path rather than through ``freeze``, because
    ``freeze`` validates with *this* generation's reader -- and the whole
    point is a plan one generation writes and another cannot read.
    """

    body = {**plan, "demand_source_v2": {"receipt": "r" * 64}}
    path = queue.residency_plan_path(CONSUMER)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(body))


def _denial(queue: pool.PoolQueue, key: str) -> dict[str, object] | None:
    path = adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    matching = [entry for entry in records.values()
                if isinstance(entry, dict) and entry.get("action_key") == key]
    if not matching:
        return None
    return max(matching, key=lambda entry: float(entry.get("denied_unix", 0.0)))


TIERS = {TIER: {"tier_id": TIER, "tier": "stage", "mountpoint": "/stage/prewarm"}}


# -- the loop says which consumer, and why ----------------------------------


def test_the_window_names_the_consumer_whose_plan_it_cannot_read(queue) -> None:
    """The 25 idle minutes, in one event the loop's own log carries."""

    plan = _plan(queue)
    _publish_consumer(queue, plan)
    _write_plan_from_a_newer_writer(queue, plan)

    events = tier_loop.residency_window(queue, tiers=TIERS)

    unreadable = [event for event in events if event["event"] == "plan-unreadable"]
    assert len(unreadable) == 1, "a skipped consumer left no record"
    assert unreadable[0]["consumer"] == CONSUMER
    assert "demand_source_v2" in str(unreadable[0]["error"]), \
        "the event must name what the reader refused, not merely that it did"
    # ...and nothing was published on a plan nobody could read.
    assert not queue.item_path(pool.READY, _hexkey("mover0")).exists()


def test_the_window_records_a_denial_an_operator_reads(queue) -> None:
    """``pbstatus`` aggregates claim denials per host; the log is on one box."""

    plan = _plan(queue)
    _publish_consumer(queue, plan)
    _write_plan_from_a_newer_writer(queue, plan)

    tier_loop.residency_window(queue, tiers=TIERS)

    denial = _denial(queue, CONSUMER)
    assert denial is not None, "the starvation was visible nowhere an operator looks"
    assert denial["reason"] == "residency_plan_unreadable"
    assert "demand_source_v2" in str(denial["evidence"])


def test_a_consumer_with_no_plan_at_all_is_not_an_event(queue) -> None:
    """``None`` stays the ordinary answer: almost no action is staged.

    An event per cycle per unstaged consumer would bury the one that matters.
    """

    plan = _plan(queue)
    _publish_consumer(queue, plan)              # no plan frozen for it

    assert tier_loop.residency_window(queue, tiers=TIERS) == []
    assert _denial(queue, CONSUMER) is None


def test_a_readable_plan_is_still_windowed_and_composed(queue) -> None:
    """Regression: the plan the reader knows is published exactly as before."""

    plan = _plan(queue)
    residency_plan.freeze(queue, plan)
    _publish_consumer(queue, plan)

    events = tier_loop.residency_window(queue, tiers=TIERS)

    assert [event["phase"] for event in events
            if event["event"] == "mover-published"] == ["phase-0", "phase-1"]
    assert not [event for event in events if event["event"] == "plan-unreadable"]
    assert _denial(queue, CONSUMER) is None


def test_a_plan_carrying_demand_source_reads_on_this_generation(queue) -> None:
    """The incident's actual key: #609 added it, and this reader holds it."""

    plan = _plan(queue)
    plan["demand_source"] = {"receipt": "r" * 64}
    residency_plan.freeze(queue, plan)

    read = residency_plan.read(queue, CONSUMER)
    assert read is not None and read["demand_source"] == {"receipt": "r" * 64}


# -- and the consumer's own verdict says it ---------------------------------


def test_the_claim_says_the_plan_is_unreadable_not_that_the_map_is_late(
    queue,
) -> None:
    """``map_not_composed`` reads as "wait a moment"; this one never resolves."""

    plan = _plan(queue)
    _publish_consumer(queue, plan, pinned_tier=False)
    _write_plan_from_a_newer_writer(queue, plan)
    # Every lead resident, so the only thing between this item and a claim is
    # the map -- and the map is what a plan nobody can read never gets.
    _make_the_lead_resident(queue, plan)

    assert queue.claim(owner="worker", capacity={"cpu": 4, "mem_gb": 4}) is None
    denial = _denial(queue, CONSUMER)
    assert denial is not None
    assert denial["reason"] == "residency_plan_unreadable", \
        "the consumer was told to wait for a map that will never be composed"


def test_a_missing_map_with_a_readable_plan_is_still_map_not_composed(
    queue,
) -> None:
    """Regression: the ordinary race between a pin and the next tier cycle."""

    plan = _plan(queue)
    residency_plan.freeze(queue, plan)
    _publish_consumer(queue, plan, pinned_tier=False)
    _make_the_lead_resident(queue, plan)

    assert queue.claim(owner="worker", capacity={"cpu": 4, "mem_gb": 4}) is None
    denial = _denial(queue, CONSUMER)
    assert denial is not None and denial["reason"] == "residency_map_not_composed"

    # ...and once the loop composes it, the item claims.
    residency_map.write_map(
        queue.residency_map_path(CONSUMER),
        residency_map.compose([{
            "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
            "consumer_action_key": CONSUMER,
            "mover_action_key": _hexkey("mover0"),
            "tier_id": TIER, "stage_root": "/stage/prewarm",
            "manifest_sha256": MANIFEST,
            "entries": {residency_map.residency_map_key("/pool/a.bin", 0): {
                "stage_path": "/stage/prewarm/a.bin", "bytes": 4096,
                "offset": 0, "sha256": "d" * 64}},
        }]))
    claimed = queue.claim(owner="worker", capacity={"cpu": 4, "mem_gb": 4})
    assert claimed is not None and claimed["action_key"] == CONSUMER
