"""#1247: a READY row's declared manifest becomes a tier residency plan.

The planner is the submitter's own sealing path invoked by the tier role
(``tools/fleet/manifest_promotion.py``): for the first READY rows in claim
order it seals movement nodes off the row's sealed request and freezes the
plan, and the tier loop's ordinary adoption pass publishes the movers.  A row
with a filed plan stands down; a row without a manifest is not the planner's;
the bound is rows, not bytes, because the resident-byte bound is the tier's
window and eviction.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import prismabuild.storage_tiers as storage_tiers  # noqa: F401
from prismabuild import residency_plan

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from prewarm_fixture import Fleet, phase_table  # noqa: E402
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import manifest_promotion  # noqa: E402

HOST = "dl380g10"
STAGE_TIER = "prismabuild-stage:dl380g10"
MIB = 1024 * 1024

NAMED_FILES = [("model-00001.safetensors", 64 * MIB),
               ("model-00002.safetensors", 64 * MIB)]


def _stage_tier(fleet: Fleet) -> dict:
    return {
        "schema": "prismabuild.storage_tier.v1",
        "tier": "stage",
        "tier_id": STAGE_TIER,
        "host": HOST,
        "mountpoint": str(fleet.root / "stage"),
        "mover_python": sys.executable,
        "mover_tools_root": str(Path(__file__).resolve().parents[1]
                                / "tools" / "fleet"),
    }


def _ready_item(fleet: Fleet, key: str) -> dict:
    return json.loads(
        (fleet.queue.root / "ready" / f"{key}.json").read_text())


def _manifest_row(fleet: Fleet, seed: str, *, annotations=None,
                  files=None) -> str:
    files = files if files is not None else [
        fleet.file(name, size) for name, size in NAMED_FILES]
    return fleet.action(seed, files, annotations=annotations)


def test_a_declaring_row_gains_a_filed_plan_and_a_tier_receipt(tmp_path):
    """One READY manifest row: plan filed, movers sealed, receipt stamped."""

    fleet = Fleet(tmp_path)
    key = _manifest_row(fleet, "row-a",
                        annotations={"phases": phase_table(NAMED_FILES)})

    outcomes = manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, _stage_tier(fleet),
        ready=[_ready_item(fleet, key)])

    assert len(outcomes) == 1, outcomes
    assert outcomes[0]["outcome"] == "planned", outcomes[0]
    assert outcomes[0]["phases"] == 2
    # The plan is filed under the consumer's key, first-writer.
    plan = residency_plan.read(fleet.queue, key)
    assert plan is not None
    assert str(plan["tier_id"]) == STAGE_TIER
    # Every phase names a mover; the movement requests are sealed in the CAS
    # for the tier loop's adoption pass to publish.
    for phase in plan["phases"]:
        mover = str(phase["mover_row"]["action_key"])
        assert (Path(fleet.cas_root) / "requests" / mover[:2]
                / f"{mover}.json").exists()
    # The receipt carries the additive tier block.
    record = fleet.queue.prewarm(key)
    assert record is not None
    tier = record.get("tier")
    assert isinstance(tier, dict)
    assert tier["destination"] == manifest_promotion.TIER_RECEIPT_DESTINATION
    assert tier["status"] == "planned"
    assert tier["phases"] == 2


def test_a_row_with_a_filed_plan_stands_down(tmp_path):
    """The planner never touches a consumer that already has a plan."""

    fleet = Fleet(tmp_path)
    key = _manifest_row(fleet, "row-b",
                        annotations={"phases": phase_table(NAMED_FILES)})
    assert manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, _stage_tier(fleet),
        ready=[_ready_item(fleet, key)])[0]["outcome"] == "planned"

    outcomes = manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, _stage_tier(fleet),
        ready=[_ready_item(fleet, key)])

    assert outcomes[0]["outcome"] == "stands_down"


def test_a_row_without_a_manifest_is_not_the_planners(tmp_path):
    """A plain READY row is examined and passed over, never refused."""

    fleet = Fleet(tmp_path)
    key = fleet.action("row-c", FILES, with_manifest=False)

    outcomes = manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, _stage_tier(fleet),
        ready=[_ready_item(fleet, key)])

    assert outcomes[0]["outcome"] == "no_manifest"
    assert residency_plan.read(fleet.queue, key) is None


def test_the_streaming_rule_plans_one_row_per_cycle(tmp_path):
    """The bound is rows: the second row waits for the next cycle."""

    fleet = Fleet(tmp_path)
    annotations = {"phases": phase_table(NAMED_FILES)}
    first = _manifest_row(fleet, "row-d", annotations=annotations)
    second = _manifest_row(fleet, "row-e", annotations=annotations)

    outcomes = manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, _stage_tier(fleet),
        ready=[_ready_item(fleet, first), _ready_item(fleet, second)])

    assert [outcome["outcome"] for outcome in outcomes] == ["planned"]
    assert residency_plan.read(fleet.queue, first) is not None
    assert residency_plan.read(fleet.queue, second) is None


def test_a_refusal_is_receipted_and_never_raises(tmp_path):
    """A tier with no mountpoint refuses the row and records the reason."""

    fleet = Fleet(tmp_path)
    key = _manifest_row(fleet, "row-f",
                        annotations={"phases": phase_table(NAMED_FILES)})
    broken = _stage_tier(fleet)
    broken["mountpoint"] = "relative/and/refused"

    outcomes = manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, broken,
        ready=[_ready_item(fleet, key)])

    assert outcomes[0]["outcome"] == "refused"
    assert outcomes[0]["reason"]
    tier = (fleet.queue.prewarm(key) or {}).get("tier")
    assert isinstance(tier, dict) and tier["status"] == "refused"
    assert residency_plan.read(fleet.queue, key) is None
