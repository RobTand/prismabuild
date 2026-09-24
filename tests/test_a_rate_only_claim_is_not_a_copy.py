"""A claim that reserves only a tier's fill rate is not a copy onto it (#1060).

Production shape (R13 Stage A, 2026-09-23): the paced produced export
reserves ``fill_mb_s_pool_side@<stage tier>`` (``produced_spool.py``, #747)
and writes under its template's output prefix, never onto the stage.  Its
sealed command carries ``--pace-mb-s``/``--pace-tier`` and no range, and its
sealed request declares no produced-output template.  The claim census read
any ``<kind>@<tier>`` demand as a movement node on the tier, so while one
export was claimed every ownership census on the tier tainted with "mover
seals no range": the dead-owner retirement and the held-key eviction under
pressure both retained, blind, and a consumer waiting for stage room waited on
a verdict that had nothing to do with it.

A rate reservation names no bytes.  The pool's publish gate already draws
that line (only occupancy kinds need a range or a working window), and every
copy onto a tier demands the tier's capacity kind.  So a claim is a copy onto
the tier only when it demands a non-rate kind there, and a copy that seals no
range still taints the pass, as before.

Every fixture is a temp stage root registered to a fake queue, never the live
stage or queue.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))

import test_dead_owner_fragment_blocks_then_retires as base  # noqa: E402
from test_dead_owner_fragment_blocks_then_retires import fleet  # noqa: E402,F401
from prismabuild import pool, residency_map, storage_tiers  # noqa: E402
import stage_release  # noqa: E402

TIER = base.TIER
NAMES = base.NAMES
SIZE = base.SIZE
FILL = f"{storage_tiers.FILL_KIND}{storage_tiers.TIER_DEMAND_SEPARATOR}{TIER}"
STAGE = (f"{storage_tiers.capacity_kind_of(TIER)}"
         f"{storage_tiers.TIER_DEMAND_SEPARATOR}{TIER}")
OTHER_TIER = "prismabuild-stage:sparklina"


def _claim_export(queue: pool.PoolQueue, cas: Path, *,
                  resources: dict[str, int] | None = None) -> str:
    """One claimed paced export, shaped the way ``produced_spool`` seals it.

    Published and claimed through the queue, then given the export's tier
    demand the way ``bench_tier_cycle.build_no_range_claims`` does: the
    census reads only the claim record and the sealed request.  The request
    names no range and no produced-output template.
    """

    key = base._key()
    base._publish(queue, key, cas_root=str(cas), max_attempts=1)
    claimed = queue.item_path(pool.CLAIMED, key)
    item = json.loads(claimed.read_text())
    item["resources"] = (resources if resources is not None
                         else {"cpu": 1, "mem_gb": 1, FILL: 510})
    claimed.write_text(json.dumps(item))
    request = cas / "requests" / key[:2] / f"{key}.json"
    request.parent.mkdir(parents=True, exist_ok=True)
    request.write_text(json.dumps({
        "action_key": key, "inputs": [],
        "params": {
            "command": ["/usr/bin/python3", "tools/fleet/produced_export.py",
                        "--queue", str(queue.root), "--manifest", "/m.json",
                        "--manifest-sha256", "c" * 64,
                        "--pace-mb-s", "510", "--pace-tier", TIER],
            "demand": dict(item["resources"]),
            "produced_spool": {"manifest_sha256": "c" * 64,
                               "owner": "d" * 64, "batch_id": "batch-0"},
        }}))
    return key


def _charged_orphan(fleet) -> tuple[str, str]:
    """A failed consumer's executed mover that landed and still holds its GiB."""

    queue, stage, _cas = fleet
    consumer, _generation = base._fail_consumer(queue)
    mover = base._key()
    base._publish(queue, mover, max_attempts=1)
    for name in NAMES:
        base._stage_marked(stage, name)
    base._write_fragment(queue, stage, consumer, mover, NAMES)
    queue.record_move(mover, {
        "consumer_action_key": consumer, "tier_id": TIER,
        "stage_root": str(stage), "manifest_sha256": "a" * 64,
        "range_start_bytes": 0, "range_end_bytes": len(NAMES) * SIZE,
        "range_bytes": len(NAMES) * SIZE,
        "bytes_staged": len(NAMES) * SIZE,
        "entries_declared": len(NAMES), "entries_staged": len(NAMES),
        "complete": True, "seconds": 1.0, "unix": time.time()})
    queue.finish(mover, status="executed", detail={"returncode": 0})
    # The whole tier is this orphan's: a window that needs one GiB finds none
    # free, so the pressure pass must evict it.
    queue.mint_tier_capacity(TIER, {"stage_gib": 1})
    assert queue.tier_ledger(TIER).acquire(mover, {"stage_gib": 1}) is True
    assert queue.tier_ledger(TIER).available().get("stage_gib", 0) == 0
    return consumer, mover


def _receipts_for(receipts: list[dict], mover: str) -> list[dict]:
    return [entry for entry in receipts if entry.get("action_key") == mover]


def test_a_rate_only_export_does_not_block_a_dead_owners_retirement(fleet) -> None:
    """The #839 dead owner retires while a paced export is claimed."""

    queue, stage, cas = fleet
    consumer, mover = base._dead_owner(fleet)
    export = _claim_export(queue, cas)

    receipts = stage_release.sweep(
        queue, stage_roots={TIER: str(stage)}, pressure={TIER: 0})

    own = _receipts_for(receipts, mover)
    errors = [error for entry in own for error in entry.get("errors", [])]
    assert not any(export[:12] in error for error in errors), errors
    assert any(entry.get("complete") is True for entry in own), own
    assert not any((stage / name).exists() for name in NAMES)
    assert not residency_map.fragment_path(
        queue.root / pool.RESIDENCY, consumer, mover).exists()
    # The export's claim is untouched: it is still the worker's.
    assert queue.item_path(pool.CLAIMED, export).exists()


def test_a_rate_only_export_does_not_block_an_orphan_under_pressure(fleet) -> None:
    """The held-key pass evicts a dead owner the window needs room from."""

    queue, stage, cas = fleet
    consumer, mover = _charged_orphan(fleet)
    export = _claim_export(queue, cas)

    receipts = stage_release.sweep(
        queue, stage_roots={TIER: str(stage)}, pressure={TIER: 1})

    own = _receipts_for(receipts, mover)
    errors = [error for entry in own for error in entry.get("errors", [])]
    assert not any(export[:12] in error for error in errors), errors
    assert [entry.get("reason") for entry in own
            if entry.get("complete") is True] == ["orphan-sweep"], own
    assert not any((stage / name).exists() for name in NAMES)
    assert queue.tier_ledger(TIER).holder_tokens(mover) == {}
    assert queue.tier_ledger(TIER).available().get("stage_gib", 0) == 1


@pytest.mark.parametrize("resources", [
    {"cpu": 1, STAGE: 1},
    {"cpu": 1, STAGE: 1, FILL: 510},
    # A kind the tier does not deal in is not a rate either: unknown demand
    # on the tier reads as a copy and fails the pass closed.
    {"cpu": 1, f"mystery_gib@{TIER}": 1, FILL: 510},
], ids=["capacity", "capacity-and-fill", "unknown-kind"])
def test_a_copy_that_seals_no_range_still_taints(fleet, resources) -> None:
    """The kept refusal: occupancy demand with no sealed range is not unowned."""

    queue, stage, cas = fleet
    consumer, mover = base._dead_owner(fleet)
    claim = _claim_export(queue, cas, resources=resources)

    receipts = stage_release.sweep(
        queue, stage_roots={TIER: str(stage)}, pressure={TIER: 0})

    own = _receipts_for(receipts, mover)
    assert own and not any(entry.get("complete") is True for entry in own), own
    errors = [error for entry in own for error in entry.get("errors", [])]
    assert any(f"{claim[:12]}: mover seals no range" in error
               for error in errors), errors
    assert all((stage / name).exists() for name in NAMES)
    assert residency_map.fragment_path(
        queue.root / pool.RESIDENCY, consumer, mover).exists()


@pytest.mark.parametrize("resources", [
    {"cpu": 1, "mem_gb": 1, FILL: 510},
    # Occupancy on another tier and fill on this one: a copy onto the other
    # tier, nothing onto this one.
    {"cpu": 1, f"stage_gib@{OTHER_TIER}": 1, FILL: 510},
], ids=["fill-only", "fill-here-occupancy-elsewhere"])
def test_the_claim_census_reads_a_rate_only_claim_as_no_copy(
        fleet, resources) -> None:
    queue, _stage, cas = fleet
    _claim_export(queue, cas, resources=resources)

    paths, tainted = stage_release._claimed_paths(queue, TIER)

    assert tainted == []
    assert paths == set()
