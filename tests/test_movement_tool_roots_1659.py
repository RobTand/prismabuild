"""The tool root a box announces, and the export a producer seals (#1659).

A submitter seals the path a tier announced, because the box that runs the mover
is not the box that seals it.  A box that holds the root-owned copy of its
generation announces the copy's tool root; a box without one announces its own
directory, as before.  The sealer never swaps in a path from its own box, so a
partial deployment cannot name a file the executing box lacks.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, produced_spool, resource_scope, runtime_publication, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"


@pytest.fixture
def retained(movement_authority):
    return resource_scope.RETAINED_GENERATION_STORE / movement_authority.name


def test_a_box_with_the_copy_announces_the_copys_tool_root(movement_authority, retained, monkeypatch):
    monkeypatch.setattr(tier_loop, "MOVER_TOOLS_ROOT", str(retained / "tools" / "fleet"))
    assert tier_loop.announced_tools_root() == str(movement_authority / "tools" / "fleet")


@pytest.mark.parametrize("lacks", ["no-store", "other-generation", "checkout"])
def test_a_box_without_the_copy_announces_its_own_directory(
        movement_authority, retained, monkeypatch, tmp_path, lacks):
    own = retained / "tools" / "fleet"
    if lacks == "no-store":
        monkeypatch.setattr(runtime_publication, "PROTECTED_GENERATION_STORE", tmp_path / "none")
    elif lacks == "other-generation":
        # This loop's generation has no copy; some other generation has one.
        from movement_publication_support import retained_generation
        own = retained_generation(retained.parent, "d" * 12 + "-1791400000-" + "e" * 12,
                                  commit="d" * 40) / "tools" / "fleet"
    else:
        own = Path(tier_loop.__file__).resolve().parent
    monkeypatch.setattr(tier_loop, "MOVER_TOOLS_ROOT", str(own))
    assert tier_loop.announced_tools_root() == str(own)


def test_the_tier_cycle_announces_the_tool_root_a_submitter_seals_from(
        movement_authority, retained, monkeypatch, tmp_path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.mint_tier_capacity(TIER, {"stage_gib": 5})
    stage = tmp_path / "stage"
    stage.mkdir()
    monkeypatch.setattr(tier_loop, "MOVER_TOOLS_ROOT", str(retained / "tools" / "fleet"))

    def discover(**_kwargs):
        return {TIER: {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1, "tier_id": TIER,
                       "host": "dl380g10", "tier": "stage", "mountpoint": str(stage),
                       "capacity_bytes": 8 * storage_tiers.GIB}}

    announced = tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                                receipts=tier_loop.ReceiptCache(), discover=discover)
    assert announced[0]["mover_tools_root"] == str(movement_authority / "tools" / "fleet")
    stored = {str(r["tier_id"]): r for r in queue.tiers()}[TIER]
    assert stored["mover_tools_root"] == str(movement_authority / "tools" / "fleet")
    # The announcement is rebuilt each cycle: when the copy goes, the next one says so.
    monkeypatch.setattr(runtime_publication, "PROTECTED_GENERATION_STORE", tmp_path / "none")
    again = tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                            receipts=tier_loop.ReceiptCache(), discover=discover)
    assert again[0]["mover_tools_root"] == str(retained / "tools" / "fleet")


def test_a_producer_seals_its_export_from_the_copy_it_holds(movement_authority, retained, monkeypatch):
    """An export runs on its producer's own host, so the producer's copy is the executing host's."""
    export = retained / "tools" / "fleet" / "produced_export.py"
    monkeypatch.setattr(produced_spool, "__file__", str(retained / "src" / "prismabuild" / "produced_spool.py"))
    assert produced_spool.export_tool() == movement_authority / "tools" / "fleet" / "produced_export.py"
    monkeypatch.setattr(runtime_publication, "PROTECTED_GENERATION_STORE", retained.parent / "none")
    assert produced_spool.export_tool() == export


def test_a_checkout_seals_its_export_from_the_checkout(movement_authority):
    tool = produced_spool.export_tool()
    assert tool == Path(produced_spool.__file__).resolve().parents[2] / "tools" / "fleet" / "produced_export.py"


def test_the_movement_environment_allow_list_is_built_from_the_pools_names():
    from prismabuild import movement_actions as ma
    assert ma.MOVEMENT_EXTRA_ENVIRONMENT == (pool.CONTAINER_OWNER_ENV, pool.CONTAINER_MARKER_ENV)
