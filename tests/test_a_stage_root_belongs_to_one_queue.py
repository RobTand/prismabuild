"""A stage root belongs to one queue, and the sweep deletes nothing else's (#628).

2026-09-18, 16:0xZ, on the storage box: a test announced ``/stage/prewarm`` to
a queue under ``tmp_path``, ran one tier cycle, and the sweep inside it walked
the real stage.  That queue's fragments attributed nothing, the prewarm xattr
marked nothing, so ``reconcile`` deleted 671 GB of staged shards -- the whole
window of the run that was reading them.  Every rule the sweep had decided
*what* to delete; none asked *whose* stage it was walking.

So the tier loop marks its own box's stage root with the queue it belongs to
before it announces the tier, and ``sweep``, ``reconcile`` and ``evict`` refuse
-- nothing deleted, tokens kept, the reason in the receipt -- under a root whose
marker is missing, unreadable, malformed or another queue's.  The last test is
the incident's exact shape, run against a root that is already registered.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, residency_map, storage_tiers  # noqa: E402

import stage_release  # noqa: E402
import tier_loop  # noqa: E402

CONSUMER = "c" * 64
MOVER = "4" * 64
TIER = "prismabuild-stage:dl380g10"
GIB = storage_tiers.GIB


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


@pytest.fixture()
def stage(tmp_path: Path) -> Path:
    root = tmp_path / "stage"
    root.mkdir()
    return root


def _staged_file(stage: Path, relative: str, size: int = 4096) -> Path:
    path = stage / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\0" * size)
    return path


def _held_mover(queue: pool.PoolQueue, stage: Path, path: Path) -> None:
    """A finished mover holding two tokens for one staged file, by fragment."""

    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    ledger = queue.tier_ledger(TIER)
    assert ledger.acquire(MOVER, {"stage_gib": 2})
    queue.record_move(MOVER, {"consumer_action_key": CONSUMER, "tier_id": TIER,
                              "stage_root": str(stage), "complete": True,
                              "bytes_staged": 2 * GIB})
    residency_map.write_fragment(queue.root / pool.RESIDENCY, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": MOVER,
        "tier_id": TIER, "stage_root": str(stage), "manifest_sha256": "a" * 64,
        "entries": {residency_map.residency_map_key("/mnt/shared/x", 0): {
            "stage_path": str(path), "bytes": path.stat().st_size,
            "sha256": "b" * 64, "offset": 0}},
    })


def _marker(stage: Path) -> dict[str, object]:
    return json.loads((stage / stage_release.STAGE_ROOT_MARKER).read_text())


# -- registration ------------------------------------------------------------


def test_registering_writes_the_marker_and_is_idempotent(queue, stage) -> None:
    assert stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage) == "registered"
    marker = _marker(stage)
    assert marker["schema"] == stage_release.STAGE_ROOT_MARKER_SCHEMA_V1
    assert marker["queue_root"] == os.path.realpath(str(queue.root))
    assert marker["tier_id"] == TIER
    first = (stage / stage_release.STAGE_ROOT_MARKER).stat().st_mtime_ns

    assert stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage) == "registered"

    assert (stage / stage_release.STAGE_ROOT_MARKER).stat().st_mtime_ns == first, \
        "an already-correct marker is left alone, not rewritten every cycle"
    assert stage_release.stage_root_refusal(queue, stage) is None


def test_registering_never_takes_a_root_from_another_queue(queue, stage, tmp_path) -> None:
    """Two owners is the state this refuses; an operator moves a marker by hand."""

    other = pool.PoolQueue(tmp_path / "other-queue")
    other.ensure_layout()
    assert stage_release.register_stage_root(other, tier_id=TIER, stage_root=stage) == "registered"

    outcome = stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)

    assert outcome.startswith("stage_root_belongs_to_another_queue")
    assert _marker(stage)["queue_root"] == os.path.realpath(str(other.root))


def test_a_root_this_queue_cannot_write_is_reported_not_raised(queue, stage) -> None:
    """The Sparks mount the stage read-only; the tier is still announced."""

    if os.geteuid() == 0:
        pytest.skip("root writes anywhere")
    stage.chmod(0o500)
    try:
        outcome = stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    finally:
        stage.chmod(0o700)

    assert outcome.startswith("stage_root_marker_unwritable")
    assert not (stage / stage_release.STAGE_ROOT_MARKER).exists()
    assert stage_release.stage_root_refusal(queue, stage) == "stage_root_unregistered"


def test_a_missing_root_is_unwritable_too(queue, tmp_path) -> None:
    outcome = stage_release.register_stage_root(
        queue, tier_id=TIER, stage_root=tmp_path / "never-mounted")

    assert outcome.startswith("stage_root_marker_unwritable")


# -- the three deleters refuse -----------------------------------------------


def test_a_reconcile_of_an_unregistered_root_deletes_nothing(queue, stage) -> None:
    nobodys = _staged_file(stage, "models/shard-0.safetensors")
    partial = _staged_file(stage, "models/.shard-1.safetensors.partial")

    receipt = stage_release.reconcile(queue, tier_id=TIER, stage_root=str(stage),
                                      wanted=set())

    assert receipt["skipped"] == "stage_root_unregistered"
    assert receipt["event"] == stage_release.STAGE_ROOT_REFUSED_EVENT
    assert receipt["complete"] is False
    assert receipt["entries_deleted"] == 0 and receipt["partials_deleted"] == 0
    assert nobodys.exists() and partial.exists()


def test_an_egress_on_an_unregistered_root_keeps_bytes_and_tokens(queue, stage) -> None:
    path = _staged_file(stage, "models/shard-0.safetensors")
    _held_mover(queue, stage, path)

    receipt = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                                  stage_root=str(stage))

    assert receipt["skipped"] == "stage_root_unregistered"
    assert receipt["complete"] is False
    assert receipt["entries_deleted"] == 0 and receipt["tokens_released"] == 0
    assert path.exists()
    assert queue.tier_ledger(TIER).holder_tokens(MOVER) == {"stage_gib": 2}
    fragment = residency_map.fragment_path(queue.root / pool.RESIDENCY, CONSUMER, MOVER)
    assert fragment.exists(), "a refused egress keeps the fragment for the owner"


def test_a_sweep_of_an_unregistered_root_refuses_once_per_tier(queue, stage) -> None:
    """Neither the held-key evictions nor the reconciliation run."""

    path = _staged_file(stage, "models/shard-0.safetensors")
    _held_mover(queue, stage, path)          # an orphan: no live item names it
    nobodys = _staged_file(stage, "models/nobodys.bin")

    swept = stage_release.sweep(queue, stage_roots={TIER: str(stage)})

    assert [r["event"] for r in swept] == [stage_release.STAGE_ROOT_REFUSED_EVENT]
    assert swept[0]["tier_id"] == TIER
    assert swept[0]["skipped"] == "stage_root_unregistered"
    assert path.exists() and nobodys.exists()
    assert queue.tier_ledger(TIER).holder_tokens(MOVER) == {"stage_gib": 2}


def test_another_queues_root_is_refused_by_name(queue, stage, tmp_path) -> None:
    other = pool.PoolQueue(tmp_path / "other-queue")
    other.ensure_layout()
    stage_release.register_stage_root(other, tier_id=TIER, stage_root=stage)
    nobodys = _staged_file(stage, "models/nobodys.bin")

    receipt = stage_release.reconcile(queue, tier_id=TIER, stage_root=str(stage),
                                      wanted=set())

    assert receipt["skipped"] == (
        f"stage_root_belongs_to_another_queue: {os.path.realpath(str(other.root))}")
    assert nobodys.exists()


def test_a_marker_that_is_not_a_marker_refuses(queue, stage) -> None:
    (stage / stage_release.STAGE_ROOT_MARKER).write_text("not json")
    nobodys = _staged_file(stage, "models/nobodys.bin")

    receipt = stage_release.reconcile(queue, tier_id=TIER, stage_root=str(stage),
                                      wanted=set())

    assert receipt["skipped"].startswith("stage_root_marker_invalid")
    assert nobodys.exists()


# -- a registered root behaves as before, and keeps its marker ---------------


def test_a_registered_root_is_swept_and_the_marker_survives(queue, stage) -> None:
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    nobodys = _staged_file(stage, "models/nobodys.bin")
    path = _staged_file(stage, "models/shard-0.safetensors")
    _held_mover(queue, stage, path)

    swept = stage_release.sweep(queue, stage_roots={TIER: str(stage)})

    assert [r["reason"] for r in swept] == ["orphan-sweep", "unattributed-reconcile"]
    assert not path.exists() and not nobodys.exists()
    assert queue.tier_ledger(TIER).holder_tokens(MOVER) == {}
    assert (stage / stage_release.STAGE_ROOT_MARKER).is_file(), \
        "the sweep must not delete the fact that lets it sweep"
    assert stage_release.stage_root_refusal(queue, stage) is None


# -- the tier loop registers its own box's stage, and only that --------------


def _record(stage: Path, host: str = "dl380g10") -> dict[str, object]:
    return {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
            "tier_id": f"prismabuild-stage:{host}", "host": host,
            "tier": "stage", "mountpoint": str(stage),
            "capacity_bytes": 8 * GIB}


def test_the_loop_registers_the_stage_it_announces(queue, stage) -> None:
    announced = tier_loop.cycle(
        queue, host="dl380g10", source_pool="storage_pool",
        receipts=tier_loop.ReceiptCache(),
        discover=lambda **_kwargs: {TIER: _record(stage)})

    assert announced[0]["stage_root_owner"] == "registered"
    assert _marker(stage)["queue_root"] == os.path.realpath(str(queue.root))
    stored = {str(r["tier_id"]): r for r in queue.tiers()}[TIER]
    assert stored["stage_root_owner"] == "registered"


def test_another_boxs_stage_is_left_to_its_own_loop(queue, stage) -> None:
    record = _record(stage, host="elsewhere")
    announced = tier_loop.cycle(
        queue, host="dl380g10", source_pool="storage_pool",
        receipts=tier_loop.ReceiptCache(),
        discover=lambda **_kwargs: {record["tier_id"]: record})

    assert "stage_root_owner" not in announced[0]
    assert not (stage / stage_release.STAGE_ROOT_MARKER).exists()


def test_the_incidents_shape_deletes_nothing_now(queue, stage, tmp_path) -> None:
    """A tier cycle from a throwaway queue against a stage the fleet owns.

    This is what ran on the storage box on 2026-09-18: a test's queue under
    ``tmp_path``, the real mountpoint in the announced record, one cycle.  The
    fleet's marker is there, so the cycle announces the refusal instead of
    registering, and the sweep inside it touches nothing.
    """

    fleet = pool.PoolQueue(tmp_path / "prismabuild-fleet" / "pb-queue")
    fleet.ensure_layout()
    stage_release.register_stage_root(fleet, tier_id=TIER, stage_root=stage)
    shard = _staged_file(stage, "models/GLM-5.3-Flash/model-00001.safetensors")
    partial = _staged_file(stage, "models/GLM-5.3-Flash/.model-00002.safetensors.partial")

    announced = tier_loop.cycle(
        queue, host="dl380g10", source_pool="storage_pool",
        receipts=tier_loop.ReceiptCache(),
        discover=lambda **_kwargs: {TIER: _record(stage)})
    swept = stage_release.sweep(queue, stage_roots={TIER: str(stage)})

    assert announced[0]["stage_root_owner"].startswith("stage_root_belongs_to_another_queue")
    assert shard.exists() and partial.exists()
    assert [r["event"] for r in swept] == [stage_release.STAGE_ROOT_REFUSED_EVENT]
    assert _marker(stage)["queue_root"] == os.path.realpath(str(fleet.root))


# -- a root the loop cannot register offers no capacity (#631) ----------------


def test_an_unregistered_root_mints_no_capacity(queue, stage, tmp_path) -> None:
    """A full root that cannot take the marker must admit nothing more.

    On 2026-09-18 the movers filled ``prismabuild-stage/prewarm`` to the last
    byte before the loop wrote its ~300-byte ownership marker, and the loop
    then refused every deletion under the root -- including the egress rows
    that are the only way room is made.  A root the loop cannot register now
    mints zero occupancy, so a fresh root always marks before its first mover
    and a full one stops admitting instead of deadlocking.
    """

    other = pool.PoolQueue(tmp_path / "other-queue")
    other.ensure_layout()
    stage_release.register_stage_root(other, tier_id=TIER, stage_root=stage)

    announced = tier_loop.cycle(
        queue, host="dl380g10", source_pool="storage_pool",
        receipts=tier_loop.ReceiptCache(),
        discover=lambda **_kwargs: {TIER: _record(stage)})

    record = announced[0]
    assert record["stage_root_owner"].startswith("stage_root_belongs_to_another_queue")
    assert record["tokens"].get("stage_gib") is None
    assert "unregistered root offers no capacity" in str(record["capacity_basis"])
    ledger = queue.tier_ledger(TIER)
    assert ledger.available().get("stage_gib", 0) == 0
    # The refusal is loud on the announced record, and the sweep inside the
    # cycle refused on the same fact rather than deleting under it.
    stored = {str(r["tier_id"]): r for r in queue.tiers()}[TIER]
    assert stored["stage_root_owner"] == record["stage_root_owner"]


def test_an_unregistered_root_with_held_tokens_never_indexes_a_missing_kind(
    queue, stage, tmp_path,
) -> None:
    """The ``available()[kind]`` KeyError the issue notes, driven end to end.

    An in-flight mover holds 2 GiB against a root another queue owns.  The
    cycle mints nothing, keeps the held reservation, and runs the pressure,
    window and sweep reads over a ledger whose free set has no ``stage_gib``
    key at all -- every one of them through ``.get``, never ``[]``.
    """

    other = pool.PoolQueue(tmp_path / "other-queue")
    other.ensure_layout()
    stage_release.register_stage_root(other, tier_id=TIER, stage_root=stage)
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    assert queue.tier_ledger(TIER).acquire("5" * 64, {"stage_gib": 2})

    announced = tier_loop.cycle(
        queue, host="dl380g10", source_pool="storage_pool",
        receipts=tier_loop.ReceiptCache(),
        discover=lambda **_kwargs: {TIER: _record(stage)})
    swept = stage_release.sweep(queue, stage_roots={TIER: str(stage)})

    ledger = queue.tier_ledger(TIER)
    assert announced[0]["tokens"].get("stage_gib") is None
    assert ledger.capacity().get("stage_gib", 0) == 2  # held, not re-minted
    assert ledger.available().get("stage_gib", 0) == 0
    assert ledger.holder_tokens("5" * 64).get("stage_gib", 0) == 2
    assert [r["event"] for r in swept] == [stage_release.STAGE_ROOT_REFUSED_EVENT]


def test_a_fresh_root_registers_then_mints(queue, stage) -> None:
    """The ordering the gate exists to guarantee: mark first, admit after."""

    announced = tier_loop.cycle(
        queue, host="dl380g10", source_pool="storage_pool",
        receipts=tier_loop.ReceiptCache(),
        discover=lambda **_kwargs: {TIER: _record(stage)})

    record = announced[0]
    assert record["stage_root_owner"] == "registered"
    assert record["tokens"].get("stage_gib") == 8
    assert queue.tier_ledger(TIER).available().get("stage_gib", 0) == 8
