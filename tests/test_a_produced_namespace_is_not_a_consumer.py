"""A produced-output namespace is not a consumer directory (#798).

The global residency store now carries produced-output bookkeeping beside
the fragments: ``produced-output-templates/``, ``produced-output-scopes/``
and ``produced-output-batches/`` are records, and the produced *fragments*
live one level deeper, under ``produced-output-fragments/<namespace>/``.
The census that attributes staged bytes to their owners treated every
non-reserved child of the store as a flat consumer namespace:
``attributed_stage_paths`` called ``residency_map.read_fragments`` with the
directory name as a consumer key, and a name that is not a 64-character
action key raised ``ResidencyMapError`` out of
``tier_loop.cycle -> sweep_orphans -> stage_release.sweep -> reconcile``,
killing the tier service every cycle before it published a lead.

Skipping the produced namespaces wholesale is not the fix: produced
fragments vouch for real staged bytes and must protect them from the global
orphan reconciliation.  The census therefore traverses both layouts with
one strict reader; a directory it cannot classify, or a fragment it cannot
read or validate, is taint -- the pass deletes nothing and says why -- never
a crash and never an unowned file.

Fixtures build a tiny real produced state through the supported API
(``declare_template``/``bind_instance``/``admit_instance``/``commit_batch``
plus the real mover writing its fragment under ``output_fragment_root``)
next to a legacy flat consumer, and drive the actual
``tier_loop.sweep_orphans`` boundary rather than a helper.
"""
from __future__ import annotations

from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, produced_output as po, residency_map  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402

import test_produced_output_lifecycle_r2 as produced  # noqa: E402

TIER = produced.STAGE_TIER
KIND = produced.STAGE_BARE
LEGACY_CONSUMER = "a" * 64
LEGACY_MOVER = "b" * 64
PRODUCED_MOVER = produced.MOVER0
LEGACY_SOURCE = "/mnt/shared/legacy.bin"
LEGACY_BYTES = 4096


def _staged(stage: Path, name: str, size: int = LEGACY_BYTES) -> Path:
    path = stage / name
    path.write_bytes(b"\0" * size)
    return path


def _legacy_fragment(queue: pool.PoolQueue, stage: Path, staged: Path) -> None:
    residency_map.write_fragment(queue.root / pool.RESIDENCY, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": LEGACY_CONSUMER,
        "mover_action_key": LEGACY_MOVER,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": "1" * 64,
        "entries": {
            residency_map.residency_map_key(LEGACY_SOURCE, 0): {
                "stage_path": str(staged), "bytes": LEGACY_BYTES,
                "offset": 0, "sha256": "2" * 64}}})


def _register(queue: pool.PoolQueue, stage: Path) -> None:
    assert stage_release.register_stage_root(
        queue, tier_id=TIER, stage_root=str(stage)) == "registered"


def _sweep(queue: pool.PoolQueue, stage: Path) -> list[dict[str, object]]:
    return tier_loop.sweep_orphans(queue, {
        TIER: {"tier_id": TIER, "tier": "stage", "mountpoint": str(stage)}})


def _reconcile_receipt(events: list[dict[str, object]]) -> dict[str, object] | None:
    found = [event for event in events
             if event.get("reason") == "unattributed-reconcile"]
    return found[-1] if found else None


@pytest.fixture()
def legacy_only(tmp_path: Path):
    """One legacy flat consumer, its staged file and its held token."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.mint_tier_capacity(TIER, {KIND: 2})
    stage = tmp_path / "stage"
    stage.mkdir()
    _register(queue, stage)
    legacy = _staged(stage, "legacy.bin")
    _legacy_fragment(queue, stage, legacy)
    assert queue.tier_ledger(TIER).acquire(LEGACY_MOVER, {KIND: 1}) is True
    return SimpleNamespace(queue=queue, stage=stage, legacy_path=legacy)


@pytest.fixture()
def world(tmp_path: Path):
    """The real gap: legacy flat fragments plus a live produced batch."""

    origin = tmp_path / "outputs"
    origin.mkdir(parents=True)
    queue = produced._queue(tmp_path)
    stage = tmp_path / "stage"
    stage.mkdir()
    _register(queue, stage)
    legacy = _staged(stage, "legacy.bin")
    _legacy_fragment(queue, stage, legacy)

    template = produced._template(str(origin))
    bound = produced._bind_live(queue, tmp_path, template)
    instance = bound["instance"]
    assert po.admit_instance(queue, instance, template)["ok"] is True
    assert po.require_prewrite(
        queue, instance, template, batch_id="w0", tier=TIER,
        class_bytes={"payload": 64, "checkpoint": 0, "temp": 0},
        paths=[str(origin / "w0.pt")])["ok"] is True
    desc = produced._payload_desc(origin, template, instance, "w0.pt", b"A" * 64)
    batch = po.commit_batch(queue, instance, template, [desc], batch_id="w0",
                            tier=TIER, mover_key=PRODUCED_MOVER)
    assert batch["ok"] is True, batch
    out_base = po.output_fragment_root(queue.root / pool.RESIDENCY)
    moved = produced._move(queue, batch, origin, stage, out_base, tmp_path, "w0")
    assert moved["complete"] is True, moved
    namespace = str(batch["batch_namespace"])
    fragments = residency_map.read_fragments(out_base, namespace)
    assert len(fragments) == 1, fragments
    produced_path = Path(str(next(
        iter(fragments[0]["entries"].values()))["stage_path"]))
    assert produced_path.exists()
    # The produced store carries reader_lease bookkeeping too: the mover's
    # material sidecar (real, written by the copy) and, when a pin is taken
    # with this root, a leases directory.  Neither is a fragment namespace.
    assert (out_base / "material").is_dir()
    leases = out_base / "leases"
    leases.mkdir(exist_ok=True)
    (leases / "not-a-fragment.json").write_text("{}\n")
    # Both movers are resident: their tier tokens are what makes their
    # fragments attribution in the sweep's ``wanted`` set.
    assert queue.tier_ledger(TIER).acquire(LEGACY_MOVER, {KIND: 1}) is True
    assert queue.tier_ledger(TIER).acquire(PRODUCED_MOVER, {KIND: 1}) is True
    return SimpleNamespace(
        queue=queue, stage=stage, origin=origin, out_base=out_base,
        legacy_path=legacy, produced_path=produced_path, namespace=namespace,
        ns_dir=out_base / namespace, template=template, instance=instance,
        batch=batch)


# ------------------------------------------------------- the crash itself


def test_a_tier_sweep_over_produced_namespaces_does_not_crash(world) -> None:
    """The real cycle boundary: no raise, nothing deleted, nothing unknown."""

    legacy_held = world.queue.tier_ledger(TIER).holder_tokens(LEGACY_MOVER)
    produced_held = world.queue.tier_ledger(TIER).holder_tokens(PRODUCED_MOVER)
    assert legacy_held and produced_held

    events = _sweep(world.queue, world.stage)

    assert events == [], events
    assert world.legacy_path.exists()
    assert world.produced_path.exists()
    assert world.queue.tier_ledger(TIER).holder_tokens(
        LEGACY_MOVER) == legacy_held
    assert world.queue.tier_ledger(TIER).holder_tokens(
        PRODUCED_MOVER) == produced_held


def test_both_fragment_layouts_are_attributed(world) -> None:
    """The attribution census reads the produced namespace, not just skips it."""

    both = stage_release.attributed_stage_paths(
        world.queue, wanted={LEGACY_MOVER, PRODUCED_MOVER})
    assert both == {str(world.legacy_path), str(world.produced_path)}
    assert stage_release.attributed_stage_paths(
        world.queue, wanted={LEGACY_MOVER}) == {str(world.legacy_path)}
    assert stage_release.attributed_stage_paths(
        world.queue, wanted={PRODUCED_MOVER}) == {str(world.produced_path)}


def test_a_produced_fragment_protects_a_shared_stage_path(world) -> None:
    """A produced-layout co-owner keeps protecting a physical staged file."""

    produced_ns = "f" * 64
    other_mover = "9" * 64
    residency_map.write_fragment(world.out_base, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": produced_ns,
        "mover_action_key": other_mover,
        "tier_id": TIER, "stage_root": str(world.stage),
        "manifest_sha256": "3" * 64,
        "entries": {
            residency_map.residency_map_key(LEGACY_SOURCE, 0): {
                "stage_path": str(world.legacy_path), "bytes": LEGACY_BYTES,
                "offset": 0, "sha256": "4" * 64}}})

    owners, taint = stage_release._fragment_owners(
        world.queue.root / pool.RESIDENCY, {str(world.legacy_path)},
        except_consumer=LEGACY_CONSUMER, except_mover=LEGACY_MOVER)

    assert taint == []
    assert owners == {str(world.legacy_path): {(produced_ns, other_mover)}}


def test_the_walked_root_is_the_exclusion_namespace(world) -> None:
    """Self-exclusion is scoped to the root being walked, not to a key alone."""

    # A base-store walk: the nested produced fragment is another namespace
    # domain, so naming its key as the exception must not drop its proof.
    owners, taint = stage_release._fragment_owners(
        world.queue.root / pool.RESIDENCY, {str(world.produced_path)},
        except_consumer=world.namespace, except_mover=PRODUCED_MOVER)
    assert taint == []
    assert owners == {
        str(world.produced_path): {(world.namespace, PRODUCED_MOVER)}}

    # The produced store's own child is that mover's self, and is excluded.
    owners, taint = stage_release._fragment_owners(
        world.out_base, {str(world.produced_path)},
        except_consumer=world.namespace, except_mover=PRODUCED_MOVER)
    assert taint == []
    assert owners == {}


def test_a_produced_root_walk_sees_a_legacy_coowner(world) -> None:
    """The reverse direction: produced-root caller, foreign legacy co-owner."""

    co_consumer = "7" * 64
    co_mover = "8" * 64
    residency_map.write_fragment(world.queue.root / pool.RESIDENCY, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": co_consumer, "mover_action_key": co_mover,
        "tier_id": TIER, "stage_root": str(world.stage),
        "manifest_sha256": "5" * 64,
        "entries": {
            residency_map.residency_map_key(LEGACY_SOURCE, 0): {
                "stage_path": str(world.produced_path), "bytes": LEGACY_BYTES,
                "offset": 0, "sha256": "6" * 64}}})

    # Excluding the produced mover's own key leaves the legacy co-owner of
    # the same physical staged path fully protected.
    owners, taint = stage_release._fragment_owners(
        world.out_base, {str(world.produced_path)},
        except_consumer=world.namespace, except_mover=PRODUCED_MOVER)
    assert taint == []
    assert owners == {str(world.produced_path): {(co_consumer, co_mover)}}

    # And naming the legacy key as the exception from the produced root does
    # not drop it either: it is not filed in the walked root's namespace.
    owners, taint = stage_release._fragment_owners(
        world.out_base, {str(world.produced_path)},
        except_consumer=co_consumer, except_mover=co_mover)
    assert taint == []
    assert owners == {
        str(world.produced_path): {
            (world.namespace, PRODUCED_MOVER), (co_consumer, co_mover)}}


# ------------------------------------------------------- unclean states


def test_a_true_orphan_is_still_cleaned_up(world) -> None:
    """Attribution did not become a blanket keep: unowned bytes still go."""

    orphan = _staged(world.stage, "orphan.bin", 128)

    events = _sweep(world.queue, world.stage)

    receipt = _reconcile_receipt(events)
    assert receipt is not None, events
    assert receipt["complete"] is True, receipt
    assert receipt["entries_deleted"] == 1, receipt
    assert receipt["bytes_deleted"] == 128, receipt
    assert not orphan.exists()
    assert world.legacy_path.exists()
    assert world.produced_path.exists()


def test_an_unknown_residency_directory_retains(world) -> None:
    """A directory the census cannot classify is unknown ownership."""

    orphan = _staged(world.stage, "orphan.bin", 128)
    unknown = world.queue.root / pool.RESIDENCY / "not-a-namespace"
    unknown.mkdir()
    (unknown / "record.json").write_text("{}\n")

    events = _sweep(world.queue, world.stage)

    receipt = _reconcile_receipt(events)
    assert receipt is not None, events
    assert receipt["complete"] is False, receipt
    assert receipt["entries_deleted"] == 0, receipt
    assert receipt["skipped"] == "attribution_unreadable", receipt
    assert "not-a-namespace" in " ".join(receipt["errors"]), receipt
    assert orphan.exists()
    assert world.legacy_path.exists()
    assert world.produced_path.exists()


def test_a_symlinked_produced_container_taints_instead_of_recursing(world) -> None:
    """A link back into the store is unknown ownership, never a walk."""

    orphan = _staged(world.stage, "orphan.bin", 128)
    (world.out_base / po.OUTPUT_FRAGMENTS_SUBDIR).symlink_to(
        "..", target_is_directory=True)

    events = _sweep(world.queue, world.stage)

    receipt = _reconcile_receipt(events)
    assert receipt is not None, events
    assert receipt["complete"] is False, receipt
    assert receipt["entries_deleted"] == 0, receipt
    assert receipt["skipped"] == "attribution_unreadable", receipt
    assert orphan.exists()
    assert world.produced_path.exists()


def test_a_nested_produced_container_is_unknown(world) -> None:
    """One produced container, at the base store; a second is not a layout."""

    orphan = _staged(world.stage, "orphan.bin", 128)
    (world.out_base / po.OUTPUT_FRAGMENTS_SUBDIR).mkdir()

    events = _sweep(world.queue, world.stage)

    receipt = _reconcile_receipt(events)
    assert receipt is not None, events
    assert receipt["complete"] is False, receipt
    assert receipt["entries_deleted"] == 0, receipt
    assert receipt["skipped"] == "attribution_unreadable", receipt
    assert orphan.exists()
    assert world.produced_path.exists()


def test_a_misfiled_mover_cannot_vanish_as_self(world) -> None:
    """A record whose name disagrees with its mover is unknown, never self."""

    orphan = _staged(world.stage, "orphan.bin", 128)
    source = world.ns_dir / f"{PRODUCED_MOVER}.json"
    misfiled = world.ns_dir / f"{'0' * 64}.json"
    misfiled.write_text(source.read_text())

    # The valid record still vouches, and the mismatched duplicate is
    # reported rather than dropped; naming the mover as the exception must
    # not make the misfiled record vanish.
    owners, taint = stage_release._fragment_owners(
        world.out_base, {str(world.produced_path)},
        except_consumer=world.namespace, except_mover=PRODUCED_MOVER)
    assert any("another mover" in item for item in taint), taint
    assert owners == {}

    events = _sweep(world.queue, world.stage)

    receipt = _reconcile_receipt(events)
    assert receipt is not None, events
    assert receipt["complete"] is False, receipt
    assert receipt["entries_deleted"] == 0, receipt
    assert receipt["skipped"] == "attribution_unreadable", receipt
    assert orphan.exists()
    assert world.produced_path.exists()
    assert misfiled.exists()


def test_a_corrupt_legacy_fragment_is_not_read_as_unowned(legacy_only) -> None:
    """read_fragments' reader tolerance must not become a GC deletion."""

    orphan = _staged(legacy_only.stage, "orphan.bin", 128)
    fragment = (legacy_only.queue.root / pool.RESIDENCY / LEGACY_CONSUMER
                / f"{LEGACY_MOVER}.json")
    fragment.write_text('{"schema": "prismaquant.prismabuild.residency_map')

    events = _sweep(legacy_only.queue, legacy_only.stage)

    receipt = _reconcile_receipt(events)
    assert receipt is not None, events
    assert receipt["complete"] is False, receipt
    assert receipt["entries_deleted"] == 0, receipt
    assert orphan.exists()
    assert legacy_only.legacy_path.exists()
    assert fragment.exists()


def test_a_corrupt_produced_fragment_retains(world) -> None:
    """The same strict rule one level into the produced namespace."""

    orphan = _staged(world.stage, "orphan.bin", 128)
    corrupt = world.ns_dir / f"{PRODUCED_MOVER}.json"
    corrupt.write_text('{"schema": "prismaquant.prismabuild.residency_map')

    events = _sweep(world.queue, world.stage)

    receipt = _reconcile_receipt(events)
    assert receipt is not None, events
    assert receipt["complete"] is False, receipt
    assert receipt["entries_deleted"] == 0, receipt
    assert receipt["skipped"] == "attribution_unreadable", receipt
    assert orphan.exists()
    assert world.produced_path.exists()
