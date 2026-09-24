"""An uncharged dead owner's bytes are room the tier can take back (#1061).

Live shape (2026-09-23, owner ``ff696cef``): a failed consumer's executed
DONE mover filed a move receipt that never completed (4 of 5 entries), so
the ledger released its stage token when the mover finished.  Its staged
files, its fragment and its material sidecar stayed, and they are coherent:
nothing in them is stale, so the stale-mention prune (#853) has nothing to
do.  The held-key orphan pass reads only held keys, so it never sees the
owner, and the zero-charge dead-owner branch (#839, #866) excludes an owner
that carries material.  Those bytes can never be given back, however much
room the tier's window needs.

The mint already counts them correctly: supply is ZFS writable plus landed,
and a holder with an incomplete receipt counts as in flight, so bytes no
token holds sit outside writable and outside every token.  Charging these
owners would subtract their bytes twice.  They stay uncharged; what changes
is that the held-key pass takes them as candidates under pressure, oldest
first, crediting the bytes each eviction deletes against the room it needs.

Every fixture is a temp stage root registered to a fake queue (never a real
/stage or /ram).  The staged files are sparse: a 1 GiB file is a length,
not a payload, and nothing hashes it.
"""
from __future__ import annotations

import os
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(ROOT / "tests"))

import test_dead_owner_fragment_blocks_then_retires as base  # noqa: E402
from test_dead_owner_fragment_blocks_then_retires import fleet  # noqa: E402,F401
from prismabuild import pool, reader_lease, residency_map, storage_tiers  # noqa: E402
import prewarm_loop  # noqa: E402
import stage_release  # noqa: E402

TIER = base.TIER
GIB = storage_tiers.GIB
DIGEST = "b" * 64
MANIFEST = "a" * 64


@pytest.fixture(autouse=True)
def _fresh_process_state():
    stage_release.reset_skip_checkpoints()
    stage_release.reset_holder_reports()
    yield
    stage_release.reset_skip_checkpoints()
    stage_release.reset_holder_reports()


def _staged(stage: Path, name: str, size: int) -> Path:
    """One sparse staged file in its own ``.pbrange`` directory, marked.

    The prewarm mark is what every stage publication sets; without it the
    reconciliation could remove the file for a reason this file is not
    about.
    """

    path = stage / "models" / f"{name}.pbrange" / f"0-{size}"
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with open(path, "wb") as stream:
            stream.truncate(size)
        os.setxattr(path, prewarm_loop.STAGE_SOURCE_XATTR,
                    f"/originals/{name}@0".encode())
    return path


def dead_owner(fleet, names, *, unix: float, size: int = GIB,
               charged: bool = False) -> tuple[str, str]:
    """A failed consumer's executed DONE mover, coherent, with material.

    ``charged=False`` is the #1061 shape: the move receipt did not complete,
    so no stage token is held.  Each staged file is ``size`` bytes, and the
    fragment and the material date every one, so the owner is coherent and
    the stale-mention prune leaves it whole.  ``unix`` is the receipt's time,
    which orders the orphan pass.
    """

    queue, stage, _ = fleet
    root = queue.residency_fragment_root()
    consumer, _generation = base._fail_consumer(queue)
    mover = base._key()
    base._publish(queue, mover, max_attempts=1)
    mentions: dict[str, dict[str, object]] = {}
    entries: dict[str, dict[str, object]] = {}
    for name in names:
        path = _staged(stage, name, size)
        key = residency_map.residency_map_key(f"/pool/models/{name}", 0)
        mentions[key] = {"stage_path": str(path), "bytes": size,
                         "sha256": DIGEST,
                         "file_id": reader_lease.stat_identity(str(path))}
        entries[key] = {"stage_path": str(path), "bytes": size,
                        "sha256": DIGEST, "offset": 0}
    reader_lease.write_material(
        root, consumer_action_key=consumer, mover_action_key=mover,
        tier_id=TIER, stage_root=str(stage), manifest_sha256=MANIFEST,
        generation="c" * 32, entries=mentions)
    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": MANIFEST, "entries": entries})
    queue.record_move(mover, {
        "consumer_action_key": consumer, "tier_id": TIER,
        "stage_root": str(stage), "manifest_sha256": MANIFEST,
        "range_start_bytes": 0, "range_end_bytes": size * (len(names) + 1),
        "range_bytes": size * (len(names) + 1),
        "bytes_staged": size * len(names),
        "entries_declared": len(names) + 1, "entries_staged": len(names),
        "complete": charged, "errors": [], "seconds": 20.0, "unix": unix})
    queue.finish(mover, status="executed", detail={"returncode": 0})
    if charged:
        queue.mint_tier_capacity(TIER, {"stage_gib": len(names)})
        assert queue.tier_ledger(TIER).acquire(
            mover, {"stage_gib": len(names)}) is True
    else:
        assert mover not in queue.tier_ledger(TIER).held_keys()
    return consumer, mover


def _sweep(queue, stage, pressure):
    return stage_release.sweep(queue, stage_roots={TIER: str(stage)},
                               pressure=pressure)


def _owned(queue, consumer: str, mover: str) -> bool:
    """Whether the owner's fragment and material are both still filed."""

    root = queue.residency_fragment_root()
    fragment = residency_map.fragment_path(root, consumer, mover).exists()
    material = reader_lease.material_path(root, consumer, mover).exists()
    assert fragment == material, (fragment, material)
    return fragment


def _files(stage: Path, names) -> list[bool]:
    return [(stage / "models" / f"{name}.pbrange" / f"0-{GIB}").exists()
            for name in names]


def test_an_uncharged_coherent_owner_gives_its_room_under_pressure(fleet):
    """The #1061 red: under pressure, the owner's bytes must come back."""

    queue, stage, _ = fleet
    consumer, mover = dead_owner(fleet, ["solo-00001"], unix=100.0)
    # The dead-owner pass alone keeps it: coherent, so nothing to prune.
    first = stage_release.sweep_dead_owner_fragments(
        queue, stage_roots={TIER: str(stage)})
    assert [entry.get("retained_reason") for entry in first
            if entry.get("action_key") == mover] == [""], first
    assert _owned(queue, consumer, mover)

    receipts = _sweep(queue, stage, {TIER: 1})

    assert not _owned(queue, consumer, mover), (
        f"uncharged coherent dead owner {mover[:12]} kept its 1 GiB while "
        f"the tier needed 1 GiB: {receipts}")
    assert _files(stage, ["solo-00001"]) == [False]


def test_three_owners_and_one_owners_room_evicts_exactly_the_oldest(fleet):
    """Oldest first, and stop once the deleted bytes cover the room."""

    queue, stage, _ = fleet
    # Published out of age order, so the order is the receipt's time and
    # not the order the owners were built in.
    middle = dead_owner(fleet, ["age-00200"], unix=200.0)
    newest = dead_owner(fleet, ["age-00300"], unix=300.0)
    oldest = dead_owner(fleet, ["age-00100"], unix=100.0)

    receipts = _sweep(queue, stage, {TIER: 1})

    assert [_owned(queue, *owner) for owner in (oldest, middle, newest)] == [
        False, True, True], receipts
    assert _files(stage, ["age-00100", "age-00200", "age-00300"]) == [
        False, True, True]
