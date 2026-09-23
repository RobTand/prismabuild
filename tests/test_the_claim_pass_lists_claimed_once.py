"""One claim pass lists ``claimed/`` once, however many items are ready (#993).

Before this, ``_claim`` listed ``claimed/`` under every ready item's
transition lock, so one pass over 40 ready items cost 40 ``READDIR``s of one
directory on the NFS export, on every loop of every box.  The listing is now
taken once per pass, after the first lock acquisition, and the one decision
that a stale listing could get wrong -- a rename over a live claim record --
is re-checked for its own key, fresh, just before the rename.

``passes/`` is read only for records this box could place: an item it will
skip on placement is skipped whatever its aging count, so reading that
count cost one sidecar read per foreign item per poll for nothing.

Nothing here touches the live queue, a real pool or a real device.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from prismabuild import adaptive_cpu, pool  # noqa: E402

READY_ITEMS = 40


def _publish(queue: pool.PoolQueue, index: int, tags: list[str]) -> str:
    key = f"{index + 1:064x}"
    queue.publish(action_key=key, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py",
                  resources={"cpu": 1}, tags=tags)
    return key


def _count_listings(monkeypatch: pytest.MonkeyPatch,
                    directory: Path) -> list[str]:
    """Every ``os.listdir``/``os.scandir`` of ``directory`` from now on."""

    calls: list[str] = []
    real_listdir, real_scandir = os.listdir, os.scandir

    def listdir(path=".", *args, **kwargs):  # type: ignore[no-untyped-def]
        if isinstance(path, (str, os.PathLike)) and Path(path) == directory:
            calls.append("listdir")
        return real_listdir(path, *args, **kwargs)

    def scandir(path=".", *args, **kwargs):  # type: ignore[no-untyped-def]
        if isinstance(path, (str, os.PathLike)) and Path(path) == directory:
            calls.append("scandir")
        return real_scandir(path, *args, **kwargs)

    monkeypatch.setattr(os, "listdir", listdir)
    monkeypatch.setattr(os, "scandir", scandir)
    return calls


def _count_passes_reads(monkeypatch: pytest.MonkeyPatch,
                        queue: pool.PoolQueue) -> list[str]:
    """Every ``passes/`` sidecar the pool reads from now on, by key."""

    reads: list[str] = []
    real = pool._read_json
    passes = queue.root / pool.PASSES

    def reading(path, *args, **kwargs):  # type: ignore[no-untyped-def]
        if Path(path).parent == passes:
            reads.append(Path(path).stem)
        return real(path, *args, **kwargs)

    monkeypatch.setattr(pool, "_read_json", reading)
    return reads


def _denials(queue: pool.PoolQueue) -> dict[str, str]:
    path = adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    return {str(value["action_key"]): str(value["reason"])
            for value in records.values()}


def test_a_pass_over_40_foreign_items_lists_claimed_once(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    for index in range(READY_ITEMS):
        _publish(queue, index, ["elsewhere"])
    listings = _count_listings(monkeypatch, queue.dir(pool.CLAIMED))
    assert queue.claim(tags=["here"], owner="worker") is None
    assert len(listings) == 1, listings
    # Every item was still judged, and said why it was skipped.
    denials = _denials(queue)
    assert len(denials) == READY_ITEMS
    assert set(denials.values()) == {"placement_mismatch"}


def test_a_pass_reads_passes_only_for_items_it_could_place(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    foreign = [_publish(queue, index, ["elsewhere"])
               for index in range(READY_ITEMS)]
    mine = _publish(queue, READY_ITEMS, ["here"])
    reads = _count_passes_reads(monkeypatch, queue)
    claimed = queue.claim(tags=["here"], owner="worker")
    assert claimed is not None and claimed["action_key"] == mine
    assert mine in reads
    assert not set(reads) & set(foreign), sorted(set(reads) & set(foreign))


def test_aging_still_orders_the_items_this_box_can_place(
        tmp_path: Path) -> None:
    """Reading ``passes/`` lazily does not change the order it decides."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    older = _publish(queue, 0, ["here"])
    aged = _publish(queue, 1, ["here"])
    queue.passes_path(aged).parent.mkdir(parents=True, exist_ok=True)
    queue.passes_path(aged).write_text(json.dumps(
        {"action_key": aged, "passes": 5}) + "\n")
    claimed = queue.claim(tags=["here"], owner="worker")
    assert claimed is not None and claimed["action_key"] == aged
    assert older != aged


def test_a_pass_that_claims_still_lists_claimed_once(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    for index in range(READY_ITEMS - 1):
        _publish(queue, index, ["elsewhere"])
    mine = _publish(queue, READY_ITEMS - 1, ["here"])
    listings = _count_listings(monkeypatch, queue.dir(pool.CLAIMED))
    claimed = queue.claim(tags=["here"], owner="worker")
    assert claimed is not None and claimed["action_key"] == mine
    assert len(listings) == 1, listings


def test_a_claim_record_filed_after_the_listing_is_never_renamed_over(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The pass listing is a hint; the rename's own key is checked fresh.

    A claim record for a key the pass has not reached yet can appear after
    the pass listed ``claimed/`` (another box's claim beside a requeued ready
    record, or residue a crash left mid-transition).  ``os.rename`` replaces
    its destination, so renaming over it would lose that claim.  The pass
    must refuse the key, exactly as a fresh listing under its lock did.
    """

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    _publish(queue, 0, ["elsewhere"])       # the first lock: the listing
    late = _publish(queue, 1, ["here"])     # reached after the listing
    claimed_dir = queue.dir(pool.CLAIMED)
    residue = {"action_key": late, "claimed_by": "another-box",
               "claimed_unix": 1.0}
    real_listdir = os.listdir

    def listdir(path=".", *args, **kwargs):  # type: ignore[no-untyped-def]
        names = real_listdir(path, *args, **kwargs)
        if (isinstance(path, (str, os.PathLike)) and Path(path) == claimed_dir
                and not (claimed_dir / f"{late}.json").exists()):
            (claimed_dir / f"{late}.json").write_text(json.dumps(residue))
        return names

    monkeypatch.setattr(os, "listdir", listdir)
    assert queue.claim(tags=["here"], owner="worker") is None
    assert json.loads((claimed_dir / f"{late}.json").read_text()) == residue
    assert queue.item_path(pool.READY, late).exists()
    assert _denials(queue).get(late) == "already_claimed"
