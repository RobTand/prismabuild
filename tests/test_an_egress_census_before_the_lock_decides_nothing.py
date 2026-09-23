"""The census an egress takes before the stage lock decides nothing (#988).

Since #988 ``stage_release.evict`` and ``stage_release.reconcile`` take their
ownership censuses once before the stage ownership lock, as hints, so the
pass under the lock re-parses only the documents that changed.  Everything
the delete decision stands on is still taken under the lock.  Each test here
changes ownership in the gap between the hint and the lock -- a reader's pin,
a co-owner's fragment, a fragment rewritten in place, a file replaced after
the reconciliation's walk -- and checks that the pass under the lock sees
the change and keeps the file.

Each gap is reached by wrapping the last helper that runs before the lock
(``_entry_fences`` for the egress, ``_unattributed_candidates`` for the
reconciliation), so no test sleeps.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, reader_lease, residency_map, storage_tiers  # noqa: E402
import stage_release  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
SIZE = 4096
DIGEST = "b" * 64
ATTEMPT = {"nonce": "n1", "scope_id": "s1"}
HOLDER = {"host": "test-host", "pid": 4242}


def _key(label: str) -> str:
    return hashlib.sha256(f"test-988-hint:{label}".encode()).hexdigest()


CONSUMER = _key("consumer")
MOVER = _key("mover")
MANIFEST = _key("manifest")
OTHER_CONSUMER = _key("other-consumer")
OTHER_MOVER = _key("other-mover")


@pytest.fixture()
def fleet(tmp_path: Path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    stage = tmp_path / "stage"
    stage.mkdir()
    stage = stage.resolve()
    assert stage_release.register_stage_root(
        queue, tier_id=TIER, stage_root=stage) == "registered"
    return queue, stage, queue.root / pool.RESIDENCY


def _staged(stage: Path, number: int) -> Path:
    # Same-length names, so one fragment can be rewritten in place to name
    # another entry without changing its size.
    return stage / "landed" / f"part-{number}.bin"


def _fragment(root: Path, stage: Path, consumer: str, mover: str,
              paths: dict[str, Path]) -> Path:
    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage), "manifest_sha256": MANIFEST,
        "entries": {residency_map.residency_map_key(source, 0): {
            "stage_path": str(path), "bytes": SIZE, "sha256": DIGEST,
            "offset": 0} for source, path in paths.items()}})
    return residency_map.fragment_path(root, consumer, mover)


def _land(queue: pool.PoolQueue, stage: Path, root: Path) -> list[Path]:
    """Three staged entries under one complete mover that holds a token."""

    paths = {f"/pool/part-{number}.bin": _staged(stage, number)
             for number in range(3)}
    for path in paths.values():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"\0" * SIZE)
    _fragment(root, stage, CONSUMER, MOVER, paths)
    reader_lease.write_material(
        root, consumer_action_key=CONSUMER, mover_action_key=MOVER,
        tier_id=TIER, stage_root=str(stage), manifest_sha256=MANIFEST,
        generation=reader_lease.mint_generation(),
        entries={residency_map.residency_map_key(source, 0): {
            "stage_path": str(path), "bytes": SIZE, "sha256": DIGEST,
            "file_id": reader_lease.stat_identity(str(path))}
            for source, path in paths.items()})
    queue.publish(action_key=MOVER, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py",
                  resources={"cpu": 1, "mem_gb": 1, STAGE_KIND: 1},
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                             "tier_id": TIER, "manifest_sha256": MANIFEST,
                             "manifest_bytes": 1 << 40,
                             "range_start_bytes": 0,
                             "range_end_bytes": storage_tiers.GIB},
                  max_attempts=1, retry_safe=False)
    claimed = queue.claim(capacity={"cpu": 4, "mem_gb": 8}, tags=["dl380g10"])
    assert claimed is not None and claimed["action_key"] == MOVER
    queue.record_move(MOVER, {
        "consumer_action_key": CONSUMER, "tier_id": TIER,
        "stage_root": str(stage), "manifest_sha256": MANIFEST,
        "range_start_bytes": 0, "range_end_bytes": storage_tiers.GIB,
        "bytes_staged": storage_tiers.GIB, "complete": True})
    queue.finish(MOVER, status="executed")
    assert queue.tier_ledger(TIER).holder_tokens(MOVER) == {"stage_gib": 1}
    return list(paths.values())


def _between_the_hint_and_the_lock(monkeypatch, act) -> list[int]:
    """Run ``act`` once, after the egress's hint census, before its lock."""

    original = stage_release._entry_fences
    calls: list[int] = []

    def fences(*args, **kwargs):
        if not calls:
            act()
        calls.append(1)
        return original(*args, **kwargs)

    monkeypatch.setattr(stage_release, "_entry_fences", fences)
    return calls


def _evict(queue: pool.PoolQueue, stage: Path) -> dict[str, object]:
    return stage_release.evict(
        queue, MOVER, consumer_action_key=CONSUMER, stage_root=str(stage),
        reason="beyond-horizon", whole=True)


def test_a_pin_filed_after_the_hint_declines_the_whole_egress(
        fleet, monkeypatch) -> None:
    """A reader that pins in the gap is seen under the lock: nothing goes."""

    queue, stage, root = fleet
    paths = _land(queue, stage, root)

    def pin() -> None:
        got = reader_lease.acquire(
            queue, consumer_action_key=CONSUMER, attempt=ATTEMPT,
            tier_id=TIER, epoch="", span={"start_bytes": 0, "end_bytes": SIZE},
            holder=HOLDER, acquire_token="t-988",
            covers=[{"mover_action_key": MOVER, "manifest_sha256": MANIFEST}])
        assert got["ok"], got

    calls = _between_the_hint_and_the_lock(monkeypatch, pin)
    receipt = _evict(queue, stage)

    assert calls, "the gap was never reached"
    assert receipt["complete"] is False, receipt
    assert "pinned" in receipt["declined"], receipt
    assert all(path.exists() for path in paths), "a pinned range went"
    assert queue.tier_ledger(TIER).holder_tokens(MOVER) == {"stage_gib": 1}
    assert residency_map.fragment_path(root, CONSUMER, MOVER).exists()


def test_a_co_owner_fragment_filed_after_the_hint_keeps_its_file(
        fleet, monkeypatch) -> None:
    """A second owner that vouches in the gap keeps the file it names."""

    queue, stage, root = fleet
    paths = _land(queue, stage, root)

    def co_own() -> None:
        _fragment(root, stage, OTHER_CONSUMER, OTHER_MOVER,
                  {"/pool/part-0.bin": paths[0]})

    _between_the_hint_and_the_lock(monkeypatch, co_own)
    receipt = _evict(queue, stage)

    assert receipt["complete"] is True, receipt
    assert receipt["entries_shared"] == 1, receipt
    assert receipt["entries_deleted"] == 2, receipt
    assert paths[0].exists(), "a co-owned file went"
    assert not paths[1].exists() and not paths[2].exists()


def test_a_fragment_rewritten_in_place_after_the_hint_is_read_again(
        fleet, monkeypatch) -> None:
    """Same inode, same size: the version fence still sees the rewrite.

    A co-owner's fragment names an unrelated file when the hint parses it,
    and is then rewritten in place -- same inode, same length -- to name one
    of the egressing mover's files.  Only mtime and ctime moved, and the
    pass under the lock must re-read it rather than reuse the hint's parse.
    """

    queue, stage, root = fleet
    paths = _land(queue, stage, root)
    unrelated = _staged(stage, 7)
    unrelated.write_bytes(b"\0" * SIZE)
    fragment = _fragment(root, stage, OTHER_CONSUMER, OTHER_MOVER,
                         {"/pool/part-0.bin": unrelated})
    before = fragment.read_bytes()
    after = before.replace(str(unrelated).encode(), str(paths[0]).encode())
    assert len(after) == len(before) and after != before
    inode = os.stat(fragment).st_ino

    def rewrite() -> None:
        with open(fragment, "r+b") as stream:
            stream.write(after)

    _between_the_hint_and_the_lock(monkeypatch, rewrite)
    receipt = _evict(queue, stage)

    assert os.stat(fragment).st_ino == inode, "the rewrite kept its inode"
    assert receipt["entries_shared"] == 1, receipt
    assert paths[0].exists(), "a stale parse let a co-owned file go"
    assert not paths[1].exists() and not paths[2].exists()


def test_the_receipt_records_the_census_before_the_lock_and_the_hold(
        fleet) -> None:
    queue, stage, root = fleet
    _land(queue, stage, root)
    receipt = _evict(queue, stage)

    assert receipt["complete"] is True, receipt
    assert receipt["entries_judged"] == 3
    for field in stage_release.LOCK_SCOPE_FIELDS:
        assert isinstance(receipt[field], (int, float)), (field, receipt)
    assert receipt["unlink_s"] <= receipt["lock_held_s"]
    assert receipt["census_validate_s"] <= receipt["lock_held_s"]


def _orphan(stage: Path, name: str) -> Path:
    path = stage / "orphans" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\0" * SIZE)
    return path


def _after_the_walk(monkeypatch, act) -> None:
    original = stage_release._unattributed_candidates

    def candidates(*args, **kwargs):
        found = original(*args, **kwargs)
        act()
        return found

    monkeypatch.setattr(stage_release, "_unattributed_candidates", candidates)


def test_reconcile_leaves_a_file_that_changed_after_its_walk(
        fleet, monkeypatch) -> None:
    """The walk's identity must still hold under the lock, or the file stays."""

    queue, stage, _root = fleet
    changed = _orphan(stage, ".changed.bin.partial")
    steady = _orphan(stage, ".steady.bin.partial")

    def grow() -> None:
        with open(changed, "ab") as stream:
            stream.write(b"\1")

    _after_the_walk(monkeypatch, grow)
    receipt = stage_release.reconcile(queue, tier_id=TIER,
                                      stage_root=str(stage), wanted=set())

    assert changed.exists(), "a file that changed after the walk went"
    assert not steady.exists()
    assert receipt["entries_deleted"] == 1, receipt
    assert receipt["entries_judged"] == 2, receipt
    assert receipt["left_since_walk"] == 1, receipt
    assert isinstance(receipt["lock_held_s"], float), receipt


def test_reconcile_keeps_a_file_a_fragment_names_after_its_walk(
        fleet, monkeypatch) -> None:
    """Attribution is re-read under the lock, not taken from the walk."""

    queue, stage, root = fleet
    named = _orphan(stage, ".named.bin.partial")

    def vouch() -> None:
        _fragment(root, stage, OTHER_CONSUMER, OTHER_MOVER,
                  {"/pool/named.bin": named})

    _after_the_walk(monkeypatch, vouch)
    receipt = stage_release.reconcile(queue, tier_id=TIER,
                                      stage_root=str(stage),
                                      wanted={OTHER_MOVER})

    assert named.exists(), "a file a wanted fragment names went"
    assert receipt["entries_deleted"] == 0, receipt
    assert receipt["left_since_walk"] == 1, receipt


def test_reconcile_skips_before_the_lock_while_a_mover_is_in_flight(
        fleet, monkeypatch) -> None:
    """A mover in flight skips the pass without queueing for the lock."""

    queue, stage, _root = fleet
    orphan = _orphan(stage, ".orphan.bin.partial")
    queue.publish(action_key=_key("in-flight"), cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py", tags=["dl380g10"],
                  resources={"cpu": 1, "mem_gb": 1, STAGE_KIND: 1},
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                             "tier_id": TIER, "manifest_sha256": MANIFEST,
                             "manifest_bytes": 8192,
                             "range_start_bytes": 0, "range_end_bytes": 8192})
    assert stage_release.movers_in_flight(queue, tier_id=TIER) == {
        _key("in-flight")}
    asked: list[str] = []
    original = queue.stage_ownership_lock

    def lock(*args, **kwargs):
        asked.append(str(args[0]) if args else "")
        return original(*args, **kwargs)

    monkeypatch.setattr(queue, "stage_ownership_lock", lock)
    receipt = stage_release.reconcile(queue, tier_id=TIER,
                                      stage_root=str(stage), wanted=set())

    assert receipt["skipped"] == "movers_in_flight", receipt
    assert orphan.exists()
    assert asked == [], "the skip queued for the stage lock"

