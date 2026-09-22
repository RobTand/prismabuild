"""A dead owner's fragment blocks publication until housekeeping retires it (#839).

Production shape (R3 forward-004): a FAILED consumer's WITHDRAWN mover leaves
a fragment naming a staged destination with no material sidecar, no move
receipt, and no held tokens. The shared publisher's proof search walks every
fragment, finds the vouch without a date, and refuses replacement even after
the grace -- while the held-key sweep never sees the owner (no tokens, no
receipt) and reconcile keeps the marked file as prewarm-owned. The obstruction
is permanent until something routes that exact stale owner through `evict`.

These use a real synthetic stage (temp stage root registered to a fake
queue, never real /stage or /ram): real queue rows for the terminal states,
a validated fragment, marked staged files, the real `_StagedPublisher._decide`
for the refusal, and the real `sweep`/`evict` for housekeeping. No payload is
hashed beyond the small fixture bytes.
"""
from __future__ import annotations

import os
import uuid
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

from prismabuild import pool, residency_map  # noqa: E402
import prewarm_loop  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
NAMES = ["model-00087-range.bin", "model-00090-prefix.bin"]
SIZE = 4096


def _key() -> str:
    return uuid.uuid4().hex + uuid.uuid4().hex


@pytest.fixture()
def fleet(tmp_path: Path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    stage = tmp_path / "stage"
    stage.mkdir()
    assert stage_release.register_stage_root(
        queue, tier_id=TIER, stage_root=stage) == "registered"
    cas = tmp_path / "cas"
    cas.mkdir()
    return queue, stage, cas


def _publish(queue: pool.PoolQueue, key: str, **kw: object) -> dict:
    queue.publish(
        action_key=key,
        cas_root=kw.pop("cas_root", "/cas"),
        checkout_root=kw.pop("checkout_root", "/co"),
        worker_script=kw.pop("worker_script", "/w.py"),
        resources=kw.pop("resources", {"cpu": 1}),
        **kw,
    )
    claimed = queue.claim(capacity={"cpu": 4})
    assert claimed is not None and claimed["action_key"] == key
    return claimed


def _fail_consumer(queue: pool.PoolQueue) -> tuple[str, float]:
    key = _key()
    holder = _publish(queue, key, max_attempts=1)
    generation = float(holder["published_unix"])
    queue.finish(key, status="failed", detail={"returncode": 1})
    assert queue.item_path(pool.FAILED, key).exists()
    return key, generation


def _withdraw_mover(queue: pool.PoolQueue, *, conclude: bool = True) -> str:
    key = _key()
    holder = _publish(queue, key, max_attempts=1)
    queue.withdraw(key, reason="stale test owner", by="test")
    if conclude:
        # The worker observes the cancellation and concludes: the claim row
        # is retired by its terminal transition, exactly as production
        # leaves it (withdrawn marker filed, no live row, zero tokens,
        # no receipt).
        queue.finish(key, status="withdrawn", detail={"returncode": -15},
                     claim_snapshot=holder)
    assert queue.item_path(pool.WITHDRAWN, key).exists()
    return key


def _stage_marked(stage: Path, name: str) -> Path:
    path = stage / name
    path.write_bytes(b"\0" * SIZE)
    os.setxattr(path, prewarm_loop.STAGE_SOURCE_XATTR,
                f"/originals/{name}@0".encode())
    return path


def _write_fragment(queue: pool.PoolQueue, stage: Path, consumer: str,
                    mover: str, names: list[str]) -> Path:
    return residency_map.write_fragment(queue.root / pool.RESIDENCY, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": "a" * 64,
        "entries": {
            residency_map.residency_map_key(str(stage / name), 0): {
                "stage_path": str(stage / name), "bytes": SIZE,
                "sha256": "b" * 64, "offset": 0,
            } for name in names
        },
    })


def _dead_owner(fleet) -> tuple[str, str]:
    """Failed consumer + withdrawn mover, fragment but nothing else."""
    queue, stage, _cas = fleet
    consumer, _generation = _fail_consumer(queue)
    mover = _withdraw_mover(queue)
    for name in NAMES:
        _stage_marked(stage, name)
    _write_fragment(queue, stage, consumer, mover, NAMES)
    assert queue.move_record(mover) is None, "dead mover must file no receipt"
    assert mover not in queue.tier_ledger(TIER).held_keys()
    assert not queue.item_path(pool.CLAIMED, mover).exists()
    return consumer, mover


def _publisher(fleet, mover: str, consumer: str) -> stage_move._StagedPublisher:
    queue, stage, cas = fleet
    return stage_move._StagedPublisher(
        queue=queue, stage_root=stage,
        residency_root=queue.root / pool.RESIDENCY,
        mover_action_key=mover, manifest_sha256="a" * 64,
        tier_id=TIER, cas_root=cas, consumer_action_key=consumer)


def test_dead_owner_fragment_blocks_a_same_path_mover(fleet) -> None:
    """The gap, characterized: real refusal, and reconcile keeps the files.

    The held-key sweep cannot see the owner (no tokens, no receipt) and
    reconcile keeps each marked file as prewarm-owned, while the shared
    publisher refuses replacement after the grace.  All three halves are
    pinned here; the retirement itself belongs to the test below.
    """
    queue, stage, _cas = fleet
    consumer, mover = _dead_owner(fleet)
    publisher = _publisher(fleet, _key(), _key())
    verdict = publisher._decide(stage / NAMES[0], SIZE, "c" * 64,
                                computed=None, source_id=None, heal=True)
    assert verdict[0] == "refuse"
    assert "published elsewhere" in verdict[1]
    assert "still unproven after the grace" in verdict[1]
    reconciled = stage_release.reconcile(
        queue, tier_id=TIER, stage_root=str(stage), wanted=set())
    assert reconciled["unowned_left"] == len(NAMES), reconciled
    assert all((stage / name).exists() for name in NAMES)
    assert residency_map.fragment_path(
        queue.root / pool.RESIDENCY, consumer, mover).exists()


def test_housekeeping_retires_the_dead_owner_and_unblocks(fleet) -> None:
    """Standard housekeeping routes the exact stale owner through `evict`."""
    queue, stage, _cas = fleet
    consumer, mover = _dead_owner(fleet)
    receipts = stage_release.sweep_dead_owner_fragments(
        queue, stage_roots={TIER: str(stage)})
    retired = [entry for entry in receipts
               if entry.get("action_key") == mover
               and entry.get("complete") is True]
    assert retired, f"dead owner {mover[:12]} was not retired: {receipts}"
    assert not any((stage / name).exists() for name in NAMES)
    assert not residency_map.fragment_path(
        queue.root / pool.RESIDENCY, consumer, mover).exists()
    publisher = _publisher(fleet, _key(), _key())
    assert publisher._decide(stage / NAMES[0], SIZE, "c" * 64,
                             computed=None, source_id=None,
                             heal=True)[0] == "replace"


def test_a_live_owner_is_retained(fleet) -> None:
    """Neither a live consumer nor a live mover is a dead owner."""
    queue, stage, _cas = fleet
    consumer = _key()
    _publish(queue, consumer, max_attempts=1)
    mover = _key()
    _publish(queue, mover, max_attempts=1)
    for name in NAMES:
        _stage_marked(stage, name)
    fragment = _write_fragment(queue, stage, consumer, mover, NAMES)
    receipts = stage_release.sweep_dead_owner_fragments(
        queue, stage_roots={TIER: str(stage)})
    assert not [entry for entry in receipts
                if entry.get("action_key") == mover
                and entry.get("complete") is True]
    assert all((stage / name).exists() for name in NAMES)
    assert fragment.exists()


def test_a_tainted_census_refuses_the_pass(fleet) -> None:
    """Unknown ownership retains: an unreadable fragment refuses the pass."""
    queue, stage, _cas = fleet
    consumer, mover = _dead_owner(fleet)
    residue = queue.root / pool.RESIDENCY / consumer / "residue.json"
    residue.write_bytes(b"{not json")
    receipts = stage_release.sweep_dead_owner_fragments(
        queue, stage_roots={TIER: str(stage)})
    assert receipts and all(entry.get("complete") is not True
                            for entry in receipts)
    assert all((stage / name).exists() for name in NAMES)
    assert residency_map.fragment_path(
        queue.root / pool.RESIDENCY, consumer, mover).exists()


def test_a_path_shared_with_a_live_owner_is_kept(fleet) -> None:
    """`evict` sharing discipline: the live co-owner's bytes survive."""
    queue, stage, _cas = fleet
    consumer, mover = _dead_owner(fleet)
    live_consumer = _key()
    _publish(queue, live_consumer, max_attempts=1)
    live_mover = _key()
    _publish(queue, live_mover, max_attempts=1)
    live_fragment = _write_fragment(
        queue, stage, live_consumer, live_mover, NAMES[:1])
    receipts = stage_release.sweep_dead_owner_fragments(
        queue, stage_roots={TIER: str(stage)})
    assert [entry for entry in receipts
            if entry.get("action_key") == mover
            and entry.get("complete") is True]
    assert (stage / NAMES[0]).exists(), "live co-owner's file must survive"
    assert not (stage / NAMES[1]).exists()
    assert live_fragment.exists()
    assert not residency_map.fragment_path(
        queue.root / pool.RESIDENCY, consumer, mover).exists()


def test_a_surviving_covered_claim_is_skipped_not_evicted(fleet) -> None:
    """No eviction while any claimed row survives, withdrawal or not.

    The worker's terminal transition is what retires a claim row; until it
    does, the row may still belong to a running copy, so discovery skips the
    owner outright -- the same live-row discipline the held-key sweep keeps.
    """
    queue, stage, _cas = fleet
    consumer, _generation = _fail_consumer(queue)
    mover = _withdraw_mover(queue, conclude=False)
    assert queue.item_path(pool.CLAIMED, mover).exists()
    for name in NAMES:
        _stage_marked(stage, name)
    fragment = _write_fragment(queue, stage, consumer, mover, NAMES)
    receipts = stage_release.sweep_dead_owner_fragments(
        queue, stage_roots={TIER: str(stage)})
    assert not [entry for entry in receipts
                if entry.get("action_key") == mover
                and entry.get("complete") is True]
    assert all((stage / name).exists() for name in NAMES)
    assert fragment.exists()
