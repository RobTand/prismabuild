"""Held tier tokens equal bytes on the stage, at every instant (#583).

That is the whole invariant, and every case here is a way of breaking it.

Releasing a mover's tier tokens at ``finish`` bounds *concurrent copies*, not
*resident bytes*: twenty-one movers of 34.4 GB run one after another leave 722
GB on a 721 GB stage while the ledger reads its full supply free at every step,
and the twenty-second is admitted on tokens for bytes that will ENOSPC.  So a
mover that staged what it declared keeps its tokens from ``finish`` until an
egress deletes its files, and everything that left nothing behind releases.

The other direction matters just as much: tokens held for bytes that are gone
are capacity nobody can use.  So the egress deletes and releases as one
operation, an error in the deletes keeps the tokens, and a mover no live item
names any more is swept.
"""
from __future__ import annotations

import errno
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import adaptive_cpu, pool, residency_map, storage_tiers  # noqa: E402
import stage_release  # noqa: E402

MOVER = "1" * 64
CONSUMER = "2" * 64
EGRESS = "3" * 64
TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
MANIFEST = "9" * 64
DIGEST = "1" * 64


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    q.mint_tier_capacity(TIER, {"stage_gib": 8})
    return q


def _publish(q: pool.PoolQueue, key: str, resources: dict[str, int], **kw) -> None:
    q.publish(action_key=key, cas_root=q.root / "cas", checkout_root=q.root / "co",
              worker_script=q.root / "worker.py", resources=resources, **kw)


def _residency(**overrides) -> dict[str, object]:
    block = {
        "schema": pool.RESIDENCY_SCHEMA_V1,
        "tier_id": TIER,
        "manifest_sha256": MANIFEST,
        "manifest_bytes": 1 << 40,
        "range_start_bytes": 0,
        "range_end_bytes": 2 * storage_tiers.GIB,
    }
    block.update(overrides)
    return block


def _claim_mover(q: pool.PoolQueue, *, host: str = "dl380g10") -> dict[str, object]:
    _publish(q, MOVER, {"cpu": 1, "mem_gb": 1, STAGE_KIND: 2},
             residency=_residency(), max_attempts=1, retry_safe=False)
    claimed = q.claim(capacity={"cpu": 4, "mem_gb": 8}, tags=[host])
    assert claimed is not None and claimed["action_key"] == MOVER
    return claimed


def _receipt(q: pool.PoolQueue, *, staged: int = 2 * storage_tiers.GIB,
             stage_root: Path | None = None, entries: dict | None = None,
             **overrides) -> None:
    record = {
        "consumer_action_key": CONSUMER,
        "tier_id": TIER,
        "stage_root": str(stage_root or "/stage/prewarm"),
        "manifest_sha256": MANIFEST,
        "range_start_bytes": 0,
        "range_end_bytes": 2 * storage_tiers.GIB,
        "bytes_staged": staged,
        "complete": staged == 2 * storage_tiers.GIB,
    }
    record.update(overrides)
    q.record_move(MOVER, record)


def _held(q: pool.PoolQueue) -> dict[str, int]:
    return q.tier_ledger(TIER).holder_tokens(MOVER)


def _denial(q: pool.PoolQueue, key: str) -> dict[str, object] | None:
    path = adaptive_cpu.local_state_base(q.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    matching = [e for e in records.values()
                if isinstance(e, dict) and e.get("action_key") == key]
    return max(matching, key=lambda e: float(e.get("denied_unix", 0.0))) if matching else None


# ---------------------------------------------------------------- the pin


def test_a_mover_that_staged_its_range_keeps_its_tier_tokens(queue) -> None:
    _claim_mover(queue)
    assert _held(queue) == {"stage_gib": 2}
    _receipt(queue)

    queue.finish(MOVER, status="executed")

    assert _held(queue) == {"stage_gib": 2}, "the bytes are still on the stage"
    # The host's tokens go: the box is free for other work the instant the
    # copy stops.  Only the tier's stay.
    assert queue.ledger("dl380g10").holder_tokens(MOVER) == {}
    assert (queue.root / pool.DONE / f"{MOVER}.json").exists()


@pytest.mark.parametrize(
    "status,receipt",
    [
        ("failed", dict(staged=2 * storage_tiers.GIB)),
        ("executed", dict(staged=0)),
        ("executed", dict(staged=storage_tiers.GIB)),
        ("executed", dict(refusal="residency_overran_reservation")),
        ("cache_hit", dict(staged=2 * storage_tiers.GIB)),
    ],
)
def test_everything_that_left_nothing_behind_releases(queue, status, receipt) -> None:
    """A status alone cannot tell a mover that copied 2 GB from one that copied none."""

    _claim_mover(queue)
    _receipt(queue, **receipt)

    queue.finish(MOVER, status=status)

    assert _held(queue) == {}


def test_a_mover_with_no_receipt_at_all_releases(queue) -> None:
    """The receipt is the evidence; absent evidence is not a pin."""

    _claim_mover(queue)
    queue.finish(MOVER, status="executed")
    assert _held(queue) == {}


def test_a_receipt_for_another_tier_does_not_pin_this_one(queue) -> None:
    _claim_mover(queue)
    _receipt(queue, tier_id="prismabuild-stage:elsewhere")
    queue.finish(MOVER, status="executed")
    assert _held(queue) == {}


def test_an_ordinary_action_is_unaffected(queue) -> None:
    """No residency block, no pin, and no new read on the way out."""

    _publish(queue, CONSUMER, {"cpu": 1, "mem_gb": 1})
    claimed = queue.claim(capacity={"cpu": 4, "mem_gb": 8}, tags=["dl380g10"])
    assert claimed is not None
    queue.finish(CONSUMER, status="executed")
    assert queue.ledger("dl380g10").holder_tokens(CONSUMER) == {}
    assert queue.tier_ledger(TIER).holder_tokens(CONSUMER) == {}


# ------------------------------------------------------------- the gate


def _finished_pinned_mover(queue) -> None:
    _claim_mover(queue)
    _receipt(queue)
    queue.finish(MOVER, status="executed")


def _compose_map(queue, *, consumer: str = CONSUMER) -> Path:
    """The composed map, which admission requires as well as the pin."""

    return residency_map.write_map(
        queue.residency_map_path(consumer),
        residency_map.compose([{
            "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
            "consumer_action_key": consumer, "mover_action_key": MOVER,
            "tier_id": TIER, "stage_root": "/stage/prewarm",
            "manifest_sha256": MANIFEST,
            "entries": {residency_map.residency_map_key("/pool/a.bin", 0): {
                "stage_path": "/stage/prewarm/a.bin", "bytes": 4096,
                "offset": 0, "sha256": DIGEST}},
        }]))


def test_a_consumer_is_admitted_on_a_lead_that_holds_its_bytes(queue) -> None:
    _finished_pinned_mover(queue)
    _compose_map(queue)
    _publish(queue, CONSUMER, {"cpu": 1, "mem_gb": 1},
             residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                        "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 40,
                        "leads": [MOVER]})

    claimed = queue.claim(capacity={"cpu": 4, "mem_gb": 8}, tags=["dl380g10"])

    assert claimed is not None and claimed["action_key"] == CONSUMER
    assert claimed["residency_verdict"]["state"] == "resident"


def test_a_pinned_range_with_no_composed_map_is_refused(queue) -> None:
    """Both halves, or neither: the bytes have to be there *and* addressable.

    The launcher sets ``PRISMABUILD_RESIDENCY_MAP`` only when the composed map
    exists, so a consumer admitted before the tiers loop has composed it runs
    with no map at all -- reading the pool at full cost and finishing clean.
    That failure is invisible in the receipt, which is the one failure mode
    this whole change exists to remove.
    """

    _finished_pinned_mover(queue)
    _publish(queue, CONSUMER, {"cpu": 1, "mem_gb": 1},
             residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                        "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 40,
                        "leads": [MOVER]})

    assert queue.claim(capacity={"cpu": 4, "mem_gb": 8}, tags=["dl380g10"]) is None
    denial = _denial(queue, CONSUMER)
    assert denial is not None and denial["reason"] == "residency_map_not_composed"
    # Refused without ageing the item or taking a token: the loop composes on
    # its next cycle and the item is admitted then, unchanged.
    assert queue.passes(CONSUMER) == 0
    assert queue.ledger("dl380g10").holder_tokens(CONSUMER) == {}

    _compose_map(queue)
    claimed = queue.claim(capacity={"cpu": 4, "mem_gb": 8}, tags=["dl380g10"])
    assert claimed is not None and claimed["action_key"] == CONSUMER


def test_a_stalled_mount_denies_instead_of_escaping_the_claim_scan(
    queue, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ESTALE out of the map's stat is a denial, never an exception.

    ``Path.exists`` answers ``False`` for ENOENT and ENOTDIR and *raises* for
    everything else -- and everything else is what this mount does: the fleet
    sees RDMA remote-access errors on a roughly quarter-hour cadence (#575).
    An escaping OSError does not deny one item, it ends the whole claim scan
    on that box, which is the #592 failure again one layer up.  The mount is
    the thing mutated here, because the mount is the input under test.
    """

    _finished_pinned_mover(queue)
    _compose_map(queue)
    _publish(queue, CONSUMER, {"cpu": 1, "mem_gb": 1},
             residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                        "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 40,
                        "leads": [MOVER]})
    real = pool.PoolQueue.residency_map_path

    class _StalledPath:
        def __init__(self, path: Path) -> None:
            self._path = path

        def __str__(self) -> str:
            return str(self._path)

        def exists(self) -> bool:
            raise OSError(errno.ESTALE, "Stale file handle")

    monkeypatch.setattr(pool.PoolQueue, "residency_map_path",
                        lambda self, key: _StalledPath(real(self, key)))

    # The claim returns, rather than raising out of the scan...
    assert queue.claim(capacity={"cpu": 4, "mem_gb": 8}, tags=["dl380g10"]) is None
    denial = _denial(queue, CONSUMER)
    assert denial is not None and denial["reason"] == "residency_map_unreadable"
    verdict = denial["evidence"]["residency"]
    assert "Stale file handle" in verdict["error"]
    assert verdict["map_path"] == str(real(queue, CONSUMER))
    # ...and it is a stall, not a verdict: nothing aged, nothing taken.
    assert queue.passes(CONSUMER) == 0
    assert queue.ledger("dl380g10").holder_tokens(CONSUMER) == {}

    # The mount comes back and the same item is admitted, unchanged.
    monkeypatch.setattr(pool.PoolQueue, "residency_map_path", real)
    claimed = queue.claim(capacity={"cpu": 4, "mem_gb": 8}, tags=["dl380g10"])
    assert claimed is not None and claimed["action_key"] == CONSUMER
    assert claimed["residency_verdict"]["state"] == "resident"


def test_a_stalled_mount_leaves_the_map_out_of_the_launch_environment(
    queue, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The launcher stats the same file, and must not die on it either."""

    real = pool.PoolQueue.residency_map_path

    class _StalledPath:
        def __str__(self) -> str:
            return "/stalled"

        def exists(self) -> bool:
            raise OSError(errno.EIO, "Input/output error")

    monkeypatch.setattr(pool.PoolQueue, "residency_map_path",
                        lambda self, key: _StalledPath())
    item = {"action_key": CONSUMER, "residency": {"leads": [MOVER]}}

    # Unset, rather than a path this process could not stat: an action told to
    # read a map it cannot open would decide it had been staged and then read
    # the pool anyway, which is the difference the receipt cannot show.
    assert queue.residency_map_environment(item) == {}
    monkeypatch.setattr(pool.PoolQueue, "residency_map_path", real)


def test_a_lead_that_finished_holding_nothing_is_refused(queue) -> None:
    """``executed`` is not residency: a mover that moved nothing ends executed too."""

    _claim_mover(queue)
    _receipt(queue, staged=0)
    queue.finish(MOVER, status="executed")
    _publish(queue, CONSUMER, {"cpu": 1, "mem_gb": 1},
             residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                        "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 40,
                        "leads": [MOVER]})

    claimed = queue.claim(capacity={"cpu": 4, "mem_gb": 8}, tags=["dl380g10"])

    assert claimed is None
    denial = _denial(queue, CONSUMER)
    assert denial is not None and denial["reason"] == "residency_lead_unpinned"
    assert denial["evidence"]["residency"]["pending"][0]["status"] == "unpinned"
    # Refused before any token moved, and without ageing the item.
    assert queue.ledger("dl380g10").holder_tokens(CONSUMER) == {}
    assert queue.passes(CONSUMER) == 0


def test_a_lead_whose_bytes_an_egress_took_back_is_refused_again(queue) -> None:
    """The gate reads the ledger, so residency can be lost as well as gained."""

    _finished_pinned_mover(queue)
    _publish(queue, CONSUMER, {"cpu": 1, "mem_gb": 1},
             residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                        "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 40,
                        "leads": [MOVER]})
    queue.release_tier_reservations(MOVER)

    assert queue.claim(capacity={"cpu": 4, "mem_gb": 8}, tags=["dl380g10"]) is None
    assert _denial(queue, CONSUMER)["reason"] == "residency_lead_unpinned"


def test_a_block_that_names_no_tier_reads_as_it_did_before(queue) -> None:
    """The pin check needs a tier; without one the gate is #590's gate."""

    _claim_mover(queue)
    _receipt(queue)
    queue.finish(MOVER, status="executed")
    queue.release_tier_reservations(MOVER)
    _publish(queue, CONSUMER, {"cpu": 1, "mem_gb": 1},
             residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                        "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 40,
                        "leads": [MOVER]})

    _compose_map(queue)
    claimed = queue.claim(capacity={"cpu": 4, "mem_gb": 8}, tags=["dl380g10"])
    assert claimed is not None and claimed["action_key"] == CONSUMER


# ------------------------------------------------------------- reclaim


def test_reclaim_will_not_quietly_unpin_a_staged_range(queue) -> None:
    _finished_pinned_mover(queue)

    with pytest.raises(pool.PoolContractError, match="still holds tier tokens"):
        queue.reclaim_terminal_reservation(MOVER)
    assert _held(queue) == {"stage_gib": 2}

    result = queue.reclaim_terminal_reservation(MOVER, unpin=True)
    assert result["released"] == 2
    assert _held(queue) == {}


# ------------------------------------------------------------- the egress


def _staged_files(tmp_path: Path, queue: pool.PoolQueue, count: int = 3) -> Path:
    stage = tmp_path / "stage"
    stage.mkdir(exist_ok=True)
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    entries = {}
    for index in range(count):
        path = stage / "sub" / f"part-{index}.bin"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x" * 16)
        entries[residency_map.residency_map_key(f"/mnt/shared/part-{index}", 0)] = {
            "stage_path": str(path), "bytes": 16, "offset": 0, "sha256": DIGEST}
    residency_map.write_fragment(queue.root / pool.RESIDENCY, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": MOVER,
        "tier_id": TIER, "stage_root": str(stage), "manifest_sha256": MANIFEST,
        "entries": entries})
    return stage


def test_an_egress_deletes_the_bytes_and_returns_the_tokens(queue, tmp_path) -> None:
    stage = _staged_files(tmp_path, queue)
    _claim_mover(queue)
    _receipt(queue, stage_root=stage)
    queue.finish(MOVER, status="executed")
    assert _held(queue) == {"stage_gib": 2}

    receipt = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                                  stage_root=str(stage))

    assert receipt["complete"] is True
    assert receipt["entries_deleted"] == 3 and receipt["bytes_deleted"] == 48
    assert receipt["tokens_released"] == 2
    assert _held(queue) == {}
    assert not list(stage.rglob("*.bin"))
    # The directories the range left behind go too, but never the stage itself.
    assert not (stage / "sub").exists() and stage.exists()
    assert residency_map.read_fragments(queue.root / pool.RESIDENCY, CONSUMER) == []


def test_a_second_egress_of_the_same_range_is_a_no_op(queue, tmp_path) -> None:
    """The tier loop may publish one while a sweep is doing the same work."""

    stage = _staged_files(tmp_path, queue)
    _claim_mover(queue)
    _receipt(queue, stage_root=stage)
    queue.finish(MOVER, status="executed")
    stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                        stage_root=str(stage))

    again = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                                stage_root=str(stage))

    assert again["complete"] is True
    assert again["entries_deleted"] == 0 and again["tokens_released"] == 0


def test_bytes_that_could_not_be_deleted_keep_their_tokens(queue, tmp_path,
                                                           monkeypatch) -> None:
    """Releasing tokens for bytes still on the stage is the failure to prevent."""

    stage = _staged_files(tmp_path, queue)
    _claim_mover(queue)
    _receipt(queue, stage_root=stage)
    queue.finish(MOVER, status="executed")
    # Mutate the driver, not the fixture: unlink fails the way a read-only or
    # busy dataset fails, and the files stay where they are.
    monkeypatch.setattr(stage_release.os, "unlink",
                        lambda *a, **k: (_ for _ in ()).throw(OSError("EROFS")))

    receipt = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                                  stage_root=str(stage))

    assert receipt["complete"] is False and receipt["errors"]
    assert receipt["tokens_released"] == 0
    assert _held(queue) == {"stage_gib": 2}
    assert residency_map.read_fragments(queue.root / pool.RESIDENCY, CONSUMER) != []


def test_a_fragment_that_cannot_be_read_keeps_its_tokens(queue, tmp_path) -> None:
    """Absent and unreadable are different: one means gone, the other means unknown.

    A missing fragment says this mover has nothing on the stage. A fragment
    that exists and does not parse says its bytes may well be there and this
    egress cannot name them, so releasing would hand the ledger capacity that
    is occupied.
    """

    stage = _staged_files(tmp_path, queue)
    _claim_mover(queue)
    _receipt(queue, stage_root=stage)
    queue.finish(MOVER, status="executed")
    # Mutate what the driver reads, not the code that reads it.
    (queue.root / pool.RESIDENCY / CONSUMER / f"{MOVER}.json").write_text("{not json")

    receipt = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                                  stage_root=str(stage))

    assert receipt["complete"] is False and receipt["errors"]
    assert receipt["tokens_released"] == 0
    assert _held(queue) == {"stage_gib": 2}
    assert len(list(stage.rglob("*.bin"))) == 3, "nothing was deleted on a guess"


def test_a_fragment_naming_a_path_outside_the_stage_never_reaches_the_unlink(
        queue, tmp_path) -> None:
    """The writer refuses such an entry, so the reader refuses the document."""

    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    outside = tmp_path / "not-the-stage.bin"
    outside.write_bytes(b"keep me")
    (queue.root / pool.RESIDENCY / CONSUMER).mkdir(parents=True)
    (queue.root / pool.RESIDENCY / CONSUMER / f"{MOVER}.json").write_text(json.dumps({
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": MOVER,
        "tier_id": TIER, "stage_root": str(stage), "manifest_sha256": MANIFEST,
        "entries": {residency_map.residency_map_key("/mnt/shared/x", 0): {
            "stage_path": str(outside), "bytes": 7, "offset": 0,
            "sha256": DIGEST}}}))

    receipt = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                                  stage_root=str(stage))

    assert receipt["complete"] is False
    assert outside.exists(), "a fragment is not authority over the whole filesystem"


def test_a_missing_fragment_still_returns_the_tokens(queue, tmp_path) -> None:
    """Otherwise a lost fragment costs the stage its capacity for good."""

    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    _claim_mover(queue)
    _receipt(queue, stage_root=stage)
    queue.finish(MOVER, status="executed")

    receipt = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                                  stage_root=str(stage))

    assert receipt["complete"] is True and receipt["tokens_released"] == 2
    assert _held(queue) == {}


# ------------------------------------------------------------- the sweep


def test_a_mover_no_live_item_names_is_swept(queue, tmp_path) -> None:
    """A withdrawn consumer would otherwise hold the stage for the fleet's life."""

    stage = _staged_files(tmp_path, queue)
    _claim_mover(queue)
    _receipt(queue, stage_root=stage)
    queue.finish(MOVER, status="executed")

    swept = stage_release.sweep(queue, stage_roots={TIER: str(stage)})

    assert [entry["action_key"] for entry in swept] == [MOVER]
    assert swept[0]["reason"] == "orphan-sweep"
    assert _held(queue) == {}


def test_a_mover_a_ready_consumer_still_names_is_left_alone(queue, tmp_path) -> None:
    stage = _staged_files(tmp_path, queue)
    _claim_mover(queue)
    _receipt(queue, stage_root=stage)
    queue.finish(MOVER, status="executed")
    _publish(queue, CONSUMER, {"cpu": 1, "mem_gb": 1},
             residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                        "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 40,
                        "leads": [MOVER]})

    assert stage_release.sweep(queue, stage_roots={TIER: str(stage)}) == []
    assert _held(queue) == {"stage_gib": 2}
    assert list(stage.rglob("*.bin"))
