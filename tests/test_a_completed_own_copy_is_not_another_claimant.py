"""The evicted mover's own completed copy is never another claimant.

A movement node files its final ``record_move`` receipt and only then does the
worker retire its ``claimed/`` row: ``stage_move.main`` runs ``move``, files the
receipt, and returns, while the claim exists through the whole copy and until
the worker's terminal bookkeeping.  A consumer that reads its bytes in that
window and retires the stage copy used to see the mover's own still-live claim
as a *distinct* pending publisher: ``stage_release._evict_owned`` called
``_claimed_paths`` without excluding the mover being evicted, so every entry
was skipped as ``in-flight-copy``, the mover's own duplicate was decharged, its
only fragment was dropped, and the egress reported ``complete`` --- bytes
nobody vouches for and a token destroyed, with free capacity still reading 0.

The proof that settles the claim is the mover's own filed receipt, bound to the
live claim it is read beside: complete, matching consumer, tier, stage root,
manifest and range extent, and filed no earlier than the claim it completes (a
receipt older than the claim is a previous attempt's, and a requeued mover may
be writing again).  Until that proof exists the own claim's derived paths defer
--- the file, the fragment and the charge all stay --- rather than sharing, and
foreign claims, reader pins and source handoffs keep exactly the protection
they had.

Every test drives the real ``stage_release.evict`` against a real tier ledger.
The one-token fixtures make the defect's signature impossible to miss: a false
shared decharge destroys the only token, and the tier's free capacity never
comes back.
"""
from __future__ import annotations

import hashlib
import json
import socket
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, reader_lease, residency_map  # noqa: E402
import stage_release  # noqa: E402

CONSUMER = "c" * 64
FOREIGN_CONSUMER = "d" * 64
MOVER = "1" * 64
FOREIGN = "2" * 64
TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
MANIFEST = "a" * 64
DIGEST = "b" * 64
SOURCE = "/mnt/shared/model/layer.safetensors"
SIZE = 4096
ATTEMPT = {"nonce": "n1", "scope_id": "s1"}
HOLDER = {"host": "test-host", "pid": 4242}
OWN_DEFERRED = "own-copy-in-flight"


@pytest.fixture()
def fleet(tmp_path: Path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.mint_tier_capacity(TIER, {"stage_gib": 1})
    stage = tmp_path / "stage"
    stage.mkdir()
    assert stage_release.register_stage_root(
        queue, tier_id=TIER, stage_root=stage) == "registered"
    return queue, stage


def _stage_object(stage: Path, relative: str = "model/layer.safetensors",
                  size: int = SIZE) -> Path:
    path = stage / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\0" * size)
    return path


def _fragment(queue: pool.PoolQueue, stage: Path, staged: Path, *,
              consumer: str = CONSUMER, mover: str = MOVER,
              source: str = SOURCE) -> None:
    residency_map.write_fragment(queue.root / pool.RESIDENCY, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage), "manifest_sha256": MANIFEST,
        "entries": {residency_map.residency_map_key(source, 0): {
            "stage_path": str(staged), "bytes": SIZE, "offset": 0,
            "sha256": DIGEST}}})


def _sealed_claim(cas: Path, key: str, source: str, size: int) -> None:
    """The sealed request + manifest blob a claimed mover's range is read from."""

    manifest = {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {}, "annotations": {},
        "mount_prefix": "/mnt/shared",
        "entries": [{"path": source, "offset": 0, "bytes": size,
                     "sha256": None}],
        "entry_count": 1, "total_bytes": size,
    }
    blob = json.dumps(manifest).encode("utf-8")
    digest = hashlib.sha256(blob).hexdigest()
    shard = cas / "blobs" / digest[:2]
    shard.mkdir(parents=True, exist_ok=True)
    (shard / digest).write_bytes(blob)
    request = {
        "action_key": key,
        "params": {"command": ["python3", "stage_move.py",
                               "--range-start-bytes", "0",
                               "--range-end-bytes", str(size)]},
        "inputs": [{"id": "pbcampaign.data-manifest", "sha256": digest,
                    "bytes": len(blob)}],
    }
    shard = cas / "requests" / key[:2]
    shard.mkdir(parents=True, exist_ok=True)
    (shard / f"{key}.json").write_text(json.dumps(request))


def _claim_mover(queue: pool.PoolQueue, key: str, *,
                 source: str = SOURCE, size: int = SIZE,
                 tag: str = "dl380g10") -> dict[str, object]:
    """Publish and claim one mover row, leaving its CLAIMED file live."""

    cas = queue.root / "cas"
    _sealed_claim(cas, key, source, size)
    queue.publish(
        action_key=key, cas_root=cas, checkout_root=queue.root / "co",
        worker_script=queue.root / "worker.py", tags=[tag],
        resources={"cpu": 1, "mem_gb": 1, STAGE_KIND: 1},
        max_attempts=1, retry_safe=False,
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": MANIFEST, "manifest_bytes": size,
                   "range_start_bytes": 0, "range_end_bytes": size})
    claimed = queue.claim(capacity={"cpu": 4, "mem_gb": 8}, tags=[tag])
    assert claimed is not None and claimed["action_key"] == key
    return claimed


def _receipt(queue: pool.PoolQueue, key: str, *, stage: Path,
             complete: bool = True, **overrides: object) -> None:
    """The mover's own final receipt, filed while its claim row is still live."""

    record: dict[str, object] = {
        "consumer_action_key": CONSUMER,
        "tier_id": TIER,
        "stage_root": str(stage),
        "manifest_sha256": MANIFEST,
        "range_start_bytes": 0,
        "range_end_bytes": SIZE,
        "range_bytes": SIZE,
        "bytes_staged": SIZE if complete else 0,
        "entries_declared": 1,
        "entries_staged": 1 if complete else 0,
        "complete": complete,
        "host": socket.gethostname(),
        "unix": time.time(),
    }
    record.update(overrides)
    queue.record_move(key, record)


def _completed_own_copy(queue: pool.PoolQueue, stage: Path):
    """The production gap, frozen: receipt filed, claim row still present."""

    staged = _stage_object(stage)
    _fragment(queue, stage, staged)
    _claim_mover(queue, MOVER)
    _receipt(queue, MOVER, stage=stage)
    return staged


def _free(queue: pool.PoolQueue) -> int:
    return int(queue.tier_ledger(TIER).available().get("stage_gib", 0))


# ---------------------------------------------------------------- the fix


def test_a_completed_own_copy_is_not_another_claimant(fleet) -> None:
    """The mover's own completed claim must not vote itself a co-owner."""

    queue, stage = fleet
    staged = _completed_own_copy(queue, stage)
    assert queue.tier_ledger(TIER).holder_tokens(MOVER) == {"stage_gib": 1}
    assert _free(queue) == 0

    receipt = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                                  stage_root=str(stage))

    assert receipt["complete"] is True
    assert receipt["entries_deleted"] == 1 and receipt["bytes_deleted"] == SIZE
    assert receipt["entries_shared"] == 0 and receipt["entries_deferred"] == 0
    assert receipt["tokens_released"] == 1
    assert receipt["tokens_decharged"] == 0
    assert not staged.exists()
    assert residency_map.read_fragments(
        queue.root / pool.RESIDENCY, CONSUMER) == []
    assert queue.tier_ledger(TIER).holder_tokens(MOVER) == {}
    assert _free(queue) == 1, "the one token comes back exactly once"
    # The mover's own content-addressed receipt survives the retirement: the
    # egress retires bytes and charge, never another node's evidence.
    assert queue.move_path(MOVER).exists()


def test_the_second_egress_of_a_completed_own_copy_is_a_no_op(fleet) -> None:
    queue, stage = fleet
    _completed_own_copy(queue, stage)
    stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                        stage_root=str(stage))

    again = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                                stage_root=str(stage))

    assert again["complete"] is True
    assert again["entries_deleted"] == 0 and again["tokens_released"] == 0
    assert again["tokens_decharged"] == 0
    assert _free(queue) == 1, "an idempotent egress cannot free the token twice"


def test_distinct_groups_run_through_one_token_without_loss(fleet) -> None:
    """PQ's boundary groups through one token: every retirement returns it."""

    queue, stage = fleet
    for index in range(3):
        mover = f"{index + 1:064x}"
        consumer = f"{index + 0x10:064x}"
        relative = f"group{index}/boundary.pt"
        source = f"/mnt/shared/{relative}"
        staged = _stage_object(stage, relative)
        _fragment(queue, stage, staged, consumer=consumer, mover=mover,
                  source=source)
        _claim_mover(queue, mover, source=source)
        _receipt(queue, mover, stage=stage,
                 consumer_action_key=consumer)
        assert _free(queue) == 0, index

        receipt = stage_release.evict(queue, mover,
                                      consumer_action_key=consumer,
                                      stage_root=str(stage))

        assert receipt["complete"] is True, (index, receipt)
        assert receipt["entries_deleted"] == 1, (index, receipt)
        assert receipt["tokens_released"] == 1, (index, receipt)
        assert receipt["tokens_decharged"] == 0, (index, receipt)
        assert queue.tier_ledger(TIER).holder_tokens(mover) == {}, index
        assert _free(queue) == 1, index


# ------------------------------------------------- what is not proof


@pytest.mark.parametrize("name,overrides,raw", [
    ("no-receipt", None, None),
    ("incomplete", {"complete": False, "bytes_staged": 0,
                    "entries_staged": 0}, None),
    ("refused", {"refusal": "residency_moved_nothing"}, None),
    ("another-tier", {"tier_id": "prismabuild-stage:elsewhere"}, None),
    ("another-consumer", {"consumer_action_key": FOREIGN_CONSUMER}, None),
    ("another-manifest", {"manifest_sha256": "e" * 64}, None),
    ("another-stage-root", {"stage_root": "/stage/somewhere-else"}, None),
    ("another-extent", {"range_bytes": SIZE + 1}, None),
    ("another-entry-count", {"entries_declared": 2, "entries_staged": 2}, None),
    ("previous-attempt", {"unix": 1.0}, None),
    ("another-host", {"host": "elsewhere"}, None),
    ("malformed-receipt", None, "{not json"),
])
def test_an_unproven_own_receipt_retains_the_copy(fleet, name, overrides,
                                                  raw) -> None:
    """No proof, no settling: bytes, fragment and charge all stay."""

    queue, stage = fleet
    staged = _stage_object(stage)
    _fragment(queue, stage, staged)
    _claim_mover(queue, MOVER)
    if raw is not None:
        queue.move_path(MOVER).write_text(raw)
    elif overrides is not None:
        _receipt(queue, MOVER, stage=stage, **overrides)
    # "no-receipt" letters the mover file its receipt nowhere.

    receipt = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                                  stage_root=str(stage))

    assert receipt["complete"] is False, (name, receipt)
    assert receipt["entries_deleted"] == 0, (name, receipt)
    assert receipt["entries_shared"] == 0, (name, receipt)
    assert receipt["entries_deferred"] == 1, (name, receipt)
    assert receipt["deferred_own"] == [OWN_DEFERRED], (name, receipt)
    assert receipt["tokens_released"] == 0, (name, receipt)
    assert receipt["tokens_decharged"] == 0, (name, receipt)
    assert staged.exists(), name
    assert residency_map.fragment_path(
        queue.root / pool.RESIDENCY, CONSUMER, MOVER).exists(), name
    assert queue.tier_ledger(TIER).holder_tokens(MOVER) == {"stage_gib": 1}
    assert _free(queue) == 0


def test_an_unstamped_own_claim_defers(fleet) -> None:
    """A claim with no usable start stamp cannot be bound to the receipt."""

    queue, stage = fleet
    staged = _stage_object(stage)
    _fragment(queue, stage, staged)
    _claim_mover(queue, MOVER)
    _receipt(queue, MOVER, stage=stage)
    path = queue.item_path(pool.CLAIMED, MOVER)
    record = json.loads(path.read_text())
    del record["claimed_unix"]
    path.write_text(json.dumps(record))

    receipt = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                                  stage_root=str(stage))

    assert receipt["complete"] is False
    assert receipt["entries_deleted"] == 0
    assert receipt["deferred_own"] == [OWN_DEFERRED]
    assert staged.exists()
    assert queue.tier_ledger(TIER).holder_tokens(MOVER) == {"stage_gib": 1}


# ------------------------------------------- what stays protected


def test_a_foreign_claimant_still_outranks_the_completed_own_copy(fleet) -> None:
    """The mover's own claim is settled; a foreign copy is a real co-owner."""

    queue, stage = fleet
    staged = _completed_own_copy(queue, stage)
    queue.mint_tier_capacity(TIER, {"stage_gib": 1})
    _claim_mover(queue, FOREIGN)

    receipt = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                                  stage_root=str(stage))

    assert receipt["complete"] is True
    assert receipt["entries_deleted"] == 0
    assert receipt["entries_shared"] == 1
    assert receipt["shared_with"] == ["in-flight-copy"]
    assert receipt["tokens_released"] == 0
    assert receipt["tokens_decharged"] == 1, "the duplicate lives on"
    assert staged.exists()
    assert queue.item_path(pool.CLAIMED, FOREIGN).exists()


def test_a_live_pin_still_defers_a_completed_own_copy(fleet) -> None:
    """Settling the own claim is not permission to delete a pinned range."""

    queue, stage = fleet
    staged = _completed_own_copy(queue, stage)
    identity = reader_lease.stat_identity(str(staged))
    assert identity is not None
    reader_lease.write_material(
        queue.root / pool.RESIDENCY, consumer_action_key=CONSUMER,
        mover_action_key=MOVER, tier_id=TIER, stage_root=str(stage),
        manifest_sha256=MANIFEST, generation=reader_lease.mint_generation(),
        entries={residency_map.residency_map_key(SOURCE, 0): {
            "stage_path": str(staged), "bytes": SIZE, "sha256": DIGEST,
            "file_id": identity}})
    acquired = reader_lease.acquire(
        queue, consumer_action_key=CONSUMER, attempt=ATTEMPT, tier_id=TIER,
        epoch="", span={"start_bytes": 0, "end_bytes": SIZE}, holder=HOLDER,
        acquire_token="t1",
        covers=[{"mover_action_key": MOVER, "manifest_sha256": MANIFEST}])
    assert acquired.get("pin_id"), acquired

    receipt = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                                  stage_root=str(stage))

    assert receipt["complete"] is False
    assert receipt["entries_deleted"] == 0
    assert receipt["entries_deferred"] == 1
    assert receipt["deferred_own"] == [], "the own claim was settled"
    assert receipt["live_pins"], receipt
    assert receipt["tokens_released"] == 0
    assert receipt["tokens_decharged"] == 0
    assert staged.exists()
    assert queue.tier_ledger(TIER).holder_tokens(MOVER) == {"stage_gib": 1}
