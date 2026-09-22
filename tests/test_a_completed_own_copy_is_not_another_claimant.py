"""The evicted mover's own live claim is never another claimant (#793).

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

The mover's own claim is not a co-owner, and it is not settled on its receipt
either: a move receipt carries no immutable attempt identity, so a
complete-looking one cannot be told from a previous attempt's while the same
key is claimed again (2026-09-21 root QA) --- and a wall-clock stamp is no
substitute.  A live own claim therefore defers: the file, the fragment, the
material and the full charge stay, the receipt says so, and the next sweep
retires the range exactly once after the worker's terminal transition has
retired the claim.  Foreign claimants, reader pins and source handoffs keep
exactly the protection they had.

Fixtures drive the real mover lifecycle at tiny scale: seal a CAS action ->
publish -> claim -> ``stage_move.move`` -> ``record_move`` (the worker's
``finish`` is deliberately skipped, which is the production gap).  The
one-token tier makes the defect's signature impossible to miss: a false shared
decharge destroys the only token, and free capacity never comes back.  Runs
under pbtest at priority -10; never executed locally.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, reader_lease, residency_map  # noqa: E402
import prismabuild.core as pb  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402

CONSUMER = "c" * 64
FOREIGN_CONSUMER = "d" * 64
TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
SIZE = 4096
HOST_CAP = {"cpu": 8, "mem_gb": 16}
WORKER = "worker:1:abcd0001"
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


def _manifest(tmp_path: Path, name: str, size: int = SIZE) -> Path:
    """One tiny whole-file entry with real bytes and a real digest."""

    pool_dir = tmp_path / "source"
    pool_dir.mkdir(parents=True, exist_ok=True)
    payload = bytes((index * 31 + 7) % 251 for index in range(size))
    source = pool_dir / name
    source.write_bytes(payload)
    manifest = {
        "schema": pb.DATA_MANIFEST_SCHEMA_V1,
        "produced_by": {"tool": "egress-own-copy-regression"},
        "mount_prefix": str(pool_dir),
        "entries": [{"path": str(source), "offset": 0, "bytes": size,
                     "sha256": hashlib.sha256(payload).hexdigest()}],
        "entry_count": 1,
        "total_bytes": size,
        "annotations": {},
    }
    path = tmp_path / f"{name}.manifest.json"
    path.write_text(json.dumps(manifest))
    return path


def _sealed_mover(queue: pool.PoolQueue, manifest_path: Path, consumer: str,
                  size: int = SIZE) -> tuple[str, str]:
    """A real sealed v2 mover action filed through the CAS; returns (key, digest)."""

    cas = pb.PrismaBuildCAS(queue.root / "cas")
    manifest_input, _ = cas.ingest_input(
        manifest_path, input_id=pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID)
    checkout = manifest_path.parent / "checkout"
    checkout.mkdir(parents=True, exist_ok=True)
    (checkout / "task_code.py").write_text("raise SystemExit(0)\n",
                                           encoding="utf-8")
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/egress-own-copy",
                 "definition_version": "v1", "task_class": "generation",
                 "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": ["/bin/true"], "working_directory": ".",
                 "result_path": "result"},
        "inputs": [manifest_input],
        "code_closure": pb.build_code_closure(checkout, ["task_code.py"]),
        "params": {"command": ["python3", "stage_move.py",
                               "--consumer-action-key", consumer,
                               "--range-start-bytes", "0",
                               "--range-end-bytes", str(size)]},
        "environment": {"variables": {"PATH": "/usr/bin:/bin"},
                        "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    key = str(action["action_key"])
    cas.publish_action_request(action)
    return key, str(manifest_input["sha256"])


def _claim(queue: pool.PoolQueue, key: str, digest: str,
           size: int = SIZE, *, tag: str = "dl380g10") -> None:
    """Publish and claim the sealed mover; the CLAIMED row stays live."""

    queue.publish(
        action_key=key, cas_root=str(queue.root / "cas"),
        checkout_root=str(queue.root / "co"),
        worker_script=str(queue.root / "worker.py"),
        resources={"cpu": 1, "mem_gb": 1, STAGE_KIND: 1},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": digest, "manifest_bytes": size,
                   "range_start_bytes": 0, "range_end_bytes": size},
        max_attempts=1, retry_safe=False)
    claimed = queue.claim(owner=WORKER, capacity=dict(HOST_CAP), tags=[tag])
    assert claimed is not None and claimed["action_key"] == key, claimed


def _move_args(queue: pool.PoolQueue, stage: Path, manifest_path: Path,
               mover: str, consumer: str, size: int, digest: str):
    return stage_move.build_parser().parse_args([
        "--pool-root", str(queue.root),
        "--cas-root", str(queue.root / "cas"),
        "--action-key", mover,
        "--consumer-action-key", consumer,
        "--tier-id", TIER,
        "--stage-root", str(stage),
        "--manifest-sha256", digest,
        "--range-start-bytes", "0",
        "--range-end-bytes", str(size),
        "--manifest", str(manifest_path),
        "--residency-root", str(queue.root / pool.RESIDENCY),
        "--block", str(1 << 16),
        "--readers", "2", "--max-readers", "2", "--unpaced",
    ])


def _completed_gap(queue: pool.PoolQueue, tmp_path: Path, stage: Path,
                   consumer: str = CONSUMER, *, name: str = "layer.bin",
                   size: int = SIZE) -> tuple[str, str, Path]:
    """Publish -> claim -> the real copy -> record_move, without finish.

    The exact production window: the final receipt is filed while the mover's
    own CLAIMED row is still live.  Returns (key, digest, staged path).
    """

    manifest_path = _manifest(tmp_path, name, size)
    key, digest = _sealed_mover(queue, manifest_path, consumer, size)
    _claim(queue, key, digest, size)
    receipt = stage_move.move(
        _move_args(queue, stage, manifest_path, key, consumer, size, digest))
    assert receipt["complete"] is True, receipt
    queue.record_move(key, receipt)
    source_dir = tmp_path / "source"
    staged = Path(str(receipt["stage_root"])) / stage_move.stage_relative(
        str(source_dir / name), 0, size, mount_prefix=str(source_dir))
    assert staged.exists()
    return key, digest, staged


def _mutate_receipt(queue: pool.PoolQueue, key: str, **overrides: object) -> None:
    path = queue.move_path(key)
    record = json.loads(path.read_text())
    record.update(overrides)
    path.write_text(json.dumps(record))


def _free(queue: pool.PoolQueue) -> int:
    return int(queue.tier_ledger(TIER).available().get("stage_gib", 0))


def _evict(queue: pool.PoolQueue, key: str, consumer: str, stage: Path):
    return stage_release.evict(queue, key, consumer_action_key=consumer,
                               stage_root=str(stage))


def _assert_deferred(queue: pool.PoolQueue, key: str, consumer: str,
                     staged: Path, receipt: dict, label: str = "") -> None:
    """A live own claim retains file, fragment, material and charge."""

    assert receipt["complete"] is False, (label, receipt)
    assert receipt["entries_deleted"] == 0, (label, receipt)
    assert receipt["entries_shared"] == 0, (label, receipt)
    assert receipt["entries_deferred"] == 1, (label, receipt)
    assert receipt["deferred_own"] == [OWN_DEFERRED], (label, receipt)
    assert receipt["tokens_released"] == 0, (label, receipt)
    assert receipt["tokens_decharged"] == 0, (label, receipt)
    assert staged.exists(), label
    assert residency_map.fragment_path(
        queue.root / pool.RESIDENCY, consumer, key).exists(), label
    assert queue.tier_ledger(TIER).holder_tokens(key) == {"stage_gib": 1}
    assert _free(queue) == 0, label


# ---------------------------------------------------------------- the fix


def test_a_completed_own_copy_defers_then_retires_after_the_claim(fleet,
                                                                  tmp_path) -> None:
    """No deletion under a live own claim; the retry after `finish` retires."""

    queue, stage = fleet
    key, _digest, staged = _completed_gap(queue, tmp_path, stage)
    assert queue.item_path(pool.CLAIMED, key).exists()
    assert queue.tier_ledger(TIER).holder_tokens(key) == {"stage_gib": 1}
    assert _free(queue) == 0

    during = _evict(queue, key, CONSUMER, stage)
    _assert_deferred(queue, key, CONSUMER, staged, during, "receipt filed")

    # The worker's terminal transition: the claim row goes, the complete
    # receipt keeps the tier tokens pinned (residency_pin_holds).
    queue.finish(key, status="executed")
    assert not queue.item_path(pool.CLAIMED, key).exists()
    assert queue.tier_ledger(TIER).holder_tokens(key) == {"stage_gib": 1}

    after = _evict(queue, key, CONSUMER, stage)

    assert after["complete"] is True, after
    assert after["entries_deleted"] == 1 and after["bytes_deleted"] == SIZE
    assert after["entries_shared"] == 0 and after["entries_deferred"] == 0
    assert after["tokens_released"] == 1
    assert after["tokens_decharged"] == 0
    assert not staged.exists()
    assert residency_map.read_fragments(
        queue.root / pool.RESIDENCY, CONSUMER) == []
    assert queue.tier_ledger(TIER).holder_tokens(key) == {}
    assert _free(queue) == 1, "the one token comes back exactly once"
    # The mover's own content-addressed receipt survives the retirement: the
    # egress retires bytes and charge, never another node's evidence.
    assert queue.move_path(key).exists()


def test_the_second_retirement_of_the_range_is_a_no_op(fleet, tmp_path) -> None:
    queue, stage = fleet
    key, _digest, _staged = _completed_gap(queue, tmp_path, stage)
    queue.finish(key, status="executed")
    _evict(queue, key, CONSUMER, stage)

    again = _evict(queue, key, CONSUMER, stage)

    assert again["complete"] is True
    assert again["entries_deleted"] == 0 and again["tokens_released"] == 0
    assert again["tokens_decharged"] == 0
    assert _free(queue) == 1, "an idempotent egress cannot free the token twice"


def test_distinct_groups_run_through_one_token_without_loss(fleet,
                                                            tmp_path) -> None:
    """PQ's boundary groups through one token: every retirement returns it."""

    queue, stage = fleet
    for index in range(3):
        consumer = f"{index + 0x10:064x}"
        key, _digest, staged = _completed_gap(
            queue, tmp_path, stage, consumer,
            name=f"group{index}.bin")
        assert _free(queue) == 0, index

        during = _evict(queue, key, consumer, stage)
        _assert_deferred(queue, key, consumer, staged, during, f"group{index}")

        queue.finish(key, status="executed")
        receipt = _evict(queue, key, consumer, stage)

        assert receipt["complete"] is True, (index, receipt)
        assert receipt["entries_deleted"] == 1, (index, receipt)
        assert receipt["tokens_released"] == 1, (index, receipt)
        assert receipt["tokens_decharged"] == 0, (index, receipt)
        assert not staged.exists(), index
        assert queue.tier_ledger(TIER).holder_tokens(key) == {}, index
        assert _free(queue) == 1, index


# ------------------------------------------------- what is not proof


def _unlink_receipt(queue, key, _stage):
    queue.move_path(key).unlink()


def _incomplete(queue, key, _stage):
    _mutate_receipt(queue, key, complete=False, bytes_staged=0,
                    entries_staged=0)


def _refused(queue, key, _stage):
    _mutate_receipt(queue, key, refusal="residency_moved_nothing")


def _another_tier(queue, key, _stage):
    _mutate_receipt(queue, key, tier_id="prismabuild-stage:elsewhere")


def _another_consumer(queue, key, _stage):
    _mutate_receipt(queue, key, consumer_action_key=FOREIGN_CONSUMER)


def _another_manifest(queue, key, _stage):
    _mutate_receipt(queue, key, manifest_sha256="e" * 64)


def _another_stage_root(queue, key, _stage):
    _mutate_receipt(queue, key, stage_root="/stage/somewhere-else")


def _another_extent(queue, key, _stage):
    _mutate_receipt(queue, key, range_bytes=SIZE + 1)


def _another_entry_count(queue, key, _stage):
    _mutate_receipt(queue, key, entries_declared=2, entries_staged=2)


def _previous_attempt(queue, key, _stage):
    _mutate_receipt(queue, key, unix=1.0)


def _another_host(queue, key, _stage):
    _mutate_receipt(queue, key, host="elsewhere")


def _no_stamp(queue, key, _stage):
    path = queue.item_path(pool.CLAIMED, key)
    record = json.loads(path.read_text())
    record.pop("claimed_unix", None)
    path.write_text(json.dumps(record))


@pytest.mark.parametrize("name,mutate", [
    ("no-receipt", _unlink_receipt),
    ("incomplete", _incomplete),
    ("refused", _refused),
    ("another-tier", _another_tier),
    ("another-consumer", _another_consumer),
    ("another-manifest", _another_manifest),
    ("another-stage-root", _another_stage_root),
    ("another-extent", _another_extent),
    ("another-entry-count", _another_entry_count),
    ("previous-attempt", _previous_attempt),
    ("another-host", _another_host),
    ("unstamped-claim", _no_stamp),
])
def test_no_receipt_shape_settles_a_live_own_claim(fleet, tmp_path, name,
                                                   mutate) -> None:
    """Complete-looking or not, the own claim stays a possible writer."""

    queue, stage = fleet
    key, _digest, staged = _completed_gap(queue, tmp_path, stage)
    assert queue.move_path(key).exists(), name
    mutate(queue, key, stage)

    receipt = _evict(queue, key, CONSUMER, stage)

    _assert_deferred(queue, key, CONSUMER, staged, receipt, name)


# ------------------------------------------- what stays protected


def test_a_foreign_claimant_still_protects_the_bytes(fleet, tmp_path) -> None:
    """With the own claim gone, a foreign copy is a real co-owner again."""

    queue, stage = fleet
    key, _digest, staged = _completed_gap(queue, tmp_path, stage)
    queue.finish(key, status="executed")
    manifest_path = tmp_path / "layer.bin.manifest.json"
    foreign, digest = _sealed_mover(queue, manifest_path, FOREIGN_CONSUMER)
    queue.mint_tier_capacity(TIER, {"stage_gib": 2})
    _claim(queue, foreign, digest)

    receipt = _evict(queue, key, CONSUMER, stage)

    assert receipt["complete"] is True, receipt
    assert receipt["entries_deleted"] == 0
    assert receipt["entries_shared"] == 1
    assert receipt["shared_with"] == ["in-flight-copy"]
    assert receipt["tokens_released"] == 0
    assert receipt["tokens_decharged"] == 1, "the duplicate lives on"
    assert staged.exists()
    assert queue.item_path(pool.CLAIMED, foreign).exists()


def test_an_own_live_claim_outranks_a_foreign_one(fleet, tmp_path) -> None:
    """The mover being retired's own claim defers even beside a co-owner.

    Deferral is the conservative direction: it keeps the whole document's
    proof and charge instead of settling half of it against a copy that is
    still only claimed.
    """

    queue, stage = fleet
    key, _digest, staged = _completed_gap(queue, tmp_path, stage)
    manifest_path = tmp_path / "layer.bin.manifest.json"
    foreign, digest = _sealed_mover(queue, manifest_path, FOREIGN_CONSUMER)
    queue.mint_tier_capacity(TIER, {"stage_gib": 2})
    _claim(queue, foreign, digest)

    receipt = _evict(queue, key, CONSUMER, stage)

    _assert_deferred(queue, key, CONSUMER, staged, receipt, "own and foreign")
    assert receipt["shared_with"] == [], receipt


def test_a_live_pin_still_defers_a_settled_own_copy(fleet, tmp_path) -> None:
    """Once the claim is gone, a pinned range still defers on the pin."""

    queue, stage = fleet
    key, digest, staged = _completed_gap(queue, tmp_path, stage)
    queue.finish(key, status="executed")
    acquired = reader_lease.acquire(
        queue, consumer_action_key=CONSUMER, attempt=ATTEMPT, tier_id=TIER,
        epoch="", span={"start_bytes": 0, "end_bytes": SIZE}, holder=HOLDER,
        acquire_token="t1",
        covers=[{"mover_action_key": key, "manifest_sha256": digest}])
    assert acquired.get("pin_id"), acquired

    receipt = _evict(queue, key, CONSUMER, stage)

    assert receipt["complete"] is False, receipt
    assert receipt["entries_deleted"] == 0
    assert receipt["entries_deferred"] == 1
    assert receipt["deferred_own"] == [], "the own claim was settled"
    assert receipt["live_pins"] == [acquired["pin_id"]], receipt
    assert receipt["tokens_released"] == 0
    assert receipt["tokens_decharged"] == 0
    assert staged.exists()
    assert queue.tier_ledger(TIER).holder_tokens(key) == {"stage_gib": 1}
