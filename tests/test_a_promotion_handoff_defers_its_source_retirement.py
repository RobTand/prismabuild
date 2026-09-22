"""A promotion handoff defers its SSD source's retirement (#768).

The egress used to classify a path a live RAM promotion was reading as
"shared": it kept the file, dropped the stage mover's fragment and material
sidecar, and freed the mover's occupancy tokens.  The surviving SSD file then
had no same-path proof -- a RAM fragment names another tier and path and can
never satisfy ``_StagedPublisher._proof_search`` for the SSD incarnation --
so the next stage publisher spent its stall budget trying to prove a file it
could not, and the freed tokens were capacity the bytes still occupied.

A pending handoff is not a co-owner.  It is deferred: the file, this mover's
fragment, its material date and its full occupancy charge all stay until the
promotion's claim ends.  Only then does the last owner's source delete, or its
duplicate charge decharge against a genuine same-path accounted co-owner.

Drives the real mover and promotion lifecycles at tiny scale (1 MiB, one token
through the same ceil function), not a substituted source-path set:
publish -> claim -> stage_move.move -> record_move -> finish; publish the
promotion row -> claim -> ram_promote.promote; then stage_release.evict.  Runs
under pbtest at priority -10; never executed locally.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import socket
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, reader_lease, residency_map, storage_tiers  # noqa: E402
import prismabuild.core as pb  # noqa: E402
import ram_promote  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:dl380g10"
KIND = "stage_gib"
RAM_KIND = "ram_gib"
CONSUMER_A = "a" * 64
CONSUMER_B = "b" * 64
MOVER_A = "1" * 64
MOVER_B = "2" * 64
MANIFEST_SHA = "0" * 64
MIB = 1 << 20
HOST_CAP = {"cpu": 8, "mem_gb": 16}
WORKER = "worker:1:abcd0001"
SOURCE_NAME = "layer.bin"
#: Where the stage mover (and the promotion) name the one whole-file entry.
STAGED_NAME = stage_move.stage_relative(f"/m/{SOURCE_NAME}", 0, MIB,
                                        mount_prefix="/m")


def _manifest(tmp_path: Path) -> tuple[dict, Path]:
    pool_dir = tmp_path / "pool"
    pool_dir.mkdir(parents=True, exist_ok=True)
    payload = hashlib.sha256(b"promotion-handoff-source").digest() * (MIB // 32)
    (pool_dir / SOURCE_NAME).write_bytes(payload)
    manifest = {
        "schema": pb.DATA_MANIFEST_SCHEMA_V1,
        "produced_by": {"tool": "promotion-handoff-regression"},
        "mount_prefix": str(pool_dir),
        "entries": [{"path": str(pool_dir / SOURCE_NAME), "offset": 0,
                     "bytes": MIB, "sha256": hashlib.sha256(payload).hexdigest()}],
        "entry_count": 1,
        "total_bytes": MIB,
        "annotations": {"phases": [{"name": "head", "cumulative_bytes": MIB}]},
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    return manifest, manifest_path


@pytest.fixture()
def fleet(tmp_path: Path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.mint_tier_capacity(TIER, {KIND: 8})
    queue.mint_tier_capacity(RAM_TIER, {RAM_KIND: 4})
    stage = tmp_path / "stage"
    stage.mkdir()
    assert stage_release.register_stage_root(
        queue, tier_id=TIER, stage_root=stage) == "registered"
    return queue, stage


def _publish(queue: pool.PoolQueue, key: str, resources: dict,
             residency: dict) -> None:
    queue.publish(action_key=key, cas_root=str(queue.root / "cas"),
                  checkout_root=str(queue.root / "co"),
                  worker_script=str(queue.root / "worker.py"),
                  resources=resources, residency=residency,
                  max_attempts=1, retry_safe=False)


def _claim(queue: pool.PoolQueue, key: str) -> None:
    claimed = queue.claim(owner=WORKER, capacity=dict(HOST_CAP),
                          tags=["dl380g10"])
    assert claimed is not None and claimed["action_key"] == key, claimed


def _move_args(queue: pool.PoolQueue, stage: Path, manifest_path: Path,
               mover: str, consumer: str, size: int):
    return stage_move.build_parser().parse_args([
        "--pool-root", str(queue.root),
        "--cas-root", str(queue.root / "cas"),
        "--action-key", mover,
        "--consumer-action-key", consumer,
        "--tier-id", TIER,
        "--stage-root", str(stage),
        "--manifest-sha256", MANIFEST_SHA,
        "--range-start-bytes", "0",
        "--range-end-bytes", str(size),
        "--manifest", str(manifest_path),
        "--residency-root", str(queue.root / pool.RESIDENCY),
        "--block", str(1 << 16),
        "--readers", "2", "--max-readers", "2", "--unpaced",
    ])


def _stage_mover(queue: pool.PoolQueue, stage: Path, manifest_path: Path,
                 mover: str, consumer: str, size: int) -> dict:
    """The actual mover lifecycle: publish -> claim -> copy -> finish."""

    _publish(queue, mover,
             {"cpu": 1, "mem_gb": 1, f"{KIND}@{TIER}": 1},
             {"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
              "manifest_sha256": MANIFEST_SHA, "manifest_bytes": size,
              "range_start_bytes": 0, "range_end_bytes": size})
    _claim(queue, mover)
    receipt = stage_move.move(
        _move_args(queue, stage, manifest_path, mover, consumer, size))
    assert receipt["complete"] is True, receipt
    queue.record_move(mover, receipt)
    queue.finish(mover, status="executed")
    return receipt


def _promotion_claim(queue: pool.PoolQueue, stage: Path, manifest_path: Path,
                     consumer: str, size: int,
                     *, command: list | None = None) -> tuple[str, str]:
    """A claimed RAM mover row whose sealed request names its source leg.

    The request is a REAL sealed v2 action filed through the CAS, not a
    synthetic shape: ``publish`` reads the filed request through the
    production loader, which refuses a present-but-invalid request, so the
    row carries the content-addressed key the CAS derived. The key is
    returned and used for the promotion's whole lifecycle here. The
    malformed-range variant keeps its malformedness in the sealed command
    (end precedes start), exactly where the egress reads and refuses it.
    """

    if command is None:
        command = ["python3", "ram_promote.py",
                   "--source-stage-root", str(stage),
                   "--range-start-bytes", "0",
                   "--range-end-bytes", str(size)]
    cas = pb.PrismaBuildCAS(queue.root / "cas")
    manifest_input, _ = cas.ingest_input(
        manifest_path, input_id=pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID)
    checkout = manifest_path.parent / "promotion-checkout"
    checkout.mkdir(parents=True, exist_ok=True)
    (checkout / "task_code.py").write_text(
        "raise SystemExit(0)\n", encoding="utf-8")
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/promotion-handoff",
                 "definition_version": "v1", "task_class": "generation",
                 "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": ["/bin/true"], "working_directory": ".",
                 "result_path": "result"},
        "inputs": [manifest_input],
        "code_closure": pb.build_code_closure(checkout, ["task_code.py"]),
        "params": {"command": command},
        "environment": {"variables": {"PATH": "/usr/bin:/bin"},
                        "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    key = str(action["action_key"])
    cas.publish_action_request(action)
    digest = str(manifest_input["sha256"])
    _publish(queue, key,
             {"cpu": 1, "mem_gb": 1, f"{RAM_KIND}@{RAM_TIER}": 1},
             {"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": RAM_TIER,
              "manifest_sha256": digest, "manifest_bytes": size,
              "range_start_bytes": 0, "range_end_bytes": size})
    _claim(queue, key)
    return key, digest


def _promote(queue: pool.PoolQueue, stage: Path, ram: Path,
             manifest_path: Path, key: str, consumer: str, size: int) -> dict:
    """The actual promotion lifecycle: prove the source, copy, land the map."""

    args = ram_promote.build_parser().parse_args([
        "--pool-root", str(queue.root),
        "--manifest", str(manifest_path),
        "--consumer-action-key", consumer,
        "--tier-id", RAM_TIER,
        "--ram-root", str(ram),
        "--source-stage-root", str(stage),
        "--manifest-sha256", MANIFEST_SHA,
        "--range-start-bytes", "0",
        "--range-end-bytes", str(size),
        "--action-key", key,
        "--residency-root", str(queue.root / pool.RESIDENCY),
        "--block", str(1 << 16),
        "--readers", "2",
    ])
    receipt = ram_promote.promote(args)
    assert receipt["complete"] is True, receipt
    queue.record_move(key, receipt)
    return receipt


def _paths(queue: pool.PoolQueue, consumer: str, mover: str) -> dict:
    residency = queue.root / pool.RESIDENCY
    return {
        "residency": residency,
        "fragment": residency_map.fragment_path(residency, consumer, mover),
        "material": reader_lease.material_path(residency, consumer, mover),
        "retiring": reader_lease.retiring_path(
            reader_lease.leases_root(queue, residency), consumer, mover),
    }


def test_egress_during_a_promotion_defers_the_source_until_the_handoff_ends(
        fleet, tmp_path: Path) -> None:
    queue, stage = fleet
    _, manifest_path = _manifest(tmp_path)
    _stage_mover(queue, stage, manifest_path, MOVER_A, CONSUMER_A, MIB)
    ram = tmp_path / "ram"
    ram.mkdir()
    assert storage_tiers.ensure_ram_epoch(ram, host="test") is not None
    promo, _ = _promotion_claim(queue, stage, manifest_path, CONSUMER_A, MIB)
    _promote(queue, stage, ram, manifest_path, promo, CONSUMER_A, MIB)

    paths = _paths(queue, CONSUMER_A, MOVER_A)
    staged = stage / STAGED_NAME
    ledger = queue.tier_ledger(TIER)
    assert staged.exists() and paths["fragment"].exists()
    assert paths["material"].exists()
    assert ledger.holder_tokens(MOVER_A).get(KIND) == 1

    # Egress while the promotion's claim is live: a pending copy handoff, so
    # the whole document and its charge stay.  Not shared, not freed.
    first = stage_release.evict(queue, MOVER_A,
                                consumer_action_key=CONSUMER_A,
                                stage_root=str(stage))
    assert first["complete"] is False, first
    assert first["entries_deferred"] == 1, first
    assert first["deferred_handoffs"] == ["promotion-handoff"], first
    assert first["entries_shared"] == 0, first
    assert first["bytes_shared"] == 0, first
    assert first["tokens_released"] == 0, first
    assert first["tokens_decharged"] == 0, first
    assert first["retiring"] is False, first
    assert staged.exists(), "the handoff's source bytes must survive"
    assert paths["fragment"].exists(), "its same-path proof must survive"
    assert paths["material"].exists(), "its material date must survive"
    assert not paths["retiring"].exists(), (
        "no retiring mark while the handoff still has to prove its source")
    assert ledger.holder_tokens(MOVER_A).get(KIND) == 1, (
        "no early free credit while the handoff still holds the source")

    # No mark closes the generation, so the proof-only acquire the promotion
    # itself uses before its copy still proves the pending source -- and
    # leaves no durable pin behind.
    proof = reader_lease.acquire(
        queue, consumer_action_key=CONSUMER_A,
        attempt={"nonce": promo, "scope_id": "egress-handoff-test"},
        tier_id=TIER, epoch="",
        span={"start_bytes": 0, "end_bytes": MIB},
        holder={"host": socket.gethostname(), "pid": os.getpid()},
        acquire_token="egress-handoff-test:proof-only",
        covers=[{"mover_action_key": MOVER_A,
                 "manifest_sha256": MANIFEST_SHA}],
        expected=None, residency_root=paths["residency"], file_pin=False)
    assert proof.get("ok") is True, proof
    assert [entry["stage_path"] for entry in proof["entries"]] == [str(staged)]
    owners, taint = reader_lease.live_for(
        queue, {os.path.normpath(str(staged))},
        residency_root=paths["residency"])
    assert taint == [] and owners == {}, (
        "a proof-only acquire must not leak a durable pin")

    # The promotion's action concludes: the claim goes, the handoff ends.
    queue.finish(promo, status="executed")
    assert not (queue.dir(pool.CLAIMED) / f"{promo}.json").exists()

    second = stage_release.evict(queue, MOVER_A,
                                 consumer_action_key=CONSUMER_A,
                                 stage_root=str(stage))
    assert second["complete"] is True, second
    assert second["entries_deleted"] == 1, second
    assert second["tokens_released"] == 1, second
    assert second["tokens_decharged"] == 0, second
    assert not staged.exists()
    assert not paths["fragment"].exists()
    assert not paths["material"].exists()
    assert not paths["retiring"].exists()
    assert ledger.holder_tokens(MOVER_A).get(KIND, 0) == 0

    # Idempotent: the concluded egress cannot free or decharge a second time.
    third = stage_release.evict(queue, MOVER_A,
                                consumer_action_key=CONSUMER_A,
                                stage_root=str(stage))
    assert third["complete"] is True, third
    assert third["entries_deleted"] == 0, third
    assert third["tokens_released"] == 0, third
    assert third["tokens_decharged"] == 0, third
    assert ledger.holder_tokens(MOVER_A).get(KIND, 0) == 0


def test_a_pending_handoff_outranks_a_same_path_co_owner_until_it_ends(
        fleet, tmp_path: Path) -> None:
    """A co-owner proves the bytes for a publisher, never for the promotion.

    Promotion cover resolves in its own consumer/manifest namespace
    (``ram_promote``'s coverage loop and ``reader_lease.acquire``), so B's
    same-path fragment cannot substitute for A's pending proof acquisition.
    While the claim is live A's document and full charge stay even though B
    also vouches; only after the handoff ends does the ordinary shared
    decharge settle.
    """

    queue, stage = fleet
    _, manifest_path = _manifest(tmp_path)
    _stage_mover(queue, stage, manifest_path, MOVER_A, CONSUMER_A, MIB)
    _stage_mover(queue, stage, manifest_path, MOVER_B, CONSUMER_B, MIB)
    ram = tmp_path / "ram"
    ram.mkdir()
    assert storage_tiers.ensure_ram_epoch(ram, host="test") is not None
    promo, _ = _promotion_claim(queue, stage, manifest_path, CONSUMER_A, MIB)

    paths_a = _paths(queue, CONSUMER_A, MOVER_A)
    paths_b = _paths(queue, CONSUMER_B, MOVER_B)
    staged = stage / STAGED_NAME
    ledger = queue.tier_ledger(TIER)

    # The claim is live and the promotion has not acquired yet: the handoff
    # outranks B's shared skip, so A keeps its own proof and full charge.
    during = stage_release.evict(queue, MOVER_A,
                                 consumer_action_key=CONSUMER_A,
                                 stage_root=str(stage))
    assert during["complete"] is False, during
    assert during["entries_deferred"] == 1, during
    assert during["deferred_handoffs"] == ["promotion-handoff"], during
    assert during["entries_shared"] == 0, during
    assert during["tokens_released"] == 0, during
    assert during["tokens_decharged"] == 0, during
    assert staged.exists()
    assert paths_a["fragment"].exists() and paths_a["material"].exists()
    assert paths_b["fragment"].exists() and paths_b["material"].exists()
    assert ledger.holder_tokens(MOVER_A).get(KIND) == 1
    assert ledger.holder_tokens(MOVER_B).get(KIND) == 1

    # The real promotion proves its own consumer's source cover before it
    # copies: with A's retained fragment it completes, and the cover names A
    # -- B's fragment alone would have left it a source-coverage-gap.
    promotion = _promote(queue, stage, ram, manifest_path, promo,
                         CONSUMER_A, MIB)
    assert [cover["mover_action_key"]
            for cover in promotion["source_covers"]] == [MOVER_A], promotion
    assert (ram / STAGED_NAME).exists()

    # The handoff ends; now the ordinary shared settlement applies.
    queue.finish(promo, status="executed")
    shared = stage_release.evict(queue, MOVER_A,
                                 consumer_action_key=CONSUMER_A,
                                 stage_root=str(stage))
    assert shared["complete"] is True, shared
    assert shared["entries_shared"] == 1, shared
    assert shared["entries_deleted"] == 0, shared
    assert shared["bytes_shared"] == MIB, shared
    assert shared["tokens_released"] == 0, shared
    assert shared["tokens_decharged"] == 1, shared
    assert staged.exists()
    assert paths_b["fragment"].exists()
    assert not paths_a["fragment"].exists()
    assert ledger.holder_tokens(MOVER_A).get(KIND, 0) == 0
    assert ledger.holder_tokens(MOVER_B).get(KIND) == 1

    last = stage_release.evict(queue, MOVER_B,
                               consumer_action_key=CONSUMER_B,
                               stage_root=str(stage))
    assert last["complete"] is True, last
    assert last["entries_deleted"] == 1, last
    assert last["tokens_released"] == 1, last
    assert not staged.exists()
    assert ledger.holder_tokens(MOVER_B).get(KIND, 0) == 0


def test_a_handoff_without_material_retains_bytes_fragment_and_charge(
        fleet, tmp_path: Path) -> None:
    queue, stage = fleet
    _, manifest_path = _manifest(tmp_path)
    _stage_mover(queue, stage, manifest_path, MOVER_A, CONSUMER_A, MIB)
    paths = _paths(queue, CONSUMER_A, MOVER_A)
    # An unqualifiable date is unknown, never a reason to drop the proof or
    # the charge while a live handoff still reads the bytes.
    paths["material"].unlink()
    promo, _ = _promotion_claim(queue, stage, manifest_path, CONSUMER_A, MIB)

    staged = stage / STAGED_NAME
    ledger = queue.tier_ledger(TIER)
    first = stage_release.evict(queue, MOVER_A,
                                consumer_action_key=CONSUMER_A,
                                stage_root=str(stage))
    assert first["complete"] is False, first
    assert first["entries_deferred"] == 1, first
    assert first["retiring"] is False, first
    assert first["tokens_released"] == 0, first
    assert staged.exists() and paths["fragment"].exists()
    assert ledger.holder_tokens(MOVER_A).get(KIND) == 1

    (queue.dir(pool.CLAIMED) / f"{promo}.json").unlink()
    second = stage_release.evict(queue, MOVER_A,
                                 consumer_action_key=CONSUMER_A,
                                 stage_root=str(stage))
    assert second["complete"] is True, second
    assert second["entries_deleted"] == 1, second
    assert second["tokens_released"] == 1, second
    assert not staged.exists()


def test_a_malformed_promotion_claim_taints_and_retains(
        fleet, tmp_path: Path) -> None:
    queue, stage = fleet
    _, manifest_path = _manifest(tmp_path)
    _stage_mover(queue, stage, manifest_path, MOVER_A, CONSUMER_A, MIB)
    _promotion_claim(queue, stage, manifest_path, CONSUMER_A, MIB,
                     command=["python3", "ram_promote.py",
                              "--source-stage-root", str(stage),
                              "--range-start-bytes", str(MIB),
                              "--range-end-bytes", "0"])

    paths = _paths(queue, CONSUMER_A, MOVER_A)
    staged = stage / STAGED_NAME
    ledger = queue.tier_ledger(TIER)
    receipt = stage_release.evict(queue, MOVER_A,
                                  consumer_action_key=CONSUMER_A,
                                  stage_root=str(stage))
    assert receipt["complete"] is False, receipt
    assert receipt["entries_deleted"] == 0, receipt
    assert any("invalid source range" in error
               for error in receipt["errors"]), receipt
    assert staged.exists() and paths["fragment"].exists()
    assert paths["material"].exists()
    assert ledger.holder_tokens(MOVER_A).get(KIND) == 1
