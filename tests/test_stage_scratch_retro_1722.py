"""Issue 1722 rollup pin: stage recovery, funding release, scratch lifetime.

Each test ties one acceptance line to its composition. Deep suites pin
the parts. These tests pin the wiring between them.
"""
from __future__ import annotations

import hashlib
import json
import os
import secrets
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import client, core as pb, local_scratch, pool  # noqa: E402
from prismabuild import produced_output as po  # noqa: E402
from prismabuild import resource_scope, storage_tiers  # noqa: E402
import prewarm_loop  # noqa: E402
import stage_reclaim  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402
from stage_move import stage_relative  # noqa: E402

from admitted_queue_fixture import AdmittedQueueFixture  # noqa: E402
from test_scratch_declaration_record import live_record, process, runtime  # noqa: E402,F401
from test_scratch_lifetime_pool import leaf, lifetime_state  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
KIND = "stage_gib"
GIB = storage_tiers.GIB
SIZE = 4096


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


# -- source-mark reclaim and restore ---------------------------------------


def _staged(stage: Path, mount: Path, name: str) -> Path:
    return stage / stage_relative(f"{mount}/{name}", 0, SIZE,
                                  mount_prefix=str(mount))


def _stage_copy(stage: Path, mount: Path, name: str, payload: bytes) -> Path:
    path = _staged(stage, mount, name)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    os.setxattr(path, prewarm_loop.STAGE_SOURCE_XATTR, b"0:0:0")
    return path


@pytest.fixture()
def staged(tmp_path: Path):
    """One sole digest-proven copy, one unpaired copy, one odd original."""
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    stage = tmp_path / "stage"
    stage.mkdir()
    assert stage_release.register_stage_root(
        queue, tier_id=TIER, stage_root=stage) == "registered"
    mount = tmp_path / "originals"
    mount.mkdir()
    good = hashlib.sha256(b"g" * SIZE).digest() * (SIZE // 32)
    (mount / "sole.bin").write_bytes(good)
    sole = _stage_copy(stage, mount, "sole.bin", good)
    _stage_copy(stage, mount, "unpaired.bin", good)
    (mount / "odd.bin").write_bytes(b"o" * SIZE)
    with tempfile.TemporaryDirectory(prefix="pb-1722-", dir="/dev/shm") as root:
        quarantine = Path(root)
        assert stage.stat().st_dev != quarantine.stat().st_dev
        yield queue, stage, mount, quarantine, good, sole


def _reclaim_params(staged, **kwargs):
    queue, stage, mount, quarantine, _good, _sole = staged
    params = {"tier_id": TIER, "stage_root": str(stage),
              "mount_prefix": str(mount),
              "quarantine_root": str(quarantine), "run_id": "run-1722"}
    params.update(kwargs)
    return queue, params


def test_reclaim_moves_sole_copies_and_restores_them_intact(staged) -> None:
    """Sole copies reach quarantine. Restore returns them intact."""
    queue, params = _reclaim_params(staged)
    _queue, stage, mount, quarantine, good, sole = staged
    dry = stage_reclaim.reclaim_source_marks(queue, **params)
    assert dry["complete"] is True, dry["errors"]
    rows = {row["stage_rel"]: row for row in dry["entries"]}
    sole_rel = str(sole.relative_to(stage))
    assert rows[sole_rel]["status"] == "paired"
    assert rows[sole_rel]["sha256"] == hashlib.sha256(good).hexdigest()
    unpaired_rel = str(_staged(stage, mount, "unpaired.bin").relative_to(stage))
    assert rows[unpaired_rel]["status"] == "original_missing"
    applied = stage_reclaim.reclaim_source_marks(queue, apply=True, **params)
    assert applied["complete"] is True, applied["errors"]
    assert applied["entries_moved"] == 1
    assert not sole.exists()
    manifest = stage_reclaim.read_manifest(quarantine / "run-1722")
    assert len(manifest["entries"]) == 1
    entry = manifest["entries"][0]
    assert entry["sha256"] == hashlib.sha256(good).hexdigest()
    assert Path(entry["quarantine_path"]).read_bytes() == good
    restored = stage_reclaim.restore_run(
        queue, stage_root=str(stage),
        quarantine_root=str(quarantine), run_id="run-1722")
    assert restored["complete"] is True, restored["errors"]
    assert restored["entries_restored"] == 1
    assert sole.read_bytes() == good
    assert os.getxattr(sole, prewarm_loop.STAGE_SOURCE_XATTR) == b"0:0:0"


# -- dead-producer rollback through the cycle ------------------------------


def _template(prefix: str) -> dict:
    body = {
        "schema": po.TEMPLATE_SCHEMA_V1,
        "version": 1,
        "template_id": "prepaid-v1",
        "output_prefix": prefix,
        "slots": {"s0": {"class": "payload"}},
        "durable_maxima": {
            "payload_max_bytes": 1 << 20,
            "checkpoint_max_bytes": 1 << 20,
            "temp_max_bytes": 1 << 20,
        },
        "working_demands": {TIER: {"minimum_gib": 1, "window_gib": 2}},
        "permitted_tiers": [TIER],
    }
    return po.validate_template(body)


def _queue(tmp_path: Path, gib: int = 6) -> pool.PoolQueue:
    queue = AdmittedQueueFixture(
        pool.PoolQueue(tmp_path / "pb-queue"), capacity={"cpu": 8, "mem_gb": 16},
        default_demand={"cpu": 1, "mem_gb": 1})
    queue.ensure_layout()
    queue.mint_tier_capacity(TIER, {KIND: gib})
    return queue


def _bind(queue, template, owner):
    terms = po.owner_demand_terms(template)
    queue.publish(action_key=owner, cas_root="/cas", worker_script="/w.py",
                  checkout_root="/co", resources={"cpu": 1, "mem_gb": 1, **terms},
                  produced_output_template=template, max_attempts=1)
    claimed = queue.claim(owner="w-owner")
    assert claimed is not None and claimed["action_key"] == owner
    nonce = secrets.token_hex(16)
    scope = resource_scope.ResourceScope(
        owner, nonce, 1 * 1024 ** 3, queue.root / "telemetry" / f"{owner}.json")
    intake = ("prismabuild-job"
              + hashlib.sha256((owner + nonce).encode()).hexdigest()[:32]
              + ".slice")
    scope._adopt_created_scope({
        "scope_id": intake, "token": secrets.token_hex(32),
        "cgroup_path": str(Path("/sys/fs/cgroup/prismabuild.slice") / intake),
    })
    control = scope.control_record()
    path = queue.item_path(pool.CLAIMED, owner)
    live = pool._read_json(path)
    assert live is not None
    live["resource_scope"] = control
    pool._write_json_atomic(path, live)
    env = {"PRISMABUILD_ACTION_KEY": owner,
           "PRISMABUILD_ACTION_NONCE": control["nonce"],
           "PRISMABUILD_ACTION_SCOPE": control["scope_id"]}
    po.declare_template(queue.root, template)
    inst = po.bind_instance(queue, template, owner_action_key=owner,
                            claim_snapshot=claimed, env=env)
    po.declare_instance(queue.root, inst)
    assert po.admit_instance(queue, inst, template)["ok"] is True
    return inst


def _one_batch(queue, tmp_path, template, inst, owner, mover, batch_id, name):
    origin = tmp_path / "outputs"
    origin.mkdir(parents=True, exist_ok=True)
    path = origin / name
    payload = b"x" * 1024
    path.write_bytes(payload)
    descs = [po.validate_descriptor({
        "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": "s0",
        "artifact_class": "payload", "path": str(path),
        "bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest(),
        "producer_generation": po.mint_generation(),
        "owner_action_key": inst["owner_action_key"],
        "owner_attempt": dict(inst["owner_attempt"]),
    }, template, inst)]
    classes = {"payload": len(payload), "checkpoint": 0, "temp": 0}
    out = po.require_prewrite(queue, inst, template, batch_id=batch_id,
                              tier=TIER, class_bytes=classes,
                              paths=[str(path)])
    assert out.get("ok") is True, out
    manifest = po.output_manifest_sha256(descs)
    total = len(payload)
    staged = queue.stage_output_intent(
        tier_id=TIER, owner_key=owner, mover_key=mover, instance=inst,
        template=template, batch_id=batch_id, descriptors=descs)
    assert staged.get("ok") is True, staged
    queue.publish(action_key=mover, cas_root="/cas", worker_script="/w.py",
                  checkout_root="/co",
                  resources={"cpu": 1, "mem_gb": 1, f"{KIND}@{TIER}": 1},
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                             "tier_id": TIER, "manifest_sha256": manifest,
                             "manifest_bytes": total,
                             "range_start_bytes": 0, "range_end_bytes": total},
                  produced_output_batch=pool.PoolQueue.build_produced_output_batch_ref(
                      instance=inst, template=template, batch_id=batch_id,
                      descriptors=descs, tier_id=TIER))
    funded = queue.fund_output_batch(
        tier_id=TIER, owner_key=owner, mover_key=mover, instance=inst,
        template=template, batch_id=batch_id, descriptors=descs)
    assert funded.get("ok") is True, funded
    return path


def _discover(tmp_path, capacity_bytes=6 * GIB):
    def discover(**_kwargs):
        return {TIER: {
            "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
            "tier_id": TIER, "host": "dl380g10", "tier": "stage",
            "mountpoint": str(tmp_path / "stage"),
            "capacity_bytes": capacity_bytes,
            "capacity_source": storage_tiers.WRITABLE_CAPACITY_SOURCE,
        }}
    return discover


def test_cycle_rolls_back_dead_producer_funds_token_first(tmp_path, monkeypatch) -> None:
    """The cycle frees tokens before it closes the funding marker."""
    queue = _queue(tmp_path)
    ledger = queue.tier_ledger(TIER)
    owner = _hexkey("1722-owner")
    mover = _hexkey("1722-mover")
    live_owner = _hexkey("1722-live-owner")
    live_mover = _hexkey("1722-live-mover")
    template = _template(str(tmp_path / "outputs"))
    inst = _bind(queue, template, owner)
    live_inst = _bind(queue, template, live_owner)
    dead_path = _one_batch(queue, tmp_path, template, inst, owner,
                           mover, "dead", "dead.bin")
    _one_batch(queue, tmp_path, template, live_inst, live_owner,
               live_mover, "live", "live.bin")
    receipts = tier_loop.ReceiptCache()
    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=receipts, discover=_discover(tmp_path))
    queue.finish(owner, status="failed", detail={"returncode": 1})
    assert queue.withdraw(mover, by="test", reason="producer failed") is not None
    dead_path.unlink()
    order: list[str] = []
    release = pool.PoolQueue.release_tier_holder
    mark = pool.PoolQueue._release_never_started_funding_locked

    def release_first(self, tier_id, holder, *args, **kwargs):
        if holder == mover:
            order.append("tokens")
        return release(self, tier_id, holder, *args, **kwargs)

    def mark_second(self, mover_key, tier_id, *args, **kwargs):
        if mover_key == mover:
            order.append("marker")
        return mark(self, mover_key, tier_id, *args, **kwargs)

    monkeypatch.setattr(pool.PoolQueue, "release_tier_holder", release_first)
    monkeypatch.setattr(pool.PoolQueue, "_release_never_started_funding_locked",
                        mark_second)
    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=receipts, discover=_discover(tmp_path))
    assert queue.read_output_funding(mover, TIER)["state"] == "released"
    assert ledger.holder_tokens(mover).get(KIND, 0) == 0
    assert "tokens" in order and "marker" in order
    assert order.index("tokens") < order.index("marker"), order
    live = queue.read_output_funding(live_mover, TIER)
    assert live is not None and live["state"] == "transferring"
    assert ledger.holder_tokens(live_mover).get(KIND, 0) == 1


# -- exact waste and deficit reserve ---------------------------------------


def test_waste_is_exact_and_deficit_keeps_live_owners(tmp_path) -> None:
    """Waste reads whole GiB. A short deficit refuses without touching live holds."""
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    writable_gib = 60
    discover = _discover(tmp_path, capacity_bytes=writable_gib * GIB)
    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(), discover=discover)
    ledger = queue.tier_ledger(TIER)
    mover = _hexkey("1722-landed")
    live = _hexkey("1722-live-hold")
    assert ledger.acquire(mover, {KIND: 3})
    assert ledger.acquire(live, {KIND: 2})
    span = int(2.4 * GIB)
    queue.record_move(mover, {"tier_id": TIER, "complete": True,
                              "bytes_staged": span,
                              "range_start_bytes": 0, "range_end_bytes": span,
                              "manifest_sha256": "9" * 64})
    announced = tier_loop.cycle(
        queue, host="dl380g10", source_pool="storage_pool",
        receipts=tier_loop.ReceiptCache(), discover=discover)
    record = announced[0]
    assert record["landed_bytes"] == span
    assert record["landed_rounding_gib"] == 3 - span // GIB
    assert record["landed_rounding_gib"] == 1
    assert record["in_flight_unknown_gib"] == 2
    assert record["in_flight_rounding_gib"] == 0
    free = ledger.available().get(KIND, 0)
    state, _detail = queue.take_tier_advance(TIER, "grant-1722", free + 1, KIND)
    assert state == "tier-short"
    assert ledger.holder_tokens(mover).get(KIND, 0) == 3
    assert ledger.holder_tokens(live).get(KIND, 0) == 2
    assert ledger.available().get(KIND, 0) == free
    state, _detail = queue.take_tier_advance(TIER, "grant-1722", free, KIND)
    assert state == "taken"
    assert ledger.holder_tokens(live).get(KIND, 0) == 2
    assert ledger.available().get(KIND, 0) == 0


# -- SDK lifetime contract -------------------------------------------------


SELECTION = {"schema": "prismabuild.scratch_lifetime_selection.v1", "entries": [
    {"root_env": "TEMP_ROOT", "name": "row-temp", "lifetime": "ephemeral"},
    {"root_env": "CACHE_ROOT", "name": "compile", "lifetime": "persistent"}]}


def _declaration(tmp_path, *, host="fixture", published=7.0, nonce="b" * 32):
    key = "a" * 64
    scope = ("prismabuild-job"
             + hashlib.sha256((key + nonce).encode()).hexdigest()[:32]
             + ".slice")
    return {"schema": local_scratch.EPHEMERAL_SCRATCH_SCHEMA_V1,
            "lifetime": "ephemeral", "root_env": "ROOT", "max_env": "MAX",
            "root": str(tmp_path / "scratch"), "max_bytes": 1024,
            "name": "row-temp", "owner_action_key": key,
            "owner_published_unix": published, "owner_host": host,
            "owner_attempt": {"nonce": nonce, "scope_id": scope}}


def test_sdk_lifetime_binds_host_generation_attempt(tmp_path) -> None:
    """The SDK contract and the leaf path name host, generation, attempt."""
    assert client.SDK_VERSION == 6
    assert client.SCRATCH_LIFETIME_TAG == "scratch-lifetime-v1"
    assert client.SCRATCH_LIFETIME_TAG in client.CAPABILITIES
    assert callable(client.build_scratch_lifetime_selection)
    selection = client.build_scratch_lifetime_selection([
        {"root_env": "TEMP_ROOT", "name": "row-temp", "lifetime": "ephemeral"}])
    assert selection["schema"] == client.SCRATCH_LIFETIME_SELECTION_SCHEMA_V1
    first = local_scratch.ephemeral_scratch_path(_declaration(tmp_path))
    assert first.name == "row-temp"
    assert "a" * 64 in first.parts
    assert ("b" * 32) in first.parts
    assert (local_scratch.ephemeral_scratch_path(_declaration(tmp_path))
            == first)
    assert (local_scratch.ephemeral_scratch_path(
        _declaration(tmp_path, host="other")) != first)
    assert (local_scratch.ephemeral_scratch_path(
        _declaration(tmp_path, published=8.0)) != first)
    assert (local_scratch.ephemeral_scratch_path(
        _declaration(tmp_path, nonce="c" * 32)) != first)


@pytest.fixture()
def lifetime(runtime, monkeypatch):
    create, calls = runtime
    return lifetime_state(create, calls, monkeypatch)


def test_finish_wipes_ephemeral_and_releases_charge(
        lifetime, monkeypatch) -> None:
    """Finish deletes the ephemeral leaf and frees the held capacity."""
    queue, item, _variables, _calls = lifetime
    process(monkeypatch)
    outcome = queue.execute(item, containment=True)
    old_leaf = leaf(item)
    assert old_leaf.is_dir()
    (old_leaf / "temp").write_bytes(b"temporary")
    result = queue.finish(item["action_key"], status=outcome["status"],
                          detail=outcome, claim_snapshot=item)
    assert not old_leaf.exists()
    assert queue.ledger().held() == {}
    assert result != queue.item_path(pool.CLAIMED, item["action_key"])
