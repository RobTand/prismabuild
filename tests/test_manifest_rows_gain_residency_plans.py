"""#1247: a READY row's declared manifest becomes a tier residency plan.

The planner is the submitter's own sealing path invoked by the tier role
(``tools/fleet/manifest_promotion.py``): for the first READY rows in claim
order it seals movement nodes off the row's sealed request and freezes the
plan, and the tier loop's ordinary adoption pass publishes the movers.  A row
with a filed plan stands down; a row without a manifest is not the planner's;
the bound is rows, not bytes, because the resident-byte bound is the tier's
window and eviction.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

import prismabuild.pool as pool
import prismabuild.storage_tiers as storage_tiers
from prismabuild import residency_plan

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import manifest_promotion  # noqa: E402

HOST = "dl380g10"
STAGE_TIER = "prismabuild-stage:dl380g10"


def _manifest(entries: list[dict]) -> dict:
    total = sum(int(entry["bytes"]) for entry in entries)
    phases, position = [], 0
    for index, entry in enumerate(entries):
        position += int(entry["bytes"])
        phases.append({"name": f"phase-{index}",
                       "bytes": int(entry["bytes"]),
                       "cumulative_bytes": position})
    return {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "entry_count": len(entries),
        "mount_prefix": "/mnt/shared",
        "total_bytes": total,
        "entries": entries,
        "annotations": {"row_id": "row-test", "phases": phases},
    }


def _queue_at(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    return queue


def _stage_tier(tmp_path: Path) -> dict:
    return {
        "schema": "prismabuild.storage_tier.v1",
        "tier": "stage",
        "tier_id": STAGE_TIER,
        "host": HOST,
        "mountpoint": str(tmp_path / "stage"),
        "mover_python": sys.executable,
        "mover_tools_root": str(Path(__file__).resolve().parents[1]
                                / "tools" / "fleet"),
    }


def _publish_row(queue: pool.PoolQueue, tmp_path: Path, key: str,
                 manifest: dict | None) -> Path:
    """Publish a READY row; with a manifest, seal its request in the CAS."""

    cas_root = tmp_path / "cas"
    if manifest is not None:
        blob = json.dumps(manifest).encode("utf-8")
        digest = hashlib.sha256(blob).hexdigest()
        (cas_root / "blobs" / digest[:2]).mkdir(parents=True, exist_ok=True)
        (cas_root / "blobs" / digest[:2] / digest).write_bytes(blob)
        manifest_input = {"id": "pbcampaign.data-manifest",
                          "sha256": digest, "bytes": len(blob)}
    else:
        manifest_input = None
    request = {
        "schema": "prismaquant.prismabuild.action_request.v1",
        "action_key": key,
        "task": {"argv": ["/bin/true"], "working_directory": "."},
        "params": {"cwd": "."},
        "inputs": ([manifest_input] if manifest_input is not None else []),
    }
    if manifest_input is not None:
        request["params"]["data_manifest"] = {
            "input": manifest_input,
            "entry_count": manifest["entry_count"],
            "mount_prefix": manifest["mount_prefix"],
            "total_bytes": manifest["total_bytes"],
        }
    (cas_root / "requests" / key[:2]).mkdir(parents=True, exist_ok=True)
    (cas_root / "requests" / key[:2] / f"{key}.json").write_text(
        json.dumps(request))
    queue.publish(
        action_key=key, cas_root=cas_root,
        checkout_root=tmp_path / "checkout",
        worker_script=tmp_path / "worker.py", tags=[HOST],
        resources={"cpu": 1, "mem_gb": 2})
    return cas_root


def _ready_item(queue: pool.PoolQueue, key: str) -> dict:
    import json as _json
    return _json.loads(
        (queue.root / "ready" / f"{key}.json").read_text())


def test_a_declaring_row_gains_a_filed_plan_and_a_tier_receipt(tmp_path):
    """One READY manifest row: plan filed, movers sealed, receipt stamped."""

    queue = _queue_at(tmp_path)
    key = "a" * 64
    entries = [{"path": "/mnt/shared/models/x/model-00001.safetensors",
                "offset": 0, "bytes": 64 * 1024 * 1024},
               {"path": "/mnt/shared/models/x/model-00002.safetensors",
                "offset": 0, "bytes": 64 * 1024 * 1024}]
    cas_root = _publish_row(queue, tmp_path, key, _manifest(entries))

    outcomes = manifest_promotion.promote_ready_manifest_rows(
        queue, cas_root, _stage_tier(tmp_path),
        ready=[_ready_item(queue, key)])

    assert len(outcomes) == 1
    assert outcomes[0]["outcome"] == "planned", outcomes[0]
    assert outcomes[0]["phases"] == 2
    # The plan is filed under the consumer's key, first-writer.
    plan = residency_plan.read(queue, key)
    assert plan is not None
    assert str(plan["tier_id"]) == STAGE_TIER
    # Every phase names a mover; the movement requests are sealed in the CAS
    # for the tier loop's adoption pass to publish.
    for phase in plan["phases"]:
        mover = str(phase["mover_row"]["action_key"])
        assert (Path(cas_root) / "requests" / mover[:2]
                / f"{mover}.json").exists()
    # The receipt carries the additive tier block.
    record = queue.prewarm(key)
    assert record is not None
    tier = record.get("tier")
    assert isinstance(tier, dict)
    assert tier["destination"] == manifest_promotion.TIER_RECEIPT_DESTINATION
    assert tier["status"] == "planned"
    assert tier["phases"] == 2


def test_a_row_with_a_filed_plan_stands_down(tmp_path):
    """The planner never touches a consumer that already has a plan."""

    queue = _queue_at(tmp_path)
    key = "b" * 64
    entries = [{"path": "/mnt/shared/models/x/model-00001.safetensors",
                "offset": 0, "bytes": 32 * 1024 * 1024}]
    cas_root = _publish_row(queue, tmp_path, key, _manifest(entries))
    assert manifest_promotion.promote_ready_manifest_rows(
        queue, cas_root, _stage_tier(tmp_path),
        ready=[_ready_item(queue, key)])[0]["outcome"] == "planned"

    outcomes = manifest_promotion.promote_ready_manifest_rows(
        queue, cas_root, _stage_tier(tmp_path),
        ready=[_ready_item(queue, key)])

    assert outcomes[0]["outcome"] == "stands_down"


def test_a_row_without_a_manifest_is_not_the_planners(tmp_path):
    """A plain READY row is examined and passed over, never refused."""

    queue = _queue_at(tmp_path)
    key = "c" * 64
    cas_root = _publish_row(queue, tmp_path, key, None)

    outcomes = manifest_promotion.promote_ready_manifest_rows(
        queue, cas_root, _stage_tier(tmp_path),
        ready=[_ready_item(queue, key)])

    assert outcomes[0]["outcome"] == "no_manifest"
    assert residency_plan.read(queue, key) is None


def test_the_streaming_rule_plans_one_row_per_cycle(tmp_path):
    """The bound is rows: the second row waits for the next cycle."""

    queue = _queue_at(tmp_path)
    entries = [{"path": "/mnt/shared/models/x/model-00001.safetensors",
                "offset": 0, "bytes": 32 * 1024 * 1024}]
    manifest = _manifest(entries)
    first, second = "d" * 64, "e" * 64
    cas_root = _publish_row(queue, tmp_path, first, manifest)
    _publish_row(queue, tmp_path, second, manifest)

    outcomes = manifest_promotion.promote_ready_manifest_rows(
        queue, cas_root, _stage_tier(tmp_path),
        ready=[_ready_item(queue, first), _ready_item(queue, second)])

    assert [outcome["outcome"] for outcome in outcomes] == ["planned"]
    assert residency_plan.read(queue, first) is not None
    assert residency_plan.read(queue, second) is None


def test_a_refusal_is_receipted_and_never_raises(tmp_path):
    """A tier with no mountpoint refuses the row and records the reason."""

    queue = _queue_at(tmp_path)
    key = "f" * 64
    entries = [{"path": "/mnt/shared/models/x/model-00001.safetensors",
                "offset": 0, "bytes": 32 * 1024 * 1024}]
    cas_root = _publish_row(queue, tmp_path, key, _manifest(entries))
    broken = _stage_tier(tmp_path)
    broken["mountpoint"] = "relative/and/refused"

    outcomes = manifest_promotion.promote_ready_manifest_rows(
        queue, cas_root, broken, ready=[_ready_item(queue, key)])

    assert outcomes[0]["outcome"] == "refused"
    assert outcomes[0]["reason"]
    tier = (queue.prewarm(key) or {}).get("tier")
    assert isinstance(tier, dict) and tier["status"] == "refused"
    assert residency_plan.read(queue, key) is None
