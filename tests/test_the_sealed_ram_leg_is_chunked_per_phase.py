"""Chunking is a sealing-time property: the submitter cuts the ram leg.

``pbrun`` seals every movement node the window will ever publish, because an
action key is a hash and the coordinator cannot publish children the
submitter never sealed.  A phase bigger than the ram tier's effective chunk
seals one promotion node plus one egress node per chunk, in read order, each
carrying its phase, its chunk index and its chunk range -- the movement node
shape is otherwise today's, so the window, the sweep and the map need no new
node kind.  A phase that fits in one chunk seals today's whole-phase pair,
and a plan sealed before this change keeps the leg it was frozen with.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys
import types

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402

CONSUMER = "c" * 64
STAGE_TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:dl380g10"
RAM_KIND = f"ram_gib@{RAM_TIER}"
GIB = storage_tiers.GIB
#: The live arithmetic: window 160, chunk 40, a 123 GiB phase in 4 chunks.
WINDOW_GIB = 160
CHUNK_GIB = 40
BIG_PHASE_GIB = 123
SMALL_PHASE_GIB = 10
READERS = 4


def _manifest() -> dict[str, object]:
    sizes = [BIG_PHASE_GIB * GIB, SMALL_PHASE_GIB * GIB]
    entries, table, running = [], [], 0
    for index, size in enumerate(sizes):
        entries.append({"path": f"/mnt/shared/part-{index}", "offset": 0,
                        "bytes": size, "sha256": None})
        running += size
        table.append({"name": f"phase-{index}", "bytes": size,
                      "cumulative_bytes": running})
    return {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {}, "annotations": {"phases": table},
        "mount_prefix": "/mnt/shared", "entries": entries,
        "entry_count": len(entries), "total_bytes": running,
    }


class _Cas:
    def __init__(self, manifest_path: Path) -> None:
        self._manifest = manifest_path
        self.actions: dict[str, dict] = {}

    def input_path(self, entry):
        return self._manifest

    def publish_action_request(self, action) -> None:
        self.actions[str(action["action_key"])] = dict(action)


def _template(digest: str, size: int) -> dict[str, object]:
    import pbrun

    return {
        "cas": None, "marker_root": Path("/home/rob/tmp/markers"),
        "checkout_identity": {"commit": "a" * 40},
        "log_name": "x.log", "stamp_name": "pbrun.stamp",
        "task": {"definition_id": "fleet/pbrun", "definition_version": "v1",
                  "task_class": "generation", "determinism": "stochastic",
                  "artifact_family": "generic", "artifact_kind": "generic",
                  "working_directory": "."},
        "inputs": [{"id": "pbrun.checkout-snapshot", "sha256": "b" * 64,
                    "bytes": 4096}],
        "code_closure": pbrun.build_stamp_closure("pbrun.stamp", "{}"),
        "params": {"command": ["true"], "cwd": "/home/rob", "demand": {"cpu": 1},
                   "placement": {"required_tags": []},
                   "checkout_snapshot": {
                       "schema": pb.PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V1,
                       "commit": "a" * 40, "subdirectory": ".",
                       "input": {"id": pb.PBRUN_CHECKOUT_SNAPSHOT_INPUT_ID,
                                 "sha256": "b" * 64, "bytes": 4096}},
                   "retry_policy": {"max_attempts": 1, "retry_safe": False},
                   "data_manifest": {"input": {"sha256": digest, "bytes": size}}},
        "environment": {"variables": {"PATH": "/usr/bin"}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    }


def _seal(tmp_path: Path, queue: pool.PoolQueue):
    import pbrun

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(_manifest()))
    digest = hashlib.sha256(manifest_path.read_bytes()).hexdigest()

    def discover(**_kwargs):
        return {
            STAGE_TIER: {
                "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                "tier_id": STAGE_TIER, "host": "dl380g10", "tier": "stage",
                "mountpoint": str(tmp_path / "stage"),
                "capacity_bytes": 512 * GIB},
            RAM_TIER: {
                "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                "tier_id": RAM_TIER, "host": "dl380g10", "tier": "ram",
                "mountpoint": str(tmp_path / "ram"),
                "capacity_bytes": WINDOW_GIB * GIB,
                "window_gib": WINDOW_GIB,
                "promotion_chunk_gib": CHUNK_GIB},
        }

    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(), discover=discover)
    tier = pbrun.resolve_stage_tier(queue, None)
    args = types.SimpleNamespace(
        priority=-10, max_attempts=1, retry_safe=False,
        residency="stage", residency_tier=None, residency_mover_mem_gb=1,
        residency_mover_readers=READERS, residency_mover_max_attempts=3)
    cas = _Cas(manifest_path)
    staged = pbrun.residency_stage_rows(
        _template(digest, manifest_path.stat().st_size),
        consumer_action_key=CONSUMER, tier=tier, args=args, queue=queue,
        cas=cas)
    staged["cas"] = cas
    return staged


def test_a_phase_bigger_than_the_chunk_seals_one_node_per_chunk(
        tmp_path) -> None:
    staged = _seal(tmp_path, pool.PoolQueue(tmp_path / "pb-queue"))
    plan = staged["plan"]
    assert isinstance(plan, dict)
    big = plan["phases"][0]  # type: ignore[index]

    assert big["name"] == "phase-0"
    assert "ram_mover_row" not in big and "ram_egress_row" not in big
    chunks = big["ram_chunks"]
    assert [(chunk["chunk_index"], chunk["start_bytes"], chunk["end_bytes"])
            for chunk in chunks] == [
        (0, 0, 40 * GIB), (1, 40 * GIB, 80 * GIB),
        (2, 80 * GIB, 120 * GIB), (3, 120 * GIB, 123 * GIB)]
    for chunk in chunks:
        gib = (chunk["end_bytes"] - chunk["start_bytes"]) // GIB
        assert chunk["stage_gib"] == gib
        mover = chunk["ram_mover_row"]
        assert mover["resources"][RAM_KIND] == gib
        pin = mover["residency"]
        assert pin["tier_id"] == RAM_TIER
        assert (pin["range_start_bytes"], pin["range_end_bytes"]) == (
            chunk["start_bytes"], chunk["end_bytes"])
    keys = [chunk["ram_mover_row"]["action_key"] for chunk in chunks]
    keys += [chunk["ram_egress_row"]["action_key"] for chunk in chunks]
    assert len(set(keys)) == 8
    # Every sealed node is published to the CAS: the coordinator can only
    # publish children the submitter sealed.
    cas = staged["cas"]
    assert isinstance(cas, _Cas)
    for key in keys:
        assert key in cas.actions


def test_a_phase_that_fits_seals_todays_whole_phase_pair(tmp_path) -> None:
    staged = _seal(tmp_path, pool.PoolQueue(tmp_path / "pb-queue"))
    plan = staged["plan"]
    assert isinstance(plan, dict)
    small = plan["phases"][1]  # type: ignore[index]

    assert "ram_chunks" not in small
    pin = small["ram_mover_row"]["residency"]
    assert (pin["range_start_bytes"], pin["range_end_bytes"]) == (
        123 * GIB, 133 * GIB)
    assert small["ram_mover_row"]["resources"][RAM_KIND] == SMALL_PHASE_GIB


def test_the_sealed_plan_freezes_and_names_every_chunk_key(tmp_path) -> None:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    staged = _seal(tmp_path, queue)
    plan = staged["plan"]
    assert isinstance(plan, dict)

    frozen = residency_plan.freeze(queue, plan)

    assert len(residency_plan.ram_mover_keys(frozen)) == 5
    # The stage leg chunks beside the ram leg -- one chunk family across
    # tiers (#675): the cycle announces the ram record's chunk on the stage
    # record too, so the consumer's leads open with the first stage chunk's
    # mover rather than a whole-phase one.
    assert residency_plan.leads_for(frozen) == [
        str(frozen["phases"][0]["stage_chunks"][0]["mover_row"][  # type: ignore[index]
            "action_key"])]
