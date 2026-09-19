"""Chunking is a sealing-time property: the submitter cuts the stage leg.

``pbrun`` seals every movement node the window will ever publish, because an
action key is a hash and the coordinator cannot publish children the
submitter never sealed.  A phase bigger than the stage tier's effective
chunk seals one movement node plus one egress node per chunk, in read order,
each carrying its phase, its chunk index and its chunk range -- the movement
node shape is otherwise today's, so the window, the sweep and the map need
no new node kind.  A phase that fits in one chunk seals today's whole-phase
pair, and a tier that announces no sizing seals it too.
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
STAGE_KIND = f"stage_gib@{STAGE_TIER}"
GIB = storage_tiers.GIB
#: The live arithmetic: chunk 40, a 123 GiB phase in 4 chunks.
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


def _seal(tmp_path: Path, queue: pool.PoolQueue, *,
          with_ram_tier: bool = True, tier_override=None):
    import pbrun

    manifest_path = tmp_path / "manifest.json"
    if not manifest_path.exists():
        manifest_path.write_text(json.dumps(_manifest()))
    digest = hashlib.sha256(manifest_path.read_bytes()).hexdigest()

    def discover(**_kwargs):
        tiers = {
            STAGE_TIER: {
                "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                "tier_id": STAGE_TIER, "host": "dl380g10", "tier": "stage",
                "mountpoint": str(tmp_path / "stage"),
                "capacity_bytes": 512 * GIB},
        }
        if with_ram_tier:
            tiers[RAM_TIER] = {
                "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                "tier_id": RAM_TIER, "host": "dl380g10", "tier": "ram",
                "mountpoint": str(tmp_path / "ram"),
                "capacity_bytes": 160 * GIB,
                "window_gib": 160,
                "promotion_chunk_gib": CHUNK_GIB}
        return tiers

    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(), discover=discover)
    tier = pbrun.resolve_stage_tier(queue, None)
    if tier_override is not None:
        tier = tier_override(dict(tier))
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
    staged["tier"] = tier
    return staged


def test_a_phase_bigger_than_the_chunk_seals_one_node_per_chunk(
        tmp_path) -> None:
    staged = _seal(tmp_path, pool.PoolQueue(tmp_path / "pb-queue"))
    plan = staged["plan"]
    assert isinstance(plan, dict)
    big = plan["phases"][0]  # type: ignore[index]

    assert big["name"] == "phase-0"
    assert "mover_row" not in big and "egress_row" not in big
    chunks = big["stage_chunks"]
    assert [(chunk["chunk_index"], chunk["start_bytes"], chunk["end_bytes"])
            for chunk in chunks] == [
        (0, 0, 40 * GIB), (1, 40 * GIB, 80 * GIB),
        (2, 80 * GIB, 120 * GIB), (3, 120 * GIB, 123 * GIB)]
    assert [chunk["stage_gib"] for chunk in chunks] == [40, 40, 40, 3]
    for chunk in chunks:
        gib = chunk["stage_gib"]
        assert chunk["mover_row"]["resources"][STAGE_KIND] == gib
        pin = chunk["mover_row"]["residency"]
        assert pin["tier_id"] == STAGE_TIER
        assert (pin["range_start_bytes"], pin["range_end_bytes"]) == (
            chunk["start_bytes"], chunk["end_bytes"])
    keys = [chunk["mover_row"]["action_key"] for chunk in chunks]
    keys += [chunk["egress_row"]["action_key"] for chunk in chunks]
    assert len(set(keys)) == 8
    # Every sealed node is published to the CAS: the coordinator can only
    # publish children the submitter sealed.
    cas = staged["cas"]
    assert isinstance(cas, _Cas)
    for key in keys:
        assert key in cas.actions


def test_a_chunk_mover_carries_todays_argv_over_its_own_range(
        tmp_path) -> None:
    """The movement node shape is otherwise today's: the same flags, the
    chunk's range, a chunk suffix on the log -- so the window, the sweep
    and the map need no new node kind."""

    import pbrun

    staged = _seal(tmp_path, pool.PoolQueue(tmp_path / "pb-queue"))
    tier = staged["tier"]
    cas = staged["cas"]
    assert isinstance(cas, _Cas)
    big = staged["plan"]["phases"][0]  # type: ignore[index]
    chunk = big["stage_chunks"][1]
    digest = hashlib.sha256(
        (tmp_path / "manifest.json").read_bytes()).hexdigest()
    body = cas.actions[chunk["mover_row"]["action_key"]]
    assert body["params"]["command"] == [
        str(tier["mover_python"]),
        str(Path(str(tier["mover_tools_root"])) / "stage_move.py"),
        "--pool-root", str(pbrun.SH / "pb-queue"),
        "--cas-root", str(pbrun.SH / "cas"),
        "--consumer-action-key", CONSUMER,
        "--tier-id", STAGE_TIER,
        "--stage-root", str(tier["mountpoint"]),
        "--manifest-sha256", digest,
        "--range-start-bytes", str(40 * GIB),
        "--range-end-bytes", str(80 * GIB),
        "--readers", str(READERS)]
    assert body["task"]["result_path"] == "stage-move-0000-phase-0-c01.log"
    egress = cas.actions[chunk["egress_row"]["action_key"]]
    assert egress["task"]["result_path"] == (
        "stage-release-0000-phase-0-c01.log")
    assert egress["params"]["command"][:6] == [
        str(tier["mover_python"]),
        str(Path(str(tier["mover_tools_root"])) / "stage_release.py"),
        "--pool-root", str(pbrun.SH / "pb-queue"),
        "--mover-action-key", chunk["mover_row"]["action_key"]]


def test_a_phase_that_fits_seals_todays_whole_phase_pair(tmp_path) -> None:
    staged = _seal(tmp_path, pool.PoolQueue(tmp_path / "pb-queue"))
    plan = staged["plan"]
    assert isinstance(plan, dict)
    small = plan["phases"][1]  # type: ignore[index]

    assert "stage_chunks" not in small
    pin = small["mover_row"]["residency"]
    assert (pin["range_start_bytes"], pin["range_end_bytes"]) == (
        123 * GIB, 133 * GIB)
    assert small["mover_row"]["resources"][STAGE_KIND] == SMALL_PHASE_GIB


def test_a_tier_announcing_no_sizing_seals_whole_phase_pairs(tmp_path) -> None:
    """A stage record from a generation predating the announcement seals
    the shape the window has published since #583 -- one pair per phase."""

    staged = _seal(tmp_path, pool.PoolQueue(tmp_path / "pb-queue"),
                   with_ram_tier=False,
                   tier_override=lambda tier: (
                       {key: value for key, value in tier.items()
                        if key != "promotion_chunk_gib"}))
    plan = staged["plan"]
    assert isinstance(plan, dict)

    for phase in plan["phases"]:
        assert "stage_chunks" not in phase
        assert "mover_row" in phase and "egress_row" in phase


def test_a_window_gib_without_a_chunk_derives_the_quarter(tmp_path) -> None:
    """The fallback arm, shaped like the ram leg's: a record carrying a
    window but no pin cuts window quarters rather than guessing."""

    staged = _seal(tmp_path, pool.PoolQueue(tmp_path / "pb-queue"),
                   with_ram_tier=False,
                   tier_override=lambda tier: (
                       {**{key: value for key, value in tier.items()
                           if key != "promotion_chunk_gib"},
                        "window_gib": 160}))
    plan = staged["plan"]
    assert isinstance(plan, dict)
    big = plan["phases"][0]  # type: ignore[index]

    assert [(chunk["chunk_index"], chunk["start_bytes"], chunk["end_bytes"])
            for chunk in big["stage_chunks"]] == [
        (0, 0, 40 * GIB), (1, 40 * GIB, 80 * GIB),
        (2, 80 * GIB, 120 * GIB), (3, 120 * GIB, 123 * GIB)]


def test_the_sealed_plan_freezes_and_leads_with_the_first_chunk(
        tmp_path) -> None:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    staged = _seal(tmp_path, queue)
    plan = staged["plan"]
    assert isinstance(plan, dict)

    frozen = residency_plan.freeze(queue, plan)

    assert len(residency_plan.stage_mover_keys(frozen)) == 5
    assert len(residency_plan.mover_keys(frozen)) == 10
    assert residency_plan.leads_for(frozen) == [
        str(frozen["phases"][0]["stage_chunks"][0]["mover_row"][  # type: ignore[index]
            "action_key"])]
