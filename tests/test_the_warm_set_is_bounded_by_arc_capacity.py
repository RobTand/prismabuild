"""``arc_gib`` is minted, spent, held and returned -- or it is a number nobody uses (#638).

The ARC tier announced 233 GiB every cycle and no action had ever asked for a
byte of it, so nothing bounded how much of one box's RAM the warm set tried to
occupy and successive phases evicted each other.  A mover that warms its range
into the file server's ARC now reserves that RAM the way it reserves the SSD it
copies onto: one ``arc_gib`` token per GiB of the same range, on the ARC tier of
the same box.

``arc_gib`` is **occupancy**, not a rate.  The bytes sit in RAM for as long as
the range is staged, so the tokens stay held past ``finish`` exactly as
``stage_gib`` does, and come back when the egress deletes the range (#636 is
the distinction; ``fill_mb_s_pool_side`` is the kind that returns at the end of
the copy).

ZFS exposes no pin and the ARC target ``c`` is volatile -- it fell 99 GB inside
one five-minute window on 2026-09-11 -- so this is a **budget**, never a
guarantee.  ``residency_verdict`` keeps gating on stage residency alone, which
is durable and checkable, and this file asserts that too.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys
import types

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402

CONSUMER = "c" * 64
STAGE = "prismabuild-stage:dl380g10"
ARC = "arc:dl380g10"
GIB = storage_tiers.GIB
PHASE_BYTES = 2 * GIB


def test_arc_capacity_is_occupancy_not_a_rate() -> None:
    """The one classification this whole file rests on."""

    assert storage_tiers.ARC_CAPACITY_KIND not in pool.TIER_RATE_KINDS
    assert storage_tiers.FILL_KIND in pool.TIER_RATE_KINDS


def test_a_range_asks_the_arc_for_what_it_asks_the_stage_for() -> None:
    demand = storage_tiers.residency_demand(
        tier_id=STAGE, range_start_bytes=0, range_end_bytes=PHASE_BYTES,
        arc_tier_id=ARC)

    assert demand[f"stage_gib@{STAGE}"] == 2
    assert demand[f"arc_gib@{ARC}"] == 2


def test_a_range_with_no_arc_tier_asks_for_no_arc() -> None:
    demand = storage_tiers.residency_demand(
        tier_id=STAGE, range_start_bytes=0, range_end_bytes=PHASE_BYTES)

    assert [key for key in demand if key.startswith("arc_gib")] == []


# ---------------------------------------------------------------- end to end


def _manifest(phases: int = 2) -> dict[str, object]:
    entries, table, running = [], [], 0
    for index in range(phases):
        entries.append({"path": f"/mnt/shared/part-{index}", "offset": 0,
                        "bytes": PHASE_BYTES, "sha256": None})
        running += PHASE_BYTES
        table.append({"name": f"phase-{index}", "bytes": PHASE_BYTES,
                      "cumulative_bytes": running})
    return {
        "schema": pb.DATA_MANIFEST_SCHEMA_V1,
        "produced_by": {}, "annotations": {"phases": table},
        "mount_prefix": "/mnt/shared", "entries": entries,
        "entry_count": len(entries), "total_bytes": running,
    }


class _Cas:
    def __init__(self, manifest_path: Path) -> None:
        self._manifest = manifest_path
        self.requested: list[str] = []

    def input_path(self, entry):
        return self._manifest

    def publish_action_request(self, action) -> None:
        self.requested.append(str(action["action_key"]))


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
                   "retry_policy": {"max_attempts": 1},
                   "data_manifest": {"input": {"sha256": digest, "bytes": size}}},
        "environment": {"variables": {"PATH": "/usr/bin"}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    }


def _fleet(tmp_path: Path, *, arc_gib: int, primarycache: str = "all"):
    """A box announcing both tiers, and one submission sealed against them."""

    import pbrun

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(_manifest()))
    digest = hashlib.sha256(manifest_path.read_bytes()).hexdigest()

    def discover(**_kwargs):
        return {
            STAGE: {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                    "tier_id": STAGE, "host": "dl380g10", "tier": "stage",
                    "mountpoint": str(tmp_path / "stage"),
                    "primarycache": primarycache,
                    "capacity_bytes": PHASE_BYTES},
            ARC: {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                  "tier_id": ARC, "host": "dl380g10", "tier": "arc",
                  "capacity_bytes": arc_gib * GIB},
        }

    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(), discover=discover)
    tier = pbrun.resolve_stage_tier(queue, None)
    args = types.SimpleNamespace(
        priority=-10, max_attempts=1, retry_safe=True,
        residency="stage", residency_tier=None, residency_mover_mem_gb=1,
        residency_mover_readers=4, residency_mover_max_attempts=3)
    staged = pbrun.residency_stage_rows(
        _template(digest, manifest_path.stat().st_size),
        consumer_action_key=CONSUMER, tier=tier, args=args, queue=queue,
        cas=_Cas(manifest_path))
    residency_plan.freeze(queue, staged["plan"])
    queue.publish(action_key=CONSUMER, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py",
                  resources={"cpu": 1, "mem_gb": 1}, tags=["dl380g10"],
                  residency=staged["residency"])
    return types.SimpleNamespace(queue=queue, staged=staged, discover=discover,
                                 stage=tmp_path / "stage")


def _lead(fleet) -> str:
    return str(fleet.staged["plan"]["phases"][0]["mover_row"]["action_key"])


def _claim_the_lead(fleet):
    tier_loop.cycle(fleet.queue, host="dl380g10", source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(), discover=fleet.discover)
    claim = fleet.queue.claim(capacity={"cpu": 8, "mem_gb": 16}, tags=["dl380g10"])
    assert claim is not None and claim["action_key"] == _lead(fleet)
    return claim


def _finish_the_lead(fleet, claim) -> None:
    mover = _lead(fleet)
    fleet.queue.record_move(mover, {
        "tier_id": STAGE, "consumer_action_key": CONSUMER, "complete": True,
        "bytes_staged": PHASE_BYTES, "entries_staged": 1,
        "range_start_bytes": 0, "range_end_bytes": PHASE_BYTES,
        "stage_root": str(fleet.stage),
        "disk_pacing": {"mean_self_read_mb_s": 0.0}})
    fleet.queue.finish(mover, status="executed", claim_snapshot=claim)


def test_a_warmed_range_holds_arc_tokens_while_it_is_admitted(tmp_path: Path) -> None:
    fleet = _fleet(tmp_path, arc_gib=8)
    mover = _lead(fleet)

    claim = _claim_the_lead(fleet)

    assert fleet.queue.tier_ledger(STAGE).holder_tokens(mover) == {"stage_gib": 2}
    assert fleet.queue.tier_ledger(ARC).holder_tokens(mover) == {"arc_gib": 2}
    assert fleet.queue.tier_ledger(ARC).available().get("arc_gib", 0) == 6

    _finish_the_lead(fleet, claim)

    # Occupancy, not a rate: the bytes are in RAM for as long as they are on
    # the stage, so the tokens outlive the copy exactly as ``stage_gib`` does.
    assert fleet.queue.tier_ledger(ARC).holder_tokens(mover) == {"arc_gib": 2}


def test_a_released_warm_range_returns_its_arc_tokens(tmp_path: Path) -> None:
    fleet = _fleet(tmp_path, arc_gib=8)
    mover = _lead(fleet)
    claim = _claim_the_lead(fleet)
    _finish_the_lead(fleet, claim)

    # What a finished copy gives back is the rate it no longer draws...
    fleet.queue.release_tier_rate_reservations(mover)
    assert fleet.queue.tier_ledger(ARC).holder_tokens(mover) == {"arc_gib": 2}

    # ...and what the egress gives back is the occupancy it deleted.
    fleet.queue.release_tier_reservations(mover)
    assert fleet.queue.tier_ledger(ARC).holder_tokens(mover) == {}
    assert fleet.queue.tier_ledger(ARC).available().get("arc_gib", 0) == 8


def test_a_range_larger_than_the_arc_seals_no_arc_leg(tmp_path: Path) -> None:
    """A demand nothing can ever satisfy is a mover that never runs.

    The ARC is a budget for the warm set; a phase bigger than the whole cache
    cannot be held in it, so it is staged and read off the SSD, and the plan
    says why rather than deadlocking on a token that will never exist.
    """

    fleet = _fleet(tmp_path, arc_gib=1)
    row = fleet.staged["plan"]["phases"][0]["mover_row"]

    assert [key for key in row["resources"] if key.startswith("arc_gib")] == []
    assert "arc" in json.dumps(fleet.staged["plan"]["demand_source"])

    claim = _claim_the_lead(fleet)
    assert fleet.queue.tier_ledger(ARC).holder_tokens(claim["action_key"]) == {}


def test_a_stage_the_arc_may_not_hold_seals_no_arc_leg(tmp_path: Path) -> None:
    fleet = _fleet(tmp_path, arc_gib=8, primarycache="metadata")
    row = fleet.staged["plan"]["phases"][0]["mover_row"]

    assert [key for key in row["resources"] if key.startswith("arc_gib")] == []


def test_arc_residency_never_gates_admission(tmp_path: Path) -> None:
    """ZFS exposes no pin, so the verdict stays a stage question.

    A lead holding its stage tokens and no ARC token at all still reads as
    resident: the map is the fact, the ARC is the speed.
    """

    fleet = _fleet(tmp_path, arc_gib=8)
    mover = _lead(fleet)
    claim = _claim_the_lead(fleet)
    _finish_the_lead(fleet, claim)
    # Take the ARC budget away entirely; residency is unmoved by it.
    fleet.queue.tier_ledger(ARC).release(mover)

    item = pool._read_json(fleet.queue.item_path(pool.READY, CONSUMER))
    verdict = fleet.queue.residency_verdict(item)

    # The discriminating state: the lead is pinned on the tier that can be
    # pinned, and only the composed map is still outstanding.  A verdict that
    # had learned to ask the ARC would read ``lead_unpinned`` here.
    assert verdict["state"] == "map_not_composed"
