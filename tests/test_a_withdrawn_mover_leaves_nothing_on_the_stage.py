"""The stage gets its bytes back from a mover nobody will ever release (#608).

Observed on `prismabuild-stage:dl380g10` after `pbrun --withdraw` of a failed
consumer's eight remaining movers, one of them claimed mid-copy: the withdrawal
reported `released 0 token(s); release pending on dl380g10`, the worker stopped
the action, the ledger went back to reading its full supply free -- and 130.5 GB
stayed on the stage. The shards that mover had already verified and renamed into
place, its `.partial` temporaries, and ~34 GB from manual staging tests before
the tier had a ledger.

`stage_release.sweep` walks the tier ledger's **held keys**, so bytes no key
holds are invisible to it for the life of the fleet. This file drives the real
sweep against a real directory and asserts the three rules the reconciliation
turns on: a withdrawn mover's files go, a live plan's files stay, and nothing at
all happens while a mover could still be writing.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, residency_map, storage_tiers  # noqa: E402
import prewarm_loop  # noqa: E402
import stage_release  # noqa: E402

CONSUMER = "c" * 64
MOVER = "4" * 64
TIER = "prismabuild-stage:dl380g10"
GIB = storage_tiers.GIB


def _staged_file(stage: Path, relative: str, size: int = 4096) -> Path:
    path = stage / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\0" * size)
    return path


def _fragment(root: Path, stage: Path, paths: dict[str, Path],
              *, mover: str = MOVER) -> None:
    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": "a" * 64,
        "entries": {
            residency_map.residency_map_key(f"/mnt/shared/{name}", 0): {
                "stage_path": str(path), "bytes": path.stat().st_size,
                "sha256": "b" * 64, "offset": 0,
            }
            for name, path in paths.items()
        },
    })


@pytest.fixture()
def fleet(tmp_path: Path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    return queue, stage


def _sweep(queue, stage) -> list[dict[str, object]]:
    return stage_release.sweep(queue, stage_roots={TIER: str(stage)})


def test_a_withdrawn_movers_verified_shards_and_partials_are_both_evicted(fleet):
    """The 130 GB case: the fragment names the shards, the naming names the rest."""

    queue, stage = fleet
    shard = _staged_file(stage, "model-00088-of-00120.safetensors", 8192)
    partial = _staged_file(
        stage, "row-0076/cache/.model_layers_4_mlp_experts_42_up_proj.pt.partial",
        4096)
    _fragment(queue.root / pool.RESIDENCY, stage, {"shard": shard})
    # Withdrawn: no item in the queue names this mover, and its tokens are back.
    assert not queue.tier_ledger(TIER).held_keys()

    events = _sweep(queue, stage)
    assert len(events) == 1, events
    event = events[0]
    assert event["event"] == stage_release.UNATTRIBUTED_EVENT
    assert event["entries_deleted"] == 2
    assert event["partials_deleted"] == 1
    assert event["bytes_deleted"] == 8192 + 4096
    assert not shard.exists() and not partial.exists()
    assert event["complete"] is True


def test_a_file_a_live_plan_names_is_untouched(fleet):
    """A pinned mover a live consumer still plans to read is attribution."""

    queue, stage = fleet
    shard = _staged_file(stage, "model-00001-of-00120.safetensors", 8192)
    _fragment(queue.root / pool.RESIDENCY, stage, {"shard": shard})
    # The consumer is live and its residency block names this mover as a lead.
    queue.publish(action_key=CONSUMER, cas_root=str(queue.root / "cas"),
                  checkout_root=str(queue.root), worker_script="w.py",
                  tags=["dl380g10"], resources={"cpu": 1, "mem_gb": 1},
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                             "manifest_sha256": "a" * 64,
                             "manifest_bytes": 8192, "tier_id": TIER,
                             "leads": [MOVER]})

    events = _sweep(queue, stage)
    assert events == [] or all(e["entries_deleted"] == 0 for e in events)
    assert shard.exists()


def test_nothing_is_deleted_while_a_mover_could_still_be_writing(fleet):
    """A ready mover's destination is in no fragment until it is verified."""

    queue, stage = fleet
    orphan = _staged_file(stage, "left-over.safetensors", 8192)
    queue.publish(action_key=MOVER, cas_root=str(queue.root / "cas"),
                  checkout_root=str(queue.root), worker_script="w.py",
                  tags=["dl380g10"],
                  resources={"cpu": 4, "mem_gb": 1, f"stage_gib@{TIER}": 1},
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                             "manifest_sha256": "a" * 64,
                             "manifest_bytes": 8192, "tier_id": TIER,
                             "range_start_bytes": 0, "range_end_bytes": 8192})

    assert stage_release.movers_in_flight(queue, tier_id=TIER) == {MOVER}
    receipt = stage_release.reconcile(queue, tier_id=TIER, stage_root=str(stage),
                                     wanted=set())
    assert receipt["skipped"] == "movers_in_flight"
    assert receipt["entries_deleted"] == 0
    assert orphan.exists()


def test_a_prewarm_stage_object_is_not_a_movers_leftover(fleet):
    """The prewarm loop stages into the same pool and marks what it owns."""

    queue, stage = fleet
    marked = _staged_file(stage, "prewarmed.bin", 4096)
    try:
        os.setxattr(marked, prewarm_loop.STAGE_SOURCE_XATTR, b"/mnt/shared/x@0")
    except OSError:
        pytest.skip("this filesystem carries no user extended attributes")
    unmarked = _staged_file(stage, "nobodys.bin", 4096)

    receipt = stage_release.reconcile(queue, tier_id=TIER,
                                     stage_root=str(stage), wanted=set())
    assert marked.exists()
    assert not unmarked.exists()
    assert receipt["entries_deleted"] == 1
    assert receipt["unowned_left"] == 1


def test_where_the_xattr_cannot_be_read_an_unmarked_file_is_left_alone(
        fleet, monkeypatch):
    """"Unmarked" means nothing on a filesystem that carries no attributes."""

    queue, stage = fleet
    unmarked = _staged_file(stage, "nobodys.bin", 4096)
    partial = _staged_file(stage, ".nobodys.bin.partial", 4096)

    def refuse(*_args, **_kwargs):
        raise OSError(95, "Operation not supported")

    monkeypatch.setattr(stage_release.os, "getxattr", refuse)
    receipt = stage_release.reconcile(queue, tier_id=TIER,
                                      stage_root=str(stage), wanted=set())
    assert unmarked.exists(), "an unanswerable question is not permission"
    assert not partial.exists(), "a mover's own temporary is self-identifying"
    assert receipt["unowned_left"] == 1
    assert receipt["partials_deleted"] == 1


def test_a_prewarm_temporary_is_left_to_the_loop_that_owns_it(fleet):
    """``reap_temporaries`` owns those, once per process, and a live one is in use."""

    queue, stage = fleet
    temporary = _staged_file(stage, "obj.bin.pbstage@0+4096.7.1.tmp", 4096)
    assert prewarm_loop._STAGE_TEMPORARY.search(temporary.name)
    receipt = stage_release.reconcile(queue, tier_id=TIER,
                                      stage_root=str(stage), wanted=set())
    assert temporary.exists()
    assert receipt["entries_deleted"] == 0


def test_a_withdrawn_movers_fragment_is_not_attribution(fleet):
    """The fragment is still on disk -- no egress ran -- and must not protect it."""

    queue, stage = fleet
    shard = _staged_file(stage, "model-00088-of-00120.safetensors", 8192)
    root = queue.root / pool.RESIDENCY
    _fragment(root, stage, {"shard": shard})
    assert residency_map.fragment_path(root, CONSUMER, MOVER).exists()
    assert stage_release.attributed_stage_paths(queue, wanted={MOVER}) == {str(shard)}
    assert stage_release.attributed_stage_paths(queue, wanted=set()) == set()


def test_the_sweep_still_evicts_a_held_orphan_by_its_fragment(fleet):
    """The held-key half is unchanged; the reconciliation is additive."""

    queue, stage = fleet
    shard = _staged_file(stage, "model-00002-of-00120.safetensors", 8192)
    _fragment(queue.root / pool.RESIDENCY, stage, {"shard": shard})
    queue.record_move(MOVER, {
        "tier_id": TIER, "consumer_action_key": CONSUMER, "complete": True,
        "bytes_staged": 8192, "range_start_bytes": 0, "range_end_bytes": 8192,
        "stage_root": str(stage), "unix": 10.0})
    ledger = queue.tier_ledger(TIER)
    ledger.ensure_capacity({"stage_gib": 4})
    assert ledger.acquire(MOVER, {"stage_gib": 1})
    assert MOVER in ledger.held_keys()

    events = _sweep(queue, stage)
    reasons = [str(event.get("reason")) for event in events]
    assert "orphan-sweep" in reasons
    assert not shard.exists()
    assert MOVER not in queue.tier_ledger(TIER).held_keys()
