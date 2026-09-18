"""A copy has no result to replay (#624).

A mover's action key is a content hash and its receipt is filed in the CAS
like any computation's.  Republish the same key -- the window asking for a
range again after an egress, or after a copy that landed short -- and
``run-local`` answers with the old receipt as a ``cache_hit`` that moves no
byte; ``residency_pin_holds`` rightly pins nothing, and the next cycle
republishes it.  2026-09-18, run ``ad8803aa``: the 11 GiB head mover was
replayed 25 times at 0.39 s each while the consumer sat ``ready`` on
``lead_unpinned``.

The pool cannot tell a copy from a computation by its key; the publisher can.
A movement node is published with ``recompute``, the item carries it through
a requeue, and the launch appends ``--recompute``.  Everything else launches
exactly as SLURM would.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, residency_plan  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
CONSUMER = "c" * 64
MANIFEST = "9" * 64
GIB = 1 << 30
SLURM_SHAPE = ["/w/worker.py", "run-local", "--action", "/cas/requests/ab/" + "ab" * 32 + ".json",
               "--cas-root", "/cas", "--checkout-root", "/co"]


def test_the_launch_is_unchanged_by_default() -> None:
    """The SLURM pin: byte-identical argv unless the item asks otherwise."""
    assert pool.worker_argv(worker_script="/w/worker.py", action_key="ab" * 32,
                            cas_root="/cas", checkout_root="/co") == SLURM_SHAPE


def test_a_movement_node_launches_with_recompute() -> None:
    argv = pool.worker_argv(worker_script="/w/worker.py", action_key="ab" * 32,
                            cas_root="/cas", checkout_root="/co", recompute=True)
    assert argv == SLURM_SHAPE + ["--recompute"]


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    q.mint_tier_capacity(TIER, {"stage_gib": 5})
    return q


def _publish(queue: pool.PoolQueue, key: str, **extra) -> dict[str, object]:
    queue.publish(action_key=key, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
                  resources={"cpu": 1, "mem_gb": 1}, **extra)
    return pool._read_json(queue.item_path(pool.READY, key))


def test_publish_stamps_recompute_only_when_asked(queue) -> None:
    plain = _publish(queue, "d" * 64)
    node = _publish(queue, "e" * 64, recompute=True)
    assert "recompute" not in plain
    assert node["recompute"] is True


def test_recompute_survives_a_requeue(queue) -> None:
    """A requeued movement node is still a movement node: not claim-scoped."""
    assert "recompute" not in pool.PoolQueue._CLAIM_SCOPED_FIELDS
    record = dict(_publish(queue, "e" * 64, recompute=True))
    record.update({"claimed_by": "x", "claimed_unix": 1.0, "status": "failed"})
    queue._shape_as_ready_item(record, action_key="e" * 64)
    assert pool._read_json(queue.item_path(pool.READY, "e" * 64))["recompute"] is True


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _row(key: str, resources: dict[str, int], queue: pool.PoolQueue) -> dict[str, object]:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


def _plan(queue: pool.PoolQueue) -> dict[str, object]:
    built = []
    for ordinal in range(3):
        start, end = ordinal * 2 * GIB, (ordinal + 1) * 2 * GIB
        built.append({
            "name": f"phase-{ordinal}", "start_bytes": start, "end_bytes": end,
            "stage_gib": 2,
            "mover_row": {**_row(_hexkey(f"mover{ordinal}"), {STAGE_KIND: 2, "mem_gb": 1}, queue),
                          "residency": {"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                                        "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 30,
                                        "range_start_bytes": start, "range_end_bytes": end}},
            "egress_row": _row(_hexkey(f"egress{ordinal}"), {"mem_gb": 1}, queue),
        })
    return residency_plan.build_plan(
        consumer_action_key=CONSUMER, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=MANIFEST, manifest_bytes=1 << 30, phases=built)


def _claim_consumer(queue: pool.PoolQueue, plan: dict[str, object]) -> None:
    residency_plan.freeze(queue, plan)
    queue.publish(action_key=CONSUMER, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
                  resources={"cpu": 1, "mem_gb": 1},
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                             "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 30,
                             "leads": residency_plan.leads_for(plan)})


def test_the_window_publishes_its_movers_as_movement_nodes(queue, tmp_path) -> None:
    _claim_consumer(queue, _plan(queue))

    events = tier_loop.residency_window(
        queue, tiers={TIER: {"tier_id": TIER, "tier": "stage",
                             "mountpoint": str(tmp_path / "stage")}})

    published = [e["action_key"] for e in events if e["event"] == "mover-published"]
    assert published == [_hexkey("mover0"), _hexkey("mover1")]
    for key in published:
        assert pool._read_json(queue.item_path(pool.READY, key))["recompute"] is True


def test_an_egress_is_a_movement_node_too(queue, tmp_path, monkeypatch) -> None:
    """A deletion replayed from the CAS deletes nothing and returns no tokens."""
    plan = _plan(queue)
    _claim_consumer(queue, plan)
    phases = plan["phases"]
    monkeypatch.setattr(tier_loop.residency_plan, "window", lambda *a, **k: {
        "publish": [], "evict": [{"phase": "phase-0", "egress_row": phases[0]["egress_row"],
                                  "mover_action_key": _hexkey("mover0")}],
        # The window's own answer since #632; a stub that omitted it would be
        # asserting a contract the caller no longer has.
        "stall": None})

    events = tier_loop.residency_window(
        queue, tiers={TIER: {"tier_id": TIER, "tier": "stage",
                             "mountpoint": str(tmp_path / "stage")}})

    assert [e["event"] for e in events if "egress" in e["event"]] == ["egress-published"]
    item = pool._read_json(queue.item_path(pool.READY, _hexkey("egress0")))
    assert item["recompute"] is True
