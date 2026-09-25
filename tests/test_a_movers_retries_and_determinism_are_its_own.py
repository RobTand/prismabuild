"""A mover's retry policy and determinism are its own, never its consumer's (#950).

#944 sealed every movement node as portable generation work, but two fields
still came from the consumer:

* An egress inherited the consumer's ``retry_policy``, ``max_attempts`` and
  ``retry_safe``.  Under a single-attempt consumer, an egress that met one
  transient failure was never retried, and the bytes it was meant to delete
  stayed on the stage holding their tier tokens until pressure eviction.
* The movers of a ``--deterministic`` consumer were sealed deterministic.  A
  mover's result is its log, which differs from one copy to the next, so a
  re-stage under the same key -- after an eviction, when the consumer retries
  -- copied the bytes and was then refused as a CAS conflict.

A movement node is now retry-safe with a bounded attempt count of its own and
never deterministic: every staging is a real copy.

Fixture concessions.  The egress fixture seals its rows through the real
``pbrun.residency_stage_rows`` and drives them through the real
``PoolQueue.claim``/``finish``; each egress attempt runs ``stage_release.evict``
in process instead of through a worker, and the transient failure is an
injected ``EROFS`` from ``os.unlink`` on the staged files, the way a busy
dataset fails.  The
re-stage fixture is the prepaid writer integration's real mover run through
``Pool.execute``; the re-stage runs the canonical worker argv with
``--recompute`` (``pool.worker_argv``, what a movement row republished with
``recompute=True`` launches) directly, because re-claiming that lane's funded
row is outside what is under test.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "tools" / "fleet"))
sys.path.insert(0, str(REPO / "tests"))

from prismabuild import movement_actions, pool, residency_map  # noqa: E402
import prismabuild.produced_output as po  # noqa: E402
import stage_release  # noqa: E402
from test_a_stage_mover_declares_the_cpu_and_retries_it_owns import (  # noqa: E402
    CONSUMER, TIER, _seal,
)
from test_prepaid_writer_integration import (  # noqa: E402
    _announce_tier, _bind, _claim_mover, _descriptors, _execute_mover,
    _prewrite, _producer_request, _queue, _template,
)
import test_prepaid_writer_integration as prepaid  # noqa: E402

CAPACITY = {"cpu": 8, "mem_gb": 16}


def _stage_the_movers_range(queue: pool.PoolQueue, tmp_path: Path,
                            mover_row: dict, body: dict) -> Path:
    """Run the mover's claim to a pinned finish, with its bytes on the stage.

    The fragment and receipt name the share namespace the sealed command
    stages under (its ``--consumer-action-key``), as ``stage_move`` files them.
    """

    mover = str(mover_row["action_key"])
    command = list(body["params"]["command"])
    namespace = command[command.index("--consumer-action-key") + 1]
    residency = mover_row["residency"]
    stage = tmp_path / "stage"
    stage.mkdir(exist_ok=True)
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    entries = {}
    for index in range(2):
        path = stage / "sub" / f"part-{index}.bin"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x" * 16)
        entries[residency_map.residency_map_key(f"/mnt/shared/part-{index}", 0)] = {
            "stage_path": str(path), "bytes": 16, "offset": 0, "sha256": "1" * 64}
    residency_map.write_fragment(queue.root / pool.RESIDENCY, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": namespace, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": residency["manifest_sha256"], "entries": entries})

    queue.publish(**mover_row)
    claimed = queue.claim(capacity=CAPACITY, tags=["dl380g10"])
    assert claimed is not None and claimed["action_key"] == mover
    span = int(residency["range_end_bytes"]) - int(residency["range_start_bytes"])
    queue.record_move(mover, {
        "consumer_action_key": namespace, "tier_id": TIER,
        "stage_root": str(stage),
        "manifest_sha256": residency["manifest_sha256"],
        "range_start_bytes": residency["range_start_bytes"],
        "range_end_bytes": residency["range_end_bytes"],
        "bytes_staged": span, "complete": True})
    queue.finish(mover, status="executed")
    assert queue.tier_ledger(TIER).holder_tokens(mover), "the staged range is pinned"
    return stage


def test_the_egress_of_a_single_attempt_consumer_retries_and_releases(
        tmp_path, monkeypatch) -> None:
    """The acceptance fixture: one injected failure, a retry, bytes released."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    staged = _seal(tmp_path, queue)          # the consumer allows one attempt
    phase = staged["plan"]["phases"][0]
    mover_row, egress_row = phase["mover_row"], phase["egress_row"]
    mover, egress = str(mover_row["action_key"]), str(egress_row["action_key"])
    stage = _stage_the_movers_range(queue, tmp_path, mover_row,
                                    staged["cas"].actions[mover])
    held = queue.tier_ledger(TIER).holder_tokens(mover)

    queue.publish(**egress_row)
    claimed = queue.claim(capacity=CAPACITY, tags=["dl380g10"])
    assert claimed is not None and claimed["action_key"] == egress
    real_unlink = os.unlink

    def busy_dataset(path, *args, **kwargs):
        # Only the staged bytes fail, the way a busy dataset refuses them.
        if str(path).endswith(".bin"):
            raise OSError(30, "EROFS", str(path))
        return real_unlink(path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(stage_release.os, "unlink", busy_dataset)
        first = stage_release.evict(queue, mover, consumer_action_key=CONSUMER,
                                    stage_root=str(stage))
    assert first["complete"] is False and first["tokens_released"] == 0, json.dumps(first, default=str)
    queue.finish(egress, status="failed")

    assert queue.item_path(pool.READY, egress).exists(), (
        "one transient failure must not end the egress: it is retry-safe "
        "with its own attempt bound")
    assert not queue.item_path(pool.FAILED, egress).exists()
    assert queue.tier_ledger(TIER).holder_tokens(mover) == held

    claimed = queue.claim(capacity=CAPACITY, tags=["dl380g10"])
    assert claimed is not None and claimed["action_key"] == egress
    second = stage_release.evict(queue, mover, consumer_action_key=CONSUMER,
                                 stage_root=str(stage))
    queue.finish(egress, status="executed")

    assert second["complete"] is True and second["entries_deleted"] == 2
    assert queue.tier_ledger(TIER).holder_tokens(mover) == {}
    assert not list(stage.rglob("*.bin"))
    assert queue.item_path(pool.DONE, egress).exists()


@pytest.mark.parametrize("node", ["mover_row", "egress_row"])
def test_every_movement_node_seals_its_own_retry_policy(tmp_path, node) -> None:
    """Row and sealed body agree, and neither is the consumer's one attempt."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    staged = _seal(tmp_path, queue, residency_mover_max_attempts=4)
    row = staged["plan"]["phases"][0][node]
    body = staged["cas"].actions[str(row["action_key"])]
    assert body["params"]["retry_policy"] == {"max_attempts": 4, "retry_safe": True}
    assert (row["max_attempts"], row["retry_safe"]) == (4, True)


def test_a_movement_node_without_a_policy_never_takes_its_consumers() -> None:
    """The shared construction's default is a mover's, for every lane."""

    import pbrun

    template = {
        "marker_root": Path("/home/rob/tmp/markers"),
        "checkout_identity": {"commit": "a" * 40},
        "task": {"definition_id": "fleet/pbrun", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "working_directory": "."},
        "inputs": [],
        "code_closure": pbrun.build_stamp_closure("pbrun.stamp", "{}"),
        "params": {"cwd": "/home/rob",
                   "retry_policy": {"max_attempts": 1, "retry_safe": False}},
        "environment": {"variables": {"PATH": "/usr/bin"}, "toolchain": {}},
    }
    body = movement_actions.seal_movement_action(
        template, command=["/usr/bin/python3", "/opt/tools/stage_release.py"],
        demand={"cpu": 1, "mem_gb": 1}, tags=["dl380g10"], log_name="r.log")
    assert body["params"]["retry_policy"] == movement_actions.MOVEMENT_RETRY_POLICY
    assert body["params"]["retry_policy"]["retry_safe"] is True
    assert body["params"]["retry_policy"]["max_attempts"] > 1
    assert body["task"]["determinism"] == "stochastic"


def test_under_a_deterministic_consumer_a_restage_copies_again(tmp_path) -> None:
    """The acceptance fixture: stage, evict, re-stage under the same key.

    The producer request is sealed ``deterministic``.  Its mover runs for real,
    the batch's egress deletes the staged bytes, and the same mover key runs
    again with ``--recompute``.  It must copy the bytes back and publish its
    new log, not be refused as a conflicting deterministic recomputation.
    """

    cas_root = tmp_path / "cas"
    template = _template(str(tmp_path / "outputs"))
    owner = _producer_request(tmp_path, cas_root, template)
    producer = json.loads((cas_root / "requests" / owner[:2]
                           / f"{owner}.json").read_text())
    assert producer["task"]["determinism"] == "deterministic"
    q = _queue(tmp_path, gib=4)
    inst = _bind(q, template, owner, cas_root)
    payload = bytes(range(256)) * 8
    descs = _descriptors(tmp_path, template, inst, "p1", payload)
    _prewrite(q, inst, template, "b1", prepaid.TIER, descs)
    stage_root = tmp_path / "stage"
    _announce_tier(q, stage_root)
    res = po.publish_prepaid_batch(
        q, inst, template, descs, batch_id="b1", tier=prepaid.TIER,
        cas_root=cas_root, producer_action_key=owner,
        command_extra=["--unpaced"])
    assert res.get("ok") is True, res
    mover = str(res["mover_key"])

    claimed = _claim_mover(q, "w-restage")
    assert claimed["action_key"] == mover
    row = pool._read_json(q.item_path(pool.CLAIMED, mover))
    assert isinstance(row, dict)
    first = _execute_mover(q, cas_root, mover, tmp_path / "mover-checkout")
    assert first["complete"] is True, first
    staged = stage_root / "produced-output" / res["batch_namespace"] / "p1.bin"
    assert staged.read_bytes() == payload
    q.finish(mover, status="executed")
    retired = po.retire_batch(q, inst, template, "b1",
                              stage_root=str(stage_root),
                              residency_root=po.output_fragment_root(
                                  q.root / pool.RESIDENCY))
    assert retired.get("ok") is True, json.dumps(retired, default=str)
    assert not staged.exists(), "the egress evicted the staged bytes"

    argv = [sys.executable] + pool.worker_argv(
        worker_script=row["worker_script"], action_key=mover,
        cas_root=row["cas_root"], checkout_root=tmp_path / "mover-checkout",
        recompute=True)
    environment = {name: value for name, value in os.environ.items()
                   if not name.startswith("PRISMABUILD_")}
    again = subprocess.run(argv, capture_output=True, text=True,
                           env=environment, timeout=240)

    assert "CASConflictError" not in again.stderr, again.stderr[-2000:]
    assert again.returncode == 0, again.stderr[-2000:]
    assert staged.read_bytes() == payload, "the re-stage is a real copy"
    body = json.loads((cas_root / "requests" / mover[:2]
                       / f"{mover}.json").read_text())
    assert body["task"]["determinism"] == "stochastic"
