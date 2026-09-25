"""The claim pass times every per-key transition-lock hold (#1029).

REVIEW-CHECKLIST item 3: a hold longer than one syscall stamps ``*_held_s``.
The claim pass holds each candidate's transition lock across shared-mount
round trips -- the fresh ``claimed/`` listing, the ready record, the rename --
and a slow one was invisible: every sibling loop and box that met the lock
meanwhile recorded only ``transition_busy``.  Each pass now counts its holds,
sums them and names the longest with its key, on ``last_claim_pass`` and in
this loop's entry of the host's latest-only ``claim-denials.json``.

Nothing here touches the live queue, a real pool or a real device.
"""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from prismabuild import adaptive_cpu, pool  # noqa: E402

SLOW_S = 0.25

HOLDER = """
import sys
sys.path.insert(0, sys.argv[1])
from prismabuild import pool
queue = pool.PoolQueue(sys.argv[2])
with queue._transition_locked(sys.argv[3]) as acquired:
    assert acquired
    print("held", flush=True)
    sys.stdin.read()
"""


def _publish(queue: pool.PoolQueue, index: int, tags: list[str]) -> str:
    key = f"{index + 1:064x}"
    queue.publish(action_key=key, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py",
                  resources={"cpu": 1}, tags=tags)
    return key


def _snapshot(queue: pool.PoolQueue) -> dict:
    path = adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    return adaptive_cpu.read_json(path)


def test_a_slow_lookup_under_the_lock_is_timed_and_names_its_key(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    keys = [_publish(queue, index, ["here"]) for index in range(3)]
    slow, fast, claimed = keys
    real = queue._claim_blocked_fresh
    looked: list[str] = []

    def lookup(key: str):
        # The fresh ``claimed/`` listing each claim takes under its key's
        # lock: slow for one key, as an NFS round trip can be.  Two keys are
        # refused, so the pass takes a lock for each of the three.
        looked.append(key)
        if key == slow:
            time.sleep(SLOW_S)
            return True, None
        if key == fast:
            return True, None
        return real(key)

    monkeypatch.setattr(queue, "_claim_blocked_fresh", lookup)
    item = queue.claim(tags=["here"], owner="worker")

    assert item is not None and item["action_key"] == claimed
    assert sorted(looked) == sorted(keys)
    holds = queue.last_claim_pass
    assert holds is not None
    assert holds["transition_holds"] == 3
    assert holds["transition_held_max_key"] == slow
    assert holds["transition_held_max_s"] >= SLOW_S
    assert holds["transition_held_s"] >= holds["transition_held_max_s"]

    filed = _snapshot(queue)
    assert filed["schema"] == pool.CLAIM_DENIALS_SCHEMA_V1
    entry = filed["claim_passes"][str(os.getpid())]
    assert entry["transition_holds"] == 3
    assert entry["transition_held_max_key"] == slow
    assert entry["transition_held_max_s"] >= SLOW_S
    # The denials the pass filed are kept beside it, and a later denial keeps
    # the pass entry: the two writers share one file.
    reasons = {value["action_key"]: value["reason"]
               for value in filed["records"].values()}
    assert reasons[slow] == reasons[fast] == "already_claimed"
    queue.record_denial({"action_key": "f" * 64, "published_unix": 1.0},
                        "placement_mismatch", {})
    assert _snapshot(queue)["claim_passes"][str(os.getpid())] == entry


def test_a_lock_held_by_another_loop_is_not_a_hold(
        tmp_path: Path) -> None:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    key = _publish(queue, 0, ["here"])
    # Another process, as another loop is: the lock is a POSIX record lock,
    # which one process never contends with itself.
    holder = subprocess.Popen(
        [sys.executable, "-c", HOLDER, str(ROOT / "src"), str(queue.root), key],
        stdout=subprocess.PIPE, stdin=subprocess.PIPE, text=True)
    try:
        assert holder.stdout.readline().strip() == "held"
        assert queue.claim(tags=["here"], owner="worker") is None
    finally:
        holder.stdin.close()
        holder.wait(timeout=30)
    assert queue.last_claim_pass == {
        "transition_holds": 0, "transition_held_s": 0.0,
        "transition_held_max_s": 0.0, "transition_held_max_key": None}
    assert "claim_passes" not in _snapshot(queue)


def test_a_pass_that_raises_under_the_lock_still_times_the_hold(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    key = _publish(queue, 0, ["here"])

    def lookup(_key: str):
        time.sleep(SLOW_S)
        raise OSError("the export stopped answering")

    monkeypatch.setattr(queue, "_claim_blocked_fresh", lookup)
    with pytest.raises(OSError):
        queue.claim(tags=["here"], owner="worker")
    holds = queue.last_claim_pass
    assert holds["transition_holds"] == 1
    assert holds["transition_held_max_key"] == key
    assert holds["transition_held_max_s"] >= SLOW_S
