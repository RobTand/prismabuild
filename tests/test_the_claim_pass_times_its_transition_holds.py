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
from fleet_world import ActionRows

import os
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from prismabuild import adaptive_cpu, pool  # noqa: E402

from admitted_queue_fixture import AdmittedQueueFixture  # noqa: E402

SLOW_S = 0.25

HOLDER = """
import sys
sys.path.insert(0, sys.argv[1])
from prismabuild import pool
queue = pool.PoolQueue(sys.argv[2])
with queue.action_transition_lock(sys.argv[3]) as acquired:
    assert acquired
    print("held", flush=True)
    sys.stdin.read()
"""


def _publish(queue: pool.PoolQueue, index: int, tags: list[str]) -> str:
    key = f"{index + 1:064x}"
    ActionRows(queue).publish(
        key,
        cas_root=queue.root / 'cas',
        checkout_root=queue.root / 'co',
        worker_script=queue.root / 'worker.py',
        resources={'cpu': 1},
        tags=tags,
    )
    return key


def _snapshot(queue: pool.PoolQueue) -> dict:
    path = adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    return adaptive_cpu.read_json(path)


@pytest.mark.parametrize(
    "durations, max_index, expected_max_s, expected_total_s",
    [
        pytest.param((0.75, 0.125, 0.25), 0, 0.75, 1.125, id="slow-longest"),
        pytest.param((0.25, 1.25, 0.125), 1, 1.25, 1.625, id="fast-longest"),
    ],
)
def test_a_slow_lookup_under_the_lock_is_timed_and_names_its_key(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        durations: tuple[float, float, float], max_index: int,
        expected_max_s: float, expected_total_s: float) -> None:
    queue = AdmittedQueueFixture(
        pool.PoolQueue(tmp_path / "pb-queue"), capacity={"cpu": 8, "mem_gb": 16},
        default_demand={"cpu": 1, "mem_gb": 1})
    keys = [_publish(queue, index, ["here"]) for index in range(3)]
    slow, fast, claimed = keys
    real = queue._claim_blocked_fresh
    looked: list[str] = []
    duration_by_key = dict(zip(keys, durations))
    # Stay in the real monotonic epoch for any claim-path deadlines, but
    # advance only at lookup exits. Filesystem work and descheduling before
    # or after those exits must not affect the transition-hold ranking.
    clock = SimpleNamespace(value=time.monotonic())
    scripted_time = SimpleNamespace(
        **(vars(pool.time) | {"monotonic": lambda: clock.value}))

    def lookup(key: str):
        # Keep the fresh claimed/ listing real for the successful candidate.
        # Refuse two keys so the pass takes all three transition locks. Each
        # key's duration is independent of the candidate visitation order.
        looked.append(key)
        try:
            if key in (slow, fast):
                return True, None
            return real(key)
        finally:
            clock.value += duration_by_key[key]

    with monkeypatch.context() as patch:
        # Replace only pool's namespace, not the shared time module used by
        # threading, child readers or the rest of the process. Retain sleep,
        # perf_counter and wall time unchanged for other pool operations.
        patch.setattr(pool, "time", scripted_time)
        patch.setattr(queue, "_claim_blocked_fresh", lookup)
        item = queue.claim(tags=["here"], owner="worker")

    assert item is not None and item["action_key"] == claimed
    assert sorted(looked) == sorted(keys)
    holds = queue.last_claim_pass
    assert holds is not None
    assert holds["transition_holds"] == 3
    assert holds["transition_held_max_key"] == keys[max_index]
    assert holds["transition_held_max_s"] == expected_max_s
    assert holds["transition_held_s"] == expected_total_s

    filed = _snapshot(queue)
    assert filed["schema"] == pool.CLAIM_DENIALS_SCHEMA_V1
    entry = filed["claim_passes"][str(os.getpid())]
    assert entry["transition_holds"] == 3
    assert entry["transition_held_max_key"] == keys[max_index]
    assert entry["transition_held_max_s"] == expected_max_s
    assert entry["transition_held_s"] == expected_total_s
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
    queue = AdmittedQueueFixture(
        pool.PoolQueue(tmp_path / "pb-queue"), capacity={"cpu": 8, "mem_gb": 16},
        default_demand={"cpu": 1, "mem_gb": 1})
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
    queue = AdmittedQueueFixture(
        pool.PoolQueue(tmp_path / "pb-queue"), capacity={"cpu": 8, "mem_gb": 16},
        default_demand={"cpu": 1, "mem_gb": 1})
    held = _publish(queue, 0, ["here"])
    key = _publish(queue, 1, ["here"])
    rows = {item["action_key"]: item for item in queue.ready_items()}
    # These rows have aging counts.
    # Neither row has GPU or publication-canary priority.
    ready = [rows[held], rows[key]]
    looked: list[str] = []
    # Keep the real epoch for claim-path deadlines.
    clock = SimpleNamespace(value=time.monotonic())
    scripted_time = SimpleNamespace(
        **(vars(pool.time) | {"monotonic": lambda: clock.value}))

    def lookup(lookup_key: str):
        looked.append(lookup_key)
        clock.value += SLOW_S
        raise OSError("the export stopped answering")

    holder = subprocess.Popen(
        [sys.executable, "-c", HOLDER, str(ROOT / "src"), str(queue.root), held],
        stdout=subprocess.PIPE, stdin=subprocess.PIPE, text=True)
    try:
        assert holder.stdout.readline().strip() == "held"
        with monkeypatch.context() as patch:
            # Change only pool's namespace. Keep all other time attributes.
            patch.setattr(pool, "time", scripted_time)
            patch.setattr(queue, "_claim_blocked_fresh", lookup)
            with pytest.raises(OSError, match="the export stopped answering"):
                queue.claim(tags=["here"], owner="worker", ready=ready)
    finally:
        holder.stdin.close()
        holder.wait()

    assert looked == [key]
    holds = queue.last_claim_pass
    assert holds is not None
    assert holds["transition_holds"] == 1
    assert holds["transition_held_max_key"] == key
    assert holds["transition_held_max_s"] == SLOW_S
    assert holds["transition_held_s"] == SLOW_S

    filed = _snapshot(queue)
    assert filed["schema"] == pool.CLAIM_DENIALS_SCHEMA_V1
    entry = filed["claim_passes"][str(os.getpid())]
    assert entry["transition_holds"] == 1
    assert entry["transition_held_max_key"] == key
    assert entry["transition_held_max_s"] == SLOW_S
    assert entry["transition_held_s"] == SLOW_S
    # The raise stops the scan. This refusal proves it visited the held key first.
    reasons = {value["action_key"]: value["reason"]
               for value in filed["records"].values()}
    assert reasons[held] == "transition_busy"
