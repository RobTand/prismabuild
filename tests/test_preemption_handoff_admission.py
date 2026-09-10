"""A stalled withdrawal/requeue must occupy only the preemption handoff.

Private queues and logical CPU/memory tokens; no payloads or real stalls.
Events hold one handoff open while a separate queue instance tries to claim.
"""
import json
import multiprocessing
import os
from pathlib import Path
import sys
import threading
import time

import pytest

from prismabuild import adaptive_cpu, pool

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import mount_latency


CAPACITY = {"cpu": 3, "mem_gb": 8}
TIERS = {"preferred": [0, 1, 2], "fallback": []}
BACKGROUND = "a" * 64
OTHER_BACKGROUND = "b" * 64
FOREGROUND = "c" * 64
SMALL = "d" * 64
SECOND_FOREGROUND = "e" * 64


def publish(q, key, memory, priority):
    q.publish(action_key=key, cas_root=q.root / "cas", checkout_root=q.root,
              worker_script="worker.py", resources={"cpu": 1, "mem_gb": memory},
              priority=priority, retry_safe=True, max_attempts=3)


def claim(q):
    return q.claim(capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True)


@pytest.fixture
def rig(tmp_path, monkeypatch):
    monkeypatch.setattr(adaptive_cpu, "action_identity", lambda item: ("shape", False))
    monkeypatch.setattr(adaptive_cpu.Controller, "sample", lambda self: {
        "sampled_unix": time.time(), "busy_cpus": 0., "psi_some": 0.,
        "cpu_count": 3, "interval_s": 1.})
    q = pool.PoolQueue(tmp_path / "queue")
    for key in (BACKGROUND, OTHER_BACKGROUND):
        publish(q, key, 2, -10)
        held = claim(q)
        assert held is not None and held["action_key"] == key
    publish(q, FOREGROUND, 5, 0)
    return q


@pytest.mark.parametrize("stage", ["withdraw", "publish"])
def test_stalled_handoff_allows_fitting_claim_without_second_preemption(rig, monkeypatch, stage):
    entered, release = threading.Event(), threading.Event()
    original = getattr(rig, stage)
    outcome = {}

    def paused(*args, **kwargs):
        entered.set()
        assert release.wait(20), "test did not release preemption handoff"
        return original(*args, **kwargs)

    monkeypatch.setattr(rig, stage, paused)

    def run():
        try:
            outcome["item"] = claim(rig)
        except BaseException as exc:
            outcome["error"] = exc

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        assert entered.wait(20), "foreground did not reach preemption handoff"
        sibling = pool.PoolQueue(rig.root)
        # This independent denial must not select another victim before the
        # first cancellation is visible. It must still proceed to fitting work.
        publish(sibling, SECOND_FOREGROUND, 5, 0)
        publish(sibling, SMALL, 1, -10)
        winner = claim(sibling)
        assert winner is not None, "stalled preemption handoff held host admission"
        assert winner["action_key"] == SMALL
        decisions = [d for key in (BACKGROUND, OTHER_BACKGROUND)
                     for _, d in sibling.withdrawal_decisions(key)]
        assert len(decisions) == (1 if stage == "publish" else 0), (
            "a second denial preempted work during an outstanding handoff")
        assert sibling.ledger().held() == {"cpu": 3, "mem_gb": 5}
    finally:
        release.set()
        thread.join(20)
    assert not thread.is_alive()
    assert "error" not in outcome, outcome
    assert outcome["item"] is None
    decisions = [d for key in (BACKGROUND, OTHER_BACKGROUND)
                 for _, d in rig.withdrawal_decisions(key)]
    assert len(decisions) == 1
    victim = decisions[0]["action_key"]
    retry = json.loads(rig.item_path(pool.READY, victim).read_text())
    assert retry["priority"] == -10 and retry["attempts"] == 1
    assert retry["max_attempts"] == 3
    assert rig.ledger().held() == {"cpu": 3, "mem_gb": 5}, "handoff returned live tokens"


def _hold_until_crash(root, connection):
    q = pool.PoolQueue(root)
    with q._preemption_locked(q.ledger()) as acquired:
        connection.send(acquired)
        connection.recv()
        os._exit(23)


def test_handoff_exclusion_survives_contention_and_releases_after_owner_crash(rig):
    """A busy handoff refuses another process without fencing ordinary work."""
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    process = context.Process(target=_hold_until_crash, args=(rig.root, child))
    process.start()
    child.close()
    try:
        assert parent.poll(20) and parent.recv() is True
        assert claim(rig) is None
        assert not rig.withdrawal_decisions(BACKGROUND)
        assert not rig.withdrawal_decisions(OTHER_BACKGROUND)
        # A refused acquisition must not release the peer's lock.
        assert claim(pool.PoolQueue(rig.root)) is None
        assert not rig.withdrawal_decisions(BACKGROUND)
        assert not rig.withdrawal_decisions(OTHER_BACKGROUND)
        parent.send("crash")
        process.join(20)
        assert process.exitcode == 23
        # Fresh selection after kernel release, with exactly one charged retry.
        assert claim(rig) is None
        decisions = [d for key in (BACKGROUND, OTHER_BACKGROUND)
                     for _, d in rig.withdrawal_decisions(key)]
        assert len(decisions) == 1
        assert rig.ledger().held() == {"cpu": 2, "mem_gb": 4}
    finally:
        parent.close()
        if process.is_alive():
            process.kill()
        process.join(20)


def test_stalled_preemption_eligibility_proof_does_not_hold_host_admission(rig, monkeypatch):
    """CAS/withdrawal proof is advisory preparation, not capacity authority."""
    entered, release = threading.Event(), threading.Event()
    original = rig._preemption_eligible
    outcome = {}

    def paused(record):
        entered.set()
        assert release.wait(20), "test did not release preemption eligibility proof"
        return original(record)

    monkeypatch.setattr(rig, "_preemption_eligible", paused)

    def run():
        try:
            outcome["item"] = claim(rig)
        except BaseException as exc:
            outcome["error"] = exc

    stalled = threading.Thread(target=run, daemon=True)
    stalled.start()
    try:
        assert entered.wait(20), "foreground did not prepare preemption eligibility"
        sibling = pool.PoolQueue(rig.root)
        publish(sibling, SMALL, 1, -10)
        winner = claim(sibling)
        assert winner is not None and winner["action_key"] == SMALL, (
            "a stalled preemption eligibility proof held host admission")
    finally:
        release.set()
        stalled.join(20)
    assert not stalled.is_alive() and "error" not in outcome, outcome
    assert outcome["item"] is None
    decisions = [d for key in (BACKGROUND, OTHER_BACKGROUND)
                 for _, d in rig.withdrawal_decisions(key)]
    assert len(decisions) == 1


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("attempts", 1),
        ("max_attempts", 1),
        ("max_attempts", 3.0),
        ("max_attempts", float("nan")),
        ("retry_safe", False),
        ("retry_safe", 1),
        ("attempt_history", [{"status": "failed"}]),
        ("attempt_history_missing_before", 0),
        ("attempt_history_missing_before", None),
    ],
)
def test_changed_claim_cannot_use_a_stale_preemption_eligibility_proof(
    rig, monkeypatch, field, value,
):
    """A stale proof cannot turn a second holder into a preemption victim."""
    foreground = rig.item_path(pool.READY, FOREGROUND)
    item = json.loads(foreground.read_text())
    item["resources"]["mem_gb"] = 7
    foreground.write_text(json.dumps(item))
    original = rig._preemption_eligibility_proofs

    def stale(ledger, *, action_key):
        proofs = original(ledger, action_key=action_key)
        background = rig.item_path(pool.CLAIMED, BACKGROUND)
        changed = json.loads(background.read_text())
        changed[field] = value
        background.write_text(json.dumps(changed))
        other = rig.item_path(pool.CLAIMED, OTHER_BACKGROUND)
        pending = json.loads(other.read_text())
        pending["finish_pending"] = {"reason": "simulated concurrent finish"}
        other.write_text(json.dumps(pending))
        return proofs

    monkeypatch.setattr(rig, "_preemption_eligibility_proofs", stale)
    assert claim(rig) is None
    assert not rig.withdrawal_decisions(BACKGROUND)
    assert not rig.withdrawal_decisions(OTHER_BACKGROUND)
    assert rig.ledger().held() == {"cpu": 2, "mem_gb": 4}


def test_nonfinite_retry_budget_is_ineligible_before_proof_preparation(rig):
    for key in (BACKGROUND, OTHER_BACKGROUND):
        path = rig.item_path(pool.CLAIMED, key)
        record = json.loads(path.read_text())
        record["max_attempts"] = float("nan")
        path.write_text(json.dumps(record))
    assert claim(rig) is None
    assert not rig.withdrawal_decisions(BACKGROUND)
    assert not rig.withdrawal_decisions(OTHER_BACKGROUND)
    assert rig.ledger().held() == {"cpu": 2, "mem_gb": 4}


@pytest.mark.parametrize("stage", ["_select_background_holder", "_preempt_selected_holder"])
def test_handoff_error_releases_exclusion_without_returning_live_tokens(rig, monkeypatch, stage):
    original = getattr(rig, stage)

    def fail(*args, **kwargs):
        raise OSError("simulated preemption I/O failure")

    monkeypatch.setattr(rig, stage, fail)
    with pytest.raises(OSError, match="simulated preemption"):
        claim(rig)
    assert rig.ledger().held() == {"cpu": 2, "mem_gb": 4}
    monkeypatch.setattr(rig, stage, original)
    assert claim(rig) is None
    decisions = [d for key in (BACKGROUND, OTHER_BACKGROUND)
                 for _, d in rig.withdrawal_decisions(key)]
    assert len(decisions) == 1


def test_preemption_handoff_is_not_reported_as_host_admission(rig):
    with rig._preemption_locked(rig.ledger()) as acquired:
        assert acquired
        census = mount_latency.lock_contention(lock_dir=adaptive_cpu.BOX_STATE_ROOT)
        assert census["present"] and census["identity_complete"]
        assert census["files"] == 1, "handoff inode entered the admission census"
        assert census["holders"] == 0
