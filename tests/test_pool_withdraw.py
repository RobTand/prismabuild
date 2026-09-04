"""Cancelling an action is a verb, not a hand-edit of a live claimed record.

Before this, stopping a running action meant three steps and a race: rewrite
``max_attempts: 1`` into the claimed record so the kill would not be retried,
``kill -TERM -$pgid`` the action's process group, and hope the rewrite landed
first -- because if the kill won, ``finish`` requeued the action at its old
``max_attempts`` and the whole thing restarted.  Two records in the live
queue's ``failed/`` still show the seam: ``63248a1a108b`` (attempts 1, max 1)
and ``3de323205e10`` (attempts **2**, max 1 -- the edit landed after an attempt
had already been counted).  Both are filed as defects; both were decisions.

What is tested here is the pair of properties that makes the verb worth having:
a withdrawn action **never runs again**, whatever a concurrent worker is doing,
and the signal reaches the **action's** process group rather than the launcher
that is not in it.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

KEY_A = "a" * 64
KEY_B = "b" * 64


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _publish(q: pool.PoolQueue, key: str, **kw: object) -> None:
    q.publish(
        action_key=key,
        cas_root=kw.pop("cas_root", "/cas"),
        checkout_root=kw.pop("checkout_root", "/co"),
        worker_script=kw.pop("worker_script", "/w.py"),
        **kw,
    )


def _await(predicate, *, timeout_s: float = 20.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


def _grandchild_launcher(tmp_path: Path, pidfile: Path) -> Path:
    """A stub worker shaped like the real one: its action is a new session.

    ``core.run_local_action`` starts every action with ``start_new_session=True``
    so the action keeps holding its result lock if the worker is killed.  That
    is exactly why a signal to the launcher does not reach the action, and a
    stub without it would let a launcher-only kill pass this file's tests.
    """

    stub = tmp_path / "launcher_with_a_grandchild.py"
    stub.write_text(
        "import pathlib, subprocess, sys\n"
        "child = subprocess.Popen(['sleep', '600'], start_new_session=True)\n"
        f"pathlib.Path({str(pidfile)!r}).write_text(str(child.pid))\n"
        "sys.exit(child.wait())\n"
    )
    return stub


def _run_in_background(queue: pool.PoolQueue, item, **kw):
    outcome: dict[str, object] = {}
    thread = threading.Thread(
        target=lambda: outcome.update(queue.execute(item, **kw)), daemon=True
    )
    thread.start()
    return thread, outcome


# -- what withdrawal is ------------------------------------------------------


def test_a_withdrawn_action_is_not_a_failed_one(queue: pool.PoolQueue) -> None:
    """The failure record must not say the fleet is broken when it is not."""

    _publish(queue, KEY_A)
    result = queue.withdraw(KEY_A, reason="four suites, one box", by="rob@sparky")
    assert result["status"] == "withdrawn" and result["state"] == pool.READY
    assert not queue.item_path(pool.READY, KEY_A).exists()
    assert not queue.item_path(pool.FAILED, KEY_A).exists()
    assert not queue.item_path(pool.DONE, KEY_A).exists()
    filed = json.loads(queue.item_path(pool.WITHDRAWN, KEY_A).read_text())
    assert filed["status"] == "withdrawn"
    assert filed["reason"] == "four suites, one box"
    assert filed["withdrawn_by"] == "rob@sparky"
    # The item's own fields survive: what was withdrawn is part of the record.
    assert filed["worker_script"] == "/w.py" and filed["cas_root"] == "/cas"


def test_a_withdrawn_action_is_never_claimed_again(queue: pool.PoolQueue) -> None:
    """The guard that closes the race a hand-edit had to win.

    A ``finish`` that read its claimed record just before the withdrawal can
    still write the retry back to ``ready``.  Nothing stops that write; what
    stops the *action* is that no worker will take the record afterwards.
    """

    _publish(queue, KEY_A)
    queue.claim()
    queue.withdraw(KEY_A)
    # The losing half of the race, reproduced exactly: a requeued ready record
    # published after the withdrawal.
    _publish(queue, KEY_A)
    assert queue.item_path(pool.READY, KEY_A).exists()
    assert queue.claim() is None
    assert not queue.item_path(pool.READY, KEY_A).exists(), (
        "the stranded record is dropped, not left at the head of ready")


def test_a_withdrawn_item_is_dropped_before_a_claim_is_even_attempted(
    queue: pool.PoolQueue
) -> None:
    """The scan guard, isolated from the one behind it.

    ``claim`` writes its intent before it renames, so an intent record is proof
    that the scan let the item through and something downstream caught it.  The
    cheap guard has to be the one that fires: taking tokens and a claim for work
    that is already cancelled, and handing them straight back, is motion the
    queue should not be making on every poll.
    """

    _publish(queue, KEY_A, resources={"cpu": 1})
    queue.withdraw(KEY_A)
    _publish(queue, KEY_A, resources={"cpu": 1})
    assert queue.claim(capacity={"cpu": 2}) is None
    assert not queue.item_path(pool.INTENT, KEY_A).exists(), (
        "the item reached the claim path instead of being dropped by the scan")
    assert queue.ledger().available() == {"cpu": 2}, "no token was ever taken"


def test_a_withdrawal_landing_inside_the_claim_still_wins(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The guard behind the scan, isolated: the marker lands mid-rename.

    A withdrawal is a marker followed by a cleanup, so a claim can pass the scan
    while the marker does not exist and complete the rename after it does.  The
    window is microseconds wide and it is the only one left; without the recheck
    the cancelled action is claimed, executed, and only stopped a heartbeat
    later.
    """

    real_rename = pool.os.rename

    def rename_after_the_marker_lands(src, dst, *args, **kwargs):
        if str(src).endswith(f"{KEY_A}.json") and f"/{pool.READY}/" in str(src):
            # Exactly what ``withdraw`` does first, and nothing it does after:
            # the item is still in ``ready`` when this rename runs.
            pool._write_json_atomic(
                queue.item_path(pool.WITHDRAWN, KEY_A),
                {"action_key": KEY_A, "status": "withdrawn"})
        return real_rename(src, dst, *args, **kwargs)

    _publish(queue, KEY_A, resources={"cpu": 1})
    monkeypatch.setattr(pool.os, "rename", rename_after_the_marker_lands)
    assert queue.claim(capacity={"cpu": 2}) is None
    assert not queue.item_path(pool.CLAIMED, KEY_A).exists()
    assert queue.ledger().available() == {"cpu": 2}, "the tokens went back"


def test_finish_after_a_withdrawal_files_nothing_new(queue: pool.PoolQueue) -> None:
    _publish(queue, KEY_A)
    queue.claim()
    queue.withdraw(KEY_A)
    landed = queue.finish(KEY_A, status="failed", detail={"returncode": -15})
    assert landed == queue.item_path(pool.WITHDRAWN, KEY_A)
    for state in (pool.DONE, pool.FAILED, pool.READY, pool.CLAIMED):
        assert not queue.item_path(state, KEY_A).exists(), state
    assert not queue.lease_path(KEY_A).exists()


def test_finish_cannot_retry_what_was_withdrawn(queue: pool.PoolQueue) -> None:
    """The specific outcome the ``max_attempts`` rewrite existed to prevent."""

    _publish(queue, KEY_A, max_attempts=3)
    queue.claim()
    queue.withdraw(KEY_A)
    queue.finish(KEY_A, status="failed")
    assert not queue.item_path(pool.READY, KEY_A).exists()
    assert queue.claim() is None


def test_a_withdrawn_claim_is_not_requeued_by_the_reaper(
    queue: pool.PoolQueue
) -> None:
    """A withdrawal that died mid-verb must not be undone by self-healing."""

    _publish(queue, KEY_A)
    queue.claim()
    # Withdraw without the cleanup: marker written, claimed record still there
    # and its lease stale.  That is what a box dying inside ``withdraw`` leaves.
    queue.withdraw(KEY_A, signal_child=False)
    record = {"action_key": KEY_A, "worker_script": "/w.py", "cas_root": "/cas",
              "checkout_root": "/co", "claimed_host": "dl380g10", "attempts": 0}
    queue.item_path(pool.CLAIMED, KEY_A).write_text(json.dumps(record))
    assert queue.reap_stale(timeout_s=0.0) == []
    assert not queue.item_path(pool.READY, KEY_A).exists()
    assert not queue.item_path(pool.CLAIMED, KEY_A).exists()


# -- capacity ----------------------------------------------------------------


def test_a_withdrawal_returns_the_capacity_the_action_held(
    queue: pool.PoolQueue
) -> None:
    _publish(queue, KEY_A, resources={"gpu": 1, "mem_gb": 8})
    queue.claim(capacity={"gpu": 1, "mem_gb": 8})
    assert queue.ledger().available() == {}
    result = queue.withdraw(KEY_A)
    assert result["released"] == 9
    assert queue.ledger().available() == {"gpu": 1, "mem_gb": 8}


def test_capacity_goes_back_to_the_claiming_host_not_the_operators(
    queue: pool.PoolQueue
) -> None:
    """The operator withdrawing is usually not on the box holding the tokens."""

    foreign = queue.ledger("dl380g10")
    foreign.ensure_capacity({"gpu": 2})
    assert foreign.acquire(KEY_A, {"gpu": 2}) is True
    _publish(queue, KEY_A, resources={"gpu": 2})
    claimed = queue.item_path(pool.READY, KEY_A).read_text()
    record = json.loads(claimed)
    record.update({"claimed_host": "dl380g10", "claimed_by": "dl380g10:1:x",
                   "claimed_unix": time.time()})
    queue.item_path(pool.READY, KEY_A).unlink()
    queue.item_path(pool.CLAIMED, KEY_A).write_text(json.dumps(record))
    result = queue.withdraw(KEY_A)
    assert result["host"] == "dl380g10" and result["released"] == 2
    assert foreign.available() == {"gpu": 2}
    assert queue.ledger().available() == {}, "nothing was invented locally"


# -- idempotence and refusals ------------------------------------------------


def test_withdrawing_twice_keeps_the_first_decision(queue: pool.PoolQueue) -> None:
    _publish(queue, KEY_A)
    first = queue.withdraw(KEY_A, reason="the real reason", by="rob@sparky")
    filed = json.loads(queue.item_path(pool.WITHDRAWN, KEY_A).read_text())
    second = queue.withdraw(KEY_A, reason="a later, worse reason")
    again = json.loads(queue.item_path(pool.WITHDRAWN, KEY_A).read_text())
    assert first["status"] == "withdrawn"
    assert second["status"] == "already_withdrawn"
    assert again == filed, "the record of the decision is not rewritten"


def test_withdrawing_a_finished_action_says_so_rather_than_raising(
    queue: pool.PoolQueue
) -> None:
    _publish(queue, KEY_A)
    queue.claim()
    queue.finish(KEY_A, status="executed")
    result = queue.withdraw(KEY_A)
    assert result["status"] == "already_finished" and result["state"] == pool.DONE
    assert not queue.item_path(pool.WITHDRAWN, KEY_A).exists()


def test_withdrawing_something_the_queue_never_had_is_refused(
    queue: pool.PoolQueue
) -> None:
    with pytest.raises(pool.PoolContractError):
        queue.withdraw(KEY_B)


# -- naming the action -------------------------------------------------------


def test_a_key_prefix_is_enough_to_name_an_action(queue: pool.PoolQueue) -> None:
    """Twelve characters is what every log line and every submit prints."""

    _publish(queue, KEY_A)
    assert queue.find_key(KEY_A[:12]) == KEY_A
    assert queue.find_key(KEY_A) == KEY_A


def test_an_ambiguous_prefix_is_refused_not_guessed(queue: pool.PoolQueue) -> None:
    _publish(queue, "ab" + "c" * 62)
    _publish(queue, "ab" + "d" * 62)
    with pytest.raises(pool.PoolContractError) as caught:
        queue.find_key("ab")
    assert "names 2 actions" in str(caught.value)
    with pytest.raises(pool.PoolContractError):
        queue.find_key("ff")


def test_a_prefix_that_is_not_a_digest_is_a_typo_not_a_glob(
    queue: pool.PoolQueue
) -> None:
    """The match is a glob, and a key is hex; ``*`` must not mean "all of them"."""

    _publish(queue, KEY_A)
    for typo in ("*", "nosuchkey", "[a-b]", ""):
        with pytest.raises(pool.PoolContractError):
            queue.find_key(typo)
    assert queue.item_path(pool.READY, KEY_A).exists()


def test_a_prefix_still_resolves_after_the_action_is_terminal(
    queue: pool.PoolQueue
) -> None:
    """So the operator is told "already finished", not "no such action"."""

    _publish(queue, KEY_A)
    queue.claim()
    queue.finish(KEY_A, status="executed")
    assert queue.find_key(KEY_A[:12]) == KEY_A


# -- the signal --------------------------------------------------------------


def test_the_signal_reaches_the_action_group_not_only_the_launcher(
    queue: pool.PoolQueue, tmp_path: Path
) -> None:
    """The distinguishing test: kill the work, not the process that started it.

    The stub mirrors ``core.run_local_action`` -- its action runs in a new
    session -- so a withdrawal that signalled only the launcher would leave the
    ``sleep`` running and this would fail.  ``kill -TERM -$pgid`` on that group
    is exactly the step the operator was doing by hand.
    """

    pidfile = tmp_path / "grandchild.pid"
    stub = _grandchild_launcher(tmp_path, pidfile)
    _publish(queue, KEY_A, worker_script=str(stub))
    item = queue.claim()
    # Heartbeat long enough that the cooperative poll inside ``execute`` cannot
    # fire: what stops the action here must be the withdrawal's own signal.
    thread, outcome = _run_in_background(queue, item, heartbeat_s=30.0)
    assert _await(lambda: pidfile.exists()), "the stub never started its action"
    grandchild = int(pidfile.read_text())
    assert _await(lambda: (json.loads(queue.lease_path(KEY_A).read_text())
                           .get("child_pid") is not None))

    result = queue.withdraw(KEY_A, reason="stop it")

    signalled = result["signalled"] or {}
    assert signalled.get("action_pgids") == [grandchild], (
        "the group signalled must be the action's own, not the launcher's")
    assert any(sent.startswith("TERM -") for sent in signalled.get("signals", []))
    thread.join(timeout=30.0)
    assert not thread.is_alive(), "execute must not hang on the killed pipes"
    assert _await(lambda: not pool._process_alive(grandchild)), (
        "the action outlived its withdrawal")
    assert outcome["status"] == "withdrawn", (
        "a decision must not be logged as a defect")


def test_a_withdrawal_from_another_box_still_stops_the_action(
    queue: pool.PoolQueue, tmp_path: Path
) -> None:
    """No signal can cross a box, so the worker watches for the marker itself."""

    pidfile = tmp_path / "grandchild.pid"
    stub = _grandchild_launcher(tmp_path, pidfile)
    _publish(queue, KEY_A, worker_script=str(stub))
    item = queue.claim()
    thread, outcome = _run_in_background(queue, item, heartbeat_s=0.2)
    assert _await(lambda: pidfile.exists())
    grandchild = int(pidfile.read_text())

    # ``signal_child=False`` is what being on another box amounts to: the
    # marker is all that crosses.
    queue.withdraw(KEY_A, signal_child=False)

    thread.join(timeout=30.0)
    assert not thread.is_alive()
    assert outcome["status"] == "withdrawn"
    assert _await(lambda: not pool._process_alive(grandchild))


def test_a_withdrawn_action_is_never_started(
    queue: pool.PoolQueue, tmp_path: Path
) -> None:
    """Withdrawn in the window between the claim's rename and the launch."""

    marker = tmp_path / "it_ran"
    stub = tmp_path / "eager_worker.py"
    stub.write_text(f"import pathlib; pathlib.Path({str(marker)!r}).write_text('x')\n")
    _publish(queue, KEY_A, worker_script=str(stub))
    item = queue.claim()
    queue.withdraw(KEY_A)
    outcome = queue.execute(item)
    assert outcome["status"] == "withdrawn" and outcome["returncode"] is None
    assert not marker.exists(), "the cancelled action was started anyway"


def test_signalling_is_idempotent_when_the_action_is_already_gone() -> None:
    """Withdrawing twice, or after the box died, must not raise."""

    finished = subprocess.Popen([sys.executable, "-c", "pass"])
    finished.wait()
    outcome = pool.terminate_action(finished.pid, grace_s=0.2)
    assert outcome["action_pgids"] == [] and outcome["signals"] == []
    assert outcome["still_alive"] is False


def test_a_recycled_pid_is_not_signalled(queue: pool.PoolQueue) -> None:
    """A lease outlives its process; a pid number does not stay meaningful."""

    assert pool.launcher_owns_action(os.getpid(), KEY_A) is False
    assert pool.launcher_owns_action(2 ** 30, KEY_A) is False
    _publish(queue, KEY_A)
    queue.claim()
    lease = json.loads(queue.lease_path(KEY_A).read_text())
    lease["child_pid"] = os.getpid()          # this test process, not an action
    queue.lease_path(KEY_A).write_text(json.dumps(lease))
    assert queue.withdraw(KEY_A)["signalled"] is None
