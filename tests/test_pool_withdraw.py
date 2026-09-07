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
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

# Unique per process, never a fixed string.  ``find_launcher_pids`` scans
# every process on the box for a key, and agents run this suite concurrently on
# boxes they share: with a constant key, one run's withdrawal finds another
# run's launcher and kills it.  A test that can reach outside its own tmp_path
# is not a private-root test however private its queue is.
KEY_A = uuid.uuid4().hex + uuid.uuid4().hex
KEY_B = uuid.uuid4().hex + uuid.uuid4().hex

#: A box that is provably not this one.  Naming a real fleet member as "the
#: other box" reads fine until the suite runs ON that member: the local ledger
#: and the "foreign" one become the same directory and the test's own premise
#: is gone.  The full suite runs on dl380g10 (`pbtest`'s whole reason for
#: existing is that its 80 x86 cores are idle), so the name it used was exactly
#: the one that could not be used.
ELSEWHERE = f"not-{socket.gethostname()}-{uuid.uuid4().hex[:8]}"


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


@pytest.fixture()
def pidfile(tmp_path: Path):
    """Where a stub records its action's pid -- and the sweep for it.

    A test that fails partway through leaves a ``sleep 600`` behind on a box
    other agents are working on, and the leak is not merely untidy: the /proc
    scan finds launchers by action key, so a process left running by one run is
    something a later run can find and signal.  Cleaning up is part of keeping
    the private root private.
    """

    path = tmp_path / "action.pid"
    yield path
    try:
        pid = int(path.read_text())
    except (OSError, ValueError):
        return
    for target in (lambda: os.killpg(pid, signal.SIGKILL), lambda: os.kill(pid, signal.SIGKILL)):
        try:
            target()
        except OSError:
            pass


def _publish(q: pool.PoolQueue, key: str, **kw: object) -> None:
    q.publish(
        action_key=key,
        cas_root=kw.pop("cas_root", "/cas"),
        checkout_root=kw.pop("checkout_root", "/co"),
        worker_script=kw.pop("worker_script", "/w.py"),
        **kw,
    )


def _old_bytes(q: pool.PoolQueue, monkeypatch) -> pool.PoolQueue:
    """The same queue as a worker running pre-withdraw bytes sees it.

    A worker loop holds the module it imported at start until the runtime
    rolls, so part of the fleet is running ``main``'s ``pool.py`` during the
    transition.  The ONLY behavioural difference between that ``finish`` and
    this branch's is that it does not consult the withdrawal marker -- the
    lost-race branch, the retry branch, the release and the unlinks are
    identical text -- so blinding the marker lookup is a faithful model of it
    rather than a fake of the thing under test.
    """

    old = pool.PoolQueue(q.root)
    monkeypatch.setattr(old, "withdrawal_covers", lambda *args, **kwargs: None)
    return old


def _strand_a_requeue(q: pool.PoolQueue, key: str, monkeypatch) -> dict:
    """Produce the losing half of the race, through the real retry branch.

    A fresh ``publish`` is NOT this state and must not stand in for it: a
    re-submission is a new generation and a new request, which is the whole
    distinction this file's guards turn on.  What the race actually leaves is a
    worker that read its claimed record before the withdrawal reached it and
    requeued afterwards -- so the record is put back exactly as that worker
    still had it, and ``finish``'s own retry branch writes it to ``ready`` with
    the withdrawn generation's ``published_unix`` on it.
    """

    held = json.loads(q.item_path(pool.CLAIMED, key).read_text())
    q.withdraw(key, signal_child=False)
    q.item_path(pool.CLAIMED, key).write_text(json.dumps(held))
    _old_bytes(q, monkeypatch).finish(key, status="failed", detail={})
    stranded = q.item_path(pool.READY, key)
    assert stranded.exists(), "the retry branch really did write one back"
    return json.loads(stranded.read_text())


def _superseded(q: pool.PoolQueue, key: str) -> list[dict]:
    directory = q.dir(pool.WITHDRAWN) / "superseded"
    if not directory.is_dir():
        return []
    return [json.loads(path.read_text())
            for path in sorted(directory.glob(f"{key}.*.json"))]


def _await(predicate, *, timeout_s: float = 20.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()
def _await_pid(path: Path, *, timeout_s: float = 20.0) -> int:
    """The pid a helper wrote, once the file actually holds one.

    ``write_text`` creates the file before it writes it, so waiting on
    ``exists()`` and then calling ``int()`` can read an empty file and raise
    ``invalid literal for int() with base 10: ''``. Waiting for a parsable pid
    waits for the write instead.
    """

    pid = 0

    def written() -> bool:
        nonlocal pid
        try:
            text = path.read_text().strip()
        except OSError:
            return False
        if not text.isdigit():
            return False
        pid = int(text)
        return True

    assert _await(written, timeout_s=timeout_s), f"nothing wrote a pid to {path}"
    return pid


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


def test_a_withdrawn_action_is_never_claimed_again(
    queue: pool.PoolQueue, monkeypatch
) -> None:
    """The guard that closes the race a hand-edit had to win.

    A ``finish`` that read its claimed record just before the withdrawal can
    still write the retry back to ``ready``.  Nothing stops that write; what
    stops the *action* is that no worker will take the record afterwards --
    and that the record is FILED rather than made to disappear.
    """

    _publish(queue, KEY_A, max_attempts=3)
    generation = queue.claim()["published_unix"]
    stranded = _strand_a_requeue(queue, KEY_A, monkeypatch)
    assert stranded["published_unix"] == generation

    assert queue.claim() is None
    assert not queue.item_path(pool.READY, KEY_A).exists(), (
        "the stranded record is dropped, not left at the head of ready")
    dropped = [r for r in _superseded(queue, KEY_A) if r.get("status") == "dropped"]
    assert dropped and dropped[0]["published_unix"] == generation, (
        "a record the queue removes and does not run has to be filed somewhere")


def test_a_withdrawn_item_is_dropped_before_a_claim_is_even_attempted(
    queue: pool.PoolQueue, monkeypatch
) -> None:
    """The scan guard, isolated from the one behind it.

    ``claim`` writes its intent before it renames, so an intent record is proof
    that the scan let the item through and something downstream caught it.  The
    cheap guard has to be the one that fires: taking tokens and a claim for work
    that is already cancelled, and handing them straight back, is motion the
    queue should not be making on every poll.
    """

    _publish(queue, KEY_A, resources={"cpu": 1}, max_attempts=3)
    queue.claim(capacity={"cpu": 2})
    _strand_a_requeue(queue, KEY_A, monkeypatch)
    # The first claim -- the one that got as far as running -- left its own
    # intent behind, and intent is not cleared on success.  Clear it so what is
    # asserted below is this claim's behaviour and not that one's dropping.
    queue.item_path(pool.INTENT, KEY_A).unlink(missing_ok=True)
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


def test_withdrawal_retains_claim_bytes_for_the_marker_aware_worker(queue):
    _publish(queue, KEY_A, max_attempts=3)
    queue.claim()
    path = queue.item_path(pool.CLAIMED, KEY_A)
    before = path.read_bytes()
    result = queue.withdraw(KEY_A, signal_child=False)
    assert result["released"] == 0
    assert path.read_bytes() == before
    queue.finish(KEY_A, status="failed", detail={"returncode": -15})
    assert not queue.item_path(pool.READY, KEY_A).exists()
    assert queue.withdrawn_keys() == frozenset([KEY_A])


def test_marker_aware_reaper_concludes_the_cancelled_claim(queue):
    _publish(queue, KEY_A, max_attempts=3)
    queue.claim()
    queue.withdraw(KEY_A, signal_child=False)
    queue.reap_stale(timeout_s=-1)
    assert not queue.item_path(pool.READY, KEY_A).exists()
    assert not queue.item_path(pool.CLAIMED, KEY_A).exists()
    assert queue.item_path(pool.WITHDRAWN, KEY_A).exists()


def test_a_completed_action_that_lost_its_claim_is_filed_under_done(
    queue: pool.PoolQueue
) -> None:
    """What ``finish``'s lost-race branch actually does with a completed run.

    Pinned because the first pass's own remainder mis-stated it: it said a
    pre-publish worker that runs the action to completion has its outcome filed
    ``failed/<key>.json`` with status ``finish_lost_race``.  It does not.
    ``finish_lost_race`` is the FAILURE spelling; a run that executed keeps its
    own status and lands in ``done``.  The safety conclusion is unchanged --
    ``pool_reset`` scans ``failed/`` only -- but the mechanism is the other
    branch, and a remainder that names the wrong one cannot be checked.
    """

    executed = queue.finish(KEY_A, status="executed", detail={"returncode": 0})
    assert executed == queue.item_path(pool.DONE, KEY_A)
    assert json.loads(executed.read_text())["status"] == "executed"

    other = KEY_B
    failed = queue.finish(other, status="failed", detail={"returncode": 1})
    assert failed == queue.item_path(pool.FAILED, other)
    assert json.loads(failed.read_text())["status"] == "finish_lost_race"


def test_a_withdrawn_claim_is_not_requeued_by_the_reaper(
    queue: pool.PoolQueue
) -> None:
    """A withdrawal that died mid-verb must not be undone by self-healing."""

    _publish(queue, KEY_A)
    queue.claim()
    # Withdraw without the cleanup: marker written, claimed record still there
    # and its lease stale.  That is what a box dying inside ``withdraw`` leaves.
    queue.withdraw(KEY_A, signal_child=False)
    # A record ``claim()`` finished writing always carries ``claimed_unix``;
    # without it (and with a fresh intent marker) the reaper would read the
    # record as still inside ``claim()``'s rename-to-rewrite window and leave
    # it for the claimant to conclude (#36).
    record = {"action_key": KEY_A, "worker_script": "/w.py", "cas_root": "/cas",
              "checkout_root": "/co", "claimed_host": ELSEWHERE, "attempts": 0,
              "claimed_unix": 0.0}
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
    assert result["released"] == 0
    assert queue.ledger().available() == {}
    queue.finish(KEY_A, status="withdrawn")
    assert queue.ledger().available() == {"gpu": 1, "mem_gb": 8}


def test_capacity_goes_back_to_the_claiming_host_not_the_operators(
    queue: pool.PoolQueue
) -> None:
    """The operator withdrawing is usually not on the box holding the tokens."""

    foreign = queue.ledger(ELSEWHERE)
    foreign.ensure_capacity({"gpu": 2})
    assert foreign.acquire(KEY_A, {"gpu": 2}) is True
    _publish(queue, KEY_A, resources={"gpu": 2})
    claimed = queue.item_path(pool.READY, KEY_A).read_text()
    record = json.loads(claimed)
    record.update({"claimed_host": ELSEWHERE, "claimed_by": f"{ELSEWHERE}:1:x",
                   "claimed_unix": time.time()})
    queue.item_path(pool.READY, KEY_A).unlink()
    queue.item_path(pool.CLAIMED, KEY_A).write_text(json.dumps(record))
    result = queue.withdraw(KEY_A)
    # The operator's box cannot signal the holder, so the release is the
    # holder's to make and this reports where it is pending. Releasing here
    # would hand back a token the action is still running on.
    assert result["host"] == ELSEWHERE and result["released"] == 0
    assert result["stop_pending"]["holder_host"] == ELSEWHERE
    assert foreign.available() == {}
    # The holder's own conclusion, or the reaper once its lease stops beating,
    # returns the tokens -- and to the claiming host's ledger, not this one.
    # The claim has no lease, so age it past the reaper's grace for a fresh
    # claim; on the fleet a holder that never acknowledged is long past it.
    pending = json.loads(queue.item_path(pool.CLAIMED, KEY_A).read_text())
    pending["claimed_unix"] = time.time() - 2 * pool.HEARTBEAT_S
    queue.item_path(pool.CLAIMED, KEY_A).write_text(json.dumps(pending))
    assert queue.reap_stale(timeout_s=-1) == []
    assert foreign.available() == {"gpu": 2}
    assert foreign.held_keys() == []
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


def test_the_worker_stops_its_own_action_group_after_withdrawal(
    queue: pool.PoolQueue, tmp_path: Path, pidfile: Path
) -> None:
    """The distinguishing test: kill the work, not the process that started it.

    The stub mirrors ``core.run_local_action`` -- its action runs in a new
    session -- so a withdrawal that signalled only the launcher would leave the
    ``sleep`` running and this would fail.  ``kill -TERM -$pgid`` on that group
    is exactly the step the operator was doing by hand.
    """

    stub = _grandchild_launcher(tmp_path, pidfile)
    _publish(queue, KEY_A, worker_script=str(stub))
    item = queue.claim()
    # The claiming worker owns the process handle and stops its own payload.
    thread, outcome = _run_in_background(queue, item, heartbeat_s=0.1)
    grandchild = _await_pid(pidfile)
    assert _await(lambda: (json.loads(queue.lease_path(KEY_A).read_text())
                           .get("child_pid") is not None))

    result = queue.withdraw(KEY_A, reason="stop it")

    assert result["signalled"] is None
    assert result["released"] == 0
    thread.join(timeout=30.0)
    assert not thread.is_alive(), "execute must not hang on the killed pipes"
    assert _await(lambda: not pool._process_alive(grandchild)), (
        "the action outlived its withdrawal")
    assert outcome["status"] == "withdrawn", (
        "a decision must not be logged as a defect")


def test_withdrawal_reaps_an_owned_container_before_releasing_capacity(
    queue: pool.PoolQueue, tmp_path: Path, pidfile: Path, monkeypatch
) -> None:
    """A Docker payload is no longer in the action's process tree.

    ``containerd-shim`` reparents the container process, so killing every
    process group below the PrismaBuild launcher can succeed while the GPU
    payload remains alive.  This test gives the detached process the same
    ownership identity the pbrun Docker shim records, then makes the container
    runtime calls deterministic: the pid is the container id and ``rm -f`` is
    SIGKILL.  Capacity may return only after that independently-owned process
    is gone.
    """

    container_pidfile = tmp_path / "container.pid"
    stub = tmp_path / "launcher_with_reparented_container.py"
    stub.write_text(
        "import os, pathlib, subprocess, sys\n"
        "first = os.fork()\n"
        "if first == 0:\n"
        "    os.setsid()\n"
        "    second = os.fork()\n"
        "    if second != 0: os._exit(0)\n"
        f"    pathlib.Path({str(container_pidfile)!r}).write_text(str(os.getpid()))\n"
        "    os.execl('/usr/bin/sleep', 'sleep', '600')\n"
        "os.waitpid(first, 0)\n"
        "action = subprocess.Popen(['/usr/bin/sleep', '600'], start_new_session=True)\n"
        f"pathlib.Path({str(pidfile)!r}).write_text(str(action.pid))\n"
        "sys.exit(action.wait())\n"
    )

    owner = "c" * 64
    _publish(queue, KEY_A, worker_script=str(stub),
             resources={"gpu": 1, "mem_gb": 8})
    queued = json.loads(queue.item_path(pool.READY, KEY_A).read_text())
    queued["container_owner"] = owner
    queue.item_path(pool.READY, KEY_A).write_text(json.dumps(queued))
    item = queue.claim(capacity={"gpu": 1, "mem_gb": 8})
    marker = queue.root / "container-owners" / f"{owner}.used"
    marker.parent.mkdir(parents=True)
    marker.write_text(owner)

    thread, outcome = _run_in_background(queue, item, heartbeat_s=0.1)
    action_pid = _await_pid(pidfile)
    container_pid = _await_pid(container_pidfile)

    def owned_ids(actual_owner: str) -> list[str]:
        assert actual_owner == owner
        return [str(container_pid)] if pool._process_alive(container_pid) else []

    def remove(ids: list[str]) -> list[str]:
        for raw in ids:
            os.kill(int(raw), signal.SIGKILL)
        assert _await(lambda: not pool._process_alive(container_pid))
        return list(ids)

    monkeypatch.setattr(pool, "_docker_owned_container_ids", owned_ids,
                        raising=False)
    monkeypatch.setattr(pool, "_docker_remove_containers", remove, raising=False)
    try:
        result = queue.withdraw(KEY_A, reason="stop the serve")
        assert result["released"] == 0
        assert _await(lambda: not pool._process_alive(action_pid))
        thread.join(timeout=5.0)
        queue.finish(KEY_A, status="withdrawn", detail=outcome, claim_snapshot=item)
        assert _await(lambda: not pool._process_alive(container_pid)), (
            "the reparented container survived after its GPU token was released")
        assert result["container_cleanup"]["deferred"] is True
        assert queue.ledger().available() == {"gpu": 1, "mem_gb": 8}
    finally:
        if pool._process_alive(container_pid):
            os.kill(container_pid, signal.SIGKILL)
        thread.join(timeout=30.0)
    assert outcome["status"] == "withdrawn"


def test_withdrawal_keeps_the_claim_and_tokens_when_container_cleanup_fails(
    queue: pool.PoolQueue, monkeypatch
) -> None:
    owner = "e" * 64
    _publish(queue, KEY_A, resources={"gpu": 1, "mem_gb": 8})
    queued = json.loads(queue.item_path(pool.READY, KEY_A).read_text())
    queued["container_owner"] = owner
    queue.item_path(pool.READY, KEY_A).write_text(json.dumps(queued))
    queue.claim(capacity={"gpu": 1, "mem_gb": 8})
    marker = queue.root / "container-owners" / f"{owner}.used"
    marker.parent.mkdir(parents=True)
    marker.write_text(owner)

    monkeypatch.setattr(pool, "_docker_owned_container_ids",
                        lambda actual_owner: ["stuck-container"], raising=False)
    monkeypatch.setattr(pool, "_docker_remove_containers",
                        lambda ids: list(ids), raising=False)

    result = queue.withdraw(KEY_A, signal_child=False)

    assert result["released"] == 0
    assert result["container_cleanup"]["deferred"] is True
    queue.finish(KEY_A, status="withdrawn")
    pending = pool._read_json(queue.item_path(pool.CLAIMED, KEY_A))
    assert pending["container_cleanup_pending"]["remaining"] == ["stuck-container"]
    assert queue.item_path(pool.CLAIMED, KEY_A).exists()
    assert queue.ledger().held_keys() == [KEY_A]


def test_finish_keeps_capacity_while_an_owned_container_survives(
    queue: pool.PoolQueue, monkeypatch
) -> None:
    owner = "f" * 64
    _publish(queue, KEY_A, resources={"gpu": 1})
    queued = json.loads(queue.item_path(pool.READY, KEY_A).read_text())
    queued["container_owner"] = owner
    queue.item_path(pool.READY, KEY_A).write_text(json.dumps(queued))
    claimed = queue.claim(capacity={"gpu": 1})
    marker = queue.root / "container-owners" / f"{owner}.used"
    marker.parent.mkdir(parents=True)
    marker.write_text(owner)
    monkeypatch.setattr(pool, "_docker_owned_container_ids",
                        lambda actual_owner: ["stuck-container"], raising=False)
    monkeypatch.setattr(pool, "_docker_remove_containers",
                        lambda ids: list(ids), raising=False)

    destination = queue.finish(
        KEY_A, status="executed", claim_snapshot=claimed)

    assert destination == queue.item_path(pool.CLAIMED, KEY_A)
    pending = json.loads(destination.read_text())
    assert pending["container_cleanup_pending"]["remaining"] == ["stuck-container"]
    assert queue.ledger().held_keys() == [KEY_A]
    assert not queue.item_path(pool.DONE, KEY_A).exists()


def test_a_withdrawal_from_another_box_still_stops_the_action(
    queue: pool.PoolQueue, tmp_path: Path, pidfile: Path
) -> None:
    """No signal can cross a box, so the worker watches for the marker itself."""

    stub = _grandchild_launcher(tmp_path, pidfile)
    _publish(queue, KEY_A, worker_script=str(stub))
    item = queue.claim()
    thread, outcome = _run_in_background(queue, item, heartbeat_s=0.2)
    grandchild = _await_pid(pidfile)

    # ``signal_child=False`` is what being on another box amounts to: the
    # marker is all that crosses.
    queue.withdraw(KEY_A, signal_child=False)

    thread.join(timeout=30.0)
    assert not thread.is_alive()
    assert outcome["status"] == "withdrawn"
    assert _await(lambda: not pool._process_alive(grandchild))


def test_worker_without_child_pid_stops_cooperatively_without_key_scan(
    queue: pool.PoolQueue, tmp_path: Path, pidfile: Path, monkeypatch
) -> None:
    """A content-addressed key cannot select a process from a later attempt."""
    def forbidden(*args):
        raise AssertionError("withdrawal must not scan processes by action key")
    monkeypatch.setattr(pool, "find_launcher_pids", forbidden)

    stub = _grandchild_launcher(tmp_path, pidfile)
    _publish(queue, KEY_A, worker_script=str(stub))
    item = queue.claim()
    thread, outcome = _run_in_background(queue, item, heartbeat_s=0.1)
    grandchild = _await_pid(pidfile)

    # What a pre-withdrawal worker's lease looks like.
    lease = json.loads(queue.lease_path(KEY_A).read_text())
    launcher = lease.pop("child_pid")
    queue.lease_path(KEY_A).write_text(json.dumps(lease))

    result = queue.withdraw(KEY_A)

    assert result["signalled"] is None
    thread.join(timeout=30.0)
    assert _await(lambda: not pool._process_alive(grandchild))
    assert outcome["status"] == "withdrawn"


def test_finding_a_launcher_never_matches_the_withdrawing_process(
    queue: pool.PoolQueue
) -> None:
    """Withdrawing by full digest must not make this process a target."""

    assert os.getpid() not in pool.find_launcher_pids(KEY_A)
    assert pool.find_launcher_pids("short") == []
    # This process's own command line, whatever it is, is not a launcher: the
    # scan wants the canonical ``run-local`` verb as well as the key.
    assert pool.find_launcher_pids("f" * 64) == []


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


def test_a_process_that_merely_mentions_the_key_is_not_the_launcher(
    pidfile: Path,
) -> None:
    """Naming an action is not running it.

    ``pbrun --withdraw <full digest>`` puts the key in its own command line, and
    so does any shell wrapper around it.  If a stale ``child_pid`` in a lease
    collided with such a process, a key-only test would have the withdrawal
    signal the operator's own terminal.  The canonical ``run-local`` verb is
    what separates the two, so both naming paths ask for it.
    """

    talker = subprocess.Popen(
        [sys.executable, "-c", f"import time; time.sleep(600)  # {KEY_A}"],
        start_new_session=True,
    )
    pidfile.write_text(str(talker.pid))
    try:
        assert _await(lambda: KEY_A.encode() in _cmdline_of(talker.pid))
        assert pool.launcher_owns_action(talker.pid, KEY_A) is False
        assert talker.pid not in pool.find_launcher_pids(KEY_A)
    finally:
        talker.kill()
        talker.wait(timeout=10)


def _cmdline_of(pid: int) -> bytes:
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as handle:
            return handle.read()
    except OSError:
        return b""
