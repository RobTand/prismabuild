"""``pbrun`` is the only submit path the hook allows, so it is the only exit too.

Two halves, tested where each can break.  The wait loop has to *watch*
``withdrawn/``: it already learned this lesson about ``failed/``, where sixty-six
outcomes were filed while their submitters sat in the loop for a day and were
then told the pool had never scheduled anything.  And the withdraw side has to
resolve the twelve-character prefix every log line prints, refusing an ambiguous
one rather than guessing -- the wrong guess here kills somebody else's work.
"""

from __future__ import annotations

import json
from pathlib import Path
import socket
import sys
import uuid

import pytest

# The local tree's ``prismabuild`` before ``pbrun`` puts the published copy on
# the path: the queue this file drives must be the one this file is testing.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import pbrun  # noqa: E402

# Unique per process; see the note in ``test_pool_withdraw``.
KEY_A = uuid.uuid4().hex + uuid.uuid4().hex

#: A box that is provably not this one.  Naming a real fleet member as "the
#: other box" reads fine until the suite runs ON that member: the holder these
#: tests call foreign becomes the local host and the premise is gone.  The
#: full suite runs on dl380g10, so the name these tests used was exactly the
#: one they could not use.  Same reasoning, same spelling, as
#: ``test_pool_withdraw.ELSEWHERE``.
ELSEWHERE = f"not-{socket.gethostname()}-{uuid.uuid4().hex[:8]}"


class _Queue:
    """Just enough PoolQueue for the wait loop: the terminal directories."""

    def __init__(self, root: Path):
        self.root = root
        for name in ("ready", "claimed", "done", "failed", "withdrawn"):
            (root / name).mkdir(parents=True, exist_ok=True)

    def item_path(self, state: str, key: str) -> Path:
        return self.root / state / f"{key}.json"


def _wait(monkeypatch, queue, key: str, wait_s: float = 5.0):
    monkeypatch.setattr(pbrun, "POLL_S", 0.001, raising=False)
    return pbrun.await_outcome(queue, key, wait_s=wait_s)


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _publish(q: pool.PoolQueue, key: str) -> None:
    q.publish(action_key=key, cas_root="/cas", checkout_root="/co",
              worker_script="/w.py")


# -- the wait loop -----------------------------------------------------------


def test_a_withdrawn_action_is_reported_not_waited_out(
    monkeypatch, capsys, tmp_path
) -> None:
    q = _Queue(tmp_path)
    q.item_path("withdrawn", "abc").write_text(json.dumps({
        "status": "withdrawn", "withdrawn_by": "rob@sparky",
        "reason": "ten merges stale", "withdrawn_host": "sparky"}))
    rc = _wait(monkeypatch, q, "abc")
    err = capsys.readouterr().err
    assert rc == pbrun.WITHDRAWN_EXIT, "the command did not run; do not exit 0"
    assert rc != 0
    assert "withdrawn by rob@sparky" in err and "ten merges stale" in err
    assert "gave up waiting" not in err


def test_watching_withdrawn_does_not_disturb_done_or_failed(
    monkeypatch, capsys, tmp_path
) -> None:
    q = _Queue(tmp_path)
    q.item_path("done", "abc").write_text(json.dumps(
        {"status": "executed", "detail": {"returncode": 0, "stdout": "ok\n"}}))
    assert _wait(monkeypatch, q, "abc") == 0
    q.item_path("failed", "xyz").write_text(json.dumps(
        {"status": "failed", "detail": {"returncode": 4}}))
    assert _wait(monkeypatch, q, "xyz") == 4


# -- the withdraw side -------------------------------------------------------


def test_withdrawing_by_prefix_reports_what_it_did(
    queue: pool.PoolQueue, capsys
) -> None:
    _publish(queue, KEY_A)
    rc = pbrun.withdraw_main(queue, [KEY_A[:12]], reason="stale", by="rob@sparky")
    assert rc == 0
    assert queue.item_path(pool.WITHDRAWN, KEY_A).exists()
    err = capsys.readouterr().err
    assert f"withdrew {KEY_A[:12]}" in err and "from ready" in err
    filed = json.loads(queue.item_path(pool.WITHDRAWN, KEY_A).read_text())
    assert filed["reason"] == "stale" and filed["withdrawn_by"] == "rob@sparky"


def test_one_bad_name_does_not_stop_the_others(
    queue: pool.PoolQueue, capsys
) -> None:
    """Withdrawing four suites at once is the case this exists for."""

    keys = [uuid.uuid4().hex + uuid.uuid4().hex for _ in range(4)]
    absent = (uuid.uuid4().hex + uuid.uuid4().hex)[:12]
    for key in keys:
        _publish(queue, key)
    rc = pbrun.withdraw_main(queue, [k[:12] for k in keys] + [absent])
    assert rc == 2, "the unknown name is still reported as a failure"
    for key in keys:
        assert queue.item_path(pool.WITHDRAWN, key).exists()
    assert "no action in the queue starts with" in capsys.readouterr().err


def test_an_ambiguous_prefix_withdraws_nothing(
    queue: pool.PoolQueue, capsys
) -> None:
    _publish(queue, "ab" + "c" * 62)
    _publish(queue, "ab" + "d" * 62)
    assert pbrun.withdraw_main(queue, ["ab"]) == 2
    assert list(queue.dir(pool.WITHDRAWN).glob("*.json")) == []
    assert "say more of the key" in capsys.readouterr().err


def test_a_claimed_action_on_another_box_says_the_signal_did_not_land(
    queue: pool.PoolQueue, capsys
) -> None:
    """Reading "withdrawn" must not be read as "already dead"."""

    _publish(queue, KEY_A)
    record = json.loads(queue.item_path(pool.READY, KEY_A).read_text())
    record["claimed_host"] = ELSEWHERE
    queue.item_path(pool.READY, KEY_A).unlink()
    queue.item_path(pool.CLAIMED, KEY_A).write_text(json.dumps(record))
    assert pbrun.withdraw_main(queue, [KEY_A[:12]]) == 0
    err = capsys.readouterr().err
    assert "stop requested through the generation marker" in err
    assert "checks it at the next heartbeat" in err


def test_a_withdrawal_the_holder_cannot_see_is_said_out_loud(
    queue: pool.PoolQueue, capsys, monkeypatch
) -> None:
    """Every guard the verb relies on lives in bytes the loop imported at start.

    A box that has not rolled runs the action to completion -- with the tokens
    this withdrawal just handed back, which is the load-average-371 shape the
    issue is about.  New bytes cannot reach that worker; what they can do is
    stop pretending the work has stopped.
    """

    _publish(queue, KEY_A)
    record = json.loads(queue.item_path(pool.READY, KEY_A).read_text())
    record["claimed_host"] = ELSEWHERE
    queue.item_path(pool.READY, KEY_A).unlink()
    queue.item_path(pool.CLAIMED, KEY_A).write_text(json.dumps(record))
    queue.announce(host=ELSEWHERE, tags=["x86"], has_gpu=False,
                   runtime_commit="a" * 40)
    monkeypatch.setattr(pbrun, "published_commit", lambda: "b" * 40)

    assert pbrun.withdraw_main(queue, [KEY_A[:12]]) == 0
    err = capsys.readouterr().err
    assert f"WARNING {ELSEWHERE} is running runtime " + "a" * 12 in err
    assert "not the published " + "b" * 12 in err


def test_a_holder_that_never_announced_is_reported_as_unknown(
    queue: pool.PoolQueue, capsys, monkeypatch
) -> None:
    """"Cannot tell" is a third answer, and it is not "it stopped"."""

    _publish(queue, KEY_A)
    record = json.loads(queue.item_path(pool.READY, KEY_A).read_text())
    record["claimed_host"] = ELSEWHERE
    queue.item_path(pool.READY, KEY_A).unlink()
    queue.item_path(pool.CLAIMED, KEY_A).write_text(json.dumps(record))
    monkeypatch.setattr(pbrun, "published_commit", lambda: "b" * 40)

    assert pbrun.withdraw_main(queue, [KEY_A[:12]]) == 0
    assert f"no live offer from {ELSEWHERE}" in capsys.readouterr().err


def test_a_worker_on_the_published_bytes_draws_no_warning(
    queue: pool.PoolQueue, capsys, monkeypatch
) -> None:
    _publish(queue, KEY_A)
    record = json.loads(queue.item_path(pool.READY, KEY_A).read_text())
    record["claimed_host"] = ELSEWHERE
    queue.item_path(pool.READY, KEY_A).unlink()
    queue.item_path(pool.CLAIMED, KEY_A).write_text(json.dumps(record))
    queue.announce(host=ELSEWHERE, tags=["x86"], has_gpu=False,
                   runtime_commit="b" * 40)
    monkeypatch.setattr(pbrun, "published_commit", lambda: "b" * 40)

    assert pbrun.withdraw_main(queue, [KEY_A[:12]]) == 0
    assert "WARNING" not in capsys.readouterr().err


def test_the_wait_loop_does_not_answer_a_new_run_with_an_old_withdrawal(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """The half of the blocker the caller actually sees.

    ``await_outcome`` polls ``withdrawn/<key>.json`` by name, so while the
    marker was a permanent blacklist a brand new submission was answered on its
    first poll with a stranger's ``withdrawn_by`` and reason, at exit 143.
    ``publish`` retiring the marker is what makes the loop tell the truth.
    """

    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    _publish(q, KEY_A)
    q.withdraw(KEY_A, reason="four suites, one box", by="rob@sparky")

    _publish(q, KEY_A)                      # the same command, the same tree
    q.claim()
    q.finish(KEY_A, status="executed", detail={"returncode": 0})

    monkeypatch.setattr(pbrun, "POLL_S", 0.001, raising=False)
    assert pbrun.await_outcome(q, KEY_A, wait_s=5.0) == 0
    err = capsys.readouterr().err
    assert "withdrawn by" not in err and "four suites" not in err


def test_an_already_finished_action_is_reported_not_refiled(
    queue: pool.PoolQueue, capsys
) -> None:
    _publish(queue, KEY_A)
    queue.claim()
    queue.finish(KEY_A, status="executed")
    assert pbrun.withdraw_main(queue, [KEY_A[:12]]) == 0
    assert not queue.item_path(pool.WITHDRAWN, KEY_A).exists()
    assert "had already finished" in capsys.readouterr().err


def test_withdraw_refuses_to_also_take_a_command(monkeypatch) -> None:
    """Cancelling is not a submission and must not silently become one."""

    monkeypatch.setattr(sys, "argv",
                        ["pbrun", "--withdraw", "abc", "--", "echo", "hi"])
    with pytest.raises(SystemExit) as caught:
        pbrun.main()
    assert "takes no command" in str(caught.value)


def test_the_command_line_withdraws_without_a_submission(
    queue: pool.PoolQueue, tmp_path: Path, monkeypatch, capsys
) -> None:
    """The whole path an operator actually types, argparse included."""

    _publish(queue, KEY_A)
    monkeypatch.setattr(pbrun, "SH", tmp_path)
    monkeypatch.setattr(sys, "argv",
                        ["pbrun", "--withdraw", KEY_A[:12],
                         "--reason", "load average 371"])
    assert pbrun.main() == 0
    filed = json.loads(queue.item_path(pool.WITHDRAWN, KEY_A).read_text())
    assert filed["reason"] == "load average 371"
    assert "@" in filed["withdrawn_by"], "who withdrew it is recorded"
    assert f"withdrew {KEY_A[:12]}" in capsys.readouterr().err


def test_the_foreign_holder_is_provably_not_this_box() -> None:
    """Every "another box" case above depends on this and none of them says so.

    Naming a real fleet member reads fine until the suite runs ON that
    member, where the foreign holder and the local one become the same host
    and the case tests nothing. The full suite runs on dl380g10: ``pbtest``
    exists because its 80 x86 cores are idle.
    """

    assert ELSEWHERE != socket.gethostname()
