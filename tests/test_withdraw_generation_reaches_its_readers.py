"""The generation scoping has to reach the readers outside ``pool.py``.

``withdrawal_covers`` made a withdrawal cancel a *run* rather than a key for
all time, and every guard inside the pool consults it.  Two readers outside it
did not, and both still matched on the bare key:

* ``pbrun.await_outcome`` broke out of its wait the moment
  ``withdrawn/<key>.json`` existed.  An action key is a content hash, so two
  agents running the same command against the same tree submit the same key --
  which is how four identical suites came to be on one box in the first place.
  Withdraw the run that is claimed, and the pool correctly leaves the
  submission published behind it in ``ready``; the submitter was nonetheless
  told "withdrawn by <a stranger> -- <their reason>" and given exit 143, for a
  run that then went on to execute and be filed under ``done`` with nobody
  left reading it.  That is the objection's last sentence, surviving one layer
  out from where it was answered.

* ``pool_reset`` skipped a failed item on ``path.stem in withdrawn_keys()``,
  under a comment that said in as many words that the reading was
  generation-scoped.  A comment is not the value a gate reads.

Both now call ``withdrawal_covers``, which is why ``publish`` returns the item
it created: a caller that cannot name its own generation cannot tell its own
cancellation from somebody else's.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys
import uuid

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402
import pbrun  # noqa: E402
import pool_reset  # noqa: E402

KEY_A = uuid.uuid4().hex + uuid.uuid4().hex


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _publish(q: pool.PoolQueue, key: str, **kw: object) -> dict:
    return q.publish(
        action_key=key,
        cas_root=kw.pop("cas_root", "/cas"),
        checkout_root=kw.pop("checkout_root", "/co"),
        worker_script=kw.pop("worker_script", "/w.py"),
        **kw,
    )


# -- publish names the generation it created ---------------------------------


def test_publish_hands_back_the_generation_it_stamped(
    queue: pool.PoolQueue
) -> None:
    """The item, not its path -- and the item on disk is the one returned."""

    item = _publish(queue, KEY_A)
    on_disk = json.loads(queue.item_path(pool.READY, KEY_A).read_text())
    assert item["published_unix"] == on_disk["published_unix"]
    assert item["action_key"] == KEY_A


# -- pbrun: a stranger's cancellation is not this caller's outcome -----------


def _two_generations(queue: pool.PoolQueue) -> dict:
    """The live fleet's ordinary shape: two agents, one content-addressed key.

    The first generation is claimed and running.  The second is submitted
    behind it -- ``publish`` retires no marker because there is none yet -- and
    is the one this caller is waiting for.  Then the operator withdraws, which
    reads the *claimed* record and so files its marker against generation one.
    """

    _publish(queue, KEY_A)                       # agent A, generation one
    assert queue.claim() is not None             # a worker starts running it
    mine = _publish(queue, KEY_A)                # agent B, generation two
    queue.withdraw(KEY_A, reason="four suites, one box", by="rob@sparky",
                   signal_child=False)
    # The pool did its half: the withdrawal named generation one, so this
    # caller's submission is still queued and will run.
    assert queue.item_path(pool.READY, KEY_A).exists()
    assert queue.item_path(pool.WITHDRAWN, KEY_A).exists()
    return mine


def test_a_caller_is_not_told_a_stranger_withdrew_its_run(
    queue: pool.PoolQueue, capsys
) -> None:
    """The blocker's last sentence, one layer out from where it was answered.

    Before the fix this returned 143 and printed "withdrawn by rob@sparky --
    four suites, one box" for a submission the pool had deliberately kept.
    """

    mine = _two_generations(queue)

    exit_code = pbrun.await_outcome(queue, KEY_A, wait_s=0.05, mine=mine)

    assert exit_code != pbrun.WITHDRAWN_EXIT, (
        "the withdrawal names an earlier run of this key, not this submission")
    assert exit_code == 75, "nothing terminal has landed yet, so it waits"
    said = capsys.readouterr().err
    assert "names an earlier run" in said, (
        "and it says why it is still waiting, rather than waiting in silence")
    assert "rob@sparky" not in said


def test_the_caller_then_gets_its_own_outcome(queue: pool.PoolQueue) -> None:
    """Waiting is only right if the run it waits for can still conclude."""

    mine = _two_generations(queue)
    assert queue.claim() is not None              # generation two runs
    queue.finish(KEY_A, status="executed", detail={"returncode": 0})

    assert pbrun.await_outcome(queue, KEY_A, wait_s=0.05, mine=mine) == 0
    assert queue.item_path(pool.DONE, KEY_A).exists()


def test_a_withdrawal_of_this_caller_s_own_run_still_reports_it(
    queue: pool.PoolQueue, capsys
) -> None:
    """The scoping must not cost the verb its whole point."""

    mine = _publish(queue, KEY_A)
    queue.withdraw(KEY_A, reason="wrong branch", by="rob@sparky",
                   signal_child=False)

    assert pbrun.await_outcome(
        queue, KEY_A, wait_s=0.05, mine=mine) == pbrun.WITHDRAWN_EXIT
    assert "wrong branch" in capsys.readouterr().err


def test_a_caller_that_cannot_name_a_generation_reads_the_marker(
    queue: pool.PoolQueue
) -> None:
    """``mine=None`` keeps the old reading: a bare marker is this caller's.

    Green before the change as well as after, deliberately: someone waiting on
    a submission that is not theirs has no generation to compare, and
    ``withdrawal_covers`` answers a record it cannot place with the marker --
    the safe direction, and the reading this loop already had.
    """

    _two_generations(queue)
    assert pbrun.await_outcome(
        queue, KEY_A, wait_s=0.05) == pbrun.WITHDRAWN_EXIT


# -- pool_reset: the comment's claim, made true ------------------------------


def _fleet(tmp_path: Path, monkeypatch) -> pool.PoolQueue:
    """A queue and CAS where ``pool_reset`` can recover a failed item."""

    share = tmp_path / "fleet"
    (share / "pb-queue").mkdir(parents=True)
    monkeypatch.setattr(pool_reset, "SH", share)
    monkeypatch.setattr(sys, "argv", ["pool_reset"])       # report, do not submit
    request = share / "cas" / "requests" / KEY_A[:2] / f"{KEY_A}.json"
    request.parent.mkdir(parents=True)
    request.write_text(json.dumps(
        {"params": {"command": ["python3", "-m", "pytest", "-q"]}}))
    q = pool.PoolQueue(share / "pb-queue")
    q.ensure_layout()
    return q


def _fail_the_claimed_run(q: pool.PoolQueue, cwd: Path) -> None:
    item = q.claim()
    assert item is not None
    q.finish(KEY_A, status="failed", detail={"returncode": 1})
    record = json.loads(q.item_path(pool.FAILED, KEY_A).read_text())
    record["checkout_root"] = str(cwd)
    q.item_path(pool.FAILED, KEY_A).write_text(json.dumps(record))


def test_pool_reset_skips_the_run_that_was_withdrawn(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """The behaviour the skip exists for: a decision is not a defect.

    Green before the change as well as after -- it is the half of the reading
    that was always right, and the half a generation check could break.  It is
    here so that the fix below cannot pass by deleting the skip.
    """

    q = _fleet(tmp_path, monkeypatch)
    _publish(q, KEY_A, max_attempts=1)
    assert q.claim() is not None
    q.withdraw(KEY_A, reason="wrong branch", by="rob@sparky", signal_child=False)
    # A worker on bytes that predate the verb files its own outcome, and that
    # is the record ``pool_reset`` reads.  Written without the verb's
    # ``withdrawn_unix`` stamp so the *marker* is what has to carry the skip:
    # the record's own stamp is the other, independent reading.
    marker = json.loads(q.item_path(pool.WITHDRAWN, KEY_A).read_text())
    q.item_path(pool.FAILED, KEY_A).write_text(json.dumps(
        {"action_key": KEY_A, "status": "failed",
         "published_unix": marker["published_unix"],
         "checkout_root": str(tmp_path)}))

    assert pool_reset.main() == 0
    assert "a decision, not a defect" in capsys.readouterr().out


def test_pool_reset_does_not_skip_a_later_run_of_a_withdrawn_key(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """A failed item of a run nobody cancelled is work, not a decision.

    Before the fix ``path.stem in withdrawn`` skipped it on the strength of a
    marker naming an earlier run, and printed "a decision, not a defect" over
    a suite that had genuinely failed -- so the one tool that re-submits
    failures walked past it forever.
    """

    q = _fleet(tmp_path, monkeypatch)
    _publish(q, KEY_A, max_attempts=1)                # generation one
    assert q.claim() is not None
    _publish(q, KEY_A, max_attempts=1)                # generation two
    q.withdraw(KEY_A, reason="four suites, one box", by="rob@sparky",
               signal_child=False)                    # names generation one
    _fail_the_claimed_run(q, tmp_path)                # generation two failed

    assert pool_reset.main() == 0
    reported = capsys.readouterr().out
    assert "a decision, not a defect" not in reported
    assert "would submit" in reported
