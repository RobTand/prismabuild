"""A landed ending is verified even when this box cached its attempt as absent.

#1100: ``pbrun`` on sparky waited for actions that ran on sparklina.  Each
ending landed and its ``done/`` or ``failed/`` row read fine, then outcome
verification failed with ``FileNotFoundError: [Errno 2] No such file or
directory: '<action key>'`` and ``pbrun`` exited 74.  The name is the
``attempts/<key>`` component that ``core._open_directory_nofollow`` opens
relative to the held ``attempts/`` descriptor.  The waiter's own poll looked
that name up before the other box created it (``archived_generation_outcomes``
walks ``attempts/<key>/<generation>`` on every poll that has no ending yet),
so the NFS client held a negative entry for it.  On the real mount that entry
outlived a fresh open of the parent for 25 s to more than 180 s.

The filesystem shim here stands in for that client: opening the key's
directory by name fails with ``ENOENT`` although the directory exists, until
the condition the test names ends.  These tests never point at a fleet queue.
"""

from __future__ import annotations

import errno
import json
import os
from pathlib import Path
import shutil
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402
import pbtest  # noqa: E402

KEY = "1100" + "a" * 60
SUMMARY = "=========== 61 passed, 1 skipped in 70.40s ==========="


@pytest.fixture()
def queue(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> pool.PoolQueue:
    monkeypatch.setattr(pbrun, "POLL_S", 0.01)
    made = pool.PoolQueue(tmp_path / "pb-queue")
    made.ensure_layout()
    return made


def _landed(q: pool.PoolQueue, *, status: str = "executed",
            returncode: int = 0) -> float:
    """Publish, claim and finish one run; return its generation."""

    q.publish(action_key=KEY, cas_root="/cas", checkout_root="/co",
              worker_script="/w.py", max_attempts=1)
    ready = json.loads(q.item_path(pool.READY, KEY).read_text(encoding="utf-8"))
    claim = q.claim()
    assert claim is not None and claim["action_key"] == KEY
    q.finish(KEY, status=status,
             detail={"returncode": returncode, "stdout": f"tests/x.py ...\n{SUMMARY}\n"},
             claim_snapshot=claim)
    return float(ready["published_unix"])


class StaleName:
    """Answer ``ENOENT`` for ``attempts/<KEY>`` opened by name while stale.

    ``stale()`` decides, per open, whether this client still holds the
    negative entry.  A listing of ``attempts/`` is recorded, because a
    READDIR is what refreshes the client's view of the directory's names.
    Only a ``dir_fd``-relative open of exactly ``KEY`` is refused, which is
    the call #1100's message names; every other path is the real one.
    """

    def __init__(self, q: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch,
                 stale) -> None:
        self.attempts = str(q.root / pool.ATTEMPTS)
        self.listed = 0
        self.refused = 0
        self._stale = stale
        real_open, real_listdir, real_scandir = os.open, os.listdir, os.scandir

        def fake_open(path, flags, mode=0o777, *, dir_fd=None):
            if dir_fd is not None and path == KEY and self._stale(self):
                self.refused += 1
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)
            return real_open(path, flags, mode, dir_fd=dir_fd)

        def note(path) -> None:
            if path is not None and os.fspath(path) == self.attempts:
                self.listed += 1

        def fake_listdir(path="."):
            note(path)
            return real_listdir(path)

        def fake_scandir(path="."):
            note(path)
            return real_scandir(path)

        monkeypatch.setattr(os, "open", fake_open)
        monkeypatch.setattr(os, "listdir", fake_listdir)
        monkeypatch.setattr(os, "scandir", fake_scandir)


def _lines(capsys: pytest.CaptureFixture[str]) -> list[str]:
    out, err = capsys.readouterr()
    return [line for line in (out + err).splitlines() if line.strip()]


def test_a_landed_ending_is_reported_when_the_key_is_stale_until_listed(
    queue, monkeypatch, capsys,
) -> None:
    """The #1100 shape: the name opens once the parent has been listed."""

    generation = _landed(queue)
    # Stale in every forked reader until that reader lists attempts/.
    StaleName(queue, monkeypatch, lambda shim: shim.listed == 0)

    rc = pbrun.await_outcome(queue, KEY, wait_s=30.0, generation=generation)
    lines = _lines(capsys)

    assert rc == 0, lines
    # pbtest reports this shard with its own pytest summary, not as
    # OUTCOME UNOBSERVED.
    assert pbtest.pytest_summary(lines) == SUMMARY
    assert pbtest.unobserved_outcome(lines) is None


def test_a_landed_ending_is_reported_after_one_enoent(
    queue, monkeypatch, capsys,
) -> None:
    """The issue's acceptance: one ``ENOENT``, then the name is there."""

    generation = _landed(queue)
    calls = {"n": 0}

    def once(_shim) -> bool:
        calls["n"] += 1
        return calls["n"] == 1

    StaleName(queue, monkeypatch, once)
    rc = pbrun.await_outcome(queue, KEY, wait_s=30.0, generation=generation)
    lines = _lines(capsys)
    assert rc == 0, lines
    assert pbtest.pytest_summary(lines) == SUMMARY


def test_a_failed_ending_keeps_its_own_exit_status(
    queue, monkeypatch, capsys,
) -> None:
    """Two of #1100's four actions had failed; they report failure, not 74."""

    generation = _landed(queue, status="failed", returncode=1)
    StaleName(queue, monkeypatch, lambda shim: shim.listed == 0)
    rc = pbrun.await_outcome(queue, KEY, wait_s=30.0, generation=generation)
    lines = _lines(capsys)
    assert rc == 1, lines
    assert pbtest.pytest_summary(lines) == SUMMARY


def test_a_name_listed_but_still_stale_is_read_again_inside_the_wait(
    queue, monkeypatch, capsys,
) -> None:
    """A stale entry that outlives one listing is waited out, not reported 74.

    On the real mount the entry once outlived a fresh open of its parent for
    more than 180 s.  A verification that sees the name listed and still
    cannot open it goes back to observation, as a timed-out verification
    does, and reads again inside ``--wait-s``.
    """

    generation = _landed(queue)
    clears_at = time.monotonic() + 0.5
    StaleName(queue, monkeypatch, lambda _shim: time.monotonic() < clears_at)
    rc = pbrun.await_outcome(queue, KEY, wait_s=30.0, generation=generation)
    lines = _lines(capsys)
    assert rc == 0, lines
    assert pbtest.pytest_summary(lines) == SUMMARY
    assert any("not visible here yet" in line for line in lines), lines


def test_a_stale_name_at_zero_patience_stays_unobserved(
    queue, monkeypatch, capsys,
) -> None:
    """``wait_s=0`` keeps one observation: a name it cannot open is 74."""

    generation = _landed(queue)
    StaleName(queue, monkeypatch, lambda _shim: True)
    rc = pbrun.await_outcome(queue, KEY, wait_s=0, generation=generation)
    lines = _lines(capsys)
    assert rc == pbrun.RECORD_WRITE_FAILED_EXIT, lines
    assert pbtest.unobserved_outcome(lines) == KEY[:12]


def test_an_attempt_that_is_really_missing_is_never_a_pass(
    queue, capsys,
) -> None:
    """No listing shows a removed attempt, so verification still fails closed.

    The wait ends at once with 74: a missing record is not waited on as if it
    were stale, and it is never read as a pass.
    """

    generation = _landed(queue)
    shutil.rmtree(queue.root / pool.ATTEMPTS / KEY)
    started = time.monotonic()
    rc = pbrun.await_outcome(queue, KEY, wait_s=30.0, generation=generation)
    lines = _lines(capsys)
    assert rc == pbrun.RECORD_WRITE_FAILED_EXIT, lines
    assert time.monotonic() - started < 20.0
    assert any("FileNotFoundError" in line for line in lines), lines
    assert pbtest.pytest_summary(lines) == ""
