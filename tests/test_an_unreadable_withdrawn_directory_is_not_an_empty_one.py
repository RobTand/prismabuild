"""A withdrawal directory that cannot be read must not answer "nothing".

``withdrawn_keys`` turns ``withdrawn/`` into the evidence every generation
guard in the module decides on, and pre-fix it answered ``frozenset()`` for
any ``OSError`` (``src/prismabuild/pool.py:4863`` at 67a44bb, ``except
OSError: return frozenset()``).  Its own docstring says why that is the wrong
answer -- "A withdrawal that a guard could not see is not a withdrawal" -- and
``_claim`` reads the set as *the* load-bearing half of ``withdraw``: "A
withdrawal that lands while a worker is mid-``finish`` can leave a requeued
ready record behind it, and without this guard that record is claimed and the
cancelled work runs again."

This queue lives on NFS, where ``ESTALE`` on a cached directory handle is the
ordinary way a listing fails.  The module already treats it as a first-class
event -- ``_read_json``'s ``tolerate_stale`` (#208), and
``quarantine_orphans``, which re-raises every errno that is not ``ESTALE``
rather than swallowing the class -- so one stale handle silently defeated the
operator's cancellation on every box that hit it.  ``_read_json`` states the
rule this broke: a caller with no evidence that the directory is live cannot
answer "absent", because that "would turn a broken mount into a confident
wrong verdict".

Absence of the directory itself stays tolerated: a queue whose layout has not
been created yet legitimately has no ``withdrawn/``.
"""
from __future__ import annotations

import errno
import os
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
from prismabuild import pool  # noqa: E402

KEY = "b" * 64
STALE = OSError(errno.ESTALE, "stale NFS directory handle")


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "queue")
    q.publish(
        action_key=KEY, cas_root=q.root / "cas", checkout_root=q.root / "co",
        worker_script=q.root / "worker.py", resources={"cpu": 1},
        max_attempts=2, retry_safe=True,
    )
    return q


def _listing_of(monkeypatch: pytest.MonkeyPatch, directory: Path, exc: BaseException) -> None:
    """Make ``os.listdir`` fail for exactly one directory, as NFS does."""

    real = os.listdir
    target = str(directory)

    def guarded(path, *args, **kwargs):
        if str(path) == target:
            raise exc
        return real(path, *args, **kwargs)

    monkeypatch.setattr(pool.os, "listdir", guarded)


def test_withdrawn_keys_is_loud_when_the_directory_cannot_be_read(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unreadable ``withdrawn/`` is not the same answer as an empty one."""

    queue.ensure_layout()
    _listing_of(monkeypatch, queue.dir(pool.WITHDRAWN), STALE)
    with pytest.raises(OSError) as raised:
        queue.withdrawn_keys()
    assert raised.value.errno == errno.ESTALE


def test_a_withdrawn_action_is_not_claimed_when_the_marker_cannot_be_listed(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The race ``_claim``'s withdrawal guard exists for, with a stale handle.

    An operator withdraws the action; a worker mid-``finish`` then requeues it,
    so a ready record sits behind the marker.  With ``withdrawn/`` unreadable,
    pre-fix ``withdrawn_keys`` answered the empty set, the guard never fired
    and the cancelled work was claimed and run again.
    """

    ready = queue.item_path(pool.READY, KEY)
    requeued_bytes = ready.read_bytes()
    result = queue.withdraw(KEY, reason="operator cancelled it", signal_child=False)
    assert result["status"] == "withdrawn"
    assert queue.item_path(pool.WITHDRAWN, KEY).exists()
    # The requeue that lands behind the marker: the same generation, back in
    # ready, which is exactly what the guard is written to catch.
    ready.write_bytes(requeued_bytes)

    _listing_of(monkeypatch, queue.dir(pool.WITHDRAWN), STALE)
    with pytest.raises(OSError):
        queue.claim(capacity={"cpu": 1})
    assert not queue.item_path(pool.CLAIMED, KEY).exists()


def test_a_missing_withdrawn_directory_still_answers_empty(tmp_path: Path) -> None:
    """Absence is not an error: a queue with no layout has no withdrawals."""

    assert pool.PoolQueue(tmp_path / "fresh").withdrawn_keys() == frozenset()


def test_a_withdrawn_state_path_that_is_a_file_is_not_empty(
    queue: pool.PoolQueue,
) -> None:
    directory = queue.dir(pool.WITHDRAWN)
    directory.rename(directory.with_name("withdrawn-original"))
    directory.write_text("invalid queue state", encoding="utf-8")

    with pytest.raises(NotADirectoryError):
        queue.withdrawn_keys()
