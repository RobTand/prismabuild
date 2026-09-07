"""A terminal directory that cannot be read must not answer "nothing".

``terminal_keys`` turns ``done/`` and ``failed/`` into the evidence
``terminal_outcome_covers`` decides on, and pre-fix it skipped a state
directory on any ``OSError`` (``src/prismabuild/pool.py:4811`` at 67a44bb,
``except OSError: continue``), so an unreadable ``done/`` reported that no
outcome had been filed for any action.  ``reap_stale`` then finds no filed
outcome for a generation that has one and puts it back in ``ready`` -- which
is the one thing its own docstring says a CAS hit does not license: "a CAS hit
does not license putting already-terminal work back in the queue".

This queue lives on NFS, where ``ESTALE`` on a cached directory handle is the
ordinary way a listing fails.  The module already treats it as a first-class
event -- ``_read_json``'s ``tolerate_stale`` (#208), and
``quarantine_orphans``, which re-raises every errno that is not ``ESTALE``
rather than swallowing the class.  ``_read_json`` states the rule this broke:
a caller with no evidence that the directory is live cannot answer "absent",
because that "would turn a broken mount into a confident wrong verdict".

Absence of the directory itself stays tolerated: a queue whose layout has not
been created yet legitimately has no ``done/``.
"""
from __future__ import annotations

import errno
import json
import os
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
from prismabuild import pool  # noqa: E402

KEY = "c" * 64
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


def test_terminal_keys_is_loud_when_a_state_directory_cannot_be_read(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unreadable ``done/`` is not the same answer as an empty one."""

    queue.ensure_layout()
    _listing_of(monkeypatch, queue.dir(pool.DONE), STALE)
    with pytest.raises(OSError) as raised:
        queue.terminal_keys()
    assert raised.value.errno == errno.ESTALE


def test_a_filed_outcome_is_not_requeued_when_done_cannot_be_listed(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``reap_stale`` must not put already-terminal work back in the queue.

    The claim is stranded -- no lease, claimed long enough ago to be past the
    grace -- and its generation already has a ``done`` record.  With ``done/``
    unreadable, pre-fix ``terminal_keys`` reported no terminal keys at all, the
    ``terminal_outcome_covers`` guard answered ``None``, and the reaper
    requeued an action that had already succeeded: the payload runs a second
    time and the second ``finish`` writes over the record of the first.
    """

    item = queue.claim(capacity={"cpu": 1})
    assert item is not None
    claimed_path = queue.item_path(pool.CLAIMED, KEY)
    record = json.loads(claimed_path.read_text())
    # A claimant that won the rename and then died: no lease, and old enough
    # that the reaper's grace has passed.
    record["claimed_unix"] = pool._now() - (pool.HEARTBEAT_S * 10)
    pool._write_json_atomic(claimed_path, record)
    queue.lease_path(KEY).unlink(missing_ok=True)
    pool._write_json_atomic(
        queue.item_path(pool.DONE, KEY),
        {
            "schema": pool.POOL_OUTCOME_SCHEMA_V1,
            "action_key": KEY,
            "status": "executed",
            "published_unix": record["published_unix"],
            "finished_unix": pool._now(),
            "finished_host": "some-other-box",
        },
    )

    _listing_of(monkeypatch, queue.dir(pool.DONE), STALE)
    with pytest.raises(OSError):
        queue.reap_stale()
    assert not queue.item_path(pool.READY, KEY).exists()


def test_a_missing_done_directory_still_answers_empty(tmp_path: Path) -> None:
    """Absence is not an error: a queue with no layout has no outcomes."""

    assert pool.PoolQueue(tmp_path / "fresh").terminal_keys() == frozenset()
