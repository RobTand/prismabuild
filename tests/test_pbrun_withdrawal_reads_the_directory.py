"""``--withdraw`` revalidates a terminal directory before it reads it.

PR #54 gave the lane ``read_withdrawal_marker`` and routed three readers of
``pb-queue/withdrawn`` through it, because that directory is on NFS with
default attribute caching: a lookup of a name that did not exist yet is
negatively cached, so ``exists()`` keeps answering False and ``open()`` keeps
raising ``ENOENT`` after the file has landed.  ``pbrun.terminal_record`` polls
by ``os.listdir`` for the same reason and says so.

``pbrun._file_slurm_withdrawal`` was left behind.  It asks all three terminal
directories whether this generation already ended, and it asked with
``filed.exists()``.  What that costs is the case the function exists to
recognise: the action finished a moment before the operator typed the
withdrawal.  On a stale answer the verb files a withdrawal marker and a
``withdrawn/`` record for a run that is already in ``done/``, runs ``scancel``
against a job that has gone, and reports the work as cancelled to the person
who wanted it stopped.  It succeeded.

The shape is modelled rather than reproduced, exactly as
``tests/test_slurm_withdrawal_marker_reads.py`` models it: a directory whose
entries this client denies until something lists it.  That needs no NFS mount
to state.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402

import pbrun  # noqa: E402

KEY = "ef" * 32
GENERATION = 1757000000.0


class _NegativelyCachedDirectory:
    """Every name in one directory reads as absent until somebody lists it.

    Copied from ``tests/test_slurm_withdrawal_marker_reads.py``, which is where
    PR #54 introduced it for the same failure in ``slurm_lane``.
    """

    def __init__(self, monkeypatch: pytest.MonkeyPatch, directory: Path):
        self.directory = directory
        self.revalidated = False
        real_listdir = os.listdir
        real_exists = Path.exists
        real_is_file = Path.is_file
        real_read_text = Path.read_text

        def listdir(path):
            entries = real_listdir(path)
            if Path(path) == self.directory:
                self.revalidated = True
            return entries

        def stale(path: Path) -> bool:
            return path.parent == self.directory and not self.revalidated

        def exists(this, *args, **kwargs):
            return False if stale(this) else real_exists(this, *args, **kwargs)

        def is_file(this, *args, **kwargs):
            return False if stale(this) else real_is_file(this, *args, **kwargs)

        def read_text(this, *args, **kwargs):
            if stale(this):
                raise FileNotFoundError(2, "No such file or directory", str(this))
            return real_read_text(this, *args, **kwargs)

        monkeypatch.setattr(os, "listdir", listdir)
        monkeypatch.setattr(Path, "exists", exists)
        monkeypatch.setattr(Path, "is_file", is_file)
        monkeypatch.setattr(Path, "read_text", read_text)


def _submission() -> dict:
    return {
        "action_key": KEY,
        "job_id": "1743",
        "published_unix": GENERATION,
        "published_by": "sparky",
        "attempt": 1,
        "max_attempts": 1,
        "retry_safe": False,
        "resources": {},
        "constraint": [],
        "directory": ".",
    }


def _ending(queue_root: Path, state: str) -> Path:
    directory = queue_root / state
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{KEY}.json"
    path.write_text(json.dumps({
        "schema": pool.POOL_OUTCOME_SCHEMA_V1,
        "transport": "slurm",
        "action_key": KEY,
        "status": "executed",
        "attempts": 1,
        "published_unix": GENERATION,
        "finished_host": "sparky",
        "detail": {"returncode": 0, "elapsed_s": 3.0},
    }), encoding="utf-8")
    return path


def test_an_ending_the_client_has_not_revalidated_still_stops_the_withdrawal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The action finished first, so there is nothing to withdraw.

    ``None`` is the whole answer: ``withdraw_slurm_main`` prints "already has
    an outcome filed" and runs no ``scancel``.
    """

    queue_root = tmp_path / "pb-queue"
    _ending(queue_root, pool.DONE)
    _NegativelyCachedDirectory(monkeypatch, queue_root / pool.DONE)

    marker = pbrun._file_slurm_withdrawal(
        queue_root, _submission(), reason="stale", by="rob",
        scancel_command="scancel",
    )

    assert marker is None
    assert not (queue_root / pool.WITHDRAWN / f"{KEY}.json").exists()


def test_a_failed_ending_is_read_through_the_same_revalidation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """All three terminal directories, not the one the test happened to pick."""

    queue_root = tmp_path / "pb-queue"
    _ending(queue_root, pool.FAILED)
    _NegativelyCachedDirectory(monkeypatch, queue_root / pool.FAILED)

    assert pbrun._file_slurm_withdrawal(
        queue_root, _submission(), reason="stale", by="rob",
        scancel_command="scancel",
    ) is None


def test_an_action_still_running_is_withdrawn_as_before(tmp_path: Path) -> None:
    """The revalidation must not turn a live action into an unwithdrawable one."""

    queue_root = tmp_path / "pb-queue"

    marker = pbrun._file_slurm_withdrawal(
        queue_root, _submission(), reason="superseded", by="rob",
        scancel_command="scancel",
    )

    assert marker is not None
    filed = json.loads(
        (queue_root / pool.WITHDRAWN / f"{KEY}.json").read_text(encoding="utf-8")
    )
    assert filed["status"] == "withdrawn"
    assert filed["reason"] == "superseded"
