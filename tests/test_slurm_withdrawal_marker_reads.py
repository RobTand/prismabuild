"""The lane revalidates the withdrawal marker's directory before reading it.

``pb-queue/withdrawn`` is on NFS with default attribute caching, and this
codebase already measured what that does to a lookup of a path that did not
exist yet: the client caches the negative entry, so ``exists()`` keeps
answering False and ``open()`` keeps raising ``ENOENT`` after the file has
landed.  ``pbrun.terminal_record`` polls by ``os.listdir`` for exactly that
reason, and says so.

Three reads in ``slurm_lane`` trusted the lookup instead, and what they lose is
one operator's decision.  An operator on another box withdraws a key inside the
attribute-cache window: the marker is written, the terminal record is written,
then ``scancel`` runs.  The submitter's ``wait`` returns ``CANCELLED``;
``_file_ending`` asks ``withdrawal_covers``, gets the stale negative, and calls
``publish_withdrawal``, whose own read misses too -- so the operator's marker is
replaced, ``withdrawn_by`` becomes ``slurm:scancel``, the reason is lost, and
``publish_outcome`` copies both into the terminal record.

The shape is modelled rather than reproduced: a directory whose entries this
client denies until something lists it.  That is what the fix has to defeat,
and it needs no NFS mount to state.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))

from prismabuild import pool, slurm_lane as sl  # noqa: E402

KEY = "ab" * 32


class _NegativelyCachedDirectory:
    """Every name in one directory reads as absent until somebody lists it."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, directory: Path):
        self.directory = directory
        self.revalidated = False
        self.listed: list[str] = []
        real_listdir = os.listdir
        real_exists = Path.exists
        real_is_file = Path.is_file
        real_read_text = Path.read_text

        def listdir(path):
            self.listed.append(str(path))
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


def _operators_marker(queue_root: Path, *, published_unix: float) -> Path:
    directory = queue_root / pool.WITHDRAWN
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{KEY}.json"
    path.write_text(json.dumps({
        "schema": pool.POOL_OUTCOME_SCHEMA_V1,
        "transport": "slurm",
        "action_key": KEY,
        "status": "withdrawn",
        "withdrawn_from": "slurm",
        "withdrawn_unix": published_unix,
        "published_unix": published_unix,
        "withdrawn_host": "dl380g10",
        "withdrawn_by": "rob@dl380g10",
        "reason": "superseded by the 4.75 arm",
    }), encoding="utf-8")
    return path


def test_publish_withdrawal_lists_the_directory_before_it_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Listing is what revalidates the entry, so it has to happen first."""

    queue_root = tmp_path / "pb-queue"
    _operators_marker(queue_root, published_unix=100.0)
    client = _NegativelyCachedDirectory(monkeypatch, queue_root / pool.WITHDRAWN)

    _, filed = sl.publish_withdrawal(
        queue_root=queue_root, action_key=KEY,
        reason="the job was cancelled", by="slurm:scancel",
    )

    assert str(queue_root / pool.WITHDRAWN) in client.listed
    assert filed["withdrawn_by"] == "rob@dl380g10"
    assert filed["reason"] == "superseded by the 4.75 arm"


def test_publish_withdrawal_keeps_the_first_decision_on_disk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The marker itself is not rewritten, whatever the second caller asked."""

    queue_root = tmp_path / "pb-queue"
    marker = _operators_marker(queue_root, published_unix=100.0)
    _NegativelyCachedDirectory(monkeypatch, queue_root / pool.WITHDRAWN)

    sl.publish_withdrawal(
        queue_root=queue_root, action_key=KEY,
        reason="the job was cancelled", by="slurm:scancel",
    )

    kept = json.loads(marker.read_text(encoding="utf-8"))
    assert kept["withdrawn_by"] == "rob@dl380g10"
    assert kept["reason"] == "superseded by the 4.75 arm"


def test_withdrawal_covers_finds_a_marker_a_stale_lookup_denies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``_file_ending`` asks this before it files anything under ``done``."""

    queue_root = tmp_path / "pb-queue"
    _operators_marker(queue_root, published_unix=100.0)
    _NegativelyCachedDirectory(monkeypatch, queue_root / pool.WITHDRAWN)

    found = sl.withdrawal_covers(queue_root, KEY, 100.0)
    assert found is not None
    assert found["withdrawn_by"] == "rob@dl380g10"


def test_withdrawal_covers_still_answers_only_for_this_generation(
    tmp_path: Path
) -> None:
    """Revalidating changes what is read, not which request it covers."""

    queue_root = tmp_path / "pb-queue"
    _operators_marker(queue_root, published_unix=100.0)

    assert sl.withdrawal_covers(queue_root, KEY, 100.0) is not None
    assert sl.withdrawal_covers(queue_root, KEY, 200.0) is None


def test_supersede_withdrawal_retires_a_marker_a_stale_lookup_denies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The read right after ``sbatch``, which is what plants the negative entry.

    A submission that cannot see the live marker leaves it in place, and a
    marker left in place is what makes the re-submitted action unrunnable.
    """

    queue_root = tmp_path / "pb-queue"
    marker = _operators_marker(queue_root, published_unix=100.0)
    _NegativelyCachedDirectory(monkeypatch, queue_root / pool.WITHDRAWN)

    retired = sl.supersede_withdrawal(queue_root, KEY, 200.0)
    assert retired is not None and retired["withdrawn_by"] == "rob@dl380g10"
    assert not os.path.isfile(marker)
    archived = list(
        (queue_root / pool.WITHDRAWN / "superseded").glob(f"{KEY}.*.json")
    )
    assert len(archived) == 1


def test_an_absent_marker_is_still_absent(tmp_path: Path) -> None:
    """Revalidating must not invent a decision nobody made, and a queue root
    with no ``withdrawn`` directory at all is the ordinary first case."""

    queue_root = tmp_path / "pb-queue"
    assert sl.withdrawal_covers(queue_root, KEY, 100.0) is None
    assert sl.supersede_withdrawal(queue_root, KEY, 200.0) is None
    _, filed = sl.publish_withdrawal(
        queue_root=queue_root, action_key=KEY, reason="mine", by="rob@sparky",
    )
    assert filed["withdrawn_by"] == "rob@sparky"
