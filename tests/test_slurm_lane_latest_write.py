"""``latest.json`` is written by rename, and the temp name is unique per writer.

Two boxes submitting one action key write into one lane directory on the shared
mount, and a pid is unique only within a box.  The temp name used to be
``.latest.json.<pid>.tmp``, so two writers that shared a pid shared the file:
one renamed it away, and the other's ``os.replace`` raised ``FileNotFoundError``
after its ``sbatch`` had already been accepted.  Nothing in ``pbrun`` catches
that, so the submitter died with a queued job and no ``latest.json`` entry, and
``--withdraw`` had nothing to resolve.

Driven deterministically rather than with threads: ``os.replace`` is patched so
the second writer runs to completion inside the first one's rename.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))

from prismabuild import slurm_lane as sl  # noqa: E402


def test_two_writers_sharing_a_pid_both_publish_a_whole_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The loser of the race must not lose its rename to the winner's temp."""

    monkeypatch.setattr(os, "getpid", lambda: 4242)
    path = tmp_path / "lane" / "latest.json"
    first = {"schema": sl.SUBMISSION_SCHEMA_V1, "job_id": "1001"}
    second = {"schema": sl.SUBMISSION_SCHEMA_V1, "job_id": "1002"}

    real_replace = os.replace
    reentered = False

    def replace(source, destination):
        nonlocal reentered
        if not reentered:
            # The other box, inside this box's rename: it writes its own temp
            # file and renames it into place before this one lands.
            reentered = True
            sl._write_latest(path, second)
        return real_replace(source, destination)

    monkeypatch.setattr(os, "replace", replace)
    sl._write_latest(path, first)

    assert reentered, "the interleaved writer never ran"
    landed = json.loads(path.read_text(encoding="utf-8"))
    assert landed in (first, second)
    # And no temp file is left behind for the next reader of this directory to
    # trip over.
    assert [p.name for p in path.parent.iterdir()] == ["latest.json"]


def test_the_written_bytes_reach_the_disk_before_the_rename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A rename that publishes unflushed bytes publishes a hole after a crash.

    The lane's records live on NFS and are read by another box; the pool's own
    writer fsyncs before it renames and this one claimed to do what that one
    does.
    """

    synced: list[int] = []
    real_fsync = os.fsync
    monkeypatch.setattr(
        os, "fsync", lambda fd: (synced.append(fd), real_fsync(fd))[1]
    )
    path = tmp_path / "lane" / "latest.json"
    sl._write_latest(path, {"schema": sl.SUBMISSION_SCHEMA_V1, "job_id": "7"})

    assert synced, "the temp file was renamed without being flushed to disk"
    assert json.loads(path.read_text(encoding="utf-8"))["job_id"] == "7"
