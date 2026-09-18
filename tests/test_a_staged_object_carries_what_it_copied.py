"""A stage object says which version of its source it holds, and no more.

The key names a path and a byte range, which is what a consumer looks an
object up by.  It says nothing about *which* version of that path was copied,
and the manifests this tier exists for carry ``sha256: null`` on every entry
by design, so a source rewritten to the same length between the copy and the
read would be shadowed by a stale object nothing could detect.  The reader
already stats the source; the object carries what that stat saw.

And what a dead process left behind is removed, once, by the next one.
"""
from __future__ import annotations

import errno
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prewarm_fixture import Fleet, StagePool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import prewarm_loop  # noqa: E402


class RecordsXattr:
    """The real ``os``, watching what the stage writes beside its objects."""

    def __init__(self, real) -> None:
        self._real = real
        self.calls: list[tuple[str, bytes]] = []

    def __getattr__(self, name):
        return getattr(self._real, name)

    def setxattr(self, target, name, value, *args, **kwargs):
        self.calls.append((name, value))
        return self._real.setxattr(target, name, value, *args, **kwargs)


class RefusesXattr:
    def __init__(self, real) -> None:
        self._real = real

    def __getattr__(self, name):
        return getattr(self._real, name)

    def setxattr(self, *args, **kwargs):  # noqa: ARG002
        raise OSError(errno.EOPNOTSUPP, "Operation not supported")


def test_a_committed_object_carries_its_sources_identity(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fleet = Fleet(tmp_path)
    fleet.action("row", [fleet.file("row.pt", 8192)])
    source = tmp_path / "shared" / "row.pt"
    stage = StagePool(tmp_path)
    stage.install(monkeypatch)
    watcher = RecordsXattr(os)
    monkeypatch.setattr(prewarm_loop, "os", watcher)

    event = fleet.cycle(fleet.args(stage=True, stage_free_floor_bytes=0))

    assert stage.objects() == ["row.pt.pbstage@0+8192"]
    seen = os.stat(source)
    expected = f"{seen.st_size}:{seen.st_mtime_ns}:{seen.st_ino}".encode()
    assert watcher.calls == [(prewarm_loop.STAGE_SOURCE_XATTR, expected)]
    assert event["stage"]["object_identity"]["xattr"] == \
        prewarm_loop.STAGE_SOURCE_XATTR
    if not event["stage"]["identity_unrecorded"]:
        # This filesystem kept it, so a validator can read it back off the
        # object rather than from anything this process still holds.
        staged = stage.mount / "row.pt.pbstage@0+8192"
        assert os.getxattr(str(staged),
                           prewarm_loop.STAGE_SOURCE_XATTR) == expected


def test_a_filesystem_that_refuses_the_attribute_still_stages_and_counts_it(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Identity is what makes a stale object detectable, not what makes the
    copy correct.  A tier that cannot hold it stages anyway and says how
    many objects went out undated."""

    fleet = Fleet(tmp_path)
    fleet.action("row", [fleet.file("row.pt", 8192)])
    stage = StagePool(tmp_path)
    stage.install(monkeypatch)
    monkeypatch.setattr(prewarm_loop, "os", RefusesXattr(os))

    event = fleet.cycle(fleet.args(stage=True, stage_free_floor_bytes=0))

    assert stage.objects() == ["row.pt.pbstage@0+8192"]
    assert event["stage"]["staged_bytes"] == 8192
    assert event["stage"]["identity_unrecorded"] == 1
    assert any("identity not recorded" in message
               for message in event["stage"]["errors"])


def test_a_temporary_from_a_process_that_died_is_reaped_once(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A SIGKILL mid-window leaves one temporary per in-flight reader.

    ``release`` unlinks by key and the sweep is driven from receipts, so
    neither can see them; they only shrink the tier.  The next process
    removes them, once, and only names its own writer builds.
    """

    fleet = Fleet(tmp_path)
    fleet.action("row", [fleet.file("row.pt", 8192)])
    stage = StagePool(tmp_path)
    stage.install(monkeypatch)
    stale = stage.mount / "row.pt.pbstage@0+4096.999999.1.tmp"
    stale.write_bytes(b"\0" * 4096)
    mine = stage.mount / f"row.pt.pbstage@0+2048.{os.getpid()}.1.tmp"
    mine.write_bytes(b"\0" * 2048)
    other = stage.mount / "somebody-elses.tmp"
    other.write_bytes(b"x")

    event = fleet.cycle(fleet.args(stage=True, stage_free_floor_bytes=0))

    assert event["stage"]["reaped_tmp_entries"] == 1
    assert event["stage"]["reaped_tmp_bytes"] == 4096
    assert not stale.exists()
    # A temporary carrying this pid belongs to a copy in flight, and a name
    # this loop's writer never builds is not this loop's to remove.
    assert mine.exists()
    assert other.exists()

    # Once per process, not once per cycle.
    assert fleet.cycle(fleet.args(stage=True, stage_free_floor_bytes=0)
                       )["stage"]["reaped_tmp_entries"] == 0


def test_a_dry_run_reaps_nothing(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fleet = Fleet(tmp_path)
    fleet.action("row", [fleet.file("row.pt", 8192)])
    stage = StagePool(tmp_path)
    stage.install(monkeypatch)
    stale = stage.mount / "row.pt.pbstage@0+4096.999999.1.tmp"
    stale.write_bytes(b"\0" * 4096)

    event = fleet.cycle(fleet.args(stage=True, stage_free_floor_bytes=0,
                                   dry_run=True))

    assert event["stage"]["reaped_tmp_entries"] == 0
    assert stale.exists()
