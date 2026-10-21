"""A remembered device type is fenced once per lookup (pb-tierscan).

``kept_reads._filesystem_type`` answers every directory and file trust check
the tier loop makes, about 90,000 a cycle on dl380g10 (2026-10-21).  Each
answer checked the mount namespace and polled the mount watch before the
memo lookup and again after it, and read the namespace link both times: the
loop's 6,600 ``readlink`` calls a second.

The contract is one fence after the caller's ``stat`` (``docs/design.md``,
"re-checked on every reuse").  A remembered table's lookup is a dictionary
read under the lock, with no IO a mount event could race, so the fence
before it is the whole proof.  The link names the inode the namespace
``stat`` returned, so it is read again only when that inode changes.

The table and the kernel signal are stood in, as in
``test_a_remounted_device_is_not_trusted``; the namespace is this process's
own, observed through counting wrappers.  Nothing here mounts anything.
"""

from __future__ import annotations

import io
import os
from pathlib import Path

import pytest

from prismabuild import kept_reads

MOUNTINFO = "/proc/self/mountinfo"
NAMESPACE = "/proc/self/ns/mnt"
LOOKUPS = 10


def _table(device: int, fstype: str) -> str:
    """One ``mountinfo`` line naming ``device`` as ``fstype``."""

    return (f"36 25 {os.major(device)}:{os.minor(device)} / /stand-in "
            f"rw,relatime shared:1 - {fstype} stand-in rw\n")


class _Fence:
    """What the lookups asked the kernel: namespace stats, links, polls."""

    def __init__(self, text: str) -> None:
        self.text = text
        self.stats = 0
        self.links = 0
        self.polls = 0
        self.tables = 0
        self.signal = False
        self.inode_shift = 0

    def changed(self) -> bool:
        self.polls += 1
        pending, self.signal = self.signal, False
        return pending

    def reset(self) -> None:
        self.stats = self.links = self.polls = self.tables = 0


def _named(path: object) -> str | None:
    return None if isinstance(path, int) else os.fsdecode(os.fspath(path))


@pytest.fixture()
def fence(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> _Fence:
    counted = _Fence(_table(os.stat(tmp_path).st_dev, "zfs"))
    real_open, real_stat, real_readlink = open, os.stat, os.readlink

    def opening(file, *args, **kwargs):  # type: ignore[no-untyped-def]
        if _named(file) == MOUNTINFO:
            counted.tables += 1
            mode = args[0] if args else kwargs.get("mode", "r")
            return (io.BytesIO(counted.text.encode()) if "b" in mode
                    else io.StringIO(counted.text))
        return real_open(file, *args, **kwargs)

    def stat(path, *args, **kwargs):  # type: ignore[no-untyped-def]
        info = real_stat(path, *args, **kwargs)
        if _named(path) != NAMESPACE:
            return info
        counted.stats += 1
        if not counted.inode_shift:
            return info
        fields = list(info[:10])
        fields[1] = info.st_ino + counted.inode_shift
        return os.stat_result(fields)

    def readlink(path, *args, **kwargs):  # type: ignore[no-untyped-def]
        if _named(path) == NAMESPACE:
            counted.links += 1
        return real_readlink(path, *args, **kwargs)

    monkeypatch.setattr(kept_reads, "open", opening, raising=False)
    monkeypatch.setattr(kept_reads, "_mount_table_changed", counted.changed)
    monkeypatch.setattr(kept_reads, "_filesystem_types", {})
    monkeypatch.setattr(kept_reads, "_mount_namespace", None)
    monkeypatch.setattr(kept_reads, "_mount_watch", None)
    monkeypatch.setattr(os, "stat", stat)
    monkeypatch.setattr(os, "readlink", readlink)
    return counted


def _directory(tmp_path: Path) -> Path:
    directory = tmp_path / "kept"
    directory.mkdir()
    return directory


def test_a_remembered_device_is_fenced_once_per_lookup(
        fence: _Fence, tmp_path: Path) -> None:
    directory = _directory(tmp_path)
    trust = kept_reads.filesystem_trust
    assert trust.directory_current(directory) is not None
    assert fence.tables == 1
    fence.reset()
    for _ in range(LOOKUPS):
        assert trust.directory_current(directory) is not None
    assert {"namespace stats": fence.stats, "namespace links": fence.links,
            "watch polls": fence.polls, "table reads": fence.tables} == {
        "namespace stats": LOOKUPS, "namespace links": 0,
        "watch polls": LOOKUPS, "table reads": 0}


def test_a_signalled_mount_is_still_seen_by_the_next_lookup(
        fence: _Fence, tmp_path: Path) -> None:
    directory = _directory(tmp_path)
    trust = kept_reads.filesystem_trust
    assert trust.directory_current(directory) is not None
    fence.text = _table(os.stat(tmp_path).st_dev, "nfs4")
    assert trust.directory_current(directory) is not None
    fence.signal = True
    assert trust.directory_current(directory) is None
    assert fence.tables == 2


def test_a_namespace_inode_it_has_not_accepted_is_read_again(
        fence: _Fence, tmp_path: Path) -> None:
    directory = _directory(tmp_path)
    trust = kept_reads.filesystem_trust
    assert trust.directory_current(directory) is not None
    fence.reset()
    # Another inode: its link is read, does not name it, and nothing is
    # trusted until the namespace reads consistently again.
    fence.inode_shift = 1
    assert trust.directory_current(directory) is None
    assert fence.links > 0
    fence.inode_shift = 0
    assert trust.directory_current(directory) is not None
