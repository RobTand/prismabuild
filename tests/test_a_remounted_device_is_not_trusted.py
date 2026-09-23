"""A device number remounted as another filesystem is judged again (#992).

``stage_move._trusted_directory_stamp`` trusts a directory's stamp only on a
filesystem whose directory times come from this kernel's clock, and it finds
the type by the device's ``major:minor`` in ``/proc/self/mountinfo``.  The
answer used to be remembered per device for the life of the process.
Anonymous device numbers (``0:N``) are shared by ZFS datasets, tmpfs, NFS,
overlay and FUSE mounts and are handed out again after an unmount, so a
long-lived tier loop that had seen ``0:N`` as ``zfs`` would go on trusting
stamps on a later NFS or FUSE mount given the same number.

The table is now read again whenever the kernel says the mount table
changed: ``/proc/self/mountinfo`` polls ``POLLPRI`` after any mount or
unmount in the namespace (``stage_move._mount_table_changed``).  Mounting
needs privileges this test does not have (unprivileged user namespaces are
restricted on the Sparks), so the test stands in for the table's contents
and for that signal, and drives the stamp through them.

Nothing here touches the live queue, a real stage root or a real mount.
"""

from __future__ import annotations

import io
import os
from pathlib import Path
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

import stage_move  # noqa: E402

MOUNTINFO = "/proc/self/mountinfo"


def _table(device: int, fstype: str) -> str:
    """One ``mountinfo`` line naming ``device`` as ``fstype``."""

    return (f"36 25 {os.major(device)}:{os.minor(device)} / /stand-in "
            f"rw,relatime shared:1 - {fstype} stand-in rw\n")


class _MountTable:
    """What ``/proc/self/mountinfo`` says, and whether it changed since asked."""

    def __init__(self, text: str) -> None:
        self.text = text
        self.changes = 0

    def remount(self, text: str) -> None:
        self.text = text
        self.changes += 1

    def changed(self) -> bool:
        pending, self.changes = self.changes > 0, 0
        return pending


@pytest.fixture()
def table(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> _MountTable:
    device = os.stat(tmp_path).st_dev
    stand_in = _MountTable(_table(device, "zfs"))
    real_open = open

    def opening(file, *args, **kwargs):  # type: ignore[no-untyped-def]
        if os.fspath(file) == MOUNTINFO:
            return io.StringIO(stand_in.text)
        return real_open(file, *args, **kwargs)

    monkeypatch.setattr(stage_move, "open", opening, raising=False)
    monkeypatch.setattr(stage_move, "_mount_table_changed", stand_in.changed,
                        raising=False)
    monkeypatch.setattr(stage_move, "_filesystem_types", {})
    return stand_in


def _settled_directory(tmp_path: Path) -> Path:
    """A directory whose stamp predates the coarse clock (two ticks later)."""

    directory = tmp_path / "kept"
    directory.mkdir()
    time.sleep(2 * time.clock_getres(5))
    return directory


def test_a_device_remounted_as_another_type_is_not_trusted(
        table: _MountTable, tmp_path: Path) -> None:
    directory = _settled_directory(tmp_path)
    assert stage_move._trusted_directory_stamp(directory) is not None
    # The same device number, now an NFS mount: the kernel signals the
    # change, and the next stamp is judged on the table as it now reads.
    table.remount(_table(os.stat(tmp_path).st_dev, "nfs4"))
    assert stage_move._trusted_directory_stamp(directory) is None


def test_an_unchanged_table_is_not_read_again(
        table: _MountTable, tmp_path: Path) -> None:
    """Without the signal the remembered type stands: no read per stamp."""

    directory = _settled_directory(tmp_path)
    assert stage_move._trusted_directory_stamp(directory) is not None
    table.text = _table(os.stat(tmp_path).st_dev, "nfs4")
    assert stage_move._trusted_directory_stamp(directory) is not None


def test_the_kernel_signal_is_read_without_raising() -> None:
    """The real probe polls this process's ``mountinfo`` and answers a bool."""

    assert isinstance(stage_move._mount_table_changed(), bool)
    assert isinstance(stage_move._mount_table_changed(), bool)
