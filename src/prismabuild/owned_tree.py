"""Owner locks, retention and removal for the trees PrismaBuild creates.

A tree PB makes for one owner (a pbtest attempt's base temp, a pbrun
snapshot) carries an owner lock: a regular file the owner holds ``flock``-ed
for its whole life. The kernel drops the lock with the owner's last
descriptor, even under SIGKILL, so a reaper that takes it without waiting
knows the owner is gone. Age alone proves nothing.

The lock also keeps the retention clock. The owner writes a header naming
this format and host, then ``running``. An owner that ends but keeps its
tree, or the first reaper to find the owner gone, cuts ``running`` off; that
starts the clock, and the tree goes ``RETENTION_S`` later. A name without
such a lock is never touched. Stdlib only: pbtest ships this source to the
shard's interpreter (see ``docs/design.md``, "Owned trees").
"""
from __future__ import annotations

import errno
import fcntl
import os
import random
import socket
import stat
import time

#: How long a failed or dead owner's tree stays once its clock starts.
RETENTION_S = 24 * 3600
#: The lock inside an owned directory, or the suffix of one kept beside a
#: tree that must not carry it (such as a tree that actions hash).
LOCK_NAME = "pb-owner.lock"
LOCK_SUFFIX = ".lock"
#: The most one reaping call inspects and removes.
INSPECT_PER_CALL = 256
REMOVE_PER_CALL = 4

_RUNNING = b"running\n"
_DIRECTORY = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC


def _owner_header() -> bytes:
    return f"prismabuild owner lock v1 host={socket.gethostname()}\n".encode()


def take_owner_lock(dir_fd: int, name: str) -> int:
    """Hold lock ``name`` under ``dir_fd`` for this process's life; its fd.

    A reaper's probe holds a fresh lock for an instant, so a busy lock is
    retried for a second before it is refused as a live owner's.
    """
    fd = os.open(name, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC,
                 0o600, dir_fd=dir_fd)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise OSError(errno.EINVAL, f"{name} is not a private lock file")
        for _ in range(100):
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                time.sleep(0.01)
        else:
            raise BlockingIOError(errno.EWOULDBLOCK, f"{name} has a live owner")
        os.ftruncate(fd, 0)
        os.pwrite(fd, _owner_header() + _RUNNING, 0)
    except BaseException:
        os.close(fd)
        raise
    return fd


def start_retention(fd: int) -> None:
    """Start the held lock's clock: its tree goes ``RETENTION_S`` from now."""
    os.ftruncate(fd, len(_owner_header()))
    os.utime(fd)


def release_owned_tree(dir_fd: int, name: str, lock: int, *, remove: bool) -> None:
    """End the owner of directory ``name``, whose lock is inside it.

    ``remove`` takes the tree now, lock last. Otherwise, or when removal
    leaves anything (raised after), the clock starts. The lock is released.
    """
    try:
        if remove:
            try:
                remove_tree(dir_fd, name, keep=LOCK_NAME)
                return
            except OSError:
                start_retention(lock)
                raise
        start_retention(lock)
    finally:
        os.close(lock)


def drop_owner_lock(dir_fd: int, name: str, lock: int) -> None:
    """Release the lock beside tree ``name`` once its owner has cleaned up.

    A removed tree takes its lock along; a tree still there starts its clock.
    """
    try:
        try:
            os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
        except FileNotFoundError:
            named, held = os.stat(name + LOCK_SUFFIX, dir_fd=dir_fd,
                                  follow_symlinks=False), os.fstat(lock)
            if (named.st_dev, named.st_ino) == (held.st_dev, held.st_ino):
                os.unlink(name + LOCK_SUFFIX, dir_fd=dir_fd)
            return
        start_retention(lock)
    finally:
        os.close(lock)


def remove_tree(dir_fd: int, name: str, *, keep: str | None = None,
                expect: tuple[int, int] | None = None) -> None:
    """Remove directory ``name`` under ``dir_fd`` and all below it, or raise.

    Never follows a symlink (it is unlinked, never entered) and never leaves
    the parent's device and mount. Owner rwx is restored to each directory
    this user owns before it is emptied, as pytest's ``rm_rf`` does, so a
    read-only (0555) directory does not stop it. ``keep`` (an owner lock) is
    removed last, only once nothing else is left; ``expect`` is the tree's
    required ``(st_dev, st_ino)``.
    """
    device, mount = os.fstat(dir_fd).st_dev, _mount_id_of(dir_fd)
    top = _open_for_removal(dir_fd, name, device, mount)
    if top < 0:
        raise OSError(errno.EXDEV, f"{name} is not on its parent's mount")
    try:
        info = os.fstat(top)
        if expect is not None and (info.st_dev, info.st_ino) != expect:
            raise OSError(errno.ESTALE, f"{name} was replaced")
        left = _empty_tree(top, device, mount, keep)
        if not left and keep is not None:
            try:
                os.unlink(keep, dir_fd=top)
            except FileNotFoundError:
                pass
    except RecursionError:
        raise OSError(errno.ELOOP, f"{name} is nested too deeply") from None
    finally:
        os.close(top)
    if left:
        raise OSError(errno.ENOTEMPTY, f"{name}: {left} entries could not be removed")
    os.rmdir(name, dir_fd=dir_fd)


def _mount_id_of(fd: int) -> int:
    with open(f"/proc/self/fdinfo/{fd}", "rb") as info:
        for line in info:
            if line.startswith(b"mnt_id:"):
                return int(line.split(b":", 1)[1])
    raise OSError(errno.ENOTSUP, "descriptor mount identity unknown")


def _open_for_removal(parent: int, name: str, device: int, mount: int) -> int:
    """Open directory ``name`` to empty it, or -1 when it is off this mount.

    ``O_PATH`` needs no permission and follows no link, so device and mount
    are proved before anything changes; the same handle is then reopened.
    """
    path = os.open(name, os.O_PATH | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                   dir_fd=parent)
    try:
        info = os.fstat(path)
        if info.st_dev != device or _mount_id_of(path) != mount:
            return -1
        handle = f"/proc/self/fd/{path}"
        if info.st_mode & 0o700 != 0o700 and info.st_uid == os.geteuid():
            os.chmod(handle, stat.S_IMODE(info.st_mode) | 0o700)
        return os.open(handle, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    finally:
        os.close(path)


def _empty_tree(fd: int, device: int, mount: int, keep: str | None = None) -> int:
    """Remove everything in the directory open at ``fd``; count what stays."""
    left = 0
    for name in os.listdir(fd):
        if name == keep:
            continue
        try:
            if not stat.S_ISDIR(os.stat(name, dir_fd=fd, follow_symlinks=False).st_mode):
                os.unlink(name, dir_fd=fd)
                continue
            child = _open_for_removal(fd, name, device, mount)
            if child < 0:
                left += 1
                continue
            try:
                left += _empty_tree(child, device, mount)
            finally:
                os.close(child)
            os.rmdir(name, dir_fd=fd)
        except FileNotFoundError:
            continue
        except OSError:
            left += 1
    return left


def reap_dead_trees(dir_fd: int, pattern, *, beside: bool = False, inner=None,
                    retention_s: float = RETENTION_S,
                    now: float | None = None) -> list[str]:
    """Remove the expired trees of dead owners under ``dir_fd``; name them.

    Only names ``pattern`` matches exactly are looked at, and only a tree
    with a valid lock goes: ``LOCK_NAME`` inside it, or with ``beside`` the
    sibling ``<name>.lock`` (which goes alone if its tree is gone). With
    ``inner`` each match holds trees ``inner`` names (a sealed basetemp's
    ``<key>/<attempt>``) and is removed once this call empties it. The tree
    must be a directory this user owns on ``dir_fd``'s mount; the lock a free
    single-link file this user owns whose header names this host and whose
    clock has run ``retention_s``. Bounded per call, in random order.
    """
    return _reap_trees(dir_fd, pattern, inner, beside, retention_s,
                       time.time() if now is None else now,
                       [INSPECT_PER_CALL, REMOVE_PER_CALL])


def _current_names(dir_fd: int) -> list[str]:
    """The names in ``dir_fd`` now, read through a fresh open of it.

    btrfs lists, through one open directory, only the entries that existed
    when it was opened or last rewound (seen on dl380g10, Linux 7.0), so a
    caller's long-held descriptor would hide every tree made since.
    """
    fresh = os.open(".", _DIRECTORY, dir_fd=dir_fd)
    try:
        return os.listdir(fresh)
    finally:
        os.close(fresh)


def _reap_trees(dir_fd, pattern, inner, beside, retention_s, now, budget):
    device, mount = os.fstat(dir_fd).st_dev, _mount_id_of(dir_fd)
    if beside and inner is None:
        names = [name[:-len(LOCK_SUFFIX)] for name in _current_names(dir_fd)
                 if name.endswith(LOCK_SUFFIX)
                 and pattern.fullmatch(name[:-len(LOCK_SUFFIX)])]
    else:
        names = [name for name in _current_names(dir_fd) if pattern.fullmatch(name)]
    random.shuffle(names)
    removed = []
    for name in names:
        if budget[0] <= 0 or budget[1] <= 0:
            break
        try:
            if inner is None:
                budget[0] -= 1
                if _reap_tree(dir_fd, name, beside, device, mount, retention_s, now):
                    budget[1] -= 1
                    removed.append(name)
                continue
            child = _open_owned_dir(dir_fd, name, device, mount)
            if child < 0:
                continue
            try:
                gone = _reap_trees(child, inner, None, beside, retention_s, now, budget)
            finally:
                os.close(child)
            removed += [f"{name}/{tree}" for tree in gone]
            if gone:
                os.rmdir(name, dir_fd=dir_fd)
        except OSError:
            continue
    return removed


def _open_owned_dir(parent: int, name: str, device: int, mount: int) -> int:
    """``name`` opened if it is a directory this user owns on this mount."""
    try:
        fd = os.open(name, _DIRECTORY, dir_fd=parent)
    except OSError:
        return -1
    try:
        info = os.fstat(fd)
        if (info.st_uid == os.geteuid() and info.st_dev == device
                and _mount_id_of(fd) == mount):
            return fd
    except OSError:
        pass
    os.close(fd)
    return -1


def _reap_tree(parent, name, beside, device, mount, retention_s, now) -> bool:
    """Remove one dead owner's expired tree; ``True`` once it is gone."""
    tree = lock = -1
    try:
        if beside:
            lock_dir, lock_name = parent, name + LOCK_SUFFIX
        else:
            tree = _open_owned_dir(parent, name, device, mount)
            if tree < 0:
                return False
            lock_dir, lock_name = tree, LOCK_NAME
        try:
            lock = os.open(lock_name, os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK
                           | os.O_CLOEXEC, dir_fd=lock_dir)
        except FileNotFoundError:
            return False                          # unmarked: never ours
        held = os.fstat(lock)
        if (not stat.S_ISREG(held.st_mode) or held.st_nlink != 1
                or held.st_uid != os.geteuid() or held.st_dev != device):
            return False
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False                          # its owner is alive
        named = os.stat(lock_name, dir_fd=lock_dir, follow_symlinks=False)
        body, header = os.pread(lock, 4096, 0), _owner_header()
        if ((named.st_dev, named.st_ino) != (held.st_dev, held.st_ino)
                or body not in (header, header + _RUNNING)):
            return False                          # moved, foreign or another host
        if beside:
            tree = _open_owned_dir(parent, name, device, mount)
            if tree < 0:
                try:
                    os.stat(name, dir_fd=parent, follow_symlinks=False)
                except FileNotFoundError:
                    os.unlink(lock_name, dir_fd=parent)   # its tree is gone
                    return True
                return False
        if body != header:
            start_retention(lock)                 # first seen dead
            return False
        if now - os.fstat(lock).st_mtime < retention_s:
            return False
        info = os.fstat(tree)
        try:
            remove_tree(parent, name, keep=None if beside else LOCK_NAME,
                        expect=(info.st_dev, info.st_ino))
        except OSError:
            os.utime(lock)                        # retry a window from now
            return False
        if beside:
            os.unlink(lock_name, dir_fd=parent)
        return True
    finally:
        for fd in (lock, tree):
            if fd >= 0:
                os.close(fd)
