"""Permanent-inode POSIX exclusion shared by NFS clients and local readers."""
from contextlib import contextmanager
import errno
import fcntl
import os
from pathlib import Path
import stat
import threading


_threads = {}
_registry = threading.Lock()
_owned = threading.local()


def _after_fork():
    global _threads, _registry, _owned
    _threads = {}
    _registry = threading.Lock()
    _owned = threading.local()


os.register_at_fork(after_in_child=_after_fork)


@contextmanager
def held(path: Path, *, blocking: bool = True):
    """Yield acquisition status, with same-thread nesting and crash release.

    POSIX locks are process-scoped: serialize threads before opening the inode
    and retain that one descriptor through nested calls. Closing a second
    descriptor for the inode would otherwise release this process's lock.
    The inode is never removed. NFS mounts using local-only locks are unsupported.
    """
    path = Path(path)
    path = path.parent.resolve() / path.name
    key = str(path)
    with _registry:
        mutex = _threads.setdefault(key, threading.RLock())
    if not mutex.acquire(blocking=blocking):
        yield False
        return
    descriptor = None
    acquired = False
    owners = getattr(_owned, "paths", None)
    if owners is None:
        owners = _owned.paths = set()
    try:
        if key in owners:
            yield True
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        observed = os.fstat(descriptor)
        if not stat.S_ISREG(observed.st_mode) or observed.st_nlink != 1:
            raise OSError(f"unsafe POSIX transition lock: {path}")
        try:
            fcntl.lockf(descriptor, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
        except OSError as exc:
            if not blocking and exc.errno in (errno.EACCES, errno.EAGAIN):
                yield False
                return
            raise
        acquired = True
        owners.add(key)
        yield True
    finally:
        try:
            if acquired:
                owners.remove(key)
                fcntl.lockf(descriptor, fcntl.LOCK_UN)
        finally:
            try:
                if descriptor is not None:
                    os.close(descriptor)
            finally:
                mutex.release()
