"""POSIX exclusion shared by NFS clients and local readers, with safe retirement.

A lock is a regular file locked with ``lockf``.  A lock file may be removed
only by :func:`retire`, and only under the retirement protocol below, which
is what lets ``pb_gc`` bound ``transition-locks/`` (#995) without letting two
holders in.

**The protocol.**

1. *Retire under the lock.*  :func:`retire` takes the lock (non-blocking; a
   held lock is left alone), writes :data:`TOMBSTONE` into the file,
   ``fsync``s it and unlinks the name, all before it releases.  A tombstoned
   inode is never written to again or re-linked.
2. *Check after locking.*  :func:`held` reads the inode it locked
   (``fstat``) *after* ``lockf`` returns.  An inode with no link, or whose
   bytes are :data:`TOMBSTONE`, was retired while this caller waited; the
   caller unlocks, closes and opens the name again.

**Why two holders cannot both proceed.**  Say ``H`` holds inode ``I`` and has
passed step 2.  ``H`` opened ``I`` by name, so the name named ``I`` then.  A
name changes only when a retirer unlinks it, and a retirer unlinks only an
inode it holds locked and has already tombstoned (step 1).  A tombstone
written before ``H`` locked would have failed ``H``'s check.  One written
after would need the lock ``H`` holds.  So the name names ``I`` for all of
``H``'s hold.  A second holder ``H'`` that passed step 2 on inode ``J`` also
has the name naming ``J`` for its whole hold.  If the holds overlapped, then
``I == J``, and ``lockf`` excludes two holders of one inode.

**The NFS leg.**  Step 2 relies on the client seeing the retirer's write
after taking the lock.  Linux NFS makes a lock acquisition a cache coherency
point: ``do_setlk`` flushes and then drops the inode's cached attributes and
data unless the client holds a read delegation, and a delegation is recalled
before another client may write.  The retirer's ``fsync`` (and the flush at
its unlock) puts the tombstone on the server before the lock is granted
elsewhere.  The mount must use server locks (``local_lock=none``), as before.
``O_CREAT`` opens go to the server by name (NFSv4 ``OPEN``), so an open after
a retirement reaches the new inode, or creates one.

**The ordering rule.**  The argument needs every process that takes a lock
under a retired directory to run step 2.  A process running code from before
step 2 could proceed on a retired inode.  So a retirer runs only after every
lock taker on the fleet runs this module (``pb_gc --all-lock-takers-verify``
states that acknowledgement).
"""
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


#: The bytes a retired lock file holds.  A live lock file is empty: nothing
#: but :func:`retire` ever writes to one.
TOMBSTONE = b"prismabuild posix_lock retired\n"


def _lockf(descriptor: int, blocking: bool) -> None:
    """Take the exclusive lock; the one call the retirement tests intercept."""

    fcntl.lockf(descriptor, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))


def _unsafe(observed: os.stat_result, path: Path) -> None:
    if not stat.S_ISREG(observed.st_mode) or observed.st_nlink > 1:
        raise OSError(f"unsafe POSIX transition lock: {path}")


def _retired(descriptor: int, path: Path) -> bool:
    """Whether the locked inode was retired before this caller got the lock.

    Read after ``lockf`` returns, which on NFS drops the client's cached
    attributes, so the size and link count are the server's.  The contents
    are read only when the size says a tombstone could be there.
    """

    observed = os.fstat(descriptor)
    _unsafe(observed, path)
    if observed.st_nlink == 0:
        return True
    if observed.st_size != len(TOMBSTONE):
        return False
    return os.pread(descriptor, len(TOMBSTONE), 0) == TOMBSTONE


@contextmanager
def held(path: Path, *, blocking: bool = True):
    """Yield acquisition status, with same-thread nesting and crash release.

    POSIX locks are process-scoped: serialize threads before opening the inode
    and retain that one descriptor through nested calls. Closing a second
    descriptor for the inode would otherwise release this process's lock.
    NFS mounts using local-only locks are unsupported.

    An inode :func:`retire` retired while this caller waited for it is let
    go, and the name is opened again (step 2 of the module's protocol).  A
    non-blocking caller that meets a retired inode opens the name again too:
    a retired inode is not contention.
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
        while True:
            descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
            _unsafe(os.fstat(descriptor), path)
            try:
                _lockf(descriptor, blocking)
            except OSError as exc:
                if not blocking and exc.errno in (errno.EACCES, errno.EAGAIN):
                    yield False
                    return
                raise
            acquired = True
            if not _retired(descriptor, path):
                break
            acquired = False
            try:
                fcntl.lockf(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)
                descriptor = None
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


def retire(path: Path) -> str:
    """Remove one lock file that nobody holds; ``""`` or why it was kept.

    Step 1 of the module's protocol: the lock is taken non-blocking, the
    locked inode is checked to be the one the name still names, and it is
    tombstoned, synced and unlinked before the lock is released.  A held
    lock, a missing file, a file that is not an empty regular single-link
    file, and a name that moved are all kept, with the reason.
    """

    path = Path(path)
    path = path.parent.resolve() / path.name
    key = str(path)
    with _registry:
        mutex = _threads.setdefault(key, threading.RLock())
    if not mutex.acquire(blocking=False):
        return "held by another thread of this process"
    try:
        if key in (getattr(_owned, "paths", None) or ()):
            return "held by this thread"
        try:
            directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        except OSError as exc:
            return f"cannot open the lock directory: {exc}"
        try:
            try:
                descriptor = os.open(path.name, os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK,
                                     dir_fd=directory)
            except FileNotFoundError:
                return "absent"
            except OSError as exc:
                return f"cannot open: {exc}"
            try:
                observed = os.fstat(descriptor)
                if not stat.S_ISREG(observed.st_mode) or observed.st_nlink != 1:
                    return "not a single-link regular file"
                try:
                    _lockf(descriptor, False)
                except OSError as exc:
                    if exc.errno in (errno.EACCES, errno.EAGAIN):
                        return "held"
                    return f"cannot lock: {exc}"
                try:
                    observed = os.fstat(descriptor)
                    if observed.st_nlink != 1 or observed.st_size != 0:
                        return "not an empty live lock file"
                    named = os.stat(path.name, dir_fd=directory, follow_symlinks=False)
                    if (named.st_dev, named.st_ino) != (observed.st_dev, observed.st_ino):
                        return "the name moved to another inode"
                    os.pwrite(descriptor, TOMBSTONE, 0)
                    os.fsync(descriptor)
                    os.unlink(path.name, dir_fd=directory)
                    os.fsync(directory)
                except OSError as exc:
                    return f"cannot retire: {exc}"
                finally:
                    fcntl.lockf(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)
        finally:
            os.close(directory)
    finally:
        mutex.release()
    return ""
