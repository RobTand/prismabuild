"""Internal Linux resource invalidation hints, not resource/emptiness proofs.

One producer thread multiplexes inotify, POLLPRI cgroup event files, borrowed
pidfds and a cancellation pipe. No capacity values are sampled or compared.
Consumers must reread their authority after a hint (including coverage loss).
"""
from __future__ import annotations

import ctypes
import errno
import math
import os
from pathlib import Path
import select
import struct
import threading
import time
from collections.abc import Mapping


# Recovery is a single source-wide coverage repair, not a numeric sampler per
# subscription. It also covers unavailable parents, watch limits and overflow.
_RECOVERY_S = 2.0
_MODIFY = 0x00000002
_ATTRIB = 0x00000004
_CLOSE_WRITE = 0x00000008
_MOVED_FROM = 0x00000040
_MOVED_TO = 0x00000080
_CREATE = 0x00000100
_DELETE = 0x00000200
_DELETE_SELF = 0x00000400
_MOVE_SELF = 0x00000800
_UNMOUNT = 0x00002000
_OVERFLOW = 0x00004000
_IGNORED = 0x00008000
_MASK = (_MODIFY | _ATTRIB | _CLOSE_WRITE | _MOVED_FROM | _MOVED_TO |
         _CREATE | _DELETE | _DELETE_SELF | _MOVE_SELF | _UNMOUNT)
_SELF = _DELETE_SELF | _MOVE_SELF | _UNMOUNT | _IGNORED
_HEADER = struct.Struct('iIII')
_CGROUP_EVENTS = {'cgroup.events', 'memory.events', 'memory.events.local',
                  'pids.events', 'pids.events.local'}


class LocalObservationSource:
    """Best-effort watch-before-return, latched path and process invalidations.

    ``wait(cursor, timeout_s)`` returns the current generation and the labels
    invalidated since cursor. Per-label last generations bound storage while
    retaining hints for multiple independent consumers. Timeouts and close do
    not invent a generation; close wakes all waiters without raising. Callers
    own pidfds and must keep them open until close. No process is signalled.

    Paths are absolute lexical names (symlinks are not resolved away). Watches
    cover each ancestor as well as the current target inode, so missing paths,
    atomic replacement, socket unlink/rebind and parent replacement recover.
    The two-second repair pass stats metadata only, never resource numbers.
    Unavailable inotify degrades to metadata hints while pidfds, priority files
    and cancellation remain active. The same pass retries initialization and
    watches; diagnostics report coverage errors until recovery. Unchanged
    coverage errors do not generate repeated hints.
    """

    def __init__(self, *, paths: Mapping[str, Path], pidfds: Mapping[str, int]):
        self._paths = {label: Path(os.path.abspath(path)) for label, path in paths.items()}
        self._pidfds = dict(pidfds)
        self._condition = threading.Condition()
        self._generation = 0
        self._last: dict[str, int] = {}
        self._closed = False
        self._thread = None
        self._inotify = -1
        self._reader = self._writer = -1
        self._poll = select.poll()
        self._watches = {}  # lexical path -> (watch descriptor, inode identity)
        self._routes = {}   # watch descriptor -> [(label, expected child name)]
        self._signatures = {}
        self._priority = {}  # owned fd -> (path, identity)
        self._pid_labels = {}
        for label, fd in self._pidfds.items():
            self._pid_labels.setdefault(fd, set()).add(label)
        self._counts = {'inotify_events': 0, 'priority_events': 0,
                        'pidfd_events': 0, 'overflows': 0, 'repairs': 0}
        self._errors = {}
        self._libc = None
        try:
            self._reader, self._writer = os.pipe2(os.O_NONBLOCK | os.O_CLOEXEC)
            self._poll.register(self._reader, select.POLLIN)
            for fd in self._pid_labels:
                os.fstat(fd)  # Reject an already-invalid borrowed descriptor.
                self._poll.register(fd, select.POLLIN)
            self._repair(initial=True)
            self._thread = threading.Thread(target=self._run,
                                            name='resource-observation', daemon=True)
            self._thread.start()
        except BaseException:
            self._dispose()
            raise

    @property
    def generation(self) -> int:
        with self._condition:
            return self._generation

    @property
    def diagnostics(self) -> dict:
        """Copy of bounded counters and current coverage errors, for operators."""
        with self._condition:
            return {**self._counts, 'generation': self._generation,
                    'closed': self._closed, 'errors': dict(self._errors)}

    def wait(self, after_generation: int, timeout_s: float = 30.0) -> tuple[int, set[str]]:
        if not isinstance(after_generation, int) or after_generation < 0:
            raise ValueError('after_generation must be a nonnegative integer')
        if not math.isfinite(timeout_s) or timeout_s < 0:
            raise ValueError('timeout_s must be finite and nonnegative')
        deadline = time.monotonic() + timeout_s
        with self._condition:
            if after_generation > self._generation:
                raise ValueError('cursor is ahead of this observation source')
            while not self._closed and self._generation <= after_generation:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._condition.wait(min(remaining, threading.TIMEOUT_MAX))
            return self._generation, {label for label, generation in self._last.items()
                                      if generation > after_generation}

    def close(self) -> None:
        with self._condition:
            if not self._closed:
                self._closed = True
                self._condition.notify_all()
                try:
                    os.write(self._writer, b'x')
                except (BlockingIOError, OSError):
                    pass
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join()

    def _emit(self, labels):
        if not labels:
            return
        with self._condition:
            if self._closed:
                return
            self._generation += 1
            for label in labels:
                self._last[label] = self._generation
            self._condition.notify_all()

    def _count(self, name):
        with self._condition:
            self._counts[name] += 1

    def _error(self, key, error=None):
        """Record coverage state; unchanged failures must not wake consumers."""
        with self._condition:
            previous = self._errors.get(key)
            current = None if error is None else str(error)
            if current is None:
                self._errors.pop(key, None)
            else:
                self._errors[key] = current
            return previous != current

    def _lose_inotify(self, error):
        """Drop only owned inotify state, leaving other notification sources live."""
        fd, self._inotify = self._inotify, -1
        if fd >= 0:
            try:
                self._poll.unregister(fd)
            except KeyError:
                pass
            try:
                os.close(fd)
            except OSError:
                # Linux releases the descriptor even when close reports an
                # error; retrying could close a descriptor reused elsewhere.
                pass
        self._watches.clear()
        self._routes.clear()
        with self._condition:
            self._counts['armed_pathnames'] = 0
        if self._error('inotify', error):
            self._emit(set(self._paths))

    def _restore_inotify(self):
        if self._inotify >= 0:
            return
        try:
            if self._libc is None:
                libc = ctypes.CDLL(None, use_errno=True)
                libc.inotify_init1.argtypes = [ctypes.c_int]
                libc.inotify_init1.restype = ctypes.c_int
                libc.inotify_add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p,
                                                   ctypes.c_uint32]
                libc.inotify_add_watch.restype = ctypes.c_int
                libc.inotify_rm_watch.argtypes = [ctypes.c_int, ctypes.c_int]
                libc.inotify_rm_watch.restype = ctypes.c_int
                self._libc = libc
            fd = self._libc.inotify_init1(os.O_NONBLOCK | os.O_CLOEXEC)
            if fd < 0:
                raise OSError(ctypes.get_errno(), 'inotify_init1')
            self._inotify = fd
            self._poll.register(fd, select.POLLIN)
        except (OSError, AttributeError) as error:
            self._lose_inotify(error)
            return

    @staticmethod
    def _stat(path):
        try:
            info = path.stat()
            return (info.st_dev, info.st_ino, info.st_mode, info.st_size,
                    info.st_mtime_ns, info.st_ctime_ns)
        except OSError as error:
            return ('unavailable', error.errno)

    def _repair(self, *, initial=False):
        self._count('repairs')
        self._restore_inotify()
        labels = set()
        desired = {}
        for label, target in self._paths.items():
            signature = self._stat(target)
            if not initial and self._signatures.get(label) != signature:
                labels.add(label)
            self._signatures[label] = signature
            desired.setdefault(target, []).append((label, None))
            child = target
            for parent in target.parents:
                desired.setdefault(parent, []).append((label, os.fsencode(child.name)))
                child = parent

        # Remove obsolete inode watches, retaining shared watch descriptors for
        # aliases. Ancestors stay armed while leaf watches are replaced.
        identities = {path: self._stat(path)[:2] for path in desired}
        old_descriptors = {record[0] for record in self._watches.values()}
        self._watches = {path: record for path, record in self._watches.items()
                         if path in desired and record[1] == identities[path]}
        retained = {record[0] for record in self._watches.values()}
        for wd in old_descriptors - retained:
            self._libc.inotify_rm_watch(self._inotify, wd)
        for path in desired:
            if self._inotify < 0:
                break
            key = 'watch:' + str(path)
            if path in self._watches:
                continue
            wd = self._libc.inotify_add_watch(self._inotify, os.fsencode(path), _MASK)
            if wd < 0:
                number = ctypes.get_errno()
                error = OSError(number, os.strerror(number))
                if number in (errno.EBADF, errno.EINVAL, errno.ENODEV, errno.EIO):
                    self._lose_inotify(error)
                    break
                changed = self._error(key, error)
                if changed and number not in (errno.ENOENT, errno.ENOTDIR):
                    # Lost coverage is a transition hint, not a periodic wake.
                    # Absent names remain covered by their nearest parent.
                    labels.update(label for label, _ in desired[path])
                continue
            if self._error(key):
                labels.update(label for label, _ in desired[path])
            self._watches[path] = (wd, identities[path])
        routes = {}
        for path, (wd, _) in self._watches.items():
            routes.setdefault(wd, []).extend(desired[path])
        self._routes = routes
        # Clear source failure only after the watch pass. Repeated failures
        # during rearming must not masquerade as recovery on every retry.
        if self._inotify >= 0 and self._error('inotify'):
            labels.update(self._paths)
        uncovered = self._repair_priority()
        labels.update(label for label, path in self._paths.items() if path in uncovered)
        with self._condition:
            self._counts['armed_pathnames'] = len(self._watches)
            self._counts['armed_priority_sources'] = len(self._priority)
        if not initial:
            self._emit(labels)

    def _repair_priority(self):
        uncovered = set()
        wanted = {path for path in self._paths.values() if path.name in _CGROUP_EVENTS}
        for fd, (path, identity) in list(self._priority.items()):
            if path not in wanted or self._stat(path)[:2] != identity:
                self._poll.unregister(fd)
                os.close(fd)
                del self._priority[fd]
        present = {path for path, _ in self._priority.values()}
        for path in wanted - present:
            key = 'priority:' + str(path)
            fd = -1
            try:
                fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_CLOEXEC)
                info = os.fstat(fd)
                # Reading acknowledges kernfs's current notification counter;
                # do not parse it or turn its initial value into a change.
                os.read(fd, 65536)
                self._poll.register(fd, select.POLLPRI | select.POLLERR)
                self._priority[fd] = (path, (info.st_dev, info.st_ino))
                if self._error(key):
                    uncovered.add(path)
            except OSError as error:
                if fd >= 0:
                    os.close(fd)
                changed = self._error(key, error)
                if changed and error.errno not in (errno.ENOENT, errno.ENOTDIR):
                    uncovered.add(path)
        return uncovered

    def _inotify_ready(self):
        labels = set()
        # Bound each drain so continuous publishers cannot starve cancellation.
        for _ in range(16):
            try:
                data = os.read(self._inotify, 65536)
            except BlockingIOError:
                break
            if not data:
                raise OSError(errno.ENODEV, 'inotify source reached EOF')
            offset = 0
            while offset + _HEADER.size <= len(data):
                wd, mask, _, length = _HEADER.unpack_from(data, offset)
                name = data[offset + _HEADER.size:offset + _HEADER.size + length].rstrip(b'\0')
                offset += _HEADER.size + length
                self._count('inotify_events')
                if mask & _OVERFLOW:
                    self._count('overflows')
                    labels.update(self._paths)
                else:
                    for label, child in self._routes.get(wd, ()):
                        if child is None or mask & _SELF or name == child:
                            labels.add(label)
                if mask & _IGNORED:
                    self._watches = {path: record for path, record in self._watches.items()
                                     if record[0] != wd}
        if labels:
            self._repair()
            self._emit(labels)

    def _priority_ready(self, fd, mask):
        path, _ = self._priority[fd]
        self._count('priority_events')
        self._emit({label for label, target in self._paths.items() if target == path})
        try:
            if mask & (select.POLLHUP | select.POLLNVAL):
                raise OSError(errno.ENODEV, 'event source disappeared')
            os.lseek(fd, 0, os.SEEK_SET)
            os.read(fd, 65536)
        except OSError as error:
            self._error('priority:' + str(path), error)
            self._poll.unregister(fd)
            os.close(fd)
            del self._priority[fd]

    def _run(self):
        next_repair = time.monotonic() + _RECOVERY_S
        try:
            while True:
                with self._condition:
                    if self._closed:
                        break
                timeout = max(0, math.ceil((next_repair - time.monotonic()) * 1000))
                for fd, mask in self._poll.poll(timeout):
                    if fd == self._reader:
                        return
                    if fd == self._inotify:
                        try:
                            if mask & (select.POLLERR | select.POLLHUP | select.POLLNVAL):
                                raise OSError(errno.ENODEV, 'inotify source disappeared')
                            self._inotify_ready()
                        except OSError as error:
                            self._lose_inotify(error)
                    elif fd in self._pid_labels:
                        # An exited pidfd stays readable forever: unregister
                        # before publishing its single identity-bound hint.
                        labels = self._pid_labels.pop(fd)
                        self._poll.unregister(fd)
                        if mask & select.POLLNVAL:
                            self._error('pidfd:' + str(fd), 'borrowed descriptor became invalid')
                        self._count('pidfd_events')
                        self._emit(labels)
                    elif fd in self._priority:
                        self._priority_ready(fd, mask)
                if time.monotonic() >= next_repair:
                    self._repair()
                    next_repair = time.monotonic() + _RECOVERY_S
        except Exception as error:
            # Fail visibly and unblock consumers instead of leaving a healthy-
            # looking but dead subscription. A final hint requests a reread.
            self._error('producer', error)
            self._emit(set(self._paths) | set(self._pidfds))
        finally:
            with self._condition:
                self._closed = True
                self._condition.notify_all()
            self._dispose()

    def _dispose(self):
        for fd in list(self._priority):
            os.close(fd)
        self._priority.clear()
        for name in ('_inotify', '_reader', '_writer'):
            fd = getattr(self, name)
            if fd >= 0:
                os.close(fd)
                setattr(self, name, -1)
