"""Owned, event-driven publication of a host's diagnostic adaptive mirrors.

A host hub installs one session per local adaptive state directory. One-shot
publication remains usable by older workers and owns its existing flock until
its copy process actually exits. This observer never waits for remote I/O.
"""
from __future__ import annotations

import ctypes
import errno
import fcntl
import os
from pathlib import Path
import select
import socket
import stat
import struct
import threading
import time

from . import adaptive_snapshot as snapshot

_HEADER = struct.Struct('iIII')
_MODIFY, _ATTRIB, _CLOSE_WRITE = 0x2, 0x4, 0x8
_MOVED_FROM, _MOVED_TO, _CREATE, _DELETE = 0x40, 0x80, 0x100, 0x200
_DELETE_SELF, _MOVE_SELF, _OVERFLOW, _IGNORED = 0x400, 0x800, 0x4000, 0x8000
_MASK = (_MODIFY | _ATTRIB | _CLOSE_WRITE | _MOVED_FROM | _MOVED_TO
         | _CREATE | _DELETE | _DELETE_SELF | _MOVE_SELF | 0x01000000 | 0x02000000)


class _LocalChanges:
    def __init__(self, local):
        self.local = local
        self.libc = ctypes.CDLL(None, use_errno=True)
        self.libc.inotify_init1.argtypes = [ctypes.c_int]
        self.libc.inotify_init1.restype = ctypes.c_int
        self.libc.inotify_add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
        self.libc.inotify_add_watch.restype = ctypes.c_int
        self.fd = self.libc.inotify_init1(os.O_NONBLOCK | os.O_CLOEXEC)
        if self.fd < 0:
            raise OSError(ctypes.get_errno(), 'adaptive mirror inotify_init1')
        try:
            self.parent = self._add(local.parent)
            self.source = self._add(local)
        except BaseException:
            self.close()
            raise

    def _add(self, path):
        descriptor = self.libc.inotify_add_watch(self.fd, os.fsencode(path), _MASK)
        if descriptor < 0:
            raise OSError(ctypes.get_errno(), str(path))
        return descriptor

    def rebuild(self):
        replacement = type(self)(self.local)
        self.close()
        self.fd, self.parent, self.source = replacement.fd, replacement.parent, replacement.source
        replacement.fd = None


    def consume(self):
        try:
            raw = os.read(self.fd, 65536)
        except BlockingIOError:
            return set()
        changed = set()
        offset = 0
        while offset + _HEADER.size <= len(raw):
            watch, mask, _, length = _HEADER.unpack_from(raw, offset)
            offset += _HEADER.size
            name = os.fsdecode(raw[offset:offset + length].split(b'\0', 1)[0])
            offset += length
            if mask & _OVERFLOW:
                # Lost events may include directory replacement or watch loss.
                # Install parent/source watches before the caller rereads files;
                # concurrent mutations remain queued on the replacement fd.
                self.rebuild()
                return set(snapshot.FILES)
            elif watch == self.parent:
                if name == self.local.name:
                    changed.update(snapshot.FILES)
                    self.source = self._add(self.local)
                elif not name and mask & (_DELETE_SELF | _MOVE_SELF | _IGNORED):
                    raise OSError(errno.ESTALE, 'adaptive mirror parent watch lost')
                elif not name and mask & _ATTRIB:
                    changed.update(snapshot.FILES)
            elif watch == self.source:
                if mask & (_DELETE_SELF | _MOVE_SELF | _IGNORED):
                    raise OSError(errno.ESTALE, 'adaptive mirror source watch lost')
                if not name:
                    changed.update(snapshot.FILES)
                elif name in snapshot.FILES or name == 'publisher-result.json':
                    changed.add(name)
        return changed

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


class LocalMirrorPublisher:
    """A single owned source watcher, rate deadline and copy-completion stream.

    ``with LocalMirrorPublisher(local, shared)`` owns its observer thread.
    A hub owning an event loop may instead construct it, call ``reconcile``
    when one of its ``descriptors`` becomes ready or its returned deadline expires, and
    close it at the host lifecycle boundary. No thread starts at construction.
    """

    def __init__(self, local: Path, shared: Path, *, clock=time.monotonic):
        self.local, self.shared, self.clock = Path(local), Path(shared), clock
        self._lock = os.open(self.local / 'mirror-watch.lock',
                             os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
        self._changes = None
        try:
            info = os.fstat(self._lock)
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                    or info.st_uid != os.getuid()):
                raise OSError(errno.EINVAL, 'unsafe adaptive mirror observer lock')
            fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._changes = _LocalChanges(self.local)  # Install before observing records.
            self._wake, self._stop = socket.socketpair()
        except BaseException:
            if self._changes is not None:
                self._changes.close()
            os.close(self._lock)
            raise
        self._thread = None
        self._child = None
        self._pidfd = None
        self._owner_fd = None
        self._lock_delay = .05
        self._copy_recovery = .25
        self._dirty = True
        self._retry_at = None
        self._recovery = .25
        self._closed = False
        self._counts = dict(invalidations=0, starts=0, completed=0, failed=0,
                            source_failures=0, coalesced=0)

    def fileno(self):
        return None if self._changes is None else self._changes.fd

    @property
    def descriptors(self):
        return tuple(fd for fd in (self.fileno(), self._pidfd, self._owner_fd) if fd is not None)

    def _clear_owner(self):
        if self._owner_fd is not None:
            os.close(self._owner_fd)
            self._owner_fd = None

    def _watch_owner(self):
        self._clear_owner()
        owner = snapshot._read(self.local / 'publisher-owner.json')
        pid, ticks = owner.get('pid'), owner.get('start_ticks')
        if not isinstance(pid, int) or ticks is None or snapshot._start_ticks(pid) != ticks:
            return False
        try:
            descriptor = os.pidfd_open(pid)
        except OSError:
            return False
        if snapshot._start_ticks(pid) != ticks or select.select([descriptor], [], [], 0)[0]:
            os.close(descriptor)
            return False
        self._owner_fd = descriptor
        return True

    @property
    def diagnostics(self):
        return dict(self._counts, dirty=self._dirty, retry_at=self._retry_at)

    def _completion(self):
        if self._child is None or self._child.poll() is None:
            return None
        outcome = 'completed' if self._child.returncode == 0 else 'failed'
        self._counts[outcome] += 1
        self._child = None
        if self._pidfd is not None:
            os.close(self._pidfd)
            self._pidfd = None
        return outcome

    def reconcile(self):
        """Consume latched changes and return the next absolute deadline, if any."""
        now = self.clock()
        try:
            if self._changes is None:
                self._changes = _LocalChanges(self.local)
            changed = self._changes.consume()
            source_changed = bool(changed.intersection(snapshot.FILES))
            outcome = self._completion()
            owner_ended = self._owner_fd is not None and select.select([self._owner_fd], [], [], 0)[0]
            if owner_ended:
                self._clear_owner()
            if source_changed:
                self._counts['invalidations'] += 1
                self._counts['coalesced'] += int(self._dirty)
            if source_changed or outcome == 'completed':
                self._copy_recovery = .25
            self._dirty |= bool(changed) or outcome is not None or bool(owner_ended)
            if source_changed or owner_ended or outcome == 'completed':
                self._retry_at = None
            if outcome == 'failed':
                # Failed remote copying cannot notify recovery. Retry at .25 s
                # doubling to 5 s, never before the original one-second start
                # deadline; a new source mutation, success or stop cancels it.
                due = snapshot.next_publication(self.local, now=now)
                self._retry_at = max(now + self._copy_recovery, due or now)
                self._copy_recovery = min(5., self._copy_recovery * 2)
            if self._child is not None:
                if self._pidfd is None:
                    try:
                        self._pidfd = os.pidfd_open(self._child.pid)
                    except ProcessLookupError:
                        return now
                return None  # pidfd completion, not a recurring copy check.
            if not self._dirty:
                return None
            if self._retry_at is not None and now < self._retry_at:
                return self._retry_at
            deadline = snapshot.next_publication(self.local, now=now)
            if deadline is None:
                self._dirty, self._retry_at = False, None
                self._recovery, self._copy_recovery, self._lock_delay = .25, .25, .05
                self._clear_owner()
                return None
            if deadline > now:
                self._retry_at = deadline
                return deadline
            child = snapshot.publish(self.local, self.shared)
            if child is not None:
                self._child = child
                self._counts['starts'] += 1
                self._retry_at = None
                return now  # Register completion before the next blocking wait.
            if self._watch_owner():
                return None
            # Legacy lock ownership may lack a completion callback. Each attempt
            # remains nonblocking; 50 ms-to-1 s recovery ends on owner/result
            # notification, clean state or stop and never extends a copy's life.
            self._retry_at = now + self._lock_delay
            self._lock_delay = min(1., self._lock_delay * 2)
            return self._retry_at
        except (OSError, TypeError, ValueError):
            # An unavailable local watch/read or pidfd has no usable callback.
            # Rebuild after .25 s doubling to 5 s, cancelled by recovery or stop.
            if self._changes is not None:
                self._changes.close()
                self._changes = None
            self._counts['source_failures'] += 1
            self._dirty = True
            self._retry_at = now + self._recovery
            self._recovery = min(5., self._recovery * 2)
            return self._retry_at

    def _run(self):
        deadline = self.reconcile()
        while not self._closed:
            descriptors = [self._wake, *self.descriptors]
            timeout = None if deadline is None else max(0., deadline - self.clock())
            ready, _, _ = select.select(descriptors, [], [], timeout)
            if self._wake in ready:
                return
            deadline = self.reconcile()

    def start(self):
        if self._thread is not None or self._closed:
            raise RuntimeError('adaptive mirror observer already started or closed')
        self._thread = threading.Thread(target=self._run, name='adaptive-mirrors', daemon=False)
        self._thread.start()
        return self

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._stop.send(b'x')
        if self._thread is not None:
            self._thread.join()
        if self._pidfd is not None:
            os.close(self._pidfd)
        self._clear_owner()
        if self._changes is not None:
            self._changes.close()
        self._wake.close()
        self._stop.close()
        os.close(self._lock)
        # An already launched copy retains its original publication flock and
        # _children custody. Closing the observer cannot assert remote I/O ended.

    def __enter__(self):
        return self.start()

    def __exit__(self, *_):
        self.close()
