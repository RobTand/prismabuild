"""Bounded queue hints and local, watch-before-scan namespace coverage (#1894)."""
from __future__ import annotations

from collections import deque
import ctypes
import os
from pathlib import Path
import select
import socket
import struct
import threading
import time
import uuid

from .queue_bell import Cursor, MAX_WAIT_S, REQUIRED_STATE_DIRS
from .queue_bell_topics import OutcomeNamespaces, TOPICS

IN_ATTRIB = 0x00000004
IN_CLOSE_WRITE = 0x00000008
IN_MOVED_FROM = 0x00000040
IN_MOVED_TO = 0x00000080
IN_CREATE = 0x00000100
IN_DELETE = 0x00000200
IN_DELETE_SELF = 0x00000400
IN_MOVE_SELF = 0x00000800
IN_Q_OVERFLOW = 0x00004000
IN_IGNORED = 0x00008000
IN_ISDIR = 0x40000000
IN_ONLYDIR = 0x01000000
IN_DONT_FOLLOW = 0x02000000
EVENT_HEADER = struct.Struct("iIII")
DEFAULT_RING_SIZE = 4096


class MissingQueueDirectory(OSError):
    """A required queue directory is absent, rather than an incidental file."""


class EventRing:
    def __init__(self, capacity: int = DEFAULT_RING_SIZE):
        if capacity < 1:
            raise ValueError("ring capacity must be positive")
        self.epoch, self.seq, self.floor = uuid.uuid4().hex, 0, 0
        self.events: deque[dict] = deque(maxlen=capacity)
        self.condition = threading.Lock()
        self.closed = False
        self.waiters = {}
        self.coverage_version = 1
        self.covered_topics: set[str] = set()
        self.reset_count = 0
        self.coverage_changes = 0

    def now(self) -> dict:
        with self.condition:
            return {"epoch": self.epoch, "seq": self.seq}

    def _wake(self, event=None):
        # The service caps aggregate connections at 128. At most 128 filter
        # comparisons per event; only matching/lost cursors wake and scan the
        # 4096-entry ring. No per-event all-client rescan or unbounded fanout.
        for writer, (since, dirs, keys) in self.waiters.items():
            if (event is None or since.seq < self.floor
                    or ((not dirs or event["topic"] in dirs)
                        and (not keys or event["id"] in keys))):
                try:
                    writer.send(b"x")
                except BlockingIOError:
                    pass  # An unread wakeup already makes the descriptor ready.

    def append(self, directory: str, key: str) -> None:
        if directory not in TOPICS or not TOPICS[directory].accepts_id(key):
            raise ValueError("undeclared bell topic or noncanonical identifier")
        with self.condition:
            self.seq += 1
            if len(self.events) == self.events.maxlen:
                self.floor = self.events[0]["seq"]
            event = {"seq": self.seq, "topic": directory, "id": key}
            self.events.append(event)
            self._wake(event)

    def reset(self) -> None:
        with self.condition:
            self.reset_count += 1
            self.seq += 1
            self.floor = self.seq
            self.events.clear()
            self._wake()

    def close(self) -> None:
        with self.condition:
            self.closed = True
            if self.covered_topics:
                self.covered_topics.clear()
                self.coverage_version += 1
                self.coverage_changes += 1
            self._wake()

    def set_coverage(self, topics) -> None:
        selected = set(topics)
        if not selected.issubset(TOPICS):
            raise ValueError("undeclared bell coverage")
        with self.condition:
            if selected != self.covered_topics:
                self.covered_topics = selected
                self.coverage_version += 1
                self.coverage_changes += 1
                self._wake()

    def _snapshot_v2(self) -> dict:
        return {"epoch": self.epoch, "seq": self.seq,
                "coverage": {"version": self.coverage_version,
                             "topics": sorted(self.covered_topics)},
                "diagnostics": {"ring_capacity": self.events.maxlen,
                                "retained_events": len(self.events),
                                "subscriptions": len(self.waiters),
                                "resets": self.reset_count,
                                "coverage_changes": self.coverage_changes}}

    def now_v2(self) -> dict:
        with self.condition:
            return self._snapshot_v2()

    def wait_v2(self, since: Cursor, topics: set[str], ids: set[str], timeout: float,
                coverage_version: int, connection=None) -> dict | None:
        return self._wait(since, topics or set(TOPICS), ids, timeout, connection, coverage_version)

    def wait(self, since: Cursor, dirs: set[str], keys: set[str], timeout: float,
             connection=None) -> dict | None:
        legacy = {topic.name for topic in TOPICS.values() if topic.legacy}
        return self._wait(since, dirs or legacy, keys, timeout, connection)

    def _wait(self, since, dirs, keys, timeout, connection, coverage_version=None):
        deadline = time.monotonic() + min(timeout, MAX_WAIT_S)
        reader, writer = socket.socketpair()
        writer.setblocking(False)
        try:
            with self.condition:
                self.waiters[writer] = (since, dirs, keys)
            while True:
                with self.condition:
                    cursor = {"epoch": self.epoch, "seq": self.seq}
                    reason = ("shutdown" if self.closed else
                              "epoch_mismatch" if since.epoch != self.epoch else
                              "history_lost" if not self.floor <= since.seq <= self.seq else None)
                    if coverage_version is not None:
                        cursor = self._snapshot_v2()
                        reason = (reason or ("coverage_changed" if coverage_version != self.coverage_version else
                                  "partial_coverage" if not dirs.issubset(self.covered_topics) else None))
                    if reason is not None:
                        return ({**cursor, "reset": True, "reason": reason} if coverage_version is not None
                                else {**cursor, "reset": True})
                    events = [event for event in self.events
                              if event["seq"] > since.seq
                              and (not dirs or event["topic"] in dirs)
                              and (not keys or event["id"] in keys)]
                    remaining = deadline - time.monotonic()
                    if events or remaining <= 0:
                        if coverage_version is None:
                            events = [{"seq": event["seq"], "dir": TOPICS[event["topic"]].directory,
                                       "key": event["id"]} for event in events]
                        return {**cursor, "events": events}
                ready, _, _ = select.select(
                    [reader] + ([connection] if connection is not None else []), [], [], remaining)
                if connection is not None and connection in ready:
                    # One request per connection. FIN, reset, or extra request
                    # bytes end this subscription without waiting for its timer.
                    return None
                if reader in ready:
                    reader.recv(65536)
        finally:
            with self.condition:
                self.waiters.pop(writer, None)
            reader.close()
            writer.close()


class InotifyQueue:
    def __init__(self, root: Path, *, generation: Path | None = None):
        self.root = root
        self.generation = generation
        self.live_link = generation.parent.parent / "repo" if generation is not None else None
        self.libc = ctypes.CDLL(None, use_errno=True)
        self.libc.inotify_init1.argtypes = [ctypes.c_int]
        self.libc.inotify_init1.restype = ctypes.c_int
        self.libc.inotify_add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
        self.libc.inotify_add_watch.restype = ctypes.c_int
        self.fd = self.libc.inotify_init1(os.O_NONBLOCK | os.O_CLOEXEC)
        if self.fd < 0:
            raise OSError(ctypes.get_errno(), "inotify_init1")
        self.directories = {}
        self.adapters = (OutcomeNamespaces(),)
        try:
            for path in (root, *(root / directory for directory in REQUIRED_STATE_DIRS)):
                if not path.is_dir():
                    raise MissingQueueDirectory(f"required queue directory missing: {path}")
            self._tree(Path())
            if self.live_link is not None:
                self._add(self.live_link.parent, None)
        except BaseException:
            self.close()
            raise

    def retired(self):
        if self.live_link is None:
            return False
        try:
            return self.live_link.resolve(strict=True) != self.generation
        except OSError:
            return True

    def _add(self, path, relative):
        mask = (IN_ATTRIB | IN_CLOSE_WRITE | IN_CREATE | IN_DELETE | IN_MOVED_FROM
                | IN_MOVED_TO | IN_DELETE_SELF | IN_MOVE_SELF | IN_ONLYDIR | IN_DONT_FOLLOW)
        watch_path = str(path) + "/." if str(path).startswith("/proc/self/fd/") else path
        wd = self.libc.inotify_add_watch(self.fd, os.fsencode(watch_path), mask)
        if wd < 0:
            raise OSError(ctypes.get_errno(), str(path))
        self.directories[wd] = relative

    def _allowed(self, relative):
        return any(adapter.allows(relative) for adapter in self.adapters)

    def _hint(self, relative, ring):
        for adapter in self.adapters:
            hint = adapter.hint(relative)
            if hint is not None:
                ring.append(*hint)

    @property
    def covered_topics(self):
        return tuple(topic.name for adapter in self.adapters for topic in adapter.topics)

    def _open_tree_directory(self, relative, parent_fd):
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        if parent_fd is not None:
            return os.open(relative.name, flags, dir_fd=parent_fd)
        descriptor = os.open(self.root, flags)
        try:
            for part in relative.parts:
                child = os.open(part, flags, dir_fd=descriptor)
                os.close(descriptor)
                descriptor = child
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise

    def _tree(self, relative, ring=None, parent_fd=None):
        """Install first, then scan, including subtrees moved in fully populated.

        Only the outcome namespaces are traversed, to depth three. Symlinks
        and other queue trees are never followed. Optional roots are lazy.
        """
        path = self.root / relative
        descriptor = None
        try:
            descriptor = self._open_tree_directory(relative, parent_fd)
            # The watch and scan bind the same inode. A renamed/replaced path
            # cannot redirect traversal through a symlink between those steps.
            self._add(Path(f"/proc/self/fd/{descriptor}"), relative)
            with os.scandir(descriptor) as entries:
                for entry in entries:
                    if entry.name.startswith("."):
                        continue
                    child = relative / entry.name
                    if entry.is_dir(follow_symlinks=False) and self._allowed(child):
                        self._tree(child, ring, descriptor)
                    elif ring is not None:
                        self._hint(child, ring)
        except FileNotFoundError as exc:
            # A removed child is already covered by its parent's delete event.
            if relative == Path() or relative.as_posix() in REQUIRED_STATE_DIRS:
                raise MissingQueueDirectory(f"required queue directory missing: {path}") from exc
        finally:
            if descriptor is not None:
                os.close(descriptor)

    def close(self) -> None:
        os.close(self.fd)

    def consume(self, data: bytes, ring: EventRing) -> None:
        offset = 0
        while offset < len(data):
            wd, mask, _, length = EVENT_HEADER.unpack_from(data, offset)
            name = os.fsdecode(data[offset + EVENT_HEADER.size:
                                    offset + EVENT_HEADER.size + length].split(b"\0", 1)[0])
            offset += EVENT_HEADER.size + length
            if mask & IN_Q_OVERFLOW:
                # Withdraw the coverage proof before rebuilding. A failed watch
                # must leave v2 incomplete even while existing requests unwind.
                ring.set_coverage(())
                ring.reset()
                self._tree(Path(), ring)  # Overflow may have hidden mkdir events.
                ring.set_coverage(self.covered_topics)
                continue
            if wd not in self.directories:
                continue
            relative = self.directories[wd]
            if relative is None:
                if mask & (IN_DELETE_SELF | IN_MOVE_SELF | IN_IGNORED):
                    raise OSError("queue bell lost its generation watch")
                continue  # The selector checks retirement after consuming events.
            if mask & IN_MOVE_SELF:
                raise OSError("queue bell watched directory moved")
            if mask & (IN_DELETE_SELF | IN_IGNORED):
                self.directories.pop(wd, None)
                if relative == Path() or relative.as_posix() in REQUIRED_STATE_DIRS:
                    raise OSError("queue bell lost a state directory watch")
                ring.reset()
                continue
            if not name or name.startswith("."):
                continue
            child = relative / name
            if mask & IN_ISDIR and mask & (IN_CREATE | IN_MOVED_TO) and self._allowed(child):
                self._tree(child, ring)
            self._hint(child, ring)
