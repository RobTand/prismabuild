"""Bounded queue hints and local, watch-before-scan namespace coverage (#1894)."""
from __future__ import annotations

from collections import deque
import ctypes
import errno
import os
from pathlib import Path
import select
import socket
import struct
import threading
import time
import uuid

from .queue_bell import Cursor, MAX_WAIT_S, REQUIRED_STATE_DIRS
from .queue_bell_topics import OutcomeNamespaces, WorkerOffers, HostReservations, HostInputShapes, TOPICS

IN_MODIFY = 0x00000002
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

    def reset_v2(self) -> None:
        """Invalidate v2 history through its coverage marker, preserving v1 events."""
        with self.condition:
            self.coverage_version += 1
            self.reset_count += 1
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
        self.libc.inotify_rm_watch.argtypes = [ctypes.c_int, ctypes.c_int]
        self.libc.inotify_rm_watch.restype = ctypes.c_int
        self.fd = self.libc.inotify_init1(os.O_NONBLOCK | os.O_CLOEXEC)
        if self.fd < 0:
            raise OSError(ctypes.get_errno(), "inotify_init1")
        self.directories = {}
        self.root_watches = {}
        self.adapters = (OutcomeNamespaces(), WorkerOffers(), HostReservations())
        self.absent_roots = set()
        self.incomplete_topics = set()
        self.file_watches = {}
        self.ancestor_names = {}
        self.ancestor_watches = {}
        self.host_ancestry_complete = False
        try:
            self._tree(Path())
            if self.live_link is not None:
                parent = self.live_link.parent.absolute()
                self._add(self.live_link.parent, parent if parent in self.ancestor_names else None)
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
        mask = (IN_MODIFY | IN_ATTRIB | IN_CLOSE_WRITE | IN_CREATE | IN_DELETE | IN_MOVED_FROM
                | IN_MOVED_TO | IN_DELETE_SELF | IN_MOVE_SELF | IN_ONLYDIR | IN_DONT_FOLLOW)
        watch_path = str(path) + "/." if str(path).startswith("/proc/self/fd/") else path
        return self._bind_watch(watch_path, relative, mask)

    def _bind_watch(self, path, relative, mask):
        wd = self.libc.inotify_add_watch(self.fd, os.fsencode(path), mask)
        if wd < 0:
            raise OSError(ctypes.get_errno(), str(path))
        if wd in self.directories and self.directories[wd] != relative:
            # The inode moved during reconstruction. Retire its old descriptor
            # so buffered self-events cannot remove the new name's binding.
            previous = self.directories.pop(wd)
            self.root_watches.pop(previous, None)
            self.file_watches.pop(previous, None)
            if self.ancestor_watches.get(previous) == wd:
                self.ancestor_watches.pop(previous)
            self.libc.inotify_rm_watch(self.fd, wd)
            wd = self.libc.inotify_add_watch(self.fd, os.fsencode(path), mask)
            if wd < 0:
                raise OSError(ctypes.get_errno(), str(path))
        self.directories[wd] = relative
        if relative is not None and relative.is_absolute():
            self.ancestor_watches[relative] = wd
        if relative is not None and not relative.is_absolute() and len(relative.parts) <= 1:
            self.root_watches[relative] = wd
        return wd

    def _watch_host_file(self, relative, parent_fd):
        """Bind the inode too: new external hardlinks bypass parent watches."""
        path = self.root / relative if parent_fd is None else relative.name
        descriptor = os.open(path, os.O_PATH | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent_fd)
        try:
            if not HostInputShapes.watchable("file", os.fstat(descriptor)):
                self.incomplete_topics.update(self._host_topics_at(relative))
                return
            mask = IN_MODIFY | IN_ATTRIB | IN_CLOSE_WRITE | IN_DELETE_SELF | IN_MOVE_SELF
            # This proc link names the pinned, verified regular inode, rather
            # than following a mutable queue leaf. No record bytes are opened.
            wd = self._bind_watch(Path(f"/proc/self/fd/{descriptor}"), relative, mask)
            previous = self.file_watches.get(relative)
            if previous is not None and previous != wd:
                self.libc.inotify_rm_watch(self.fd, previous)
                self.directories.pop(previous, None)
            self.file_watches[relative] = wd
        finally:
            os.close(descriptor)

    def _watch_ancestors(self):
        """Watch search permissions and path bindings before queue reconstruction.

        Ancestors are not scanned. A symlink or failed open withdraws the host
        topics, whose readers otherwise follow ancestors outside this tree.
        """
        for wd, relative in list(self.directories.items()):
            if relative in self.ancestor_names:
                self.libc.inotify_rm_watch(self.fd, wd)
                self.directories.pop(wd)
        self.ancestor_names.clear()
        self.ancestor_watches.clear()
        self.host_ancestry_complete = False
        absolute = self.root.absolute()
        if ".." in absolute.parts:
            return
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        descriptor = os.open(absolute.anchor, flags)
        ancestor = Path(absolute.anchor)
        try:
            for part in absolute.parts[1:]:
                self._add(Path(f"/proc/self/fd/{descriptor}"), ancestor)
                self.ancestor_names[ancestor] = part
                if ancestor / part != absolute:
                    child = os.open(part, flags, dir_fd=descriptor)
                    os.close(descriptor)
                    descriptor = child
                    ancestor /= part
            self.host_ancestry_complete = True
        except OSError:
            pass  # Existing partial ancestor watches cannot prove coverage.
        finally:
            os.close(descriptor)

    def _check_host_input(self, relative, *, entry=None, parent_fd=None, watch_file=True):
        kind = HostInputShapes.kind(relative)
        if kind is None:
            return
        try:
            info = (entry.stat(follow_symlinks=False) if entry is not None
                    else (self.root / relative).lstat())
            if not HostInputShapes.watchable(kind, info):
                self.incomplete_topics.update(self._host_topics_at(relative))
            elif kind == "file" and watch_file:
                self._watch_host_file(relative, parent_fd)
        except FileNotFoundError:
            return  # The watched parent covers removal or replacement.
        except OSError:
            self.incomplete_topics.update(self._host_topics_at(relative))
            return

    def _allowed(self, relative):
        return any(adapter.allows(relative) for adapter in self.adapters)

    def _hint(self, relative, ring):
        for adapter in self.adapters:
            hint = adapter.hint(relative)
            if hint is not None:
                ring.append(*hint)

    def _topics_at(self, relative):
        return {topic.name for adapter in self.adapters for topic in adapter.topics
                if relative.parts and relative.parts[0] == topic.directory}

    def _host_topics_at(self, relative):
        return {name for name in self._topics_at(relative) if not TOPICS[name].legacy}

    def _drop_host_tree(self, relative):
        # A parent rename/delete is the invalidation. Stop watching the old
        # inode before it can move outside the queue or retain an old host ID.
        name = str(relative)
        prefix = name if name.endswith('/') else name + '/'
        all_relative = name == '.'
        for wd, path in list(self.directories.items()):
            if path is None:
                continue
            candidate = str(path)
            if (candidate == name or (candidate.startswith(prefix)
                                      and (name != '/' or not candidate.startswith('//')))
                    or (all_relative and not candidate.startswith('/'))):
                self.libc.inotify_rm_watch(self.fd, wd)
                self.directories.pop(wd, None)
                self.root_watches.pop(path, None)
                self.file_watches.pop(path, None)

    @property
    def covered_topics(self):
        watched = {path for path, wd in self.root_watches.items()
                   if wd in self.directories and self.directories[wd] == path}
        if Path() not in watched:
            return ()
        ancestors_installed = (self.host_ancestry_complete
                               and all(self.directories.get(self.ancestor_watches.get(path)) == path
                                       for path in self.ancestor_names))
        # An absent optional root is covered by the installed queue-root watch.
        # Its creation withdraws that proof until installation has invalidated
        # observations made before its descendant watches existed.
        return tuple(topic.name for adapter in self.adapters for topic in adapter.topics
                     if topic.name not in self.incomplete_topics
                     and (topic.legacy or ancestors_installed)
                     and (Path(topic.directory) in watched or topic.directory in self.absent_roots))

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

        Adapters bound each namespace's directory shape. Symlinks and other
        queue trees are never followed. Optional roots are lazy.
        """
        path = self.root / relative
        descriptor = None
        try:
            if relative == Path():
                self._watch_ancestors()
            descriptor = self._open_tree_directory(relative, parent_fd)
            # The watch and scan bind the same inode. A renamed/replaced path
            # cannot redirect traversal through a symlink between those steps.
            if relative == Path():
                self.root_watches.clear()  # An overflow may hide watch removal.
                self.incomplete_topics.clear()
            self._add(Path(f"/proc/self/fd/{descriptor}"), relative)
            if relative == Path():
                # Required roots cannot depend on their names appearing in a
                # directory listing. Open each without following links, then
                # watch and scan it before any coverage can be published.
                for directory in REQUIRED_STATE_DIRS:
                    self._tree(Path(directory), ring, descriptor)
            root_names = set()
            with os.scandir(descriptor) as entries:
                for entry in HostReservations.reconstruction_inputs(relative, entries):
                    child = relative / entry.name
                    self._check_host_input(child, entry=entry, parent_fd=descriptor)
                    if entry.name.startswith(".") and not HostReservations.hidden_input(child):
                        continue
                    if relative == Path():
                        if self._allowed(child):
                            root_names.add(entry.name)
                        if entry.name in REQUIRED_STATE_DIRS:
                            continue  # Already watched and scanned explicitly.
                    if entry.is_dir(follow_symlinks=False) and self._allowed(child):
                        self._tree(child, ring, descriptor)
                    else:
                        if self._allowed(child):
                            # A host/held/free shape replaced by a symlink or
                            # non-directory is unknown, never an empty ledger.
                            self.incomplete_topics.update(self._host_topics_at(child))
                        if ring is not None:
                            self._hint(child, ring)
            if relative == Path():
                self.absent_roots = {topic.directory for adapter in self.adapters for topic in adapter.topics
                                     if not topic.required and topic.directory not in root_names}
        except OSError as exc:
            if (relative == Path() or relative.as_posix() in REQUIRED_STATE_DIRS
                    ) and exc.errno in (errno.ENOENT, errno.ENOTDIR, errno.ELOOP):
                raise MissingQueueDirectory(f"required queue directory missing or not a directory: {path}") from exc
            # A removed child is covered by its parent's hint, or by the reset
            # after installing a namespace whose name has no action identifier.
            if exc.errno != errno.ENOENT:
                raise
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
                # An overflow can hide a move out of the queue. Rebuild host
                # coverage without retaining watches on those old inodes.
                self._drop_host_tree(Path("workers"))
                self._drop_host_tree(Path("reservations"))
                self._tree(Path(), ring)  # Overflow may have hidden mkdir events.
                ring.set_coverage(self.covered_topics)
                continue
            if wd not in self.directories:
                continue
            relative = self.directories[wd]
            if self.file_watches.get(relative) == wd:
                if mask & (IN_DELETE_SELF | IN_MOVE_SELF | IN_IGNORED):
                    self.file_watches.pop(relative)
                    self.directories.pop(wd)
                    self.libc.inotify_rm_watch(self.fd, wd)
                    if mask & IN_IGNORED and not mask & IN_DELETE_SELF:
                        self.incomplete_topics.update(self._host_topics_at(relative))
                else:
                    self._check_host_input(relative, watch_file=False)
                self._hint(relative, ring)
                ring.set_coverage(self.covered_topics)
                continue
            if relative in self.ancestor_names:
                if (mask & (IN_DELETE_SELF | IN_MOVE_SELF | IN_IGNORED)
                        or (name == self.ancestor_names[relative]
                            and mask & (IN_CREATE | IN_DELETE | IN_MOVED_FROM | IN_MOVED_TO))):
                    self.host_ancestry_complete = False
                    ring.set_coverage(self.covered_topics)
                    ring.reset_v2()
                elif not name and mask & IN_ATTRIB:
                    ring.reset_v2()
                continue
            if relative is None:
                if mask & (IN_DELETE_SELF | IN_MOVE_SELF | IN_IGNORED):
                    raise OSError("queue bell lost its generation watch")
                continue  # The selector checks retirement after consuming events.
            host_topics = self._host_topics_at(relative)
            if mask & IN_MOVE_SELF:
                if host_topics:
                    self.incomplete_topics.update(host_topics)
                    ring.set_coverage(self.covered_topics)
                    ring.reset_v2()
                    self._drop_host_tree(relative)
                    continue
                raise OSError("queue bell watched directory moved")
            if mask & (IN_DELETE_SELF | IN_IGNORED):
                self.directories.pop(wd, None)
                if host_topics:
                    # Normal deletion is covered by its watched parent. An
                    # unexplained ignored watch cannot prove completeness.
                    if not mask & IN_DELETE_SELF:
                        self.incomplete_topics.update(host_topics)
                    self._hint(relative, ring)
                    ring.set_coverage(self.covered_topics)
                    ring.reset_v2()
                    continue
                if relative == Path() or relative.as_posix() in REQUIRED_STATE_DIRS:
                    raise OSError("queue bell lost a state directory watch")
                ring.reset()
                ring.set_coverage(self.covered_topics)
                continue
            if not name:
                if relative == Path() and mask & IN_ATTRIB:
                    ring.reset_v2()
                elif host_topics:
                    if len(relative.parts) == 1:
                        ring.reset_v2()
                    elif mask & IN_ATTRIB:
                        self._hint(relative, ring)
                continue
            child = relative / name
            # Only host topics consume modify-before-close events. Keep the
            # original outcome masks and v1 event projection unchanged.
            if mask & IN_MODIFY and not self._host_topics_at(child):
                continue
            self._check_host_input(child)
            ring.set_coverage(self.covered_topics)
            if name.startswith(".") and not HostReservations.hidden_input(child):
                continue
            affected = self._topics_at(child)
            host_change = self._host_topics_at(child)
            root_change = relative == Path() and bool(affected)
            if host_change and mask & IN_ISDIR and mask & (IN_DELETE | IN_MOVED_FROM):
                self._drop_host_tree(child)
            if root_change and mask & (IN_DELETE | IN_MOVED_FROM):
                if name in REQUIRED_STATE_DIRS:
                    raise OSError("queue bell lost a state directory watch")
                self.absent_roots.add(name)
            if root_change and mask & (IN_CREATE | IN_MOVED_TO):
                self.absent_roots.discard(name)
            installing = (mask & IN_ISDIR and mask & (IN_CREATE | IN_MOVED_TO)
                          and self._allowed(child))
            # A keyless namespace name cannot invalidate a descendant that an
            # observer saw and that vanished before watches were installed.
            # Withdraw coverage throughout installation, then reset history.
            keyless_change = ((installing and not any(adapter.hint(child) for adapter in self.adapters))
                              or (root_change and mask & (IN_CREATE | IN_MOVED_TO | IN_DELETE | IN_MOVED_FROM))
                              or (installing and bool(host_change)))
            if (host_change and self._allowed(child) and not mask & IN_ISDIR
                    and mask & (IN_CREATE | IN_MOVED_TO)):
                self.incomplete_topics.update(host_change)
                ring.set_coverage(self.covered_topics)
            if keyless_change:
                ring.set_coverage(set(self.covered_topics) - affected)
            if installing:
                self._tree(child, ring)
            if keyless_change:
                ring.reset_v2()
                ring.set_coverage(self.covered_topics)
            self._hint(child, ring)
