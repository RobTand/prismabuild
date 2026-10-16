"""Retained directory records and census memos fenced by local filesystem evidence."""
from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, suppress
import contextvars
from dataclasses import dataclass
from typing import Protocol
import os
from pathlib import Path
import select
import stat as statmod
import sys
import threading
import time


_FILE_VERSION_SCOPE: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "kept_reads_file_versions", default=False)


@contextmanager
def file_version_scope():
    """Allow package-side output memos to retain versions for one fleet read."""
    token = _FILE_VERSION_SCOPE.set(True)
    try:
        yield
    finally:
        _FILE_VERSION_SCOPE.reset(token)


def file_versions_enabled() -> bool:
    return _FILE_VERSION_SCOPE.get()


def metadata_version(info: "os.stat_result") -> tuple[int, int, int, int, int]:
    """Change evidence for one publication metadata file.

    Device and inode catch a replacement, size and mtime catch a rewrite,
    ctime catches a permission or ownership change that leaves mtime alone.
    The same kind of stat fence every strict reader already uses; nothing
    here invents an identity of its own.
    """

    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns,
            int(getattr(info, "st_ctime_ns", 0)))


#: The clock a local Linux filesystem stamps a directory's mtime and ctime
#: from: ``current_time()`` reads the coarse realtime clock (a multigrain
#: filesystem may read the fine one, which is never behind it).  Python names
#: the constant on some builds only; its value is 5 on every Linux ABI.
_COARSE_REALTIME: int | None = (
    int(getattr(time, "CLOCK_REALTIME_COARSE", 5))
    if sys.platform.startswith("linux") else None)

#: Filesystems whose directory timestamps this kernel writes from that clock
#: at nanosecond granularity.  A network filesystem is not among them: its
#: timestamps come from the server's clock, and its client may answer from
#: an attribute cache.  Anything unlisted lists every directory every time.
_LOCAL_CLOCK_FILESYSTEMS = frozenset({"zfs", "ext4", "xfs", "btrfs", "tmpfs"})

#: The mount table :func:`_filesystem_type` reads, and the file whose
#: ``POLLPRI`` says the table changed.
_MOUNTINFO = "/proc/self/mountinfo"

#: Direct device answers and mount-ID rows from one validated table. Tuple
#: keys name mount rows or the table marker; integer keys are stat devices.
_filesystem_types: dict[int | tuple[str, int], str | None] = {}
_MOUNT_TABLE_KEY = ("table", 0)

#: ``(pid, descriptor, poller)`` watching :data:`_MOUNTINFO`, or ``None``.
_mount_watch: tuple[int, int, "select.poll"] | None = None
_mount_lock = threading.Lock()
_mount_watch_error = False
_mount_namespace: tuple[int, int, int, str] | None = None
_mount_generation = 0
#: Parser byte caps, not syscall interruption or admission limits.
_MOUNTINFO_MAX_BYTES = 1024 * 1024
_FDINFO_MAX_BYTES = 4096


def _mount_table_changed() -> bool:
    """Whether the mount table may have changed since this was last asked.

    The kernel marks an open ``/proc/self/mountinfo`` with ``POLLPRI`` and
    ``POLLERR`` after any mount or unmount in the process's namespace, and
    the poll that reports the mark clears it.  The watch is opened on first
    use and again in a forked child, which must not clear its parent's mark
    on a shared open file.  A first call, or a table that cannot be watched,
    answers ``True``: nothing remembered before a watch existed is trusted.
    Setup/poll failure or POLLNVAL also marks ``_mount_watch_error``; the
    resolver refuses that observation rather than treating it as no event.
    """

    global _mount_watch, _mount_watch_error
    watch = _mount_watch
    if watch is None or watch[0] != os.getpid():
        try:
            descriptor = os.open(_MOUNTINFO, os.O_RDONLY | os.O_CLOEXEC)
        except OSError:
            _mount_watch_error = True
            return True
        try:
            poller = select.poll()
            poller.register(descriptor, select.POLLPRI | select.POLLERR)
        except OSError:
            os.close(descriptor)
            _mount_watch_error = True
            return True
        if watch is not None:
            # A fork closes only its inherited descriptor.
            with suppress(OSError):
                os.close(watch[1])
        _mount_watch = (os.getpid(), descriptor, poller)
        return True
    try:
        events = watch[2].poll(0)
    except OSError:
        with suppress(OSError):
            os.close(watch[1])
        _mount_watch = None
        _mount_watch_error = True
        return True
    if any(mask & select.POLLNVAL for _descriptor, mask in events):
        with suppress(OSError):
            os.close(watch[1])
        _mount_watch = None
        _mount_watch_error = True
        return True
    return any(mask & (select.POLLPRI | select.POLLERR)
               for _descriptor, mask in events)


def _namespace_identity() -> tuple[int, int, int, str] | None:
    """This process's mount namespace, not a pathname-based mount guess."""

    try:
        info = os.stat("/proc/self/ns/mnt")
        link = os.readlink("/proc/self/ns/mnt")
        if link != f"mnt:[{info.st_ino}]":
            return None
        return os.getpid(), info.st_dev, info.st_ino, link
    except OSError:
        return None


def _mount_state() -> tuple[tuple[int, int, int, str], int] | None:
    """Refresh the existing memo's namespace/watch fence; called under its lock."""

    global _mount_namespace, _mount_watch, _mount_watch_error
    global _mount_generation
    namespace = _namespace_identity()
    changed = namespace != _mount_namespace or namespace is None
    if changed:
        if _mount_watch is not None:
            with suppress(OSError):
                os.close(_mount_watch[1])
            _mount_watch = None
        _mount_namespace = namespace
    _mount_watch_error = False
    if namespace is not None:
        changed = _mount_table_changed() or changed
    if changed:
        _filesystem_types.clear()
        _mount_generation += 1
    if namespace is None or _mount_watch_error:
        return None
    return namespace, _mount_generation


def _proc_mount_text(path: str, limit: int) -> str:
    """Bounded complete procfs record; caps are parser limits, not timing bounds."""

    with open(path, "rb") as stream:
        payload = stream.read(limit + 1)
    if (not payload or len(payload) > limit or not payload.endswith(b"\n")
            or b"\x00" in payload):
        raise ValueError("incomplete or oversized procfs mount record")
    return payload.decode("utf-8")


def _decimal(value: str) -> bool:
    return value.isascii() and value.isdigit()


def _mount_path_field(value: str) -> bool:
    if not value.startswith("/"):
        return False
    parts = value.split("\\")
    return all(part[:3] in {"040", "011", "012", "134"}
               for part in parts[1:])


# Linux v6.14 nsfs_show_path prints ns_ops->name:[inode], not an absolute
# path. These proc_ns_operations names are a closed, source-proven vocabulary;
# accepting their root syntax does not make nsfs a local-clock filesystem.
_NSFS_NAMESPACE_KINDS = frozenset({
    "net", "pid", "pid_for_children", "time", "time_for_children", "uts",
    "ipc", "mnt", "user", "cgroup",
})


def _mount_root_field(value: str, fstype: str) -> bool:
    """A path root, or an nsfs-only opaque namespace dentry display token."""

    if _mount_path_field(value):
        return True
    if fstype != "nsfs":
        return False
    kind, separator, inode = value.partition(":[")
    return (separator == ":[" and kind in _NSFS_NAMESPACE_KINDS
            and inode.endswith("]") and _decimal(inode[:-1]))


def _load_mount_types() -> bool:
    """Validate one table for both indexes; no ambiguous row grants trust.

    Repeated bind rows may share a device and type. Contradictory types make
    that device unusable (not absent); duplicate mount IDs or malformed rows
    refuse the complete table, so fallback cannot reinterpret ambiguity.
    """

    try:
        text = _proc_mount_text(_MOUNTINFO, _MOUNTINFO_MAX_BYTES)
        devices: dict[int, str | None] = {}
        mounts: dict[int, str] = {}
        for line in text.splitlines():
            fields = line.split()
            # Three suffix fields follow the separator. The source field
            # may itself be '-' (e.g. tmpfs); it is not another separator.
            separator = len(fields) - 4
            if (separator < 6 or fields[separator] != "-"
                    or "-" in fields[:separator]
                    or fields[separator + 1] == "-"
                    or fields[separator + 3] == "-"):
                return False
            fstype = fields[separator + 1]  # suffix bounds proved above
            if (not _decimal(fields[0]) or int(fields[0]) <= 0
                    or not _decimal(fields[1])
                    or not _mount_root_field(fields[3], fstype)
                    or not _mount_path_field(fields[4])):
                return False
            numbers = fields[2].split(":")
            if len(numbers) != 2 or not all(_decimal(n) for n in numbers):
                return False
            device = os.makedev(int(numbers[0]), int(numbers[1]))
            mount = int(fields[0])
            if mount in mounts:
                return False
            mounts[mount] = fstype
            if device in devices and devices[device] != fstype:
                devices[device] = None
            else:
                devices[device] = fstype
        if not mounts:
            return False
    except (OSError, ValueError, OverflowError):
        return False
    for device, fstype in devices.items():
        _filesystem_types[device] = fstype
    for mount, fstype in mounts.items():
        _filesystem_types["mount", mount] = fstype
    _filesystem_types[_MOUNT_TABLE_KEY] = "validated"
    return True


def _filesystem_type(device: int) -> str | None:
    """Unambiguous direct device/type evidence from the current mount table.

    The fast path remains a device memo plus namespace/watch checks. A missing
    device is not an alias: only :func:`_object_filesystem_type` can resolve it
    from an exact descriptor. Unreadable/ambiguous tables never grant trust.
    """

    with _mount_lock:
        before = _mount_state()
        if before is None:
            return None
        if _MOUNT_TABLE_KEY not in _filesystem_types and not _load_mount_types():
            return None
        found = _filesystem_types.get(device)
        return found if _mount_state() == before else None


def _same_mount_object(info: os.stat_result, current: os.stat_result) -> bool:
    return (info.st_mode == current.st_mode
            and metadata_version(info) == metadata_version(current))


def _descriptor_mount_id(descriptor: int, info: os.stat_result) -> tuple[int, str]:
    text = _proc_mount_text(f"/proc/self/fdinfo/{descriptor}", _FDINFO_MAX_BYTES)
    fields: dict[str, list[str]] = {}
    for line in text.splitlines():
        key, separator, value = line.partition(":")
        if not separator:
            raise ValueError("malformed fdinfo")
        fields.setdefault(key, []).append(value.strip())
    ids = fields.get("mnt_id", [])
    if len(ids) != 1 or not _decimal(ids[0]) or int(ids[0]) <= 0:
        raise ValueError("ambiguous descriptor mount ID")
    inodes = fields.get("ino", [])
    if inodes and (len(inodes) != 1 or not _decimal(inodes[0])
                   or int(inodes[0]) != info.st_ino):
        raise ValueError("descriptor inode changed")
    return int(ids[0]), text


def _object_filesystem_type(info: os.stat_result, *,
                            path: Path | str | None = None,
                            descriptor: int | None = None,
                            follow_symlinks: bool = True) -> str | None:
    """Type of an observed object, with exact descriptor evidence for #1358.

    Only a device absent from a validated table uses fallback. The supplied
    descriptor stays open in its caller; otherwise O_PATH opens the exact name
    with its original follow policy. Both fstats and the final path observation
    must match the observed mode and five-field version. Mount ID, namespace
    and watch generation are checked on both sides. Snapshot/final validation
    hold the memo lock; object IO does not. No inferred alias is kept.
    """

    direct = _filesystem_type(info.st_dev)
    if direct is not None:
        return direct
    if (path is None and descriptor is None) or not (
            statmod.S_ISREG(info.st_mode) or statmod.S_ISDIR(info.st_mode)):
        return None
    with _mount_lock:
        before = _mount_state()
        if (before is None or _MOUNT_TABLE_KEY not in _filesystem_types
                or info.st_dev in _filesystem_types):
            return None
        table = _filesystem_types
    # A stalled exact-object probe must not block unrelated direct lookups.
    # Table contents are used only under the lock after revalidating its state.
    owned = None
    try:
        if descriptor is None:
            if path is None:
                return None
            flags = os.O_PATH | os.O_CLOEXEC
            if not follow_symlinks:
                flags |= os.O_NOFOLLOW
            if statmod.S_ISDIR(info.st_mode):
                flags |= os.O_DIRECTORY
            owned = descriptor = os.open(path, flags)
        if not _same_mount_object(info, os.fstat(descriptor)):
            return None
        mount, text = _descriptor_mount_id(descriptor, info)
        if (_descriptor_mount_id(descriptor, info) != (mount, text)
                or not _same_mount_object(info, os.fstat(descriptor))):
            return None
        if path is not None:
            current = os.stat(path) if follow_symlinks else os.lstat(path)
            if not _same_mount_object(info, current):
                return None
        with _mount_lock:
            if (_mount_state() != before or _filesystem_types is not table
                    or _MOUNT_TABLE_KEY not in table or info.st_dev in table):
                return None
            found = table.get(("mount", mount))
            return found if found in _LOCAL_CLOCK_FILESYSTEMS else None
    except (OSError, ValueError, OverflowError):
        return None
    finally:
        if owned is not None:
            os.close(owned)


class FilesystemTrust(Protocol):
    """Filesystem evidence; file fences must precede their stat or open."""
    def directory_before(self, path): ...
    def directory_current(self, path): ...
    def file_fence(self): ...
    def file_version(self, info, fence, *, path=None, descriptor=None,
                     follow_symlinks=True): ...
    def directory_evidence(self, directory): ...
    def ancestor_evidence(self, directory): ...
    def directory_observations(self, directories): ...
    def holds(self, evidence): ...
    def scrape_view(self): ...


@dataclass(frozen=True)
class DirectoryEvidence:
    directory: str
    _proof: tuple


@dataclass(frozen=True)
class EvidenceSet:
    evidence: tuple[DirectoryEvidence, ...]

    def __iter__(self):
        return iter(self.evidence)


def _directory_metadata(path):
    try:
        info = os.lstat(path)
    except OSError:
        return None
    if not statmod.S_ISDIR(info.st_mode):
        return None
    return (info.st_dev, info.st_ino, info.st_mtime_ns,
            int(getattr(info, "st_ctime_ns", 0)))


@dataclass(frozen=True)
class _DirectoryObservations:
    trusted: dict[str, DirectoryEvidence | None]
    _before: dict[str, tuple | None]

    def unchanged(self):
        after = {name: _directory_metadata(name) for name in self._before}
        return self._before == after


class ScrapeTrustView:
    def __init__(self, trust):
        self._trust = trust
        self._versions = {}

    def available(self, directory):
        return self._current(os.fspath(directory)) is not None

    def _current(self, name):
        if name not in self._versions:
            self._versions[name] = self._trust.directory_current(name)
        return self._versions[name]

    def holds(self, evidence):
        if evidence is None:
            return False
        if isinstance(evidence, EvidenceSet):
            return all(self.holds(item) for item in evidence)
        return self._current(evidence.directory) == evidence._proof


class LinuxFilesystemTrust:
    def directory_before(self, path):
        if _COARSE_REALTIME is None:
            return None
        try:
            before = time.clock_gettime_ns(_COARSE_REALTIME)
            info = os.lstat(path)
        except OSError:
            return None
        if not statmod.S_ISDIR(info.st_mode):
            return None
        if _object_filesystem_type(info, path=path,
                                   follow_symlinks=False) not in _LOCAL_CLOCK_FILESYSTEMS:
            return None
        ctime = int(getattr(info, "st_ctime_ns", 0))
        if info.st_mtime_ns >= before or ctime >= before:
            return None
        return (info.st_dev, info.st_ino, info.st_mtime_ns, ctime)

    def file_fence(self):
        if _COARSE_REALTIME is None:
            return None
        return time.clock_gettime_ns(_COARSE_REALTIME)

    def file_version(self, info, fence, *, path=None, descriptor=None,
                     follow_symlinks=True):
        if fence is None or int(getattr(info, "st_ctime_ns", 0)) >= fence:
            return None
        trusted = _object_filesystem_type(
            info, path=path, descriptor=descriptor,
            follow_symlinks=follow_symlinks) in _LOCAL_CLOCK_FILESYSTEMS
        return metadata_version(info) if trusted else None

    def directory_current(self, path):
        name = os.fspath(path)
        try:
            info = os.lstat(name)
        except OSError:
            return None
        if not statmod.S_ISDIR(info.st_mode):
            return None
        if _object_filesystem_type(info, path=name,
                                   follow_symlinks=False) not in _LOCAL_CLOCK_FILESYSTEMS:
            return None
        return (info.st_dev, info.st_ino, info.st_mtime_ns,
                int(getattr(info, "st_ctime_ns", 0)))

    def directory_evidence(self, directory):
        proof = self.directory_before(directory)
        return (None if proof is None else
                DirectoryEvidence(os.fspath(directory), proof))

    def ancestor_evidence(self, directory):
        current = Path(directory)
        while True:
            evidence = self.directory_evidence(current)
            if evidence is not None:
                return evidence
            if os.path.lexists(current) or current.parent == current:
                return None
            current = current.parent

    def directory_observations(self, directories):
        trusted = {os.fspath(path): self.directory_evidence(path)
                   for path in directories}
        before = {name: (evidence._proof if evidence is not None
                         else _directory_metadata(name))
                  for name, evidence in trusted.items()}
        return _DirectoryObservations(trusted, before)

    def holds(self, evidence):
        if evidence is None:
            return False
        if isinstance(evidence, EvidenceSet):
            return all(self.holds(item) for item in evidence)
        return self.directory_current(evidence.directory) == evidence._proof

    def scrape_view(self):
        return ScrapeTrustView(self)


filesystem_trust = LinuxFilesystemTrust()
NoCapture = object()


@dataclass(frozen=True)
class _CaptureSlot:
    proof: tuple | None
    value: object = NoCapture
    candidates: object = NoCapture


class CaptureRead:
    def __init__(self, owner, name, identity, old):
        self._owner = owner
        self._name = name
        self._identity = identity
        self._old = old
        self._before = None
        self._current = None
        self._fence = None
        self._finished = False
        self.hit = False
        self.value = NoCapture
        self.previous = (NoCapture if old is None else
                         old.candidates if old.candidates is not NoCapture
                         else old.value)

    def __enter__(self):
        return self

    def __exit__(self, kind, error, traceback):
        self._abort()

    def _abort(self):
        if not self._finished:
            self._owner._finish(self, NoCapture)
            self._finished = True

    def commit(self, value, *, complete=True, candidates=NoCapture):
        if self._finished:
            raise RuntimeError("capture already finalized")
        try:
            proof = self._before
            if (not complete or proof is None
                    or self._owner.trust.directory_current(self._name) != proof):
                proof = None
            slot = (None if proof is None and candidates is NoCapture else
                    _CaptureSlot(proof, value if proof is not None else NoCapture,
                                 candidates))
            self._owner._finish(self, slot)
            self._finished = True
            return value
        except BaseException:
            self._abort()
            raise


class FileCaptureRead(CaptureRead):
    def file_version(self, info, *, path=None, descriptor=None,
                     follow_symlinks=True):
        return self._owner.trust.file_version(
            info, self._fence, path=path, descriptor=descriptor,
            follow_symlinks=follow_symlinks)


class DirectoryCaptures:
    """Listing proofs and untrusted candidates owned by one reader meaning.

    Acquisition detaches old state before I/O. Only the newest acquisition
    can publish; forget and retain revoke in-flight ownership too.
    """
    def __init__(self, trust=None):
        self.trust = filesystem_trust if trust is None else trust
        self._slots = {}
        self._active = {}
        self._lock = threading.Lock()

    def acquire(self, directory):
        return self._acquire(directory, CaptureRead)

    def acquire_files(self, directory):
        return self._acquire(directory, FileCaptureRead)

    def _acquire(self, directory, read_type):
        name = os.fspath(directory)
        identity = object()
        with self._lock:
            old = self._slots.pop(name, None)
            self._active[name] = identity
        read = read_type(self, name, identity, old)
        try:
            if old is not None:
                read._current = self.trust.directory_current(name)
                if (old.proof is not None and old.value is not NoCapture
                        and read._current == old.proof):
                    read.hit = True
                    read.value = old.value
                    read._before = old.proof
                    with self._lock:
                        if self._active.get(name) is identity:
                            self._slots[name] = old
                    return read
            read._before = self.trust.directory_before(name)
            if read_type is FileCaptureRead:
                read._fence = self.trust.file_fence()
            return read
        except BaseException:
            read._abort()
            raise

    def _finish(self, read, slot):
        with self._lock:
            if self._active.get(read._name) is read._identity:
                self._active.pop(read._name)
                if slot is not NoCapture:
                    self._slots.pop(read._name, None)
                    if slot is not None:
                        self._slots[read._name] = slot

    def forget(self, directory):
        name = os.fspath(directory)
        with self._lock:
            self._slots.pop(name, None)
            self._active.pop(name, None)

    def retain(self, directories):
        wanted = {os.fspath(directory) for directory in directories}
        with self._lock:
            for kept in (self._slots, self._active):
                for name in set(kept) - wanted:
                    del kept[name]

    def forget_tree(self, directory):
        name = os.fspath(directory)
        prefix = name.rstrip(os.sep) + os.sep
        with self._lock:
            for kept in (self._slots, self._active):
                for known in list(kept):
                    if known == name or known.startswith(prefix):
                        del kept[known]


class CensusMemo:
    """Parsed census documents, reused while their file version holds (#988).

    An egress takes its ownership censuses twice: once before the stage
    ownership lock, as a hint, and again under it, where the delete decision
    is made.  The pass under the lock lists every directory again and opens
    every document again, so a file that was added, removed, replaced or
    rewritten is seen before any decision uses it.  Only a document whose
    version is unchanged skips its read and parse, and the version is taken
    with ``fstat`` on the descriptor the pass just opened.  It is the
    publication gate's own reuse fence (``kept_reads.metadata_version``,
    #761): device, inode, size, mtime and ctime.

    A read that failed is never remembered: unreadable stays freshly
    unreadable.  A claim's derived stage paths are remembered by claim key,
    because they come from the sealed request and the data manifest, and both
    are immutable under their digests; the claim listing and each claim
    record are still read fresh.  One memo lives for one call; nothing
    carries over between calls.  (:class:`CensusIndex`, the tier loop's
    subclass, is the exception, and its docstring says what it keeps.  So
    is a staged publisher's claim-cover memo, which keeps only ``claims``
    for the publisher's run: ``stage_move._StagedPublisher``, #1089.)

    It counts what it parses and what it reuses (fragments and material
    sidecars; for pins, what it parses), so a caller can record how much
    census work ran inside its hold (:func:`_locked_parse_record`).
    """

    def __init__(self, *, trust=None) -> None:
        self.trust = filesystem_trust if trust is None else trust
        self.fragments: dict[str, tuple[tuple, dict[str, object]]] = {}
        self.paths: dict[int, frozenset[str]] = {}
        self.normalized: dict[int, frozenset[str]] = {}
        self.claims: dict[tuple, frozenset[str]] = {}
        self.sources: dict[tuple, frozenset[str]] = {}
        self.pins = _PinMemo()
        self.materials: dict[str, tuple[tuple, object]] = {}
        #: A remembered fragment's file, by the parsed document's identity.
        self.files: dict[int, str] = {}
        self.parses = 0     # fragments and material sidecars parsed
        self.reuses = 0     # fragments and material sidecars reused

    def counts(self) -> tuple[int, int]:
        """``(parsed, reused)`` so far: every document kind, pins included."""

        return self.parses + self.pins.parses, self.reuses

    def paths_of(self, document: Mapping[str, object]) -> frozenset[str] | None:
        """The stage paths a remembered fragment names, or ``None``."""

        return self.paths.get(id(document))

    def normalized_paths_of(self, document: Mapping[str, object],
                            ) -> frozenset[str] | None:
        """The same paths, ``normpath``-ed once per parse, or ``None``."""

        named = self.paths.get(id(document))
        if named is None:
            return None
        normalized = self.normalized.get(id(document))
        if normalized is None:
            normalized = frozenset(os.path.normpath(path) for path in named)
            self.normalized[id(document)] = normalized
        return normalized

    def version_of(self, document: Mapping[str, object],
                   ) -> tuple[str, tuple] | None:
        """The file and version a remembered fragment was read from, or ``None``.

        The version is the one the parse, or its latest reuse, was fenced
        on: ``fstat`` of the descriptor that read it (#1056 fences a skip
        checkpoint on the co-owner fragments a verdict relied on).
        """

        key = self.files.get(id(document))
        if key is None:
            return None
        hit = self.fragments.get(key)
        if hit is None or hit[1] is not document:
            return None
        return key, hit[0]

    def forget(self, key: str) -> None:
        """Drop one fragment file's parse, and the path set keyed by it."""

        old = self.fragments.pop(key, None)
        if old is not None:
            self.paths.pop(id(old[1]), None)
            self.normalized.pop(id(old[1]), None)
            self.files.pop(id(old[1]), None)


class _PinMemo(dict):
    """``reader_lease.live_for``'s pin memo, counting the pins it parses.

    ``live_for`` stores a pin's parse exactly once per parse and never on a
    reuse, so the number of stores is the number of pins it parsed.
    """

    def __init__(self) -> None:
        super().__init__()
        self.parses = 0

    def __setitem__(self, key: str, value: object) -> None:
        self.parses += 1
        super().__setitem__(key, value)


class CensusIndex(CensusMemo):
    """A census memo the tier loop keeps from one cycle to the next (#992).

    Every cycle took the residency census from nothing: it listed every
    namespace directory and parsed every fragment, 429 namespaces, 1,957
    produced-output directories and 240,705 fragment entries on the live
    shape, and nothing had changed since the cycle before.  This keeps what
    a census read, and re-reads only what changed.

    Two layers, both fences the census already trusts:

    * **A document** is reused while its file version holds -- device,
      inode, size, mtime and ctime, taken with ``fstat`` on the descriptor
      that read it (:class:`CensusMemo`, the publication gate's #761 fence).
    * **A directory** is not listed again while its ``lstat`` version equals
      a stamp taken before its last listing, and no document in it is opened
      while its ``stat`` version still equals the one it was parsed at
      (:func:`kept_reads.filesystem_trust.directory_before` has the argument for the
      directory; a stamp is refused inside the clock tick it was taken in,
      on a network filesystem, and on anything that is not a directory).
      The stamp is taken before the listing and compared again after it,
      and only an equal pair with no unreadable entry is remembered, the way
      a skip checkpoint is (:func:`_install_skip_checkpoint`).  So a census
      under the stage lock still sees a fragment added, removed, replaced or
      rewritten since the hint, as :class:`CensusMemo` promises, and costs
      one ``lstat`` per directory and one ``stat`` per fragment.

    A directory whose listing produced taint is never remembered: unreadable
    stays freshly unreadable.  A document that left its directory is
    forgotten when the directory is listed again, and a directory that left
    its level when the level is, so what is kept is bounded by what is on
    disk.

    Reader pins are not kept across calls: :func:`reader_lease.live_for`
    lists and opens every pin each census, and its memo remembers every pin
    it ever parsed, so each call that wants one takes a fresh
    :class:`_PinMemo` (:meth:`fresh_pins`).  Neither are the claim and source
    path sets, which only an egress reads.
    """

    def __init__(self, *, trust=None) -> None:
        super().__init__(trust=trust)
        self.stage_walk = DirectoryCaptures(self.trust)
        self.walk_listed = 0    # stage directories listed
        self.walk_kept = 0      # stage directories whose listing was reused
        self.walk_probed = 0    # stage files whose marks were read
        self.walk_reused = 0    # stage files whose classification was reused
        self.namespaces = DirectoryCaptures(self.trust)
        self.levels = DirectoryCaptures(self.trust)
        #: File keys the last listing of each namespace directory read.
        self._files: dict[str, frozenset[str]] = {}
        self.listed = 0       # directories listed
        self.kept = 0         # directories whose listing was reused
        #: Dead owners whose stale-mention census a skip checkpoint stood
        #: for, and dead owners censused (#1056): the cycle line counts both,
        #: because a skip emits no per-owner receipt.
        self.stale_skipped = 0
        self.stale_censused = 0
        #: Censused owners that were idle but whose skip checkpoint was
        #: refused, by reason (#1069): the receipt names the reason, and the
        #: cycle line counts them so a cycle that keeps re-censusing an
        #: owner shows why.
        self.stale_cache_refused: dict[str, int] = {}

    def fresh_pins(self) -> _PinMemo:
        """A pin memo for one call (see the class docstring)."""

        self.pins = _PinMemo()
        return self.pins

    def forget_stage(self, directory: str) -> None:
        self.stage_walk.forget_tree(directory)

    def namespace(self, directory):
        capture = self.namespaces.acquire(directory)
        try:
            if capture.hit and not self._documents_hold(capture.value[1]):
                self.namespaces._finish(capture, None)
                capture._finished = True
                capture = self.namespaces.acquire(directory)
            if capture.hit:
                self.kept += 1
            return capture
        except BaseException:
            self.namespaces._finish(capture, None)
            capture._finished = True
            raise

    def _documents_hold(self, keys: frozenset[str]) -> bool:
        """Whether every document a kept listing read is the version parsed.

        One ``stat`` per fragment.  The directory's stamp says no fragment
        was filed, removed or renamed; this says none was rewritten in
        place, made unreadable or restored to an older mtime, each of which
        moves the file's ctime.  No writer changes a fragment in place, but
        the #988 census under the stage lock promised to see one, and this
        keeps that promise for less than a listing costs.
        """

        fence = self.trust.file_fence()
        for key in keys:
            parsed = self.fragments.get(key)
            if parsed is None:
                return False
            try:
                info = os.stat(key)
            except OSError:
                return False
            if self.trust.file_version(info, fence, path=key) != parsed[0]:
                return False
        return True

    def remember_namespace(self, directory, capture, found, read, tainted):
        name = os.fspath(directory)
        self.listed += 1
        for gone in self._files.get(name, frozenset()) - read:
            self.forget(gone)
        self._files[name] = frozenset(read)
        capture.commit((list(found), frozenset(read)), complete=not tainted)

    def forget_namespace(self, directory: str) -> None:
        self.namespaces.forget(directory)
        for gone in self._files.pop(directory, frozenset()):
            self.forget(gone)

    def remember_level(self, directory, capture, children):
        name = os.fspath(directory)
        self.listed += 1
        present = {os.path.join(name, child) for child, kind in children
                   if kind == "dir"}
        prefix = name.rstrip(os.sep) + os.sep
        for known in [path for path in self._files
                      if path.startswith(prefix)
                      and os.sep not in path[len(prefix):]
                      and path not in present]:
            self.forget_namespace(known)
        capture.commit(list(children), complete=not any(
            kind.startswith("error") for _child, kind in children))


class DirectoryRecords:
    """Queue records re-read only where they changed (#992).

    The tier loop reads the same small records every cycle: every movement
    and prewarm receipt, and every ``ready/`` and ``claimed/`` record, three
    times over.  This keeps each directory's parsed records by file version
    and each directory's listing by the stamp of
    :func:`kept_reads.filesystem_trust.directory_before`, so a directory nothing was
    filed in since is not listed, and a record whose version holds is not
    read.  Every writer of these records files by rename
    (``pool._write_json_atomic``), which is what both fences stand on.

    A read that raises is never remembered, and the raise reaches the caller
    exactly as the plain read's would.  One reader keeps one meaning per
    directory: every read of a directory must pass the same ``select`` and
    ``parse``, because what it returns from a kept listing is what the first
    one parsed.

    A changed directory stats every name it lists, not only the names it has
    not seen.  These records are replaced under their own names -- a mover
    re-run under the same content-hash key files its receipt again, and a
    prewarm receipt is pruned and filed afresh -- and a file created after an
    unlink can be given the unlinked file's inode number, so neither the name
    nor the listing's inode number says a record is unchanged.  The #761
    version, ctime included, does -- once its clock tick has passed: two
    files in one tick can share all five fields, so a version is kept only
    under :func:`kept_reads.filesystem_trust.file_version`'s rule (#1045).
    """

    def __init__(self, *, trust=None) -> None:
        self.trust = filesystem_trust if trust is None else trust
        self._captures = DirectoryCaptures(self.trust)
        self._names = DirectoryCaptures(self.trust)
        self._generations: dict[str, int] = {}
        self.listed = 0
        self.kept = 0
        self.parsed = 0

    def names(self, directory: Path, *, select,
              missing_ok: bool = True) -> frozenset[str]:
        """The names in ``directory`` that ``select(name)`` keeps.

        A names-only listing, kept under the same stamp as :meth:`read`'s:
        creating, removing or renaming an entry moves the directory's
        ``mtime`` and ``ctime``, so while its stamp holds the set is the
        same.  Where no stamp can be trusted (a network filesystem, the tick
        the directory last changed in), the directory is listed every call,
        as a plain ``os.listdir`` is.  A directory that does not exist reads
        as empty and is not remembered; any other ``OSError`` reaches the
        caller, never an empty set.  ``missing_ok=False`` also raises on
        absence, for authoritative ledger censuses that must refuse it.
        """

        with self._names.acquire(directory) as capture:
            if capture.hit:
                self.kept += 1
                return capture.value
            try:
                listed = os.listdir(directory)
            except FileNotFoundError:
                if not missing_ok:
                    raise
                return frozenset()
            self.listed += 1
            found = frozenset(child for child in listed if select(child))
            return capture.commit(found)

    def retain(self, directories) -> None:
        """Forget the listing and records of every directory not named.

        For a caller that reads a changing set of directories -- one per
        withdrawal decision, say -- so what is kept stays bounded by what it
        still reads.  A forgotten directory is listed afresh on its next
        read.  Its :meth:`generation` counter is kept, so a number a caller
        remembered from before is never handed out again.
        """

        wanted = {str(directory) for directory in directories}
        self._captures.retain(wanted)
        self._names.retain(wanted)

    def forget(self, directory) -> None:
        self._captures.forget(directory)
        self._names.forget(directory)

    def generation(self, directory: Path) -> int:
        """A number that changes whenever ``directory``'s records change.

        Equal before and after a :meth:`read` exactly when that read returned
        the same names with the same record objects, so a pure function of
        the records can be remembered under it.
        """

        return self._generations.get(str(directory), 0)

    def read(self, directory: Path, *, select, parse, thaw=None,
             keep=None, stat_parse: bool = False,
             checkpoint=None, readers: int = 1) -> list[tuple[Path, object]]:
        """``(path, record)`` for each selected name, in name order.

        ``select(entry)`` takes an ``os.DirEntry`` and says whether the name
        is read at all; ``parse(path)`` reads one record and may return
        ``None`` (listed without a record) or raise.  ``keep(record)``, when
        given, says whether a parse may be remembered: one it refuses is
        returned but parsed again next time, and the directory is listed
        again next time too.  ``thaw``, when given, turns what ``parse``
        returned into what the caller receives, on every read: a caller that
        may change the record it is handed keeps the file's bytes here and
        parses them per read, so no change leaks into the next cycle.  A
        directory that does not exist reads as empty and is not remembered.
        With ``stat_parse`` the parse is called ``parse(path, info)``, where
        ``info`` is the ``os.stat`` its version was taken from (``None`` when
        that failed), so a parse that wants the file's metadata does not
        stat the file a second time.

        ``checkpoint``, when given, is called with no arguments after each
        entry the listing yields, once the listing is complete, and before
        each entry is stat-ed and, if it changed, parsed (#1148).  It is the
        tier loop's liveness checkpoint (`tier_loop.Liveness.checkpoint`):
        a directory on a loaded pool can take minutes to list and stat, and
        the tier records must be re-announced inside that read, not only
        after it.  A checkpoint is a clock read and a comparison, against a
        stat that costs milliseconds or more on a loaded pool, so each
        entry is its own batch.  What it raises reaches the caller as a
        parse's raise would.  Without it the read is as before.

        ``readers`` above 1 stats and parses a changed directory's entries on
        that many threads (#1153): a cold read costs one seek per entry, and
        the pool serves many seeks at once, so the read costs (entries /
        readers) x latency rather than entries x latency.  Only the ``stat``
        and the ``parse`` run on the readers, including the exact-object
        trust recheck before returning an earlier parse. Everything else runs
        on the calling thread, one entry at a time and in name order, exactly as a
        serial read runs it: the version comparison, ``keep``, the #1045
        fence, the counters, and ``checkpoint``, which is called before each
        entry's result is taken.  A reader takes a run of consecutive entries
        (:data:`READ_RUN_MAX` at most) and at most ``2 x readers`` runs are in
        flight, so a raise, first in name order as a serial read's is, stops
        the read with at most that many reads wasted, and nothing a reader
        did is kept.  ``parse`` must then be safe to call from several
        threads at once; ``pool.record_store.read_queue_record`` is.
        """

        out = self._read(directory, select=select, parse=parse, keep=keep,
                         stat_parse=stat_parse, checkpoint=checkpoint,
                         readers=readers)
        if thaw is None:
            return out
        return [(path, thaw(path, kept)) for path, kept in out]

    def _read(self, directory: Path, *, select, parse, keep,
              stat_parse: bool = False,
              checkpoint=None, readers: int = 1) -> list[tuple[Path, object]]:
        with self._captures.acquire_files(directory) as capture:
            return self._read_capture(directory, capture, select=select, parse=parse,
                                      keep=keep, stat_parse=stat_parse,
                                      checkpoint=checkpoint, readers=readers)

    def _read_capture(self, directory, capture, *, select, parse, keep,
                      stat_parse, checkpoint, readers):
        name = str(directory)
        if capture.hit:
            self.kept += 1
            return [(directory / child, record)
                    for child, (_version, record) in sorted(capture.value.items())]
        previous = capture.previous
        if previous is NoCapture or capture._current is None:
            previous = {}
        try:
            if checkpoint is None:
                entries = sorted((entry for entry in os.scandir(directory)
                                  if select(entry)),
                                 key=lambda entry: entry.name)
            else:
                # A listing is one stretch too, and on a loaded pool a long
                # one (#1148): checkpoint after every entry it yields.
                selected = []
                with os.scandir(directory) as listing:
                    for entry in listing:
                        if select(entry):
                            selected.append(entry)
                        checkpoint()
                entries = sorted(selected, key=lambda entry: entry.name)
        except (FileNotFoundError, NotADirectoryError):
            if capture._old is not None:
                self._generations[name] = self._generations.get(name, 0) + 1
            return []
        self.listed += 1
        if checkpoint is not None:
            checkpoint()      # the directory is listed
        records: dict[str, tuple[tuple, object]] = {}
        out: list[tuple[Path, object]] = []
        complete = True
        changed = False

        def entry_io(entry: os.DirEntry) -> tuple | None:
            """One entry's ``stat`` and, when its version moved, its parse.

            ``None`` for an entry gone since the listing; otherwise ``(info,
            version, parsed, record)``.  The only part of an entry's read
            that may run on a reader thread: it touches nothing but the file.
            """

            path = directory / entry.name
            info = None
            try:
                # The listing's own string, not ``path``: converting a
                # ``Path`` back to a string for every name was most of a
                # 30,000-name listing's cost.
                info = os.stat(entry.path)
                version = metadata_version(info)
            except FileNotFoundError:
                return None
            except OSError:
                version = None
            hit = previous.get(entry.name)
            if (info is not None and version is not None
                    and hit is not None and hit[0] == version
                    and capture.file_version(info, path=entry.path) is not None):
                return info, version, False, hit[1]
            record = parse(path, info) if stat_parse else parse(path)
            return info, version, True, record

        try:
            for entry, outcome in _entry_reads(entries, entry_io,
                                               readers=readers,
                                               checkpoint=checkpoint):
                if outcome is None:
                    continue
                path = directory / entry.name
                info, version, parsed, record = outcome
                if version is None:
                    complete = False      # the stat failed
                if parsed:
                    self.parsed += 1
                    changed = True
                    if keep is not None and not keep(record):
                        complete = False
                        version = None
                if version is not None:
                    # A version changed in the fence's tick could be matched
                    # by a new file under this name given the freed inode
                    # (#1045): not kept, so it is read again next pass, and
                    # neither is the listing, which would lack it.
                    version = capture.file_version(info, path=entry.path)
                    if version is None:
                        complete = False
                if version is not None:
                    records[entry.name] = (version, record)
                out.append((path, record))
        except BaseException:
            self._generations[name] = self._generations.get(name, 0) + 1
            raise
        if changed or set(previous) != set(records) or not complete:
            self._generations[name] = self._generations.get(name, 0) + 1
        capture.commit(records, complete=complete, candidates=records)
        return out


#: The most entries one reader takes at a time (#1153).  Handing an entry to
#: a reader thread and taking its result back cost about as much as a warm
#: ``stat`` (``tools/fleet/bench_tier_cold_start.py``: 12,000 entries of a
#: changed directory took 0.26 to 0.28 s one entry per handoff, 0.12 to
#: 0.17 s serially, on sparky), so a reader takes a run of consecutive
#: entries and hands back the run.  16 makes the handoff a sixteenth of the
#: warm cost.  It also bounds the longest wait between two checkpoints: 16
#: cold reads, about 4 s at the 4 reads a second dl380g10's loaded pool
#: gave a serial reader on 2026-09-25, against the 90 s horizon.
READ_RUN_MAX = 16


def _entry_reads(entries, entry_io, *, readers: int, checkpoint):
    """``(entry, entry_io(entry))`` in ``entries``' order, read by ``readers``.

    ``checkpoint`` runs on the calling thread before each entry's result is
    taken: serially that is before its ``stat``, as :meth:`DirectoryRecords.read`
    promised (#1148).  With readers, each reader takes a run of consecutive
    entries (:data:`READ_RUN_MAX` at most, fewer when there are too few
    entries to give every reader two runs) and reads them in order; the
    calling thread waits for a run before its first entry and takes the
    rest of it without waiting.  A stretch between two checkpoints is
    therefore at most one run's read.  A raise from ``entry_io`` ends its
    run there, and reaches the caller at that entry's turn, after the
    entries before it; the runs still being read are not waited for: they
    touch nothing but their files.
    """

    readers = min(int(readers), len(entries))
    if readers <= 1:
        for entry in entries:
            if checkpoint is not None:
                checkpoint()      # before each entry's stat and parse
            yield entry, entry_io(entry)
        return
    size = max(1, min(READ_RUN_MAX, len(entries) // (2 * readers)))

    def read_run(run):
        done = []
        for entry in run:
            try:
                done.append((entry, entry_io(entry), None))
            except BaseException as exc:  # noqa: BLE001 -- raised in order
                done.append((entry, None, exc))
                break
        return done

    runs = iter([entries[at:at + size] for at in range(0, len(entries), size)])
    # A bounded read-ahead: two runs per reader in flight, enough to keep
    # every reader busy while the caller takes the run before, and no more,
    # so a raise early in the directory does not wait on the rest of it.
    window = 2 * readers
    executor = ThreadPoolExecutor(max_workers=readers,
                                  thread_name_prefix="directory-records")
    pending: deque = deque()
    try:
        for run in runs:
            pending.append(executor.submit(read_run, run))
            if len(pending) >= window:
                break
        while pending:
            future = pending.popleft()
            if checkpoint is not None:
                checkpoint()      # before the run's first result is taken
            done = future.result()
            following = next(runs, None)
            if following is not None:
                pending.append(executor.submit(read_run, following))
            for at, (entry, outcome, raised) in enumerate(done):
                if at and checkpoint is not None:
                    checkpoint()  # before each later entry's result is taken
                if raised is not None:
                    raise raised
                yield entry, outcome
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
