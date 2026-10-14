"""Retained directory records and census memos fenced by local filesystem evidence."""
from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, suppress
import contextvars
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


def trusted_directory_stamp(path: Path) -> tuple[int, int, int, int] | None:
    """A directory version a listing taken after this call may be reused under.

    ``(device, inode, mtime, ctime)``, or ``None`` when no listing of this
    directory may ever be reused (#992).  The argument that a later ``lstat``
    returning the same version proves the listing still current:

    * The coarse realtime clock ``T0`` is read first, then the directory's
      ``lstat``.  The version is refused unless both its mtime and its ctime
      are strictly before ``T0``.
    * Every change to a directory's entries -- a create, an unlink, a rename
      in or out -- stamps its mtime and ctime with the filesystem's current
      time.  On a filesystem in :data:`_LOCAL_CLOCK_FILESYSTEMS` that time is
      read from the same clock, and a change made after ``T0`` reads a value
      at or after ``T0``.  So once a version with both stamps before ``T0`` is
      observed, any later entry change moves the version off it, and a
      directory replaced by another has another inode.
    * A listing taken after this call is therefore still the directory's
      entries for as long as a fresh ``lstat`` returns this version.

    The refusal closes the one hole a bare stat comparison leaves: two
    changes in one clock tick share a timestamp (a coarse tick is 1 ms on
    the Sparks), so a listing taken between them would carry a version the
    second change does not move.  A refused directory is listed again on
    every pass until a tick has gone by with no change to it.

    Two things are outside the argument and stated here.  A file rewritten in
    place does not touch its directory; every writer of the documents this
    guards writes by rename (``residency_map.write_fragment``, the queue's
    ``_write_json_atomic``), which does.  And a backward step of the realtime
    clock could in principle stamp a later change with an earlier value; to
    reproduce a remembered version it would have to land on the same
    nanosecond in both mtime and ctime.
    """

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


def version_fence() -> int | None:
    """The coarse realtime clock, read before a listing or an open (#1045).

    What :func:`keepable_version` compares a file's ctime with.  ``None``
    where the coarse clock is not the one this kernel stamps file times from
    (off Linux), and then no file version is kept.
    """

    if _COARSE_REALTIME is None:
        return None
    return time.clock_gettime_ns(_COARSE_REALTIME)


def keepable_version(info: "os.stat_result", fence: int | None, *,
                      local: dict[int, bool] | None = None,
                      path: Path | str | None = None,
                      descriptor: int | None = None,
                      follow_symlinks: bool = True,
                      ) -> tuple[int, int, int, int, int] | None:
    """A file's #761 version when a record read at it may be kept, else ``None``.

    :func:`trusted_directory_stamp`'s rule, applied to one file (#1045).
    ``fence`` is :func:`version_fence`, read before the ``stat`` or
    ``fstat`` that gave ``info``.  A kept record is reused while a later
    ``stat`` returns the same ``(dev, ino, size, mtime_ns, ctime_ns)``, and
    that proves the file unchanged only if no change can reproduce it: a file
    unlinked and another created under its name in one clock tick can be
    given the freed inode number, and with the same size all five fields
    match.  So the version is refused unless its ctime is strictly before
    ``fence``: every later change to the file, a new file under its name
    included, is stamped at or after the fence and so moves the ctime.  A
    refused version is not kept, and the record is read again next pass, as
    a refused directory is listed again.

    Only on a filesystem in :data:`_LOCAL_CLOCK_FILESYSTEMS`, whose file
    times come from the clock the fence reads; a network filesystem's come
    from the server's, so none of its versions is kept. ``path`` preserves
    the stat's follow policy; ``descriptor`` is the still-open fstat source.
    Without either, only a direct mount-table device match can grant trust.
    ``local`` remains accepted for compatibility but is not trust evidence:
    a listing-local device boolean cannot survive a mount/namespace change.
    """

    if fence is None or int(getattr(info, "st_ctime_ns", 0)) >= fence:
        return None
    trusted = _object_filesystem_type(
        info, path=path, descriptor=descriptor,
        follow_symlinks=follow_symlinks) in _LOCAL_CLOCK_FILESYSTEMS
    return metadata_version(info) if trusted else None


def current_directory_version(path: Path) -> tuple[int, int, int, int] | None:
    """The same four fields as :func:`trusted_directory_stamp`, trust re-checked.

    What a remembered stamp is compared with.  ``None`` for anything that is
    not a directory or cannot be stat-ed, and for a directory whose device is
    not currently on a filesystem in :data:`_LOCAL_CLOCK_FILESYSTEMS`; none of
    those ever equals a stamp.  The trust re-check is what keeps the stamp's
    proof across a mount change (#1208): a remembered stamp was only sound
    while the directory's times came from this kernel's clock, so a comparison
    that skipped the check would go on reusing a listing after its filesystem
    left :data:`_LOCAL_CLOCK_FILESYSTEMS`.  :func:`_filesystem_type` remembers
    the answer per device only until the kernel says the mount table changed,
    so an unchanged table costs a memo lookup, not a mount-table read.
    """

    return _directory_version_at(os.fspath(path))


def _directory_version_at(name: str) -> tuple[int, int, int, int] | None:
    """:func:`current_directory_version` of a path already a string.

    The census compares one of these per directory per decision. Direct
    device matches use ``lstat`` and the namespace/watch-fenced table memo,
    with no path object; unlisted devices need the exact descriptor probe.
    """

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

    def __init__(self) -> None:
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
      (:func:`kept_reads.trusted_directory_stamp` has the argument for the
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

    def __init__(self) -> None:
        super().__init__()
        #: Stage directory (as ``str``) -> ``(stamp, children, entries)`` for
        #: :func:`_unattributed_candidates`.  ``stamp`` is
        #: :func:`kept_reads.trusted_directory_stamp`'s version, or ``None``
        #: when no listing of the directory may be reused; ``children`` are
        #: the real subdirectories the walk recursed into; ``entries`` maps a
        #: file name to ``(version, kind, mover)``, its classification from
        #: the last probe.  A stale entry is a probe hint only: the directory
        #: is listed again, and a file's classification is reused only while
        #: its own version holds (#1045).
        self.stage_walk: dict[str, tuple[
            tuple | None, tuple[str, ...],
            dict[str, tuple[tuple | None, str, str]]]] = {}
        self.walk_listed = 0    # stage directories listed
        self.walk_kept = 0      # stage directories whose listing was reused
        self.walk_probed = 0    # stage files whose marks were read
        self.walk_reused = 0    # stage files whose classification was reused
        #: Namespace directory -> ``(stamp, [(mover, document)], file keys)``.
        self.namespaces: dict[str, tuple[tuple, list[tuple[str, dict[str, object]]],
                                         frozenset[str]]] = {}
        #: Level directory -> ``(stamp, [(name, kind)])``, ``kind`` one of
        #: ``dir``, ``link``, ``other`` or an ``error: ...`` string.
        self.levels: dict[str, tuple[tuple, list[tuple[str, str]]]] = {}
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

    def stage_hit(self, directory: str,
                  ) -> tuple[tuple[str, ...],
                             dict[str, tuple[tuple | None, str, str]]] | None:
        """One stage directory's listing, when its stamp still holds.

        ``None`` for a directory never listed, one whose stamp cannot be
        trusted, or one whose version moved: the caller lists it again.
        """

        kept = self.stage_walk.get(directory)
        if kept is None or kept[0] is None:
            return None
        if current_directory_version(directory) != kept[0]:
            return None
        self.walk_kept += 1
        return kept[1], kept[2]

    def stage_memo(self, directory: str,
                   ) -> tuple[tuple | None, tuple[str, ...],
                              dict[str, tuple[tuple | None, str, str]]] | None:
        """A stage directory's last listing, stale stamps included.

        A probe hint only: the caller that cannot reuse the listing reads
        the directory again and compares each file's version before reusing
        its classification.
        """

        return self.stage_walk.get(directory)

    def remember_stage(self, directory: str, stamp: tuple | None,
                       children: "list[str]",
                       entries: dict[str, tuple[tuple | None, str, str]],
                       previous_children: "list[str] | None" = None, *,
                       retry: bool = False) -> None:
        """Record one stage listing, and drop what left it.

        The stamp is kept only while a fresh ``lstat`` still returns it, and
        only when it was trusted before the listing; otherwise ``None``,
        which carries the entries as a probe hint and nothing more.  A
        ``retry`` listing -- one where a name could not be stat-ed -- never
        keeps its stamp, because the failure did not move the directory's
        version and the name must be tried again rather than lost.  A child
        directory that is no longer in the listing loses its own listing and
        every descendant's, so the memo owns no name that is not on disk.
        """

        self.walk_listed += 1
        name = str(directory)
        if previous_children is not None:
            for gone in set(previous_children) - set(children):
                self.forget_stage(gone)
        if (retry or stamp is None
                or current_directory_version(directory) != stamp):
            stamp = None
        self.stage_walk[name] = (stamp, tuple(children), entries)

    def refresh_stage(self, directory: str,
                      entries: dict[str, tuple[tuple | None, str, str]],
                      *, retry: bool = False,
                      ) -> None:
        """Replace a reused listing's per-file classifications.

        ``retry`` drops the stamp, so a name whose ``lstat`` failed is listed
        again on the next pass instead of being hidden by the listing that
        could not read it.
        """

        kept = self.stage_walk.get(str(directory))
        if kept is not None:
            self.stage_walk[str(directory)] = (None if retry else kept[0],
                                               kept[1], entries)

    def forget_stage(self, directory: str) -> None:
        """Drop one stage directory's listing and every descendant's."""

        name = str(directory)
        self.stage_walk.pop(name, None)
        prefix = name.rstrip(os.sep) + os.sep
        for known in [path for path in self.stage_walk
                      if path.startswith(prefix)]:
            self.stage_walk.pop(known, None)

    def namespace_hit(self, directory: Path,
                      ) -> list[tuple[str, dict[str, object]]] | None:
        """The fragments a remembered listing of ``directory`` holds, or ``None``."""

        kept = self.namespaces.get(str(directory))
        if kept is None:
            return None
        if (current_directory_version(directory) != kept[0]
                or not self._documents_hold(kept[2])):
            self.namespaces.pop(str(directory), None)
            return None
        self.kept += 1
        return kept[1]

    def _documents_hold(self, keys: frozenset[str]) -> bool:
        """Whether every document a kept listing read is the version parsed.

        One ``stat`` per fragment.  The directory's stamp says no fragment
        was filed, removed or renamed; this says none was rewritten in
        place, made unreadable or restored to an older mtime, each of which
        moves the file's ctime.  No writer changes a fragment in place, but
        the #988 census under the stage lock promised to see one, and this
        keeps that promise for less than a listing costs.
        """

        fence = version_fence()
        for key in keys:
            parsed = self.fragments.get(key)
            if parsed is None:
                return False
            try:
                info = os.stat(key)
            except OSError:
                return False
            if keepable_version(info, fence, path=key) != parsed[0]:
                return False
        return True

    def remember_namespace(self, directory: Path, stamp: tuple | None,
                           found: list[tuple[str, dict[str, object]]],
                           read: set[str], tainted: bool) -> None:
        """Record one listing of ``directory``: what it found and what it read."""

        name = str(directory)
        self.listed += 1
        for gone in self._files.get(name, frozenset()) - read:
            self.forget(gone)
        self._files[name] = frozenset(read)
        if (stamp is None or tainted
                or current_directory_version(directory) != stamp):
            self.namespaces.pop(name, None)
            return
        self.namespaces[name] = (stamp, list(found), frozenset(read))

    def forget_namespace(self, directory: str) -> None:
        """Drop a namespace directory that left its level, and its documents."""

        self.namespaces.pop(directory, None)
        for gone in self._files.pop(directory, frozenset()):
            self.forget(gone)

    def level_hit(self, directory: Path) -> list[tuple[str, str]] | None:
        """A remembered classification of ``directory``'s children, or ``None``."""

        kept = self.levels.get(str(directory))
        if kept is None:
            return None
        if current_directory_version(directory) != kept[0]:
            self.levels.pop(str(directory), None)
            return None
        self.kept += 1
        return kept[1]

    def remember_level(self, directory: Path, stamp: tuple | None,
                       children: list[tuple[str, str]]) -> None:
        """Record one listing of a level; forget namespaces that left it."""

        name = str(directory)
        self.listed += 1
        present = {os.path.join(name, child) for child, kind in children
                   if kind == "dir"}
        prefix = name.rstrip(os.sep) + os.sep
        for known in [path for path in self._files
                      if path.startswith(prefix)
                      and os.sep not in path[len(prefix):]
                      and path not in present]:
            self.forget_namespace(known)
        if (stamp is None
                or any(kind.startswith("error") for _child, kind in children)
                or current_directory_version(directory) != stamp):
            self.levels.pop(name, None)
            return
        self.levels[name] = (stamp, list(children))


class DirectoryRecords:
    """Queue records re-read only where they changed (#992).

    The tier loop reads the same small records every cycle: every movement
    and prewarm receipt, and every ``ready/`` and ``claimed/`` record, three
    times over.  This keeps each directory's parsed records by file version
    and each directory's listing by the stamp of
    :func:`kept_reads.trusted_directory_stamp`, so a directory nothing was
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
    under :func:`kept_reads.keepable_version`'s rule (#1045).
    """

    def __init__(self) -> None:
        #: Directory -> ``(stamp or None, {name: (version, record)})``.
        self._directories: dict[str, tuple[tuple | None,
                                           dict[str, tuple[tuple, object]]]] = {}
        #: Directory -> a count that moves whenever its record set changes.
        self._generations: dict[str, int] = {}
        #: Directory -> ``(stamp, names)`` for :meth:`names`.
        self._names: dict[str, tuple[tuple, frozenset[str]]] = {}
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

        name = str(directory)
        kept = self._names.get(name)
        if kept is not None and current_directory_version(directory) == kept[0]:
            self.kept += 1
            return kept[1]
        self._names.pop(name, None)
        stamp = trusted_directory_stamp(directory)
        try:
            listed = os.listdir(directory)
        except FileNotFoundError:
            if not missing_ok:
                raise
            return frozenset()
        self.listed += 1
        found = frozenset(child for child in listed if select(child))
        if stamp is not None and current_directory_version(directory) == stamp:
            self._names[name] = (stamp, found)
        return found

    def retain(self, directories) -> None:
        """Forget the listing and records of every directory not named.

        For a caller that reads a changing set of directories -- one per
        withdrawal decision, say -- so what is kept stays bounded by what it
        still reads.  A forgotten directory is listed afresh on its next
        read.  Its :meth:`generation` counter is kept, so a number a caller
        remembered from before is never handed out again.
        """

        wanted = {str(directory) for directory in directories}
        for kept in (self._directories, self._names):
            for name in [name for name in kept if name not in wanted]:
                del kept[name]

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
        threads at once; ``pool._read_json`` is.
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
        name = str(directory)
        kept = self._directories.get(name)
        current = None
        if kept is not None:
            current = current_directory_version(directory)
        if kept is not None and kept[0] is not None and current == kept[0]:
            self.kept += 1
            return [(directory / child, record)
                    for child, (_version, record) in sorted(kept[1].items())]
        previous = kept[1] if kept is not None else {}
        if kept is not None and current is None:
            # The kept listing's proof left with its filesystem's trust
            # (#1208): its stamp was only sound while the directory's times
            # came from this kernel's clock.  What it parsed is then no
            # evidence for any entry, even one whose bare stat still reads the
            # same, so every entry is read again -- as a plain read on that
            # filesystem reads it -- and the fresh parses are kept only if
            # the fence below lets them.  A stamp refused for the tick the
            # directory changed in leaves ``current`` non-None: the
            # filesystem still answers for its clock, so the entries' own
            # versions are evidence and are reused.
            previous = {}
        stamp = trusted_directory_stamp(directory)
        # Read before any entry is stat-ed: a version whose ctime is not
        # strictly before it is not kept (#1045, :func:`keepable_version`).
        fence = version_fence()
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
            if self._directories.pop(name, None) is not None:
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
                    and keepable_version(info, fence, path=entry.path) is not None):
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
                    version = keepable_version(info, fence, path=entry.path)
                    if version is None:
                        complete = False
                if version is not None:
                    records[entry.name] = (version, record)
                out.append((path, record))
        except BaseException:
            self._directories.pop(name, None)
            self._generations[name] = self._generations.get(name, 0) + 1
            raise
        if changed or set(previous) != set(records) or not complete:
            self._generations[name] = self._generations.get(name, 0) + 1
        if (not complete or stamp is None
                or current_directory_version(directory) != stamp):
            stamp = None
        self._directories[name] = (stamp, records)
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
