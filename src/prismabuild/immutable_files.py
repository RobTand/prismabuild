"""Descriptor-anchored file custody and immutable first-writer publication."""
from __future__ import annotations

from collections.abc import Callable
import errno
import fnmatch
import os
from pathlib import Path
import stat
import sys
import tempfile
import time

if __package__:
    from .digest_primitives import ActionContractError, PrismaBuildError, new_sha256
else:
    from digest_primitives import ActionContractError, PrismaBuildError, new_sha256


class CASTamperError(PrismaBuildError):
    """An existing content-addressed entry failed verification."""


class CASUnavailableError(PrismaBuildError):
    """The content-addressed store could not be read reliably."""


#: The audit event a no-follow descent raises with the absolute directory it is
#: about to walk.  The descent opens ``/`` and then one ``dir_fd``-relative
#: component at a time, and CPython's ``open`` audit event carries no
#: ``dir_fd``, so without this no audit hook can tell which directory the walk
#: reaches.  ``tests/conftest.py`` refuses it under the fleet's live store
#: (#1019).  With no audit hook installed, ``sys.audit`` returns at once.
NOFOLLOW_DIRECTORY_AUDIT_EVENT = "prismabuild.open_directory_nofollow"


def absolute_nofollow_path(path: Path, *, where: str) -> Path:
    """Return an absolute lexical path that cannot walk through ``..``."""

    candidate = path if path.is_absolute() else Path.cwd() / path
    if ".." in candidate.parts:
        raise ActionContractError(f"{where} must not contain parent traversal")
    return candidate


def open_directory_nofollow(
    path: Path,
    *,
    where: str,
    create: bool = False,
    mode: int = 0o755,
) -> int:
    """Open/create an absolute directory using only mkdirat/openat operations."""

    path = absolute_nofollow_path(path, where=where)
    sys.audit(NOFOLLOW_DIRECTORY_AUDIT_EVENT, str(path))
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW
    try:
        descriptor = os.open(path.anchor, flags)
    except OSError as exc:
        raise CASUnavailableError(f"cannot open {where} filesystem root: {exc}") from exc
    try:
        for part in path.parts[1:]:
            created = False
            if create:
                try:
                    os.mkdir(part, mode=mode, dir_fd=descriptor)
                    created = True
                except FileExistsError:
                    pass
                except OSError as exc:
                    raise CASUnavailableError(
                        f"cannot create {where} component {part!r}: {exc}"
                    ) from exc
            try:
                child = os.open(part, flags, dir_fd=descriptor)
            except FileNotFoundError:
                raise
            except OSError as exc:
                if exc.errno in {errno.ENOTDIR, errno.ELOOP}:
                    raise CASTamperError(
                        f"{where} ancestor is not a real directory: {path}"
                    ) from exc
                raise CASUnavailableError(
                    f"cannot open {where} component {part!r}: {exc}"
                ) from exc
            try:
                if created:
                    os.fchmod(child, mode)
                    os.fsync(child)
                    os.fsync(descriptor)
            except OSError as exc:
                os.close(child)
                raise CASUnavailableError(
                    f"cannot durably create {where} component {part!r}: {exc}"
                ) from exc
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def assert_directory_identity(descriptor: int, path: Path, *, where: str) -> None:
    """Fail if the held directory is no longer the configured pathname."""

    try:
        current = open_directory_nofollow(path, where=where)
    except FileNotFoundError as exc:
        raise CASTamperError(f"{where} disappeared during operation: {path}") from exc
    try:
        held = os.fstat(descriptor)
        observed = os.fstat(current)
        if (held.st_dev, held.st_ino) != (observed.st_dev, observed.st_ino):
            raise CASTamperError(f"{where} changed during operation: {path}")
    finally:
        os.close(current)


def publish_immutable(
    path: Path,
    raw: bytes,
    *,
    prelink_verify: Callable[[], None] | None = None,
) -> bool:
    """Publish immutable bytes relative to a held no-follow parent FD.

    Create missing parent directories, verify the linked inode and parent
    identity, and return whether this writer won. Directory walk errors follow
    ``open_directory_nofollow``; other OS errors propagate to the caller.
    ``prelink_verify`` runs immediately before the publication link.
    """

    path = absolute_nofollow_path(path, where="publication path")
    directory_fd = open_directory_nofollow(
        path.parent, where="publication directory", create=True,
    )
    temporary_name: str | None = None
    try:
        descriptor, temporary_raw = tempfile.mkstemp(
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=f"/proc/self/fd/{directory_fd}",
        )
        temporary_name = Path(temporary_raw).name
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fchmod(handle.fileno(), 0o444)
            os.fsync(handle.fileno())
            temporary_identity = os.fstat(handle.fileno())
        if prelink_verify is not None:
            prelink_verify()
        try:
            os.link(
                temporary_name,
                path.name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
            won = True
        except FileExistsError:
            won = False
        os.fsync(directory_fd)
        if won:
            try:
                published_identity = os.stat(
                    path.name, dir_fd=directory_fd, follow_symlinks=False
                )
            except OSError as exc:
                raise CASTamperError(
                    f"published file changed before readback: {path}"
                ) from exc
            if (
                not stat.S_ISREG(published_identity.st_mode)
                or (
                    temporary_identity.st_dev,
                    temporary_identity.st_ino,
                )
                != (
                    published_identity.st_dev,
                    published_identity.st_ino,
                )
            ):
                raise CASTamperError(
                    f"published file changed before readback: {path}"
                )
        assert_directory_identity(
            directory_fd,
            path.parent,
            where="publication directory",
        )
        return won
    finally:
        if temporary_name is not None:
            try:
                os.unlink(temporary_name, dir_fd=directory_fd)
            except FileNotFoundError:
                pass
        os.close(directory_fd)


# NFS may return one internally inconsistent ctime/nlink snapshot while a
# first-writer hard link becomes visible.  Never accept that read: reopen the
# no-follow path and replay the entire read from byte zero, at most this many
# times, until one attempt is internally stable.
_STABLE_FILE_READ_ATTEMPTS = 3


# A whole-record file its writer replaces with ``os.replace`` (a progress
# record, a staged-wait record, a mover's landing report) can be replaced
# between a reader's open and its final identity check (#1017).  Such a
# reader reads again through a fresh resolution, at most this many times;
# a name that moves under every one of them is still refused.
_REPLACED_RECORD_READ_ATTEMPTS = 8


class ReplacedRecordError(CASTamperError):
    """A replace-only record exhausted its reads without stable bytes.

    The name can move to another regular inode (#1017). A rename can also
    change the incoming inode's ctime without a link-count change (#1648).
    Metadata-only instability returns no bytes. This remains a
    :class:`CASTamperError` so each caller chooses its response.
    ``ProgressWatch`` treats it as a poll that observed nothing (#1159);
    the tier announcement reader retries it.
    """


def _stable_identity(info: os.stat_result) -> tuple[int, ...]:
    """Metadata that must agree within one accepted regular-file read."""

    return (
        info.st_dev,
        info.st_ino,
        info.st_size,
        info.st_mode,
        info.st_uid,
        info.st_gid,
        info.st_mtime_ns,
        info.st_nlink,
        info.st_ctime_ns,
    )


def substantive_identity(info: os.stat_result) -> tuple[int, ...]:
    """Metadata changes that are tamper, not an NFS link-visibility retry."""

    identity = _stable_identity(info)
    return identity[:7]


def _read_file(
    path: Path,
    *,
    where: str,
    require_readonly: bool = False,
    max_bytes: int | None = None,
    replaced_leaf: bool = False,
) -> bytes:
    """Read one stable inode without following any pathname component.

    A ctime/nlink-only mismatch is discarded and retried through a fresh path
    resolution and FD.  No bytes from an unstable attempt are returned. When
    ``max_bytes`` is set, both the opening size and streamed byte count are
    bounded so a dishonest or racing size cannot cause unbounded allocation.

    ``replaced_leaf`` is for whole-record files their writer replaces with
    ``os.replace`` (#1017): a name that moved to a newer regular file during
    the read is read again from a fresh resolution, at most
    ``_REPLACED_RECORD_READ_ATTEMPTS`` times, and then refused with
    :class:`ReplacedRecordError`.  A held inode the replace unlinked while
    the name still resolved to it (ext4 moves the name after it drops the old
    target's link count, #1159) is returned as read: its bytes did not change
    and the name named it at the final check.  The no-follow, regular-file,
    size and substantive-identity checks are unchanged, and a name that moved
    to anything but a regular file is still tamper.  CAS objects, never
    replaced by design, keep the strict check (the default).

    A rename can change the incoming inode's ctime with its link count
    still one (#1648). Exhausted metadata-only retries also raise
    :class:`ReplacedRecordError`, without accepting the unstable bytes.
    """

    if max_bytes is not None and (type(max_bytes) is not int or max_bytes < 0):
        raise ActionContractError("stable regular-file read max_bytes is invalid")
    if not replaced_leaf:
        return _read_stable_file(
            path, where=where, require_readonly=require_readonly,
            max_bytes=max_bytes,
            replaced=False)
    for reread in range(_REPLACED_RECORD_READ_ATTEMPTS):
        try:
            return _read_stable_file(
                path, where=where, require_readonly=require_readonly,
                max_bytes=max_bytes,
                replaced=True)
        except ReplacedRecordError:
            if reread + 1 < _REPLACED_RECORD_READ_ATTEMPTS:
                continue
            raise ReplacedRecordError(
                f"{where} did not stabilize during "
                f"{_REPLACED_RECORD_READ_ATTEMPTS} read passes: {path}"
            ) from None
    raise AssertionError("replaced-record read loop did not return or raise")


def _read_stable_file(
    path: Path,
    *,
    where: str,
    require_readonly: bool,
    max_bytes: int | None,
    replaced: bool,
) -> bytes:
    """One :func:`_read_file` pass over its stable-read retries."""

    for attempt in range(_STABLE_FILE_READ_ATTEMPTS):
        descriptor, parent_fd = open_regular_nofollow(path, where=where)
        try:
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode):
                raise CASTamperError(f"{where} is not a regular file: {path}")
            if require_readonly and before.st_mode & 0o222:
                raise CASTamperError(f"{where} is writable: {path}")
            if max_bytes is not None and before.st_size > max_bytes:
                raise CASTamperError(f"{where} exceeds the byte bound: {path}")
            chunks: list[bytes] = []
            size = 0
            while True:
                chunk = os.read(descriptor, 1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if max_bytes is not None and size > max_bytes:
                    raise CASTamperError(f"{where} exceeds the byte bound: {path}")
                chunks.append(chunk)
            after = os.fstat(descriptor)
            if _stable_identity(before) != _stable_identity(after):
                if substantive_identity(
                    before
                ) != substantive_identity(after):
                    raise CASTamperError(
                        f"{where} changed substantively while it was read: {path}"
                    )
                assert_regular_identity(
                    descriptor, parent_fd, path, where=where, replaced=replaced)
                assert_directory_identity(
                    parent_fd, path.parent, where=f"{where} parent"
                )
                if replaced and after.st_nlink == 0:
                    # The replace unlinked the held inode, and the name
                    # still resolves to it: ext4 drops the old target's
                    # link count and moves its ctime before the VFS moves
                    # the name to the new inode (#1159).  Its bytes did not
                    # change (the substantive identity held) and the name
                    # named it at the final check, so they are the whole
                    # record.  A fresh read could land in the same window
                    # under a tight replace loop, and an unlinked inode
                    # is not the NFS link-visibility case the retry is for.
                    return b"".join(chunks)
                if attempt + 1 < _STABLE_FILE_READ_ATTEMPTS:
                    continue
                error = ReplacedRecordError if replaced else CASTamperError
                raise error(
                    f"{where} did not stabilize after "
                    f"{_STABLE_FILE_READ_ATTEMPTS} fresh reads: {path}"
                )
            assert_regular_identity(
                descriptor, parent_fd, path, where=where, replaced=replaced)
            assert_directory_identity(
                parent_fd, path.parent, where=f"{where} parent"
            )
            return b"".join(chunks)
        finally:
            os.close(descriptor)
            os.close(parent_fd)
    raise AssertionError("stable file read retry loop did not return or raise")


def read_stable_file(path: Path, *, where: str, max_bytes: int | None = None) -> bytes:
    """Read stable regular bytes through a held no-follow parent."""
    return _read_file(path, where=where, max_bytes=max_bytes)


def read_replaced_record(path: Path, *, where: str, max_bytes: int | None = None) -> bytes:
    """Read a mutable whole-record file, retrying regular-leaf replacement."""
    return _read_file(path, where=where, max_bytes=max_bytes, replaced_leaf=True)


def read_immutable_file(path: Path, *, where: str, max_bytes: int | None = None) -> bytes:
    """Read stable bytes that must remain read-only."""
    return _read_file(path, where=where, max_bytes=max_bytes, require_readonly=True)


#: ``O_NONBLOCK`` on a regular-file open exists to keep a FIFO or a device from
#: blocking before ``fstat`` can refuse it; POSIX gives it no meaning for a
#: regular file. On NFSv4 it acquires one anyway -- the client may answer
#: EAGAIN while a delegation is recalled -- so an open that is correct, and
#: would succeed a millisecond later, fails instead. The fleet's checkouts all
#: live on one NFS export and a dozen workers open the same closure stamp, which
#: is exactly the shape that provokes it: five actions in the live queue died
#: this way, each burning an attempt against its retry limit for a reason that
#: had nothing to do with the work.
_WOULD_BLOCK_OPEN_ATTEMPTS = 6


_WOULD_BLOCK_OPEN_BACKOFF_S = 0.02


def open_retrying_would_block(
    path: Path | str | bytes,
    flags: int,
    *,
    dir_fd: int | None = None,
) -> int:
    """``os.open``, with "would block" treated as transient, not terminal.

    Every other error is re-raised untouched, so each caller keeps its own
    mapping from errno to the contract violation it means; only EAGAIN is
    retried, and only for as long as the backoff allows. A path that keeps
    saying "would block" still fails -- a blocking open would be the wrong
    answer for a FIFO, which is what the flag is protecting against.
    """

    delay = _WOULD_BLOCK_OPEN_BACKOFF_S
    last = _WOULD_BLOCK_OPEN_ATTEMPTS - 1
    for attempt in range(_WOULD_BLOCK_OPEN_ATTEMPTS):
        try:
            if dir_fd is None:
                return os.open(path, flags)
            return os.open(path, flags, dir_fd=dir_fd)
        except OSError as exc:
            transient = exc.errno in {errno.EAGAIN, errno.EWOULDBLOCK}
            if not transient or attempt == last:
                raise
            time.sleep(delay)
            delay *= 2
    raise AssertionError("unreachable")


def read_regular_leaf(path: Path, *, where: str) -> bytes:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = open_retrying_would_block(path, flags)
    except OSError as exc:
        raise ActionContractError(f"cannot open {where} as a regular file: {path}") from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ActionContractError(f"{where} is not a regular file: {path}")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        after = os.fstat(descriptor)
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mode,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mode,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ):
            raise ActionContractError(f"{where} changed while it was read: {path}")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def identify_regular_leaf(path: Path, *, where: str) -> tuple[str, int]:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = open_retrying_would_block(path, flags)
    except OSError as exc:
        raise ActionContractError(
            f"cannot open {where} as a regular file: {path}"
        ) from exc
    digest = new_sha256()
    size = 0
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ActionContractError(f"{where} is not a regular file: {path}")
        while True:
            chunk = os.read(descriptor, 4 * 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
        after = os.fstat(descriptor)
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mode,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mode,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ):
            raise ActionContractError(f"{where} changed while it was read: {path}")
        return digest.hexdigest(), size
    finally:
        os.close(descriptor)


def open_regular_nofollow(path: Path, *, where: str) -> tuple[int, int]:
    """Open a regular-file candidate relative to its held no-follow parent."""

    path = absolute_nofollow_path(path, where=where)
    parent_fd = open_directory_nofollow(path.parent, where=f"{where} parent")
    try:
        candidate = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        os.close(parent_fd)
        raise
    except OSError as exc:
        os.close(parent_fd)
        raise CASUnavailableError(f"cannot inspect {where}: {path}: {exc}") from exc
    if not stat.S_ISREG(candidate.st_mode):
        os.close(parent_fd)
        raise CASTamperError(
            f"cannot open {where} as a real regular file: {path}"
        )
    flags = (
        os.O_RDONLY
        | os.O_CLOEXEC
        | os.O_NOFOLLOW
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        descriptor = open_retrying_would_block(
            path.name, flags, dir_fd=parent_fd)
    except FileNotFoundError:
        os.close(parent_fd)
        raise
    except OSError as exc:
        os.close(parent_fd)
        if exc.errno in {errno.ENOTDIR, errno.ELOOP, errno.EISDIR}:
            raise CASTamperError(
                f"cannot open {where} as a real regular file: {path}"
            ) from exc
        raise CASUnavailableError(f"cannot open {where}: {path}: {exc}") from exc
    return descriptor, parent_fd


def assert_regular_identity(
    descriptor: int, parent_fd: int, path: Path, *, where: str,
    replaced: bool = False,
) -> None:
    """Fail if a held regular inode is no longer its canonical leaf name.

    With ``replaced`` (a replace-only record, #1017), a name that now
    resolves to a different *regular* file raises :class:`ReplacedRecordError`
    so the reader can read again; anything else stays tamper.
    """

    try:
        observed = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError as exc:
        raise CASTamperError(f"{where} changed during operation: {path}") from exc
    except OSError as exc:
        # An unreadable name is not evidence that the held inode changed.
        # Recovery callers may retire work on tamper, but must retain it when
        # an NFS or permission fault prevents this final identity check.
        raise CASUnavailableError(f"cannot inspect {where}: {path}: {exc}") from exc
    held = os.fstat(descriptor)
    if not stat.S_ISREG(observed.st_mode):
        raise CASTamperError(f"{where} changed during operation: {path}")
    if (held.st_dev, held.st_ino) != (observed.st_dev, observed.st_ino):
        if replaced:
            raise ReplacedRecordError(
                f"{where} was replaced during the read: {path}")
        raise CASTamperError(f"{where} changed during operation: {path}")


def file_identity_nofollow(
    path: Path,
    *,
    where: str,
    expected_sha256: str | None = None,
    expected_bytes: int | None = None,
) -> tuple[str, int, int]:
    """Hash one stable inode reached through a fresh held no-follow parent.

    Every retry reopens the full path and hashes from byte zero.  A wrong
    expected address is immediate tamper even when metadata is also unstable.
    """

    for attempt in range(_STABLE_FILE_READ_ATTEMPTS):
        descriptor, parent_fd = open_regular_nofollow(path, where=where)
        digest = new_sha256()
        size = 0
        try:
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode):
                raise CASTamperError(f"{where} is not a regular file: {path}")
            while True:
                chunk = os.read(descriptor, 4 * 1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
                size += len(chunk)
            observed_digest = digest.hexdigest()
            if expected_bytes is not None and size != expected_bytes:
                raise CASTamperError(
                    f"{where} content differs from its expected byte count: {path}"
                )
            if expected_sha256 is not None and observed_digest != expected_sha256:
                raise CASTamperError(
                    f"{where} content differs from its expected address: {path}"
                )
            after = os.fstat(descriptor)
            if _stable_identity(before) != _stable_identity(after):
                if substantive_identity(
                    before
                ) != substantive_identity(after):
                    raise CASTamperError(
                        f"{where} changed substantively while it was read: {path}"
                    )
                assert_regular_identity(descriptor, parent_fd, path, where=where)
                assert_directory_identity(
                    parent_fd, path.parent, where=f"{where} parent"
                )
                if attempt + 1 < _STABLE_FILE_READ_ATTEMPTS:
                    continue
                raise CASTamperError(
                    f"{where} did not stabilize after "
                    f"{_STABLE_FILE_READ_ATTEMPTS} fresh reads: {path}"
                )
            assert_regular_identity(descriptor, parent_fd, path, where=where)
            assert_directory_identity(
                parent_fd, path.parent, where=f"{where} parent"
            )
            return observed_digest, size, before.st_mode
        finally:
            os.close(descriptor)
            os.close(parent_fd)
    raise AssertionError("stable file identity retry loop did not return or raise")


def unlink_nofollow(path: Path, *, where: str) -> None:
    """Unlink a leaf only through its current no-follow parent directory."""

    try:
        directory_fd = open_directory_nofollow(path.parent, where=f"{where} parent")
    except FileNotFoundError:
        return
    try:
        try:
            os.unlink(path.name, dir_fd=directory_fd)
        except FileNotFoundError:
            return
        os.fsync(directory_fd)
        assert_directory_identity(
            directory_fd, path.parent, where=f"{where} parent"
        )
    finally:
        os.close(directory_fd)


def scan_optional(directory: Path):
    """List a directory that another process may be deleting underneath us.

    Every ledger scan walks entries a concurrent worker is free to remove --
    ``release`` is documented safe to call twice, so two of them will have one
    ``rmdir`` the directory the other is mid-iteration over.  At one worker per
    box that never happens; at sixty it happens within minutes, and the worker
    dies with ``FileNotFoundError`` on ``iterdir`` rather than losing a lease
    gracefully.  A directory that vanished held nothing this caller can still
    act on, so the honest answer is an empty listing, not an exception.
    """

    try:
        return sorted(directory.iterdir())
    except (FileNotFoundError, NotADirectoryError):
        return []

def match_optional(directory: Path, pattern: str):
    """``Path.glob`` with the same disappearing-directory contract as `scan_optional`."""

    try:
        return sorted(directory.glob(pattern))
    except (FileNotFoundError, NotADirectoryError):
        return []

def scan_required(directory: Path):
    """List a directory LOUDLY: every error propagates to the caller.

    The counterpart to :func:`scan_optional` for authoritative censuses (mint
    grow, dead-name reclaim): a holder this listing cannot read makes
    the census unknown, and the caller must refuse the decision --
    retain rather than mint -- instead of narrowing the view.  Reads
    (``capacity``, ``available``) keep the tolerant :func:`scan_optional`, and a
    holder they cannot read is counted as HELD, never as free (#936):
    ``capacity`` becomes a lower bound, and the shrink
    (``retire_free_capacity``) counts the holder as holding every minted
    name that is neither free nor dead, because an under-counted held
    total would retire too few.  Neither reads an unreadable holder's
    tokens as free.
    """

    return sorted(directory.iterdir())

def match_required(directory: Path, pattern: str):
    """Error-visible name match for authoritative censuses.

    Same names as :func:`match_optional` on a readable tree (``Path.glob`` uses
    ``fnmatch`` for the leaf), but unreadable directories raise
    instead of reading empty: a hidden live holder must abort a
    reissue, never excuse one.  See :func:`scan_required`.
    """

    return sorted(path for path in scan_required(directory)
                  if fnmatch.fnmatchcase(path.name, pattern))
