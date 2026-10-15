"""Immutable first-writer publication and descriptor-anchored directory operations."""
from __future__ import annotations

from collections.abc import Callable
import errno
import os
from pathlib import Path
import stat
import sys
import tempfile

if __package__:
    from .digest_primitives import ActionContractError, PrismaBuildError
else:
    from digest_primitives import ActionContractError, PrismaBuildError


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
    where: str | None = None,
    create_parent: bool = True,
) -> bool:
    """Publish immutable bytes relative to a held no-follow parent FD.

    ``where`` names the publication for the directory-identity asserts and
    for OSError mapping.  ``None`` keeps this module's historical spellings
    (``"publication path"`` / ``"publication directory"``) and lets OSError
    propagate raw, which the pool/core/slurm_lane callers rely on.  A string
    threads the caller's name through instead and maps OSError to
    CASUnavailableError -- exactly the contract slurm's retired
    ``_atomic_publish_nofollow`` offered its two callers.

    ``create_parent`` selects the historical parent-open mode.  ``True`` is
    this module's mkdir walk; ``False`` skips creation because the caller
    pre-ensured its parents -- slurm's mode, where auto-creating CAS
    structure would mask a wiped mount.  One owner, two named parameters,
    each preserving one side's history.

    The directory diagnostics now come from this module's helpers on every
    path: same exception types at every fail-closed point, but the message
    text follows core's spellings (slurm's local _open/_assert helpers,
    which differ in more than messages, stay in place for a follow-on
    slice -- see issue #1295).
    """

    path = absolute_nofollow_path(
        path, where="publication path" if where is None else where
    )
    directory_fd = open_directory_nofollow(
        path.parent,
        where="publication directory" if where is None else where,
        create=create_parent,
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
            where="publication directory" if where is None else where,
        )
        return won
    except OSError as exc:
        if where is None:
            raise
        raise CASUnavailableError(
            f"cannot publish {where}: {path}: {exc}"
        ) from exc
    finally:
        if temporary_name is not None:
            try:
                os.unlink(temporary_name, dir_fd=directory_fd)
            except FileNotFoundError:
                pass
        os.close(directory_fd)
