"""Worker-local discovery of pbtest roots; owned_tree owns attempt expiry.

The private v1 registry contains symlinks named by layout, device and inode.
Their targets locate roots, but only the pinned identity authorizes a sweep.
This stdlib-only source also travels inside the carried shard program.
"""
from __future__ import annotations

import os
from pathlib import Path
import pwd
import random
import re
import stat

REGISTRY: Path | None = None
SWEEP_INTERVAL_S = 3600
ROOTS_PER_SWEEP = 64
_ROOT_ENTRY = re.compile(r"(tmpdir|basetemp)-([0-9]+)-([0-9]+)")
_ROOT_DIRECTORY = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC


def pbtest_registry_path() -> Path:
    return REGISTRY if REGISTRY is not None else (
        Path(pwd.getpwuid(os.geteuid()).pw_dir) / ".local/state/prismabuild/pbtest-roots-v1")


def _open_pbtest_registry(*, create: bool = False) -> int:
    registry = pbtest_registry_path()
    if create:
        registry.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = os.open(registry, _ROOT_DIRECTORY)
    try:
        info = os.fstat(descriptor)
        if info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise ValueError("pbtest root registry is not private to this uid")
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def register_pbtest_root(root_fd: int, *, basetemp: bool = False) -> None:
    """Register the already-open root before an attempt becomes visible.

    Atomic replacement publishes a complete entry, so concurrent attempts
    need no second owner lock or startup/expiry serialization. Refreshing
    its target also handles an inode reused after an old root was removed.
    The attempt's existing lock remains the only deletion authority.
    """
    info = os.fstat(root_fd)
    root = os.readlink(f"/proc/self/fd/{root_fd}")
    if root == "/" or not os.path.isabs(root) or os.path.realpath(root, strict=True) != root:
        raise ValueError("pbtest root must be a real absolute directory")
    if info.st_uid != os.geteuid() and not (info.st_uid == 0 and info.st_mode & stat.S_ISVTX):
        raise ValueError("pbtest root has an unknown owner")
    named = os.stat(root, follow_symlinks=False)
    if (named.st_dev, named.st_ino) != (info.st_dev, info.st_ino):
        raise ValueError("pbtest root identity changed before registration")
    entry = f"{'basetemp' if basetemp else 'tmpdir'}-{info.st_dev}-{info.st_ino}"
    registry = _open_pbtest_registry(create=True)
    temporary = f".{entry}.{os.urandom(8).hex()}"
    try:
        os.symlink(root, temporary, dir_fd=registry)
        os.replace(temporary, entry, src_dir_fd=registry, dst_dir_fd=registry)
    finally:
        try:
            os.unlink(temporary, dir_fd=registry)
        except FileNotFoundError:
            pass
        finally:
            os.close(registry)


def sweep_pbtest_roots(reap) -> list[tuple[str, list[str], str | None]]:
    """Run the existing bounded reaper on registered, unchanged roots.

    A missing registry is normal on a worker with no new pbtest attempts.
    Invalid or unreadable entries retain their roots and report the refusal.
    """
    try:
        registry = _open_pbtest_registry()
    except FileNotFoundError:
        return []
    try:
        entries = [name for name in os.listdir(registry) if _ROOT_ENTRY.fullmatch(name)]
        random.shuffle(entries)
        reports = []
        for entry in entries[:ROOTS_PER_SWEEP]:
            root_fd = -1
            try:
                layout, device, inode = _ROOT_ENTRY.fullmatch(entry).groups()
                root = os.readlink(entry, dir_fd=registry)
                if root == "/" or not os.path.isabs(root) or os.path.realpath(root, strict=True) != root:
                    raise ValueError("registered pbtest root is no longer a real directory")
                root_fd = os.open(root, _ROOT_DIRECTORY)
                info = os.fstat(root_fd)
                if (info.st_dev, info.st_ino) != (int(device), int(inode)):
                    raise ValueError("registered pbtest root identity changed")
                if info.st_uid != os.geteuid() and not (
                        info.st_uid == 0 and info.st_mode & stat.S_ISVTX):
                    raise ValueError("registered pbtest root has an unknown owner")
                if layout == "basetemp":
                    removed = reap(root_fd, re.compile("[0-9a-f]{64}"),
                                   inner=re.compile("[0-9a-f]{32}"))
                else:
                    removed = reap(root_fd, re.compile("pb-[a-z0-9_]{8}"))
                reports.append((entry, removed, None))
            except (OSError, ValueError) as exc:
                reports.append((entry, [], str(exc)))
            finally:
                if root_fd >= 0:
                    os.close(root_fd)
        return reports
    finally:
        os.close(registry)
