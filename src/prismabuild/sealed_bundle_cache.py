"""Bounded, disposable host-local storage for deterministic Git bundles."""
from __future__ import annotations

from contextlib import ExitStack, contextmanager
import fcntl
import os
from pathlib import Path
import re
import shutil
import stat
import tempfile
from weakref import WeakKeyDictionary
from collections.abc import Callable, Mapping

from . import digest_primitives as digests, local_scratch


CACHE_ROOT = Path.home() / "tmp" / "prismabuild-sealed-bundles"
MAX_ENTRIES = 128
MAX_BYTES = 2 * 1024**3
_ADDRESS = re.compile(r"[0-9a-f]{64}")
_BUNDLE_AUTHORITY = object()
_VERIFIED_COPIES = WeakKeyDictionary()


class _VerifiedBundle:
    """One use of a specific verified private file; no minting authority stored."""
    __slots__ = ("__weakref__",)

    def __new__(cls, authority):
        if cls is not _VerifiedBundle or authority is not _BUNDLE_AUTHORITY:
            raise ValueError("bundle verification authority required")
        return super().__new__(cls)

    def __init_subclass__(cls, **kwargs):
        raise TypeError("verified bundles cannot be subclassed")

    def __copy__(self):
        raise TypeError("verified bundles cannot be copied")

    def __deepcopy__(self, memo):
        raise TypeError("verified bundles cannot be copied")

    def __reduce_ex__(self, protocol):
        raise TypeError("verified bundles cannot be serialized")


def _bundle_copy_stamp(info: os.stat_result) -> tuple[int, ...]:
    return (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid,
            info.st_nlink, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _verify_bundle_copy(path: Path, expected_bytes: int, expected_digest: str) -> _VerifiedBundle:
    before = path.lstat()
    if (not stat.S_ISREG(before.st_mode) or before.st_size != expected_bytes
            or digests.stream_sha256(path) != expected_digest
            or _bundle_copy_stamp(path.lstat()) != _bundle_copy_stamp(before)):
        raise ValueError("cached bundle copy differs from its stored digest")
    proof = _VerifiedBundle(_BUNDLE_AUTHORITY)
    _VERIFIED_COPIES[proof] = (path, _bundle_copy_stamp(before))
    return proof


def _install_bundle_copy(proof: _VerifiedBundle, destination: Path) -> None:
    if type(proof) is not _VerifiedBundle or proof not in _VERIFIED_COPIES:
        raise ValueError("an unconsumed verified bundle is required")
    path, verified_stat = _VERIFIED_COPIES.pop(proof)
    if _bundle_copy_stamp(path.lstat()) != verified_stat:
        raise ValueError("verified bundle changed before installation")
    os.replace(path, destination)


@contextmanager
def _bundle_cache_session(address: str, *, bounded_wait: bool):
    """Hold one of sixteen fixed locks; a caller deadline never waits on it."""
    with ExitStack() as held:
        root = None
        try:
            configured = os.environ.get("PRISMABUILD_BUNDLE_CACHE_ROOT")
            if configured == "off" or MAX_ENTRIES < 1 or MAX_BYTES < 1:
                raise ValueError("bundle cache disabled")
            candidate = Path(configured) if configured is not None else CACHE_ROOT
            if not candidate.is_absolute():
                raise ValueError("cache root must be absolute")
            # Check the existing ancestor before creating anything. The exact
            # opened cache directory is checked again, including bind mounts.
            ancestor = candidate
            while not ancestor.exists():
                ancestor = ancestor.parent
            ancestor_fd = os.open(ancestor.resolve(), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                local_scratch._descriptor_identity(ancestor_fd)
            finally:
                os.close(ancestor_fd)
            candidate.mkdir(mode=0o700, parents=True, exist_ok=True)
            fd = os.open(candidate, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            held.callback(os.close, fd)
            local_scratch._descriptor_identity(fd)
            info = os.fstat(fd)
            if info.st_uid != os.getuid() or info.st_mode & 0o077:
                raise ValueError("cache root must be private to its owner")
            root = Path(f"/proc/self/fd/{fd}")
            lock = held.enter_context(_bundle_cache_lock(
                root / f".lock-{address[0]}", nonblocking=bounded_wait))
            if lock is None:
                root = None
        except (OSError, ValueError, local_scratch.LocalScratchError):
            root = None
        yield root


@contextmanager
def _bundle_cache_lock(path: Path, *, nonblocking: bool = False):
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
            raise ValueError("cache lock is not an owned regular file")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | (fcntl.LOCK_NB if nonblocking else 0))
        except BlockingIOError:
            yield None
        else:
            yield fd
    finally:
        os.close(fd)


def _bundle_cache_inventory(root: Path) -> list[tuple[Path, int]]:
    entries = []
    for entry in root.iterdir():
        if entry.name.startswith(".pending-"):
            # Fillers hold .inventory for their entire staging lifetime, so
            # this is an abandoned fill, never another live writer's file.
            shutil.rmtree(entry)
        elif _ADDRESS.fullmatch(entry.name):
            info = entry.lstat()
            if not stat.S_ISDIR(info.st_mode):
                entry.unlink()
                continue
            size = sum(child.lstat().st_size for child in entry.iterdir())
            entries.append((entry, size))
    return sorted(entries, key=lambda item: item[0].stat().st_mtime_ns)


def _trim_bundle_cache(root: Path, *, reserve_bytes: int = 0, reserve_entries: int = 0):
    entries = _bundle_cache_inventory(root)
    total = sum(size for _, size in entries)
    while entries and (len(entries) + reserve_entries > MAX_ENTRIES
                       or total + reserve_bytes > MAX_BYTES):
        entry, size = entries.pop(0)
        shutil.rmtree(entry)
        total -= size


def _reuse_bundle_cache(root: Path, address: str, destination: Path, *, bounded_wait: bool) -> bool:
    """Verify the private copy that is actually installed, under the cache lock."""
    temporary = None
    try:
        with _bundle_cache_lock(root / ".inventory", nonblocking=bounded_wait) as locked:
            if locked is None:
                return False
            _trim_bundle_cache(root)
            entry = root / address
            with ExitStack() as held:
                meta_fd = os.open(entry / "digest.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                metadata = held.enter_context(os.fdopen(meta_fd, "rb"))
                if not stat.S_ISREG(os.fstat(meta_fd).st_mode):
                    return False
                record = digests.decode_strict_json(metadata.read(4097), where="bundle cache")
                if (not isinstance(record, dict) or set(record) != {"key", "sha256", "bytes"}
                        or record["key"] != address
                        or type(record["bytes"]) is not int
                        or not 0 < record["bytes"] <= MAX_BYTES
                        or not isinstance(record["sha256"], str)
                        or not _ADDRESS.fullmatch(record["sha256"])):
                    return False
                source_fd = os.open(entry / "bundle", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                source = held.enter_context(os.fdopen(source_fd, "rb"))
                info = os.fstat(source_fd)
                if not stat.S_ISREG(info.st_mode) or info.st_size != record["bytes"]:
                    return False
                fd, name = tempfile.mkstemp(prefix=".bundle-hit-", dir=destination.parent)
                temporary = Path(name)
                with os.fdopen(fd, "wb") as output:
                    shutil.copyfileobj(source, output, length=1 << 20)
            proof = _verify_bundle_copy(temporary, record["bytes"], record["sha256"])
            _install_bundle_copy(proof, destination)
            os.utime(entry, None)
            return True
    except (OSError, ValueError):
        return False
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass


def _store_bundle_cache(root: Path, address: str, source: Path, *, bounded_wait: bool) -> None:
    temporary = None
    try:
        size = source.stat().st_size
        record = {"key": address, "sha256": digests.stream_sha256(source), "bytes": size}
        encoded = digests.sorted_json(record).encode("utf-8")
        if size + len(encoded) > MAX_BYTES:
            return
        with _bundle_cache_lock(root / ".inventory", nonblocking=bounded_wait) as locked:
            if locked is None:
                return
            entry = root / address
            if entry.exists():
                shutil.rmtree(entry)
            _trim_bundle_cache(root, reserve_bytes=size + len(encoded), reserve_entries=1)
            temporary = Path(tempfile.mkdtemp(prefix=".pending-", dir=root))
            with (temporary / "bundle").open("wb") as output, source.open("rb") as original:
                shutil.copyfileobj(original, output, length=1 << 20)
                output.flush()
                os.fsync(output.fileno())
            with (temporary / "digest.json").open("wb") as output:
                output.write(encoded)
                output.flush()
                os.fsync(output.fileno())
            directory = os.open(temporary, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            os.rename(temporary, entry)
            os.fsync(int(root.name))
    except (OSError, ValueError):
        pass
    finally:
        if temporary is not None:
            shutil.rmtree(temporary, ignore_errors=True)


def seal_bundle_cached(destination: Path, identity: Mapping[str, object],
                       seal: Callable[[], None], *, bounded_wait: bool = False,
                       unchanged: Callable[[], bool] | None = None) -> None:
    """Reuse a digest-verified bundle, or invoke the unchanged sealer once."""
    try:
        address = digests.canonical_sha256(identity)
    except (TypeError, ValueError):
        seal()
        return
    with _bundle_cache_session(address, bounded_wait=bounded_wait) as root:
        if root is not None and _reuse_bundle_cache(root, address, destination, bounded_wait=bounded_wait):
            return
        seal()
        if root is not None and (unchanged is None or unchanged()):
            _store_bundle_cache(root, address, destination, bounded_wait=bounded_wait)
