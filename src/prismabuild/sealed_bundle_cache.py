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


def _default_bundle_cache_root() -> Path:
    directory = Path(tempfile.gettempdir())
    try:
        local_scratch.root_identity(str(directory))
    except (OSError, ValueError, local_scratch.LocalScratchError):
        directory = Path.home() / "tmp"
    return directory / "prismabuild-sealed-bundles"


CACHE_ROOT = _default_bundle_cache_root()
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
    if not stat.S_ISREG(before.st_mode) or before.st_size != expected_bytes:
        raise ValueError("cached bundle copy differs from its stored size")
    actual_digest = digests.stream_sha256(path)
    if (actual_digest != expected_digest
            or _bundle_copy_stamp(path.lstat()) != _bundle_copy_stamp(before)):
        raise ValueError("cached bundle copy differs from its stored digest")
    proof = _VerifiedBundle(_BUNDLE_AUTHORITY)
    _VERIFIED_COPIES[proof] = (path, _bundle_copy_stamp(before), actual_digest)
    return proof


def _install_bundle_copy(proof: _VerifiedBundle, destination: Path) -> None:
    if type(proof) is not _VerifiedBundle or proof not in _VERIFIED_COPIES:
        raise ValueError("an unconsumed verified bundle is required")
    path, verified_stat, verified_digest = _VERIFIED_COPIES.pop(proof)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        if (_bundle_copy_stamp(os.fstat(fd)) != verified_stat
                or digests.stream_sha256(Path(f"/proc/self/fd/{fd}")) != verified_digest
                or _bundle_copy_stamp(os.fstat(fd)) != verified_stat
                or _bundle_copy_stamp(path.lstat()) != verified_stat):
            raise ValueError("verified bundle changed before installation")
        os.replace(path, destination)
    finally:
        os.close(fd)


@contextmanager
def _bundle_cache_session():
    """Pin a private cache directory on qualified host-local disk."""
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
                local_scratch.descriptor_identity(ancestor_fd)
            finally:
                os.close(ancestor_fd)
            candidate.mkdir(mode=0o700, parents=True, exist_ok=True)
            fd = os.open(candidate, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            held.callback(os.close, fd)
            local_scratch.descriptor_identity(fd)
            info = os.fstat(fd)
            if info.st_uid != os.getuid() or info.st_mode & 0o077:
                raise ValueError("cache root must be private to its owner")
            root = Path(f"/proc/self/fd/{fd}")
        except (OSError, ValueError, local_scratch.LocalScratchError):
            root = None
        yield root


@contextmanager
def _bundle_cache_lock(path: Path, *, nonblocking: bool = False, shared: bool = False):
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1:
            raise ValueError("cache lock is not an owned regular file")
        try:
            fcntl.flock(fd, (fcntl.LOCK_SH if shared else fcntl.LOCK_EX)
                        | (fcntl.LOCK_NB if nonblocking else 0))
        except BlockingIOError:
            yield None
        else:
            yield fd
    finally:
        os.close(fd)


def _bundle_lock_is_named(fd: int, path: Path) -> bool:
    try:
        held, named = os.fstat(fd), path.lstat()
        return (held.st_dev, held.st_ino) == (named.st_dev, named.st_ino)
    except FileNotFoundError:
        return False


def _remove_idle_bundle_flight(root: Path, address: str) -> None:
    pin = root / f".pin-{address}"
    with _bundle_cache_lock(pin, nonblocking=True) as locked:
        if locked is not None and _bundle_lock_is_named(locked, pin):
            (root / f".lock-{address}").unlink(missing_ok=True)
            pin.unlink(missing_ok=True)


@contextmanager
def _bundle_cache_flight(root: Path, address: str, *, bounded_wait: bool):
    """Pin the address lock's inode until every owner and waiter has left."""
    pin = root / f".pin-{address}"
    for _ in range(3):
        held = ExitStack()
        try:
            guard = held.enter_context(_bundle_cache_lock(pin, shared=True, nonblocking=bounded_wait))
            if guard is not None and not _bundle_lock_is_named(guard, pin):
                held.close()
                continue
            locked = (held.enter_context(_bundle_cache_lock(root / f".lock-{address}",
                        nonblocking=bounded_wait)) if guard is not None else None)
        except (OSError, ValueError):
            held.close()
            yield False
            return
        try:
            yield locked is not None
        finally:
            held.close()
            try:
                _remove_idle_bundle_flight(root, address)
            except (OSError, ValueError):
                pass
        return
    yield False


def _bundle_cache_record(entry: Path, address: str) -> dict:
    fd = os.open(entry / "digest.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as metadata:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > 4096:
            raise ValueError("cache digest record is not a bounded regular file")
        record = digests.decode_strict_json(metadata.read(4097), where="bundle cache")
    if (not isinstance(record, dict) or set(record) != {"key", "sha256", "bytes"}
            or record["key"] != address or type(record["bytes"]) is not int
            or not 0 < record["bytes"] <= MAX_BYTES
            or not isinstance(record["sha256"], str) or not _ADDRESS.fullmatch(record["sha256"])):
        raise ValueError("cache digest record does not match its address")
    return record


def _bundle_cache_inventory(root: Path) -> tuple[list[tuple[Path, int]], int, int]:
    entries, reserved_bytes, reserved_entries = [], 0, 0
    for entry in root.iterdir():
        if entry.name.startswith(".pending-"):
            with _bundle_cache_lock(entry / ".owner", nonblocking=True) as owner:
                if owner is not None:
                    shutil.rmtree(entry)
                else:
                    # Setup and this read both hold .inventory; live fills
                    # have already published their full space reservation.
                    address = entry.name[len(".pending-"):].split("-", 1)[0]
                    record = _bundle_cache_record(entry, address)
                    reserved_bytes += record["bytes"] + (entry / "digest.json").stat().st_size
                    reserved_entries += 1
        elif entry.name.startswith(".pin-") and _ADDRESS.fullmatch(entry.name[5:]):
            _remove_idle_bundle_flight(root, entry.name[5:])
        elif _ADDRESS.fullmatch(entry.name):
            info = entry.lstat()
            if not stat.S_ISDIR(info.st_mode):
                entry.unlink()
                continue
            size = sum(child.lstat().st_size for child in entry.iterdir())
            entries.append((entry, size))
    return sorted(entries, key=lambda item: item[0].stat().st_mtime_ns), reserved_bytes, reserved_entries


def _trim_bundle_cache(root: Path, *, reserve_bytes: int = 0, reserve_entries: int = 0):
    entries, pending_bytes, pending_entries = _bundle_cache_inventory(root)
    total = sum(size for _, size in entries) + pending_bytes
    while entries and (len(entries) + pending_entries + reserve_entries > MAX_ENTRIES
                       or total + reserve_bytes > MAX_BYTES):
        entry, size = entries.pop(0)
        shutil.rmtree(entry)
        total -= size
    return (len(entries) + pending_entries + reserve_entries <= MAX_ENTRIES
            and total + reserve_bytes <= MAX_BYTES)


@contextmanager
def _opened_bundle_cache_entry(root: Path, address: str):
    source, record = None, None
    try:
        with _bundle_cache_lock(root / ".inventory", nonblocking=True, shared=True) as locked:
            if locked is not None:
                entry = root / address
                record = _bundle_cache_record(entry, address)
                fd = os.open(entry / "bundle", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                source = os.fdopen(fd, "rb")
                info = os.fstat(fd)
                if not stat.S_ISREG(info.st_mode) or info.st_size != record["bytes"]:
                    raise ValueError("cache payload does not match its stored size")
                os.utime(entry, None)
    except (OSError, ValueError):
        if source is not None:
            source.close()
        source = None
    try:
        yield (source, record) if source is not None else None
    finally:
        if source is not None:
            source.close()


def _reuse_bundle_cache(root: Path, address: str, destination: Path) -> bool:
    """Copy from a pinned cache inode and verify outside the inventory lock."""
    temporary = None
    try:
        with _opened_bundle_cache_entry(root, address) as opened:
            if opened is None:
                return False
            source, record = opened
            fd, name = tempfile.mkstemp(prefix=".bundle-hit-", dir=destination.parent)
            temporary = Path(name)
            with os.fdopen(fd, "wb") as output:
                shutil.copyfileobj(source, output, length=1 << 20)
            proof = _verify_bundle_copy(temporary, record["bytes"], record["sha256"])
            _install_bundle_copy(proof, destination)
            return True
    except (OSError, ValueError):
        return False
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass


def _store_bundle_cache(root: Path, address: str, source: Path) -> None:
    temporary, owner = None, None
    try:
        size = source.stat().st_size
        record = {"key": address, "sha256": digests.stream_sha256(source), "bytes": size}
        encoded = digests.sorted_json(record).encode("utf-8")
        with _opened_bundle_cache_entry(root, address) as opened:
            if opened is not None:
                existing, previous = opened
                if previous == record and digests.stream_sha256(Path(f"/proc/self/fd/{existing.fileno()}")) == record["sha256"]:
                    return
        with _bundle_cache_lock(root / ".inventory", nonblocking=True) as locked:
            if locked is None:
                return
            if size + len(encoded) > MAX_BYTES:
                _trim_bundle_cache(root)
                return
            reservation = size + len(encoded)
            if not _trim_bundle_cache(root, reserve_bytes=reservation, reserve_entries=1):
                return
            space = os.statvfs(root)
            free_bytes = space.f_bavail * space.f_frsize
            filesystem_bytes = space.f_blocks * space.f_frsize
            if free_bytes - reservation < max(MAX_BYTES, (filesystem_bytes + 9) // 10):
                return
            temporary = Path(tempfile.mkdtemp(prefix=f".pending-{address}-", dir=root))
            owner = ExitStack()
            owner.enter_context(_bundle_cache_lock(temporary / ".owner"))
            (temporary / "digest.json").write_bytes(encoded)
        with (temporary / "bundle").open("wb") as output, source.open("rb") as original:
            shutil.copyfileobj(original, output, length=1 << 20)
        with _bundle_cache_lock(root / ".inventory", nonblocking=True) as locked:
            if locked is None:
                return
            entry = root / address
            if entry.exists():
                shutil.rmtree(entry)
            (temporary / ".owner").unlink()
            os.rename(temporary, entry)
            temporary = None
    except (OSError, ValueError):
        pass
    finally:
        if temporary is not None:
            try:
                with _bundle_cache_lock(root / ".inventory", nonblocking=True) as locked:
                    if locked is not None:
                        shutil.rmtree(temporary, ignore_errors=True)
            except (OSError, ValueError):
                pass
        if owner is not None:
            owner.close()


def seal_bundle_cached(destination: Path, identity: Mapping[str, object],
                       seal: Callable[[], None], *, bounded_wait: bool = False,
                       unchanged: Callable[[], bool] | None = None) -> None:
    """Reuse a digest-verified bundle, or invoke the unchanged sealer once."""
    try:
        address = digests.canonical_sha256(identity)
    except (TypeError, ValueError):
        seal()
        return
    with _bundle_cache_session() as root:
        if root is not None and _reuse_bundle_cache(root, address, destination):
            return
        if root is None:
            seal()
            return
        try:
            with _bundle_cache_lock(root / ".inventory", nonblocking=True) as locked:
                if locked is not None:
                    _trim_bundle_cache(root)
        except (OSError, ValueError):
            pass
        with _bundle_cache_flight(root, address, bounded_wait=bounded_wait) as locked:
            if locked and _reuse_bundle_cache(root, address, destination):
                return
            seal()
            if locked and (unchanged is None or unchanged()):
                _store_bundle_cache(root, address, destination)
