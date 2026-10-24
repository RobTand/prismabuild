"""Private host-local immutable source generations (not execution sandboxes).

The per-snapshot mutex serializes publication, acquisition and retirement. Each
lease also holds its own kernel shared flock for its entire lifetime. No PID,
heartbeat or on-disk verification marker can substitute for that reader lock or
for a core proof. A killed builder leaves only an unadvertised directory; a
killed reader releases its lock in the kernel.
"""
from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import json
import math
import os
from pathlib import Path
import re
import stat
import sys
import threading
import time
from typing import Any
import uuid

from . import core as pb, immutable_files
from .immutable_files import MaterializationContractError

# Deliberately outside materialize.CHECKOUT_TREE and its sibling-owner reaper.
_NAMESPACE = "shared-sources-v1"
_VERSION = "prismabuild.shared-source-materializer.v1"
_KEY = re.compile(r"[0-9a-f]{64}")
_GENERATION = re.compile(r"g-[0-9a-f]{32}")
_IDLE_SECONDS = 24 * 60 * 60
_SCAN_LIMIT = 32
_STATES: dict[tuple[int, str], _GenerationState] = {}
# Retain bounded scandir cursors so repeated bounded reaps eventually visit all
# keys, including keys behind a large number of permanently active siblings.
_SCANS: OrderedDict[tuple[int, str], Any] = OrderedDict()
_SCAN_LOCK = threading.Lock()


def _namespace(local_checkout_root: str | Path, *, create: bool) -> Path:
    base = Path(local_checkout_root)
    if not base.is_absolute() or base == Path("/") or ".." in base.parts:
        raise MaterializationContractError(
            "PRISMABUILD_LOCAL_CHECKOUT_ROOT must be an absolute non-root path")
    root = base / _NAMESPACE
    if create:
        root.mkdir(parents=True, exist_ok=True)
    if root.is_symlink():
        raise MaterializationContractError("shared source namespace is a symlink")
    return root.resolve()


def _open_lock(path: Path) -> int:
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
    observed = os.fstat(fd)
    if not stat.S_ISREG(observed.st_mode) or observed.st_nlink != 1:
        os.close(fd)
        raise MaterializationContractError("shared source lock is not a private regular file")
    return fd


@contextmanager
def _exclusive(path: Path, *, wait: bool = True) -> Iterator[bool]:
    fd = _open_lock(path)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | (0 if wait else fcntl.LOCK_NB))
        except BlockingIOError:
            yield False
        else:
            yield True
    finally:
        os.close(fd)


def _source_entries(path: Path) -> list[Path]:
    """A bounded, resumable scan, with a bounded number of retained cursors."""
    key = (os.getpid(), str(path))
    with _SCAN_LOCK:
        cursor = _SCANS.pop(key, None)
        if cursor is None:
            cursor = os.scandir(path)
        result = []
        try:
            for _ in range(_SCAN_LIMIT):
                try:
                    entry = next(cursor)
                except StopIteration:
                    cursor.close()
                    return result
                result.append(Path(entry.path))
        except BaseException:
            cursor.close()
            raise
        _SCANS[key] = cursor
        while len(_SCANS) > 64:
            _, old = _SCANS.popitem(last=False)
            old.close()
        return result


def _source_census(repository: Path) -> tuple[tuple[object, ...], ...]:
    """No content hashing or Git: every inode, including all of .git, is gated.

    ctime_ns is essential: rewriting the same number of bytes and restoring
    mtime must still invalidate a proof. atime is intentionally not identity.
    Directory metadata detects entry additions/removals; inode identity detects
    replacements. Symlinks are lstat'd, never followed during this walk.
    """
    pending = [repository]
    rows = []
    try:
        while pending:
            path = pending.pop()
            observed = path.lstat()
            mode = observed.st_mode
            if not (stat.S_ISREG(mode) or stat.S_ISDIR(mode) or stat.S_ISLNK(mode)):
                raise MaterializationContractError("shared source contains a special inode")
            if not stat.S_ISLNK(mode) and mode & 0o222:
                raise MaterializationContractError("shared source is no longer read-only")
            rows.append((
                os.fsencode(path.relative_to(repository)).hex(), observed.st_dev,
                observed.st_ino, mode, observed.st_nlink, observed.st_uid,
                observed.st_gid, observed.st_size, observed.st_mtime_ns,
                observed.st_ctime_ns,
            ))
            if stat.S_ISDIR(mode):
                with os.scandir(path) as entries:
                    pending.extend(Path(entry.path) for entry in entries)
    except OSError as exc:
        raise MaterializationContractError("cannot inspect shared source generation") from exc
    return tuple(sorted(rows))


def _freeze(repository: Path) -> None:
    pending = [repository]
    directories = []
    while pending:
        path = pending.pop()
        mode = path.lstat().st_mode
        if stat.S_ISLNK(mode):
            continue
        if stat.S_ISDIR(mode):
            directories.append(path)
            with os.scandir(path) as entries:
                pending.extend(Path(entry.path) for entry in entries)
        elif stat.S_ISREG(mode):
            path.chmod(0o444 | (mode & 0o111))
        else:
            raise MaterializationContractError("shared source contains a special inode")
    for path in reversed(directories):
        path.chmod(0o555)


@dataclass
class _GenerationState:
    repository: Path
    checkout_root: Path
    generation: str
    census: tuple[tuple[object, ...], ...]
    proof: object
    invalid: bool = False

    def check(self) -> None:
        if self.invalid:
            raise MaterializationContractError("shared source generation was invalidated")
        try:
            if _source_census(self.repository) != self.census:
                raise MaterializationContractError("shared source generation changed")
        except MaterializationContractError:
            self.invalid = True
            raise


class _SourceLease:
    """Only a live lease may use a cached proof; no proof is exposed to callers."""

    def __init__(self, state: _GenerationState, cas_root: str, reader: int):
        self._state = state
        self._cas_root = cas_root
        self._reader_fd = reader
        self._active = True
        self._guard = threading.Lock()

    @property
    def checkout_root(self) -> Path:
        return self._state.checkout_root

    @property
    def generation(self) -> str:
        return self._state.generation

    def preflight(self, action: object, *, worker_launcher_identity: object | None = None) -> dict[str, object]:
        with self._guard:
            if not self._active:
                raise MaterializationContractError("shared source lease has been released")
            self._state.check()
            result = pb._preflight_with_generation(
                action, checkout_root=self.checkout_root, cas_root=self._cas_root,
                proof=self._state.proof, worker_launcher_identity=worker_launcher_identity)
            self._state.check()
            return result

    def duplicate_reader_fd(self) -> int:
        """Pin this exact generation across worker/action descendant lifetime.

        Copies must be closed, never explicitly unlocked: flock's open file
        description remains shared across dup/fork/exec.
        """
        with self._guard:
            if not self._active:
                raise MaterializationContractError("shared source lease has been released")
            self._state.check()
            return os.dup(self._reader_fd)

    def _close(self) -> None:
        with self._guard:
            self._active = False


def _current(bucket: Path) -> dict[str, str] | None:
    path = bucket / "current.json"
    try:
        raw = immutable_files.read_stable_file(
            path, where="shared source publication", max_bytes=4096)
    except FileNotFoundError:
        return None
    try:
        record = json.loads(raw)
        if (not isinstance(record, dict) or set(record) != {"generation", "census"}
                or not isinstance(record["generation"], str)
                or not _GENERATION.fullmatch(record["generation"])
                or not isinstance(record["census"], str)
                or not _KEY.fullmatch(record["census"])):
            raise ValueError("invalid publication")
        return record
    except (ValueError, TypeError) as exc:
        raise MaterializationContractError("invalid shared source publication") from exc


def _forget(generation: Path) -> None:
    _STATES.pop((os.getpid(), str(generation)), None)


def _remove_generation(bucket: Path, name: str) -> None:
    from . import materialize

    generation = bucket / name
    materialize._remove_checkout_tree(generation)
    _forget(generation)
    (bucket / (name + ".idle.json")).unlink(missing_ok=True)
    # The key mutex is held, and the exclusive reader lock remains held by the
    # caller through unlink. No acquisition can race onto a different inode.
    (bucket / (name + ".readers")).unlink(missing_ok=True)


def _retire(bucket: Path, name: str, *, current: bool, now: float) -> None:
    with _exclusive(bucket / (name + ".readers"), wait=False) as free:
        if not free:
            return
        if current:
            idle = bucket / (name + ".idle.json")
            try:
                raw = immutable_files.read_stable_file(
                    idle, where="shared source idle clock", max_bytes=1024)
                since = json.loads(raw)["idle_since"]
                if not isinstance(since, (int, float)) or not math.isfinite(since):
                    raise ValueError("invalid idle clock")
            except (FileNotFoundError, ValueError, KeyError, TypeError):
                immutable_files.write_json_atomic(idle, {"idle_since": now})
                return
            if now - since < _IDLE_SECONDS:
                return
            # Unpublish before deleting. A crash during deletion cannot leave
            # a discoverable partial tree. New acquisitions use the key mutex.
            (bucket / "current.json").unlink()
        _remove_generation(bucket, name)


def _reap_bucket(bucket: Path, *, now: float, keep: str | None) -> None:
    for path in _source_entries(bucket):
        if _GENERATION.fullmatch(path.name) and path.name != keep:
            _retire(bucket, path.name, current=False, now=now)


def _prove(repository: Path, checkout: Path, snapshot: Mapping[str, object],
           generation: str, census: tuple[tuple[object, ...], ...]) -> _GenerationState:
    # This is a private core API, never a marker supplied in the pool item.
    proof = pb._prove_checkout_snapshot(snapshot, checkout)
    state = _GenerationState(repository, checkout, generation, census, proof)
    state.check()  # Proving must not have raced a change (or changed the tree).
    return state


def _build(bucket: Path, bundle: Path, snapshot: Mapping[str, object]) -> _GenerationState:
    from . import materialize

    name = "g-" + uuid.uuid4().hex
    generation = bucket / name
    generation.mkdir(mode=0o700)
    try:
        repository = generation / "checkout"
        checkout = materialize._populate_checkout(repository, bundle, snapshot)
        _freeze(repository)
        generation.chmod(0o555)
        census = _source_census(repository)
        state = _prove(repository, checkout, snapshot, name, census)
        # The unique path is private until this single atomic pointer publish.
        # Keeping its path stable also keeps the core proof's root binding valid.
        immutable_files.write_json_atomic(bucket / "current.json", {
            "generation": name, "census": pb.canonical_sha256(census),
        })
        _STATES[(os.getpid(), str(generation))] = state
        return state
    except BaseException:
        try:
            materialize._remove_checkout_tree(generation)
        except Exception as exc:
            print(f"prismabuild: shared builder cleanup failed in {generation}: {exc}",
                  file=sys.stderr, flush=True)
        raise


def _adopt(bucket: Path, record: Mapping[str, str],
           snapshot: Mapping[str, object]) -> _GenerationState:
    generation = bucket / record["generation"]
    state = _STATES.get((os.getpid(), str(generation)))
    if state is not None:
        state.check()
        return state
    repository = generation / "checkout"
    census = _source_census(repository)
    # Only the materializer writes this host-local publication, after core has
    # proved the entire generation. The exact inode/ctime census carries that
    # proof across worker interpreters; it is NOT an action field or a caller's
    # "verified" marker. CAS trust context and the whole normalized snapshot
    # select the bucket before reaching here. A changed/uncertain inode refuses
    # even if its bytes and mtime have been restored. This store, like PB's
    # runtime, is trusted against cooperative workloads, not hostile same-UID
    # code rewriting PB's own metadata.
    if pb.canonical_sha256(census) != record["census"]:
        raise MaterializationContractError("published shared source generation changed")
    checkout = repository / str(snapshot["subdirectory"])
    state = _GenerationState(
        repository, checkout, record["generation"], census,
        pb._CheckoutSnapshotProof(snapshot, checkout))
    state.check()
    _STATES[(os.getpid(), str(generation))] = state
    return state


def _source_request(item: Mapping[str, object]) -> tuple[dict[str, object], Path, str]:
    snapshot = pb.validate_pbrun_checkout_snapshot(item.get("checkout_snapshot"))
    try:
        cas = pb.PrismaBuildCAS(str(item["cas_root"]))
        trust_root = cas.root.resolve(strict=True)
        observed = trust_root.stat()
        if not stat.S_ISDIR(observed.st_mode):
            raise MaterializationContractError("shared source CAS root is not a directory")
        # Even a warm generation cannot turn a missing or tampered CAS input
        # into an apparently verified acquisition in another trust context.
        bundle = cas.input_path(snapshot["input"])
    except (KeyError, OSError, pb.CASTamperError, pb.CASUnavailableError) as exc:
        raise MaterializationContractError("shared source CAS input cannot be verified") from exc
    key = pb.canonical_sha256({
        "version": _VERSION, "snapshot": snapshot,
        "cas": {"root": str(trust_root), "device": observed.st_dev,
                "inode": observed.st_ino},
    })
    return snapshot, bundle, key


def _release(bucket: Path, state: _GenerationState, reader: int) -> None:
    """Release is housekeeping: it cannot fail completed work."""
    try:
        with _exclusive(bucket / "mutex"):
            os.close(reader)
            reader = -1
            with _exclusive(bucket / (state.generation + ".readers"), wait=False) as free:
                if free:
                    immutable_files.write_json_atomic(
                        bucket / (state.generation + ".idle.json"),
                        {"idle_since": time.time()})
    except Exception as exc:
        print(f"prismabuild: shared source release housekeeping failed in {bucket}: {exc}",
              file=sys.stderr, flush=True)
    finally:
        if reader >= 0:
            try:
                os.close(reader)
            except OSError as exc:
                print(f"prismabuild: shared source reader close failed: {exc}",
                      file=sys.stderr, flush=True)


@contextmanager
def checkout(item: Mapping[str, object], *, local_checkout_root: str | Path) -> Iterator[_SourceLease]:
    snapshot, bundle, key = _source_request(item)
    root = _namespace(local_checkout_root, create=True)
    bucket = root / key
    bucket.mkdir(mode=0o700, exist_ok=True)
    if bucket.is_symlink():
        raise MaterializationContractError("shared source key directory is a symlink")
    reader = -1
    with _exclusive(bucket / "mutex"):
        record = _current(bucket)
        _reap_bucket(bucket, now=time.time(), keep=None if record is None else record["generation"])
        if record is None:
            state = _build(bucket, bundle, snapshot)
        else:
            try:
                state = _adopt(bucket, record, snapshot)
            except MaterializationContractError:
                # Never reuse an uncertain generation, even if its bytes later
                # return to their sealed values. Existing leases remain pinned
                # and fail their own census gates; the next borrow may rebuild.
                (bucket / "current.json").unlink(missing_ok=True)
                raise
        try:
            reader = _open_lock(bucket / (state.generation + ".readers"))
            fcntl.flock(reader, fcntl.LOCK_SH)
            (bucket / (state.generation + ".idle.json")).unlink(missing_ok=True)
        except BaseException:
            if reader >= 0:
                os.close(reader)
            raise
    lease = _SourceLease(state, str(item["cas_root"]), reader)
    try:
        yield lease
    finally:
        lease._close()
        _release(bucket, state, reader)


def retire_idle_sources(*, local_checkout_root: str | Path, now: float) -> None:
    """Bounded, best-effort housekeeping; never wait on builders or readers."""
    try:
        root = _namespace(local_checkout_root, create=False)
        if not root.exists() or not math.isfinite(now):
            return
        for bucket in _source_entries(root):
            if not _KEY.fullmatch(bucket.name) or bucket.is_symlink():
                continue
            try:
                with _exclusive(bucket / "mutex", wait=False) as free:
                    if not free:
                        continue
                    record = _current(bucket)
                    keep = None if record is None else record["generation"]
                    _reap_bucket(bucket, now=now, keep=keep)
                    if keep is not None:
                        _retire(bucket, keep, current=True, now=now)
            except Exception as exc:
                print(f"prismabuild: shared source reaping failed in {bucket}: {exc}",
                      file=sys.stderr, flush=True)
    except Exception as exc:
        print(f"prismabuild: shared source reaping failed: {exc}", file=sys.stderr, flush=True)
