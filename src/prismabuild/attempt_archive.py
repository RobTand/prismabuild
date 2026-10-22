"""Verified live/archive access to immutable attempt stdout and stderr (#2023)."""
from __future__ import annotations

from collections.abc import Callable, Mapping
import errno
import os
from pathlib import Path
import re
import selectors
import stat
import subprocess
import sys
import tempfile
import time

from . import digest_primitives as digests, immutable_files as native

QUEUE_EVIDENCE_RETENTION_DAYS = 14
MAX_LOG_CHUNK_BYTES = 8 * 1024 * 1024
MAX_LOG_COMPRESSED_BYTES = 9 * 1024 * 1024
MAX_LOG_MANIFEST_BYTES = 1024 * 1024
MAX_LOG_INDEX_BYTES = 4096
MAX_LOG_CONFIG_BYTES = 16384
LOG_CODEC_TIMEOUT_S = 30
ATTEMPT_LOG_RESOLVER_PROTOCOL = 1
ATTEMPT_LOG_RESOLVER_CAPABILITY = "attempt-log-resolver-v1"
RETENTION_CONFIG = "attempt-log-retention.json"
RETENTION_CONFIG_SCHEMA = "prismabuild.attempt-log-retention.v1"
ARCHIVE_STORE_SCHEMA = "prismabuild.attempt-log-store.v1"
ARCHIVE_INDEX_SCHEMA = "prismabuild.attempt-log-index.v1"
ARCHIVE_MANIFEST_SCHEMA = "prismabuild.attempt-log-manifest.v1"
ATTEMPT_LOG_ARCHIVE_DATASET = "storage_pool/shared/pb-queue-evidence"
_ATTEMPT_CODEC_EXEC = """\
import ctypes, os, signal, sys
if ctypes.CDLL(None, use_errno=True).prctl(1, signal.SIGKILL, 0, 0, 0) != 0:
    raise OSError(ctypes.get_errno(), 'cannot bind archive codec to reader lifetime')
if os.getppid() != int(sys.argv[1]):
    os._exit(1)
os.execvp(sys.argv[2], sys.argv[2:])
"""
_ARCHIVE_HEX = re.compile(r"[0-9a-f]{64}\Z")
_ARCHIVE_LOG_PATH = re.compile(
    r"attempts/([0-9a-f]{64})/([0-9a-f]{64})/"
    r"([0-9]{8,})\.(stdout|stderr)\.([0-9a-f]{64})\.log\Z")


class InvalidLogDescriptor(ValueError):
    """The supplied descriptor is not a canonical immutable attempt log."""


class AttemptLogMissing(FileNotFoundError):
    """Neither live storage nor the exact archive index contains the log."""


class AttemptLogIntegrityError(RuntimeError):
    """A live log or archive disagrees with its immutable address."""


class AttemptArchiveUnavailable(OSError):
    """A configured store or its backing identity cannot be verified."""


def _validate_attempt_log_descriptor(descriptor: Mapping[str, object]) -> dict:
    if not isinstance(descriptor, Mapping):
        raise InvalidLogDescriptor("attempt log descriptor must be an object")
    path, size, sha = (descriptor.get(field) for field in ("path", "bytes", "sha256"))
    match = (_ARCHIVE_LOG_PATH.fullmatch(path)
             if isinstance(path, str) and len(path) <= 512 else None)
    if (match is None or type(size) is not int or size < 0
            or not isinstance(sha, str) or not _ARCHIVE_HEX.fullmatch(sha)
            or match[5] != sha):
        raise InvalidLogDescriptor("attempt log requires canonical path, bytes and sha256")
    # Match RecordStore's minimum-width decimal spelling, including >8 digits.
    try:
        number = int(match[3])
    except ValueError as exc:
        raise InvalidLogDescriptor("attempt log number exceeds parser bounds") from exc
    if number < 1 or match[3] != f"{number:08d}":
        raise InvalidLogDescriptor("attempt log number is not canonical")
    return dict(path=path, bytes=size, sha256=sha)


def _attempt_archive_json(path: Path, *, limit: int, immutable: bool = True) -> dict:
    reader = native.read_immutable_file if immutable else native.read_stable_file
    raw = reader(path, where="attempt archive metadata", max_bytes=limit)
    try:
        value = digests.decode_strict_json(raw, where="attempt archive metadata")
    except digests.ActionContractError as exc:
        raise AttemptLogIntegrityError(str(exc)) from exc
    if not isinstance(value, dict):
        raise AttemptLogIntegrityError("attempt archive metadata must be an object")
    return value


def _load_attempt_retention_config(queue_root: Path) -> dict | None:
    try:
        config = _attempt_archive_json(queue_root / RETENTION_CONFIG,
                                      limit=MAX_LOG_CONFIG_BYTES, immutable=False)
    except FileNotFoundError:
        if native.confirmed_metadata_absence(queue_root / RETENTION_CONFIG):
            return None
        config = _attempt_archive_json(queue_root / RETENTION_CONFIG,
                                      limit=MAX_LOG_CONFIG_BYTES, immutable=False)
    required = {"schema", "queue_id", "archive_root", "store_id", "dataset", "dataset_guid"}
    if (not required <= config.keys() or config["schema"] != RETENTION_CONFIG_SCHEMA
            or set(config) - required - {"retention_days", "reader_gate"}
            or any(not isinstance(config[field], str) or not config[field]
                   for field in required - {"schema"})):
        raise AttemptArchiveUnavailable("invalid attempt log retention configuration")
    if any(not _ARCHIVE_HEX.fullmatch(config[field]) for field in ("queue_id", "store_id")):
        raise AttemptArchiveUnavailable("queue_id and store_id must be lowercase SHA256 identities")
    root = Path(config["archive_root"])
    if not root.is_absolute() or str(root) != config["archive_root"] or ".." in root.parts:
        raise AttemptArchiveUnavailable("archive_root must be a canonical absolute path")
    if root == queue_root or root.is_relative_to(queue_root):
        raise AttemptArchiveUnavailable("attempt archive must be outside the queue")
    return config


def _check_attempt_archive_store(queue_root: Path, *, for_apply: bool = False) -> dict | None:
    try:
        config = _load_attempt_retention_config(queue_root)
        if config is None:
            return None
        root = Path(config["archive_root"])
        marker = _attempt_archive_json(root / "store.json", limit=MAX_LOG_CONFIG_BYTES)
        expected = {field: config[field] for field in ("queue_id", "store_id", "dataset", "dataset_guid")}
        expected.update(schema=ARCHIVE_STORE_SCHEMA, queue_root=str(queue_root))
        if marker != expected:
            raise AttemptArchiveUnavailable("attempt archive marker does not bind this queue/store")
        if for_apply:
            if config["dataset"] != ATTEMPT_LOG_ARCHIVE_DATASET:
                raise AttemptArchiveUnavailable("archive backing dataset is not the approved HDD dataset")
            # These are observations only. Provisioning is an explicit lead operation.
            result = subprocess.run(
                ["zfs", "get", "-H", "-p", "-o", "property,value", "guid,mountpoint,sync,compression",
                 config["dataset"]], check=True, capture_output=True, timeout=10)
            properties = dict(line.split("\t", 1) for line in result.stdout.decode().splitlines())
            mounted = subprocess.run(
                ["findmnt", "-n", "-T", str(root), "-o", "SOURCE,FSTYPE"],
                check=True, capture_output=True, timeout=10).stdout.decode().split()
            if (properties.get("guid") != config["dataset_guid"]
                    or properties.get("sync") != "standard"
                    or not properties.get("compression", "").startswith("zstd")
                    or mounted != [config["dataset"], "zfs"]
                    or not root.is_relative_to(Path(properties["mountpoint"]))):
                raise AttemptArchiveUnavailable("archive backing dataset identity/sync/compression mismatch")
        return config
    except AttemptArchiveUnavailable:
        raise
    except (OSError, ValueError, KeyError, native.CASTamperError,
            native.CASUnavailableError, AttemptLogIntegrityError, subprocess.SubprocessError) as exc:
        raise AttemptArchiveUnavailable(f"attempt archive store cannot be verified: {exc}") from exc


def _attempt_archive_index_path(config: Mapping, descriptor: Mapping) -> Path:
    parts = str(descriptor["path"]).split("/")
    return (Path(config["archive_root"]) / "index" / parts[1][:2] / parts[1] / parts[2]
            / f"{digests.canonical_sha256(descriptor)}.json")


def _refresh_attempt_archive_address(path: Path, base: Path) -> None:
    """Refresh only canonical parents on an ENOENT, stopping at the wanted name.

    scandir streams the directory instead of retaining its historical listing.
    NFS cannot notify this client of a remote publish; crossover retries are
    consequently bounded to two passes with no polling delay.
    """
    directory = base
    for name in path.relative_to(base).parts:
        try:
            fd = native.open_directory_nofollow(directory, where="attempt log parent")
        except FileNotFoundError:
            return
        try:
            with os.scandir(fd) as entries:
                found = any(entry.name == name for entry in entries)
        finally:
            os.close(fd)
        if not found:
            return
        directory /= name


def _stream_live_attempt_log(path: Path, fact: Mapping, consume: Callable[[bytes], None]) -> None:
    fd, parent = native.open_regular_nofollow(path, where="pool attempt log")
    try:
        before = os.fstat(fd)
        if before.st_mode & 0o222 or before.st_size != fact["bytes"]:
            raise AttemptLogIntegrityError("live attempt log mode/size differs from descriptor")
        digest, size = digests.new_sha256(), 0
        while True:
            raw = os.read(fd, min(1024 * 1024, fact["bytes"] - size + 1))
            if not raw:
                break
            size += len(raw)
            if size > fact["bytes"]:
                raise AttemptLogIntegrityError("live attempt log exceeds descriptor size")
            digest.update(raw)
            consume(raw)
        if size != fact["bytes"] or digest.hexdigest() != fact["sha256"]:
            raise AttemptLogIntegrityError("live attempt log differs from descriptor")
        if native.substantive_identity(before) != native.substantive_identity(os.fstat(fd)):
            raise AttemptLogIntegrityError("live attempt log changed while being read")
        # Retirement can unlink a verified immutable inode while it is open.
        # Its held bytes remain authoritative; a replacement name is not.
        try:
            native.assert_regular_identity(fd, parent, path, where="pool attempt log")
        except native.CASTamperError:
            if os.fstat(fd).st_nlink != 0 or not native.confirmed_metadata_absence(path):
                raise
        native.assert_directory_identity(parent, path.parent, where="pool attempt log parent")
    finally:
        os.close(fd)
        os.close(parent)


def _attempt_archive_zstd(raw: bytes, *, decode: bool) -> bytes:
    """Use one native thread, bounded input/output and bounded decoder memory."""
    input_limit = MAX_LOG_COMPRESSED_BYTES if decode else MAX_LOG_CHUNK_BYTES
    if len(raw) > input_limit:
        raise AttemptLogIntegrityError("attempt archive zstd input exceeds bound")
    limit = MAX_LOG_CHUNK_BYTES if decode else MAX_LOG_COMPRESSED_BYTES
    arguments = ["zstd", "-q", "-c", "-d", "-M16MB"] if decode else ["zstd", "-q", "-c", "-T1"]
    # A disk-backed stdin avoids pipe deadlock without an unbounded communicate.
    with tempfile.TemporaryFile() as source:
        source.write(raw)
        source.seek(0)
        # Set Linux parent-death custody in a fresh interpreter before exec,
        # avoiding preexec_fn in threaded callers. MCP can SIGKILL its reader.
        child = subprocess.Popen([sys.executable, "-c", _ATTEMPT_CODEC_EXEC,
                                  str(os.getpid()), *arguments], stdin=source, stdout=subprocess.PIPE,
                                 stderr=subprocess.DEVNULL)
        try:
            deadline = time.monotonic() + LOG_CODEC_TIMEOUT_S
            output = bytearray()
            with selectors.DefaultSelector() as readable:
                readable.register(child.stdout, selectors.EVENT_READ)
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0 or not readable.select(remaining):
                        raise AttemptArchiveUnavailable("attempt archive zstd deadline expired")
                    part = os.read(child.stdout.fileno(), min(1024 * 1024, limit + 1 - len(output)))
                    if not part:
                        break
                    output.extend(part)
                    if len(output) > limit:
                        raise AttemptLogIntegrityError("attempt archive zstd output exceeds bound")
            try:
                status = child.wait(timeout=max(0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired as exc:
                raise AttemptArchiveUnavailable("attempt archive zstd exit deadline expired") from exc
            if status != 0:
                raise AttemptLogIntegrityError("attempt archive zstd frame is corrupt")
            return bytes(output)
        finally:
            child.stdout.close()
            if child.poll() is None:
                child.kill()
            child.wait()


def _read_attempt_archive_manifest(config: Mapping, fact: Mapping) -> dict:
    index_path = _attempt_archive_index_path(config, fact)
    entry = _attempt_archive_json(index_path, limit=MAX_LOG_INDEX_BYTES)
    identity = {field: config[field] for field in ("queue_id", "store_id")}
    identity.update(descriptor=dict(fact))
    if (set(entry) != {"schema", "queue_id", "store_id", "descriptor", "manifest_sha256"}
            or entry.get("schema") != ARCHIVE_INDEX_SCHEMA
            or any(entry.get(key) != value for key, value in identity.items())
            or not isinstance(entry.get("manifest_sha256"), str)
            or not _ARCHIVE_HEX.fullmatch(entry["manifest_sha256"])):
        raise AttemptLogIntegrityError("attempt archive direct index identity differs")
    sha = entry["manifest_sha256"]
    path = Path(config["archive_root"]) / "manifests" / sha[:2] / f"{sha}.json"
    raw = native.read_immutable_file(path, where="attempt archive manifest", max_bytes=MAX_LOG_MANIFEST_BYTES)
    if digests.raw_sha256(raw) != sha:
        raise AttemptLogIntegrityError("attempt archive manifest digest differs")
    try:
        manifest = digests.decode_strict_json(raw, where="attempt archive manifest")
    except digests.ActionContractError as exc:
        raise AttemptLogIntegrityError(str(exc)) from exc
    if (not isinstance(manifest, dict)
            or set(manifest) != {"schema", "queue_id", "store_id", "descriptor", "chunks"}
            or manifest.get("schema") != ARCHIVE_MANIFEST_SCHEMA
            or any(manifest.get(key) != value for key, value in identity.items())
            or not isinstance(manifest.get("chunks"), list)):
        raise AttemptLogIntegrityError("attempt archive manifest identity differs")
    return manifest


def _stream_archived_attempt_log(config: Mapping, fact: Mapping, consume: Callable[[bytes], None]) -> None:
    manifest = _read_attempt_archive_manifest(config, fact)
    digest, total = digests.new_sha256(), 0
    for chunk in manifest["chunks"]:
        if (not isinstance(chunk, dict)
                or set(chunk) != {"sha256", "bytes", "compressed_sha256", "compressed_bytes"}
                or any(not isinstance(chunk[field], str) or not _ARCHIVE_HEX.fullmatch(chunk[field])
                       for field in ("sha256", "compressed_sha256"))
                or type(chunk["bytes"]) is not int or not 0 < chunk["bytes"] <= MAX_LOG_CHUNK_BYTES
                or type(chunk["compressed_bytes"]) is not int
                or not 0 < chunk["compressed_bytes"] <= MAX_LOG_COMPRESSED_BYTES
                or total + chunk["bytes"] > fact["bytes"]):
            raise AttemptLogIntegrityError("attempt archive chunk descriptor exceeds bounds")
        sha = chunk["compressed_sha256"]
        path = Path(config["archive_root"]) / "chunks" / sha[:2] / f"{sha}.zst"
        compressed = native.read_immutable_file(path, where="attempt archive chunk", max_bytes=MAX_LOG_COMPRESSED_BYTES)
        if len(compressed) != chunk["compressed_bytes"] or digests.raw_sha256(compressed) != sha:
            raise AttemptLogIntegrityError("attempt archive compressed address differs")
        raw = _attempt_archive_zstd(compressed, decode=True)
        if len(raw) != chunk["bytes"] or digests.raw_sha256(raw) != chunk["sha256"]:
            raise AttemptLogIntegrityError("attempt archive decoded address differs")
        total += len(raw)
        digest.update(raw)
        consume(raw)
    if total != fact["bytes"] or digest.hexdigest() != fact["sha256"]:
        raise AttemptLogIntegrityError("attempt archived log differs from descriptor")


def _stream_verified_attempt_log(queue_root: os.PathLike[str] | str,
                                 descriptor: Mapping[str, object], consume: Callable[[bytes], None]) -> None:
    fact = _validate_attempt_log_descriptor(descriptor)
    queue = Path(queue_root).absolute()
    live = queue / fact["path"]
    archive_address = "unconfigured archive"
    emitted = False
    def deliver(raw: bytes) -> None:
        nonlocal emitted
        emitted = True
        consume(raw)
    try:
        for crossover in range(2):
            try:
                _stream_live_attempt_log(live, fact, deliver)
                return
            except FileNotFoundError as exc:
                if emitted:
                    raise AttemptArchiveUnavailable("live log address changed after streaming began") from exc
                _refresh_attempt_archive_address(live, queue)
                try:
                    _stream_live_attempt_log(live, fact, deliver)
                    return
                except FileNotFoundError as exc:
                    if emitted:
                        raise AttemptArchiveUnavailable("live log address changed after streaming began") from exc
            config = _check_attempt_archive_store(queue)
            if config is None:
                continue
            address = _attempt_archive_index_path(config, fact)
            archive_address = str(address)
            try:
                _stream_archived_attempt_log(config, fact, deliver)
                return
            except FileNotFoundError as exc:
                if emitted:
                    raise AttemptArchiveUnavailable("archive address changed after streaming began") from exc
                # A missing published manifest/chunk is lost archive evidence,
                # not an absent index or permission to report empty history.
                if not native.confirmed_metadata_absence(address):
                    raise AttemptArchiveUnavailable("published attempt archive payload is unavailable") from exc
                _refresh_attempt_archive_address(address, Path(config["archive_root"]))
                if crossover:
                    raise AttemptLogMissing(errno.ENOENT, f"attempt log missing at {live} and {archive_address}") from exc
        raise AttemptLogMissing(errno.ENOENT, f"attempt log missing at {live} and {archive_address}")
    except (InvalidLogDescriptor, AttemptLogMissing, AttemptLogIntegrityError, AttemptArchiveUnavailable):
        raise
    except (native.CASTamperError, digests.ActionContractError) as exc:
        raise AttemptLogIntegrityError(str(exc)) from exc
    except (OSError, native.CASUnavailableError, subprocess.SubprocessError) as exc:
        raise AttemptArchiveUnavailable(f"attempt log storage unavailable: {exc}") from exc


def resolve_attempt_log(queue_root: os.PathLike[str] | str, descriptor: Mapping[str, object]) -> bytes:
    """Return exact verified live or archived bytes; live corruption never falls back.

    Raises InvalidLogDescriptor, AttemptLogMissing, AttemptLogIntegrityError or
    AttemptArchiveUnavailable. Memory is proportional to the requested log.
    """
    chunks: list[bytes] = []
    _stream_verified_attempt_log(queue_root, descriptor, chunks.append)
    return b"".join(chunks)


def _verified_attempt_log_tail(queue_root: os.PathLike[str] | str, descriptor: Mapping[str, object],
                               *, max_bytes: int) -> bytes:
    if type(max_bytes) is not int or max_bytes < 0:
        raise ValueError("attempt log tail bound must be a nonnegative integer")
    tail = bytearray()
    def retain(raw: bytes) -> None:
        if max_bytes:
            tail.extend(raw[-max_bytes:])
            del tail[:-max_bytes]
    _stream_verified_attempt_log(queue_root, descriptor, retain)
    return bytes(tail)


def _publish_attempt_log_archive(queue_root: Path, descriptor: Mapping[str, object]) -> dict:
    fact = _validate_attempt_log_descriptor(descriptor)
    config = _check_attempt_archive_store(queue_root, for_apply=True)
    if config is None:
        raise AttemptArchiveUnavailable("attempt log archive is not configured")
    index_path = _attempt_archive_index_path(config, fact)
    try:
        _stream_archived_attempt_log(config, fact, lambda raw: None)
        return _attempt_archive_json(index_path, limit=MAX_LOG_INDEX_BYTES)
    except FileNotFoundError:
        if not native.confirmed_metadata_absence(index_path):
            raise AttemptArchiveUnavailable("existing attempt archive is incomplete")
    root, chunks, pending = Path(config["archive_root"]), [], bytearray()
    manifest_budget = 4096
    def publish_chunk(raw: bytes) -> None:
        nonlocal manifest_budget
        encoded = _attempt_archive_zstd(raw, decode=False)
        sha = digests.raw_sha256(encoded)
        path = root / "chunks" / sha[:2] / f"{sha}.zst"
        native.publish_exact(path, encoded, where="attempt archive chunk", conflict_error=AttemptLogIntegrityError)
        chunk = dict(sha256=digests.raw_sha256(raw), bytes=len(raw),
                     compressed_sha256=sha, compressed_bytes=len(encoded))
        manifest_budget += len(digests._canonical_bytes(chunk)) + 1
        chunks.append(chunk)
        if manifest_budget > MAX_LOG_MANIFEST_BYTES:
            raise AttemptLogIntegrityError("attempt archive manifest exceeds bound; retain live log")
    def stage(raw: bytes) -> None:
        pending.extend(raw)
        while len(pending) >= MAX_LOG_CHUNK_BYTES:
            publish_chunk(bytes(pending[:MAX_LOG_CHUNK_BYTES]))
            del pending[:MAX_LOG_CHUNK_BYTES]
    _stream_live_attempt_log(queue_root / fact["path"], fact, stage)
    if pending:
        publish_chunk(bytes(pending))
    identity = dict(queue_id=config["queue_id"], store_id=config["store_id"], descriptor=fact)
    raw = digests._canonical_bytes(dict(schema=ARCHIVE_MANIFEST_SCHEMA, **identity, chunks=chunks))
    if len(raw) > MAX_LOG_MANIFEST_BYTES:
        raise AttemptLogIntegrityError("attempt archive manifest exceeds bound")
    sha = digests.raw_sha256(raw)
    native.publish_exact(root / "manifests" / sha[:2] / f"{sha}.json", raw,
                         where="attempt archive manifest", conflict_error=AttemptLogIntegrityError)
    entry = dict(schema=ARCHIVE_INDEX_SCHEMA, **identity, manifest_sha256=sha)
    raw_entry = digests._canonical_bytes(entry)
    if len(raw_entry) > MAX_LOG_INDEX_BYTES:
        raise AttemptLogIntegrityError("attempt archive direct entry exceeds bound")
    # First-writer publication permits adoption of another valid segmentation.
    native.publish_immutable(index_path, raw_entry)
    _stream_archived_attempt_log(config, fact, lambda raw: None)
    return _attempt_archive_json(index_path, limit=MAX_LOG_INDEX_BYTES)
