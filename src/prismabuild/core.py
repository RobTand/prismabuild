"""Deterministic PrismaBuild action keys, local execution, and immutable CAS.

This is the dependency-free core beneath a future Dagster/SLURM deployment.
It deliberately does not contain a queue or a weight cache.  An action is an
exact, self-hashed contract; a local worker executes its argv with
``shell=False`` and a closed environment; and successful file results are
published to a content-addressed store with NFS-safe, first-writer-wins hard
links.

Portable actions omit machine identity from their key.  Platform- and
host-class-keyed actions bind the corresponding explicit execution scope.
Measurement actions are never portable.  Explicit codebook-family generation
is also never portable because D29 records cross-architecture row-scale byte
drift.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager, suppress
import errno
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import platform
import re
import signal
import socket
import stat
import subprocess
import tempfile
import time

ACTION_SCHEMA_V1 = "prismaquant.prismabuild.action.v1"
ACTION_SCHEMA_V2 = "prismaquant.prismabuild.action.v2"
CODE_CLOSURE_SCHEMA_V1 = "prismaquant.prismabuild.code_closure.v1"
CAS_RECEIPT_SCHEMA_V3 = "prismaquant.prismabuild.cas_receipt.v3"
WORKER_ATTESTATION_SCHEMA_V2 = "prismaquant.prismabuild.worker_attestation.v2"
WORKER_RUNTIME_SCHEMA_V1 = "prismaquant.prismabuild.worker_runtime.v1"

#: Where a transport asks this worker to leave the action's own exit status.
#:
#: An environment variable rather than an argument, because ``pool.worker_argv``
#: is pinned byte-identical across both transports -- an action executed under
#: SLURM and the same action executed by the pull queue must be the same
#: execution -- and a flag on one of them would end that. The action's own argv
#: never sees this: ``run_local_action`` builds the sealed environment it runs
#: in, and this variable is not in it.
ACTION_STATUS_PATH_ENV = "PRISMABUILD_ACTION_STATUS_PATH"
PBRUN_STAMP_PREFIX = ".pbrun-closure."
PBRUN_RESULT_PREFIX = "pbrun_result."
PBRUN_GENERATED_FINGERPRINT_HEX_LENGTH = 16
PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V1 = (
    "prismaquant.prismabuild.pbrun_checkout_snapshot.v1"
)
PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V2 = (
    "prismaquant.prismabuild.pbrun_checkout_snapshot.v2"
)
PBRUN_CHECKOUT_SNAPSHOT_INPUT_ID = "pbrun.checkout-snapshot"
PBRUN_CHECKOUT_SNAPSHOT_REF_NAME = "prismabuild-snapshot"
LOCAL_RESULT_CLAIM_SCHEMA_V1 = "prismaquant.prismabuild.local_result_claim.v1"
INITIAL_MISS_RENDEZVOUS_MANIFEST_SCHEMA_V1 = (
    "prismaquant.prismabuild.initial_miss_rendezvous_manifest.v1"
)
INITIAL_MISS_RENDEZVOUS_PROCESS_SCHEMA_V1 = (
    "prismaquant.prismabuild.initial_miss_rendezvous_process.v1"
)
INITIAL_MISS_RENDEZVOUS_ARRIVAL_SCHEMA_V1 = (
    "prismaquant.prismabuild.initial_miss_rendezvous_arrival.v1"
)
INITIAL_MISS_RENDEZVOUS_READY_SCHEMA_V1 = (
    "prismaquant.prismabuild.initial_miss_rendezvous_ready.v1"
)
INITIAL_MISS_RENDEZVOUS_RECEIPT_SCHEMA_V1 = (
    "prismaquant.prismabuild.initial_miss_rendezvous_receipt.v1"
)
_CAS_RECEIPT_NAMESPACE = CAS_RECEIPT_SCHEMA_V3.rsplit(".", 1)[-1]

_ACTION_BODY_KEYS = frozenset(
    {
        "schema",
        "task",
        "inputs",
        "code_closure",
        "params",
        "environment",
        "execution_scope",
    }
)
_ACTION_KEYS = _ACTION_BODY_KEYS | {"action_key"}
_TASK_KEYS = frozenset(
    {
        "definition_id",
        "definition_version",
        "task_class",
        "determinism",
        "artifact_family",
        "artifact_kind",
        "argv",
        "working_directory",
        "result_path",
    }
)
_INPUT_KEYS = frozenset({"id", "sha256", "bytes"})
_CLOSURE_KEYS = frozenset({"schema", "files", "closure_sha256"})
_CLOSURE_FILE_KEYS = frozenset({"path", "sha256", "bytes"})
_ENVIRONMENT_KEYS = frozenset({"variables", "toolchain"})
_SCOPE_KEYS = frozenset({"portability", "platform_key", "host_class"})
_RECEIPT_BODY_KEYS = frozenset(
    {"schema", "action_key", "action_manifest_sha256", "result", "producer"}
)
_RECEIPT_KEYS = _RECEIPT_BODY_KEYS | {"receipt_sha256"}
_RESULT_KEYS = frozenset({"sha256", "bytes"})
_PRODUCER_KEYS = frozenset(
    {
        "schema",
        "action_key",
        "worker_id",
        "platform_key",
        "host_class",
        "evidence",
        "runtime",
        "executable",
        "toolchain",
        "inputs",
        "attestation_sha256",
    }
)
_EVIDENCE_KEYS = frozenset(
    {"source", "hostname", "system", "machine", "libc", "accelerators", "slurm"}
)
_ACCELERATOR_KEYS = frozenset(
    {"kind", "compute_capability", "driver_version"}
)
_SLURM_EVIDENCE_KEYS = frozenset(
    {"job_id", "node_name", "partition", "constraints", "cgroup"}
)
#: What the controller said about the job and its node, recorded only for a
#: ``host_class_keyed`` action.  Optional in the persisted shape so that every
#: receipt written before the controller was consulted keeps validating.
_SLURM_CONTROLLER_EVIDENCE_KEYS = frozenset(
    {"partition", "batch_host", "job_features", "node_active_features"}
)
#: How long to wait between attempts to reach the controller, in seconds.  The
#: sum bounds how long a job whose controller is down spends before it refuses.
SCONTROL_RETRY_DELAYS_S: tuple[float, ...] = (1.0, 2.0, 4.0, 8.0, 15.0)
_SCONTROL_CANDIDATES = ("/usr/bin/scontrol", "/usr/local/bin/scontrol")
_EXECUTABLE_KEYS = frozenset({"path", "resolved_path", "sha256", "bytes"})
_RUNTIME_KEYS = frozenset(
    {"schema", "launch_kind", "core", "launcher", "runtime_sha256"}
)
_TOOLCHAIN_ATTESTATION_KEYS = frozenset({"declared", "verified"})
_LOCAL_RESULT_CLAIM_BODY_KEYS = frozenset(
    {
        "schema",
        "action_key",
        "action_manifest_sha256",
        "checkout_root",
        "working_directory",
        "result_path",
    }
)
_LOCAL_RESULT_CLAIM_KEYS = _LOCAL_RESULT_CLAIM_BODY_KEYS | {"claim_sha256"}
_INITIAL_MISS_RENDEZVOUS_MANIFEST_BODY_KEYS = frozenset(
    {
        "schema",
        "rendezvous_namespace",
        "cas_root",
        "run_nonce",
        "action_key",
        "participants",
        "timeout_seconds",
    }
)
_INITIAL_MISS_RENDEZVOUS_MANIFEST_KEYS = (
    _INITIAL_MISS_RENDEZVOUS_MANIFEST_BODY_KEYS | {"manifest_sha256"}
)
_INITIAL_MISS_RENDEZVOUS_PROCESS_BODY_KEYS = frozenset(
    {
        "schema",
        "hostname",
        "pid",
        "proc_start_ticks",
        "invocation_nonce",
        "runtime",
    }
)
_INITIAL_MISS_RENDEZVOUS_PROCESS_KEYS = (
    _INITIAL_MISS_RENDEZVOUS_PROCESS_BODY_KEYS | {"process_identity_sha256"}
)
_INITIAL_MISS_RENDEZVOUS_ARRIVAL_BODY_KEYS = frozenset(
    {
        "schema",
        "manifest_sha256",
        "run_nonce",
        "action_key",
        "participant",
        "process",
        "observation",
    }
)
_INITIAL_MISS_RENDEZVOUS_ARRIVAL_KEYS = (
    _INITIAL_MISS_RENDEZVOUS_ARRIVAL_BODY_KEYS | {"arrival_sha256"}
)
_INITIAL_MISS_RENDEZVOUS_READY_BODY_KEYS = frozenset(
    {
        "schema",
        "manifest_sha256",
        "run_nonce",
        "action_key",
        "participant",
        "process_identity_sha256",
        "arrival_sha256",
        "arrival_set_sha256",
        "observation",
    }
)
_INITIAL_MISS_RENDEZVOUS_READY_KEYS = (
    _INITIAL_MISS_RENDEZVOUS_READY_BODY_KEYS | {"ready_sha256"}
)
_INITIAL_MISS_RENDEZVOUS_RECEIPT_BODY_KEYS = frozenset(
    {
        "schema",
        "manifest_sha256",
        "rendezvous_namespace",
        "cas_root",
        "run_nonce",
        "action_key",
        "participants",
        "participant",
        "process_identity_sha256",
        "arrival_sha256",
        "ready_sha256",
        "arrival_set_sha256",
        "ready_set_sha256",
    }
)
_INITIAL_MISS_RENDEZVOUS_RECEIPT_KEYS = (
    _INITIAL_MISS_RENDEZVOUS_RECEIPT_BODY_KEYS | {"receipt_sha256"}
)

_ID_RE = re.compile(r"[a-z0-9][a-z0-9._/-]{0,255}\Z")
_GIT_OBJECT_ID_RE = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
#: A snapshot ref name is an allow-list, not Git's full branch grammar: it is
#: spelled into a worker's ``git fetch`` refspec, so everything the revision
#: grammar and the option parser can reach stays out of the character class.
_SNAPSHOT_REF_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,127}\Z")
_VERSION_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+:-]{0,127}\Z")
_SCOPE_TOKEN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+:/-]{0,255}\Z")
_ENV_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_RUN_NONCE_RE = re.compile(r"[0-9a-f]{32}\Z")
_INVOCATION_NONCE_RE = re.compile(r"[0-9a-f]{32}\Z")
_HOSTNAME_RE = re.compile(
    r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?\Z"
)

_PORTABILITY = frozenset({"portable", "platform_keyed", "host_class_keyed"})
_TASK_CLASSES = frozenset({"generation", "measurement"})
_DETERMINISM = frozenset({"deterministic", "stochastic"})
_ARTIFACT_FAMILIES = frozenset({"generic", "codebook"})
_PROCESS_GROUP_GRACE_SECONDS = 5.0

#: How often the bounded group-exit wait re-probes group membership.
_PROCESS_GROUP_POLL_SECONDS = 0.01
_MAX_LOCAL_RESULT_STAGING_FILES = 64
_MAX_INITIAL_MISS_RENDEZVOUS_BYTES = 64 * 1024
_MAX_INITIAL_MISS_RENDEZVOUS_DIRECTORY_ENTRIES = 8
_MAX_INITIAL_MISS_RENDEZVOUS_TIMEOUT_SECONDS = 24 * 60 * 60
_INITIAL_MISS_RENDEZVOUS_POLL_SECONDS = 0.01
# NFS may return one internally inconsistent ctime/nlink snapshot while a
# first-writer hard link becomes visible.  Never accept that read: reopen the
# no-follow path and replay the entire read from byte zero, at most this many
# times, until one attempt is internally stable.
_STABLE_FILE_READ_ATTEMPTS = 3
_ATTESTABLE_TOOLCHAIN_KEYS = frozenset(
    {
        "argv0.sha256",
        "argv0.bytes",
        "python",
        "torch",
        "transformers",
        "vllm",
        "gridbook",
        "system",
        "machine",
        "libc",
        "cuda_compute_capability",
        "nvidia_driver",
    }
)
_PYTHON_DISTRIBUTIONS = frozenset({"torch", "transformers", "vllm", "gridbook"})


class PrismaBuildError(RuntimeError):
    """Base class for PrismaBuild core failures."""


class ActionContractError(PrismaBuildError, ValueError):
    """An action, closure, receipt, or worker identity is not exact."""


class CASTamperError(PrismaBuildError):
    """An existing content-addressed entry failed verification."""


class CASUnavailableError(PrismaBuildError):
    """The content-addressed store could not be read reliably."""


class CASConflictError(PrismaBuildError):
    """A deterministic recomputation disagreed with the canonical result."""


class LocalActionError(PrismaBuildError):
    """A local action could not execute or did not produce its declared file.

    ``returncode`` and ``signal`` carry the action's own ending when the action
    ran and ended by itself.  Everything else this error reports -- a missing
    result file, a changed closure, a timeout -- is the worker's verdict rather
    than the action's, and leaves both attributes ``None``.

    The message text is unchanged by either attribute.  A reader that scraped
    "exited with status 7" out of a stderr tail keeps working, and a reader that
    wants the number as a number no longer has to scrape anything.
    ``returncode`` follows ``subprocess``: a signalled action carries the
    negative signal number, and ``signal`` carries the positive one.
    """

    def __init__(
        self,
        *args: object,
        returncode: int | None = None,
        signal: int | None = None,
    ) -> None:
        super().__init__(*args)
        self.returncode = returncode
        self.signal = signal


class InitialMissRendezvousError(LocalActionError):
    """An opt-in initial-cache-miss rendezvous failed closed."""


class _FileLinkCountError(CASTamperError):
    """A stable file did not have the required single canonical link."""


def _fail(message: str) -> None:
    raise ActionContractError(message)


def _exact_mapping(
    value: object, *, keys: frozenset[str], where: str
) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        _fail(f"{where} must be an object")
    if any(type(key) is not str for key in value):
        _fail(f"{where} keys must be strings")
    actual = set(value)
    if actual != set(keys):
        _fail(
            f"{where} fields differ: missing={sorted(set(keys) - actual)}, "
            f"extra={sorted(actual - set(keys))}"
        )
    return value


def _text(
    value: object,
    *,
    where: str,
    pattern: re.Pattern[str] | None = None,
    allow_empty: bool = False,
    allow_control: bool = False,
) -> str:
    """Validate one text field.

    Identifiers, keys, digests, paths and tokens refuse every control
    character: nothing that names a thing may carry bytes a log line or a
    filename cannot show.  Payload text -- an argv element, an environment
    value, a params string value -- is what ``execve`` and JSON carry
    verbatim, and its only illegal byte is NUL.  A ``bash -lc`` script with a
    newline in it is an ordinary argument, not a contract violation (issue
    #21), so payload callers pass ``allow_control=True``.
    """

    if type(value) is not str or (not value and not allow_empty):
        _fail(f"{where} must be a {'string' if allow_empty else 'non-empty string'}")
    if "\x00" in value:
        _fail(f"{where} contains a NUL character")
    if not allow_control and any(ord(char) < 32 for char in value):
        _fail(f"{where} contains a NUL or control character")
    if pattern is not None and pattern.fullmatch(value) is None:
        _fail(f"{where} has an invalid value")
    return value


def _optional_token(value: object, *, where: str) -> str | None:
    if value is None:
        return None
    return _text(value, where=where, pattern=_SCOPE_TOKEN_RE)


def _nonnegative_integer(value: object, *, where: str) -> int:
    if type(value) is not int or value < 0:
        _fail(f"{where} must be a non-negative integer")
    return value


def _sha256(value: object, *, where: str) -> str:
    return _text(value, where=where, pattern=_SHA256_RE)


def _canonical_bytes(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ActionContractError("value is not finite canonical JSON data") from exc


def _canonical_file_bytes(value: object) -> bytes:
    return _canonical_bytes(value) + b"\n"


def canonical_sha256(value: object) -> str:
    """Hash canonical JSON without importing another repository module."""

    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _normalize_relative_path(value: object, *, where: str, dot_ok: bool) -> str:
    raw = _text(value, where=where)
    if "\\" in raw or re.match(r"^[A-Za-z]:", raw):
        _fail(f"{where} must be a normalized relative POSIX path")
    path = PurePosixPath(raw)
    if path.is_absolute() or any(part in {"", ".."} for part in raw.split("/")):
        _fail(f"{where} must be a normalized relative POSIX path")
    if raw == ".":
        if dot_ok:
            return raw
        _fail(f"{where} must name a file, not '.'")
    if any(part == "." for part in raw.split("/")) or str(path) != raw:
        _fail(f"{where} must be a normalized relative POSIX path")
    return raw


def _normalize_json_value(value: object, *, where: str) -> object:
    """Validate JSON recursively without Python's silent key coercions."""

    if value is None or type(value) is bool:
        return value
    if type(value) is int:
        return value
    if type(value) is float:
        if not math.isfinite(value):
            _fail(f"{where} contains a non-finite number")
        return value
    if type(value) is str:
        return _text(value, where=where, allow_empty=True, allow_control=True)
    if type(value) is list:
        return [
            _normalize_json_value(item, where=f"{where}[{index}]")
            for index, item in enumerate(value)
        ]
    if isinstance(value, Mapping):
        normalized: dict[str, object] = {}
        for raw_key, raw_value in value.items():
            key = _text(raw_key, where=f"{where} key", allow_empty=True)
            normalized[key] = _normalize_json_value(
                raw_value, where=f"{where}.{key}"
            )
        return normalized
    _fail(f"{where} contains a value that is not JSON data")


def _normalize_argv(value: object) -> list[str]:
    if type(value) is not list or not value:
        _fail("action.task.argv must be a non-empty array")
    argv = [
        _text(
            item,
            where=f"action.task.argv[{index}]",
            allow_empty=True,
            allow_control=True,
        )
        for index, item in enumerate(value)
    ]
    if not PurePosixPath(argv[0]).is_absolute():
        _fail("action.task.argv[0] must be an absolute executable path")
    return argv


def _normalize_string_mapping(
    value: object,
    *,
    where: str,
    key_pattern: re.Pattern[str] | None = None,
) -> dict[str, str]:
    if not isinstance(value, Mapping):
        _fail(f"{where} must be an object")
    normalized: dict[str, str] = {}
    for raw_key, raw_value in value.items():
        key = _text(raw_key, where=f"{where} key", pattern=key_pattern)
        normalized[key] = _text(
            raw_value, where=f"{where}.{key}", allow_empty=True, allow_control=True
        )
    return dict(sorted(normalized.items()))


def _decode_strict_json(raw: bytes, *, where: str) -> object:
    def pairs_hook(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ActionContractError(f"{where} contains duplicate key {key!r}")
            result[key] = value
        return result

    def reject_constant(value: str) -> object:
        raise ActionContractError(f"{where} contains non-finite number {value}")

    try:
        return json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=pairs_hook,
            parse_constant=reject_constant,
        )
    except ActionContractError:
        raise
    except (UnicodeDecodeError, ValueError) as exc:
        # ``json.loads`` can raise a plain ValueError when an integer exceeds
        # Python's configured digit limit.  Durable hostile input must stay in
        # the fail-closed PrismaBuild error vocabulary instead of escaping as
        # an implementation-specific parser exception.
        raise ActionContractError(f"{where} is not strict UTF-8 JSON") from exc


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


def _open_retrying_would_block(
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


def _read_regular_file(path: Path, *, where: str) -> bytes:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = _open_retrying_would_block(path, flags)
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


def _file_identity(path: Path, *, where: str) -> tuple[str, int]:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = _open_retrying_would_block(path, flags)
    except OSError as exc:
        raise ActionContractError(
            f"cannot open {where} as a regular file: {path}"
        ) from exc
    digest = hashlib.sha256()
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


def identify_executable(path: str | Path) -> dict[str, object]:
    """Return the exact regular-file identity behind an absolute argv[0]."""

    declared = Path(path)
    if not declared.is_absolute():
        _fail("executable path must be absolute")
    try:
        resolved = declared.resolve(strict=True)
    except OSError as exc:
        raise ActionContractError(
            f"cannot resolve executable path: {declared}"
        ) from exc
    digest, size = _file_identity(resolved, where="action executable")
    try:
        mode = resolved.stat().st_mode
    except OSError as exc:
        raise ActionContractError(
            f"cannot inspect executable path: {resolved}"
        ) from exc
    if mode & 0o111 == 0:
        _fail(f"action executable is not executable: {resolved}")
    return {
        "path": str(declared),
        "resolved_path": str(resolved),
        "sha256": digest,
        "bytes": size,
    }


def _identify_runtime_source(path: str | Path, *, where: str) -> dict[str, object]:
    """Return the exact regular-file identity of worker implementation source."""

    declared = Path(path)
    if not declared.is_absolute():
        _fail(f"{where} path must be absolute")
    try:
        resolved = declared.resolve(strict=True)
    except OSError as exc:
        raise ActionContractError(f"cannot resolve {where}: {declared}") from exc
    digest, size = _file_identity(resolved, where=where)
    return {
        "path": str(declared),
        "resolved_path": str(resolved),
        "sha256": digest,
        "bytes": size,
    }


# This snapshot is part of module initialization, not worker preflight.  It
# therefore precedes action loading and execution and represents the source
# implementation this worker process loaded as closely as Python exposes it.
_LOADED_WORKER_CORE_IDENTITY = _identify_runtime_source(
    Path(__file__).resolve(), where="PrismaBuild worker core"
)


def _normalize_runtime_source(value: object, *, where: str) -> dict[str, object]:
    raw = _exact_mapping(value, keys=_EXECUTABLE_KEYS, where=where)
    path = _text(raw["path"], where=f"{where}.path")
    resolved_path = _text(
        raw["resolved_path"], where=f"{where}.resolved_path"
    )
    if not Path(path).is_absolute() or not Path(resolved_path).is_absolute():
        _fail(f"{where} paths must be absolute")
    return {
        "path": path,
        "resolved_path": resolved_path,
        "sha256": _sha256(raw["sha256"], where=f"{where}.sha256"),
        "bytes": _nonnegative_integer(raw["bytes"], where=f"{where}.bytes"),
    }


def _verify_runtime_source_unchanged(
    expected: Mapping[str, object], *, where: str, changed_message: str
) -> None:
    try:
        observed = _identify_runtime_source(str(expected["path"]), where=where)
    except ActionContractError as exc:
        raise LocalActionError(f"{changed_message}: {exc}") from exc
    if observed != expected:
        raise LocalActionError(changed_message)


def _worker_runtime_identity(
    worker_launcher_identity: object | None,
) -> dict[str, object]:
    """Bind the load-time core and optional entry-point launcher snapshot."""

    core = _normalize_runtime_source(
        _LOADED_WORKER_CORE_IDENTITY,
        where="loaded PrismaBuild worker core",
    )
    _verify_runtime_source_unchanged(
        core,
        where="PrismaBuild worker core",
        changed_message="PrismaBuild worker core changed after module import",
    )
    launcher = (
        None
        if worker_launcher_identity is None
        else _normalize_runtime_source(
            worker_launcher_identity,
            where="captured PrismaBuild worker launcher",
        )
    )
    if launcher is not None:
        _verify_runtime_source_unchanged(
            launcher,
            where="PrismaBuild worker launcher",
            changed_message=(
                "PrismaBuild worker launcher changed after entry-point capture"
            ),
        )
    body: dict[str, object] = {
        "schema": WORKER_RUNTIME_SCHEMA_V1,
        "launch_kind": "in_process" if launcher is None else "script",
        "core": core,
        "launcher": launcher,
    }
    return {**body, "runtime_sha256": canonical_sha256(body)}


def _validate_worker_runtime(value: object) -> dict[str, object]:
    raw = _exact_mapping(
        value, keys=_RUNTIME_KEYS, where="worker attestation.runtime"
    )
    if raw["schema"] != WORKER_RUNTIME_SCHEMA_V1:
        _fail(
            "worker attestation.runtime.schema must be "
            f"{WORKER_RUNTIME_SCHEMA_V1!r}"
        )
    launch_kind = _text(
        raw["launch_kind"], where="worker attestation.runtime.launch_kind"
    )
    if launch_kind not in {"in_process", "script"}:
        _fail("worker attestation.runtime.launch_kind is unsupported")
    core = _normalize_runtime_source(
        raw["core"], where="worker attestation.runtime.core"
    )
    launcher_raw = raw["launcher"]
    launcher = (
        None
        if launcher_raw is None
        else _normalize_runtime_source(
            launcher_raw, where="worker attestation.runtime.launcher"
        )
    )
    if (launch_kind == "in_process") != (launcher is None):
        _fail("worker attestation runtime launch_kind and launcher disagree")
    body: dict[str, object] = {
        "schema": WORKER_RUNTIME_SCHEMA_V1,
        "launch_kind": launch_kind,
        "core": core,
        "launcher": launcher,
    }
    recorded = _sha256(
        raw["runtime_sha256"],
        where="worker attestation.runtime.runtime_sha256",
    )
    if recorded != canonical_sha256(body):
        _fail("worker attestation runtime digest does not match its body")
    return {**body, "runtime_sha256": recorded}


def _verify_worker_runtime_unchanged(runtime: object) -> None:
    expected = _validate_worker_runtime(runtime)
    core = expected["core"]
    assert isinstance(core, Mapping)
    loaded_core = _normalize_runtime_source(
        _LOADED_WORKER_CORE_IDENTITY,
        where="loaded PrismaBuild worker core",
    )
    if core != loaded_core:
        raise LocalActionError(
            "attested PrismaBuild worker core differs from module import identity"
        )
    _verify_runtime_source_unchanged(
        core,
        where="PrismaBuild worker core",
        changed_message="PrismaBuild worker core changed after module import",
    )
    launcher = expected["launcher"]
    if isinstance(launcher, Mapping):
        _verify_runtime_source_unchanged(
            launcher,
            where="PrismaBuild worker launcher",
            changed_message=(
                "PrismaBuild worker launcher changed after entry-point capture"
            ),
        )


def executable_toolchain_contract(path: str | Path) -> dict[str, str]:
    """Build the action-key fields that bind argv[0] to exact file bytes."""

    identity = identify_executable(path)
    return {
        "argv0.sha256": str(identity["sha256"]),
        "argv0.bytes": str(identity["bytes"]),
    }


def _probe_nvidia_accelerators() -> list[dict[str, str]]:
    """Read live NVIDIA compute/driver facts without importing the task stack."""

    executable = Path("/usr/bin/nvidia-smi")
    if not executable.is_file():
        return []
    argv = [
        str(executable),
        "--query-gpu=compute_cap,driver_version",
        "--format=csv,noheader,nounits",
    ]
    try:
        completed = subprocess.run(
            argv,
            shell=False,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="strict",
            env={"LANG": "C", "LC_ALL": "C"},
            timeout=10.0,
        )
    except (OSError, UnicodeError, subprocess.TimeoutExpired):
        return []
    if completed.returncode != 0:
        return []
    facts: set[tuple[str, str]] = set()
    for raw_line in completed.stdout.splitlines():
        fields = [field.strip() for field in raw_line.split(",")]
        if len(fields) != 2:
            raise ActionContractError("nvidia-smi returned malformed accelerator facts")
        capability, driver = fields
        if re.fullmatch(r"[0-9]+\.[0-9]+", capability) is None:
            raise ActionContractError("nvidia-smi returned an invalid compute capability")
        if _VERSION_RE.fullmatch(driver) is None:
            raise ActionContractError("nvidia-smi returned an invalid driver version")
        facts.add((capability, driver))
    return [
        {
            "kind": "nvidia",
            "compute_capability": capability,
            "driver_version": driver,
        }
        for capability, driver in sorted(facts)
    ]


def _constraint_tokens(value: str) -> list[str]:
    return sorted(
        {
            token
            for token in re.split(r"[^A-Za-z0-9._+:/-]+", value)
            if token
        }
    )


_CGROUP_JOB_RE = re.compile(r"(?:^|/)job_([1-9][0-9]*)(?:[./]|$)")


def _slurm_job_from_cgroup() -> tuple[str, str] | None:
    """Read the SLURM job that owns this process from its kernel-owned cgroup.

    Returns ``(job_id, cgroup_path)`` when exactly one ``job_<id>`` cgroup
    holds the process, or ``None`` when no controller row names a job.  The
    job id is derived here, not read from the environment: ``proctrack/cgroup``
    places a job's processes under ``job_<id>``, and a batch script cannot
    move itself out of that hierarchy, whereas it can export any variable.
    """

    raw = _read_regular_file(Path("/proc/self/cgroup"), where="worker cgroup")
    try:
        lines = raw.decode("utf-8").splitlines()
    except UnicodeDecodeError as exc:
        raise ActionContractError("worker cgroup is not UTF-8") from exc
    jobs: set[str] = set()
    paths: set[str] = set()
    for line in lines:
        fields = line.split(":", 2)
        if len(fields) != 3:
            continue
        match = _CGROUP_JOB_RE.search(fields[2])
        if match is not None:
            jobs.add(match.group(1))
            paths.add(fields[2])
    if not jobs:
        return None
    if len(jobs) != 1 or len(paths) != 1:
        raise ActionContractError(
            "worker cgroup membership names more than one SLURM job"
        )
    return next(iter(jobs)), _text(next(iter(paths)), where="worker SLURM cgroup")


def _verify_slurm_process_membership(job_id: str) -> str:
    """Bind a claimed SLURM job id to this process's kernel-owned cgroup."""

    if re.fullmatch(r"[1-9][0-9]*", job_id) is None:
        raise ActionContractError("SLURM_JOB_ID must be a positive numeric job id")
    kernel = _slurm_job_from_cgroup()
    if kernel is None or kernel[0] != job_id:
        raise ActionContractError(
            "SLURM environment is not attested by this process's cgroup membership"
        )
    return kernel[1]


def _run_scontrol(argv: Sequence[str]) -> subprocess.CompletedProcess[str]:
    """Run one ``scontrol`` invocation by absolute path; tests replace this."""

    executable = next(
        (candidate for candidate in _SCONTROL_CANDIDATES if Path(candidate).is_file()),
        None,
    )
    if executable is None:
        raise ActionContractError(
            "scontrol is not installed at any of "
            f"{', '.join(_SCONTROL_CANDIDATES)}; the controller cannot be asked"
        )
    return subprocess.run(
        [executable, *argv],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env={"LANG": "C", "LC_ALL": "C", **{
            key: value for key, value in os.environ.items()
            if key in {"SLURM_CONF", "SLURM_CLUSTER_NAME", "HOME"}
        }},
        timeout=30.0,
    )


def _controller_name(environment: Mapping[str, str]) -> str:
    """Name the controller for a refusal, without asking the controller."""

    cluster = environment.get("SLURM_CLUSTER_NAME") or ""
    conf = Path(environment.get("SLURM_CONF") or "/etc/slurm/slurm.conf")
    host = ""
    try:
        for line in conf.read_text(encoding="utf-8", errors="replace").splitlines():
            stripped = line.strip()
            if stripped.startswith("SlurmctldHost="):
                host = stripped.split("=", 1)[1].strip()
                break
    except OSError:
        pass
    parts = [part for part in (
        f"cluster {cluster!r}" if cluster else "",
        f"SlurmctldHost={host}" if host else "",
    ) if part]
    return "the SLURM controller" + (f" ({', '.join(parts)})" if parts else "")


def _scontrol_show(
    kind: str, name: str, *, environment: Mapping[str, str]
) -> dict[str, str]:
    """Ask the controller for one job or node record, as ``key=value`` fields.

    Retries on the schedule in ``SCONTROL_RETRY_DELAYS_S`` and then refuses.
    An unreachable controller is never read as attested.
    """

    argv = ["--oneliner", "show", kind, name]
    failures: list[str] = []
    for attempt, delay in enumerate((*SCONTROL_RETRY_DELAYS_S, None)):
        try:
            completed = _run_scontrol(argv)
        except (OSError, subprocess.TimeoutExpired) as exc:
            failures.append(f"attempt {attempt + 1}: {exc}")
        else:
            if completed.returncode == 0 and completed.stdout.strip():
                fields: dict[str, str] = {}
                for token in completed.stdout.strip().split():
                    key, sep, value = token.partition("=")
                    if sep and key not in fields:
                        fields[key] = value
                return fields
            detail = (completed.stderr or completed.stdout).strip()
            failures.append(
                f"attempt {attempt + 1}: exit {completed.returncode}: "
                f"{detail or 'no output'}"
            )
        if delay is None:
            break
        time.sleep(delay)
    raise ActionContractError(
        f"{_controller_name(environment)} did not answer `scontrol show "
        f"{kind} {name}` after {len(failures)} attempts; a host_class_keyed "
        f"action is refused rather than assumed placed: {failures[-1]}"
    )


def _feature_conjunction(value: str) -> list[str]:
    """The Feature names a job's constraint requires, or ``[]``.

    Only a plain conjunction (``a&b``) counts.  A disjunction, a bracketed
    set, a count, or a parenthesized group can be satisfied without any one
    named Feature, so such a constraint attests no class and yields ``[]``.
    """

    text = value.strip()
    if not text or text == "(null)":
        return []
    if re.search(r"[|\[\]()*]", text):
        return []
    terms = [term.strip() for term in text.split("&")]
    if any(_SCOPE_TOKEN_RE.fullmatch(term) is None for term in terms):
        return []
    return sorted(set(terms))


def _feature_list(value: str) -> list[str]:
    text = value.strip()
    if not text or text == "(null)":
        return []
    return sorted({
        token for token in (item.strip() for item in text.split(","))
        if token and _SCOPE_TOKEN_RE.fullmatch(token) is not None
    })


def _controller_evidence(
    job_id: str, *, environment: Mapping[str, str]
) -> dict[str, object]:
    """What the controller holds for this job and the node running it."""

    job = _scontrol_show("job", job_id, environment=environment)
    if job.get("JobId") != job_id:
        raise ActionContractError(
            f"the SLURM controller answered for a different job than {job_id}"
        )
    batch_host = job.get("BatchHost", "").strip().lower()
    if not batch_host or batch_host == "(null)":
        raise ActionContractError(
            "the SLURM controller reports no BatchHost for this job"
        )
    node = _scontrol_show("node", batch_host, environment=environment)
    return {
        "partition": _text(
            job.get("Partition", ""), where="scontrol Partition",
            pattern=_SCOPE_TOKEN_RE,
        ),
        "batch_host": _text(batch_host, where="scontrol BatchHost", pattern=_ID_RE),
        "job_features": _feature_conjunction(job.get("Features", "")),
        "node_active_features": _feature_list(node.get("ActiveFeatures", "")),
    }


def _collect_worker_evidence(
    environment: Mapping[str, str] | None = None,
    *,
    attest_host_class: str | None = None,
) -> dict[str, object]:
    """Collect facts from the live worker and its SLURM job, if any.

    The job id comes from this process's cgroup; ``SLURM_JOB_ID``,
    ``SLURMD_NODENAME`` and ``SLURM_JOB_PARTITION`` are recorded and checked
    for consistency but decide nothing.  When ``attest_host_class`` names a
    class, the controller is also asked for the job's constraint and the
    node's active Features, which is the only evidence that can attest it.
    """

    env = os.environ if environment is None else environment
    system = platform.system().lower()
    machine = platform.machine().lower()
    libc_name, libc_version = platform.libc_ver()
    libc = f"{libc_name.lower()}-{libc_version}" if libc_name else "unknown"
    hostname = socket.gethostname().lower()
    slurm_values = {
        "job_id": env.get("SLURM_JOB_ID"),
        "node_name": env.get("SLURMD_NODENAME"),
        "partition": env.get("SLURM_JOB_PARTITION"),
    }
    present = [value is not None for value in slurm_values.values()]
    if any(present) and not all(present):
        raise ActionContractError(
            "partial SLURM worker evidence is ambiguous; job, node, and partition are required"
        )
    slurm: dict[str, object] | None = None
    source = "local"
    kernel = _slurm_job_from_cgroup() if any(present) else None
    if all(present):
        source = "slurm"
        claimed = _text(
            slurm_values["job_id"], where="SLURM_JOB_ID", pattern=_SCOPE_TOKEN_RE
        )
        if kernel is None or kernel[0] != claimed:
            raise ActionContractError(
                "SLURM environment is not attested by this process's cgroup membership"
            )
        job_id, cgroup = kernel
        node_name = str(slurm_values["node_name"]).lower()
        partition = str(slurm_values["partition"])
        slurm = {
            "job_id": job_id,
            "node_name": _text(
                node_name, where="SLURMD_NODENAME", pattern=_ID_RE
            ),
            "partition": _text(
                partition, where="SLURM_JOB_PARTITION", pattern=_SCOPE_TOKEN_RE
            ),
            # Never set in a job's environment (SLURM sets it only for the
            # Prolog and Epilog), and submitter-writable if it were; kept as
            # recorded provenance, read by no verdict.
            "constraints": _constraint_tokens(env.get("SLURM_JOB_CONSTRAINTS", "")),
            "cgroup": cgroup,
        }
        if attest_host_class is not None:
            controller = _controller_evidence(job_id, environment=env)
            if controller["batch_host"] != slurm["node_name"]:
                raise ActionContractError(
                    "SLURMD_NODENAME disagrees with the BatchHost the SLURM "
                    "controller holds for this job"
                )
            slurm["controller"] = controller
    elif attest_host_class is not None:
        raise ActionContractError(
            "host_class_keyed actions require complete SLURM job evidence"
        )
    return {
        "source": source,
        "hostname": _text(hostname, where="worker hostname", pattern=_ID_RE),
        "system": _text(system, where="worker system", pattern=_SCOPE_TOKEN_RE),
        "machine": _text(machine, where="worker machine", pattern=_SCOPE_TOKEN_RE),
        "libc": _text(libc, where="worker libc", pattern=_SCOPE_TOKEN_RE),
        "accelerators": _probe_nvidia_accelerators(),
        "slurm": slurm,
    }


def _platform_key_from_evidence(evidence: Mapping[str, object]) -> str:
    system = str(evidence["system"])
    machine = str(evidence["machine"])
    accelerators = evidence["accelerators"]
    assert isinstance(accelerators, list)
    capabilities = {
        str(accelerator["compute_capability"])
        for accelerator in accelerators
        if isinstance(accelerator, Mapping) and accelerator.get("kind") == "nvidia"
    }
    if len(capabilities) > 1:
        raise ActionContractError(
            "worker exposes heterogeneous NVIDIA compute capabilities; platform is ambiguous"
        )
    suffix = ""
    if capabilities:
        capability = next(iter(capabilities))
        suffix = f"-sm{capability.replace('.', '')}"
    return _text(
        f"{system}-{machine}{suffix}",
        where="derived worker platform_key",
        pattern=_SCOPE_TOKEN_RE,
    )


def _worker_identity_from_evidence(evidence: Mapping[str, object]) -> str:
    slurm = evidence["slurm"]
    if isinstance(slurm, Mapping):
        return str(slurm["node_name"])
    return str(evidence["hostname"])


def _host_class_from_evidence(
    evidence: Mapping[str, object], *, expected: str | None
) -> str | None:
    """The attested host class, or ``None`` for work that keys on none.

    A class is attested only by what the controller holds: the node's
    ``ActiveFeatures`` name it, and the job's own constraint requires it, so
    the scheduler enforced the placement rather than a worker observing it.
    Partition names and ``SLURM_*`` variables are recorded evidence, never
    the verdict: no partition is a class, and a batch script can export
    anything.  Scheduler metadata is never turned into an ambient class
    assertion for portable or platform-keyed work.
    """

    if expected is None:
        return None
    slurm = evidence["slurm"]
    if not isinstance(slurm, Mapping):
        raise ActionContractError(
            "host_class_keyed actions require complete SLURM job evidence"
        )
    controller = slurm.get("controller")
    if not isinstance(controller, Mapping):
        raise ActionContractError(
            "host_class_keyed actions require the SLURM controller's record "
            "of the job and its node; environment variables attest no class"
        )
    active = controller["node_active_features"]
    required = controller["job_features"]
    assert isinstance(active, list) and isinstance(required, list)
    if expected not in active:
        raise ActionContractError(
            f"the SLURM node {controller['batch_host']} does not carry the "
            f"Feature {expected!r} the action is keyed on"
        )
    if expected not in required:
        raise ActionContractError(
            f"the SLURM job's constraint does not require the Feature "
            f"{expected!r}; placement on it was not enforced by the scheduler"
        )
    return expected


def live_platform_toolchain_contract() -> dict[str, str]:
    """The ABI and accelerator toolchain fields of this box.

    Together with ``executable_toolchain_contract`` these are the fields a
    nonportable action must declare, and they can be read only on the box
    whose facts they are.  A submitter that seals them binds the action to
    boxes that verify identically.
    """

    evidence = _collect_worker_evidence()
    fields = {
        "system": str(evidence["system"]),
        "machine": str(evidence["machine"]),
        "libc": str(evidence["libc"]),
    }
    accelerators = evidence["accelerators"]
    assert isinstance(accelerators, list)
    capabilities = {str(row["compute_capability"]) for row in accelerators}
    drivers = {str(row["driver_version"]) for row in accelerators}
    if len(capabilities) == 1 and len(drivers) == 1:
        fields["cuda_compute_capability"] = next(iter(capabilities))
        fields["nvidia_driver"] = next(iter(drivers))
    return fields


def _probe_python_toolchain(executable: Path) -> dict[str, str]:
    script = "\n".join(
        (
            "import importlib.metadata as metadata",
            "import json",
            "import platform",
            "out = {'python': platform.python_version()}",
            "for name in ('torch', 'transformers', 'vllm', 'gridbook'):",
            "    try:",
            "        out[name] = metadata.version(name)",
            "    except metadata.PackageNotFoundError:",
            "        pass",
            "print(json.dumps(out, sort_keys=True, separators=(',', ':')))",
        )
    )
    try:
        completed = subprocess.run(
            [str(executable), "-I", "-c", script],
            shell=False,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="strict",
            env={"LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"},
            timeout=30.0,
        )
    except (OSError, UnicodeError, subprocess.TimeoutExpired) as exc:
        raise ActionContractError(
            f"cannot probe declared Python toolchain through {executable}"
        ) from exc
    if completed.returncode != 0 or "\n" in completed.stdout.strip():
        raise ActionContractError(
            f"declared Python toolchain probe failed through {executable}"
        )
    value = _decode_strict_json(
        completed.stdout.strip().encode("utf-8"), where="Python toolchain probe"
    )
    if not isinstance(value, Mapping) or any(
        type(key) is not str or type(item) is not str for key, item in value.items()
    ):
        raise ActionContractError("Python toolchain probe returned malformed data")
    return dict(value)


def _toolchain_value_matches(declared: str, observed: str) -> bool:
    if declared == observed:
        return True
    short = re.fullmatch(r"([0-9]+\.[0-9]+)(?:\+([A-Za-z0-9._-]+))?", declared)
    full = re.fullmatch(
        r"([0-9]+\.[0-9]+)(?:\.[0-9]+)?(?:\+([A-Za-z0-9._-]+))?",
        observed,
    )
    return bool(
        short
        and full
        and short.group(1) == full.group(1)
        and short.group(2) == full.group(2)
    )


def build_code_closure(root: str | Path, files: Sequence[str]) -> dict[str, object]:
    """Hash a declared, path-independent code closure below ``root``.

    Only declared regular files participate.  Symlinks and traversal are
    refused, and the logical relative paths (not the checkout root) are bound
    into the closure identity.
    """

    checkout_root = Path(root)
    if not checkout_root.is_absolute():
        _fail("code closure root must be absolute")
    normalized_paths = [
        _normalize_relative_path(path, where=f"code closure file[{index}]", dot_ok=False)
        for index, path in enumerate(files)
    ]
    if not normalized_paths:
        _fail("code closure must contain at least one file")
    if len(set(normalized_paths)) != len(normalized_paths):
        _fail("code closure file paths must be unique")
    entries: list[dict[str, object]] = []
    for relative in sorted(normalized_paths):
        digest, size = _file_identity(
            checkout_root / relative, where=f"code closure file {relative!r}"
        )
        entries.append({"path": relative, "sha256": digest, "bytes": size})
    body: dict[str, object] = {
        "schema": CODE_CLOSURE_SCHEMA_V1,
        "files": entries,
    }
    return {**body, "closure_sha256": canonical_sha256(body)}


def validate_code_closure(value: object) -> dict[str, object]:
    closure = _exact_mapping(value, keys=_CLOSURE_KEYS, where="action.code_closure")
    if closure["schema"] != CODE_CLOSURE_SCHEMA_V1:
        _fail(f"action.code_closure.schema must be {CODE_CLOSURE_SCHEMA_V1!r}")
    raw_files = closure["files"]
    if type(raw_files) is not list or not raw_files:
        _fail("action.code_closure.files must be a non-empty array")
    files: list[dict[str, object]] = []
    previous: str | None = None
    for index, raw_entry in enumerate(raw_files):
        entry = _exact_mapping(
            raw_entry,
            keys=_CLOSURE_FILE_KEYS,
            where=f"action.code_closure.files[{index}]",
        )
        path = _normalize_relative_path(
            entry["path"],
            where=f"action.code_closure.files[{index}].path",
            dot_ok=False,
        )
        if previous is not None and path <= previous:
            _fail("action.code_closure.files must be unique and sorted by path")
        previous = path
        files.append(
            {
                "path": path,
                "sha256": _sha256(
                    entry["sha256"],
                    where=f"action.code_closure.files[{index}].sha256",
                ),
                "bytes": _nonnegative_integer(
                    entry["bytes"],
                    where=f"action.code_closure.files[{index}].bytes",
                ),
            }
        )
    body = {"schema": CODE_CLOSURE_SCHEMA_V1, "files": files}
    expected = canonical_sha256(body)
    recorded = _sha256(
        closure["closure_sha256"], where="action.code_closure.closure_sha256"
    )
    if recorded != expected:
        _fail("action.code_closure.closure_sha256 does not match its files")
    return {**body, "closure_sha256": recorded}


def verify_code_closure(value: object, root: str | Path) -> dict[str, object]:
    """Re-hash every closure member from a live checkout."""

    expected = validate_code_closure(value)
    live = build_code_closure(
        root, [str(entry["path"]) for entry in expected["files"]]  # type: ignore[index]
    )
    if live != expected:
        raise ActionContractError(
            "live code closure differs from the action-pinned closure"
        )
    return expected


def is_pbrun_generated_path(path: str | Path) -> bool:
    """Return whether a basename belongs to pbrun's generated-file grammar."""

    name = Path(path).name
    for prefix, suffix in (
        (PBRUN_STAMP_PREFIX, ".json"),
        (PBRUN_RESULT_PREFIX, ".txt"),
    ):
        if not (name.startswith(prefix) and name.endswith(suffix)):
            continue
        token = name[len(prefix):-len(suffix)]
        if len(token) == PBRUN_GENERATED_FINGERPRINT_HEX_LENGTH and all(
            character in "0123456789abcdef" for character in token
        ):
            return True
    return False


def pbrun_git_exclude_patterns() -> tuple[str, str]:
    """Return Git patterns for exactly pbrun's generated basenames."""

    fingerprint = "[0-9a-f]" * PBRUN_GENERATED_FINGERPRINT_HEX_LENGTH
    return (
        f"{PBRUN_STAMP_PREFIX}{fingerprint}.json",
        f"{PBRUN_RESULT_PREFIX}{fingerprint}.txt",
    )


def find_git_worktree_marker(root: str | Path) -> Path | None:
    """Find a filesystem ``.git`` marker at or above a requested cwd."""

    requested = Path(root).resolve(strict=False)
    for directory in (requested, *requested.parents):
        marker = directory / ".git"
        try:
            marker.lstat()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise ActionContractError(
                f"cannot inspect Git worktree marker {marker}: {exc}"
            ) from exc
        return marker
    return None


def git_checkout_identity(root: str | Path) -> dict[str, str]:
    """Return pbrun's canonical commit-plus-working-tree identity.

    The submitter and the trusted worker must call this same implementation.
    Hashing a JSON file that merely *claims* this identity does not bind the
    tree: a queued or retried action can otherwise execute whatever bytes the
    checkout contains when a worker eventually claims it.

    A plain directory remains representable as ``no-git`` for the legacy
    path-addressed pbrun mode. Commit-addressed materialisation is responsible
    for refusing that mode when portability is requested.
    """

    checkout = Path(root)
    repository_detected = find_git_worktree_marker(checkout) is not None

    def _git(
        *args: str,
        input_text: str | None = None,
        accepted_returncodes: tuple[int, ...] = (0,),
    ) -> str:
        try:
            completed = subprocess.run(
                ["git", "-C", str(checkout), *args],
                capture_output=True,
                text=True,
                errors="surrogateescape",
                input=input_text,
                timeout=30,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            if repository_detected:
                raise ActionContractError(
                    "cannot compute pbrun checkout identity: Git "
                    f"{' '.join(args)} failed: {exc}"
                ) from exc
            return ""
        if completed.returncode not in accepted_returncodes:
            if repository_detected:
                detail = (completed.stderr or completed.stdout).strip()
                raise ActionContractError(
                    "cannot compute pbrun checkout identity: Git "
                    f"{' '.join(args)} failed: "
                    f"{detail or completed.returncode}"
                )
            return ""
        return completed.stdout

    top_level = _git("rev-parse", "--show-toplevel").rstrip("\n")
    if top_level:
        # Identity covers the repository, even when pbrun's requested cwd is
        # a package below it. Git reports the tracked delta for that closure;
        # the filesystem special-inode scan must cover the same closure.
        checkout = Path(top_level)
        repository_detected = True
    head = _git("rev-parse", "HEAD").strip() or "no-git"

    # Let Git delimit untracked pathnames. Line-oriented porcelain C-quotes
    # newlines, quotes, and backslashes, and hand-unquoting that display form
    # can bind ``:unreadable`` instead of the actual file bytes. ``ls-files
    # -z`` emits the repository-root-relative filesystem path verbatim.
    untracked_paths = [
        path
        for path in _git(
            "ls-files", "--others", "--exclude-standard", "-z"
        ).split("\0")
        if path and not is_pbrun_generated_path(path)
    ]
    # Git deliberately omits FIFOs, sockets, and device nodes from its
    # untracked roster. Find those without opening them: opening a FIFO can
    # block forever, and no special inode has stable bytes Git can transport.
    # Prune Git-ignored directories before walking so an ignored environment
    # or cache does not turn identity into an unrelated filesystem crawl.
    ignored_directory_output = _git(
        "ls-files",
        "--others",
        "--ignored",
        "--exclude-standard",
        "--directory",
        "-z",
    )
    ignored_directories = {
        value.rstrip("/")
        for value in ignored_directory_output.split("\0")
        if value.endswith("/")
    }
    special_paths: list[str] = []

    # Use scandir directly rather than os.walk followed by a second lstat for
    # every entry.  Identity is on pbrun's submission hot path, and DirEntry
    # can answer the supported-kind predicates from the directory record on
    # common filesystems while preserving fail-closed error handling.
    pending = [checkout]
    while pending:
        current = pending.pop()
        relative_directory = current.relative_to(checkout)
        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    relative = (relative_directory / entry.name).as_posix()
                    try:
                        if entry.is_symlink():
                            continue
                        if entry.is_dir(follow_symlinks=False):
                            if (
                                entry.name != ".git"
                                and relative not in ignored_directories
                            ):
                                pending.append(Path(entry.path))
                            continue
                        if entry.is_file(follow_symlinks=False):
                            continue
                    except OSError as exc:
                        raise ActionContractError(
                            "cannot inspect pbrun checkout path "
                            f"{relative!r}: {exc}"
                        ) from exc
                    special_paths.append(relative)
        except OSError as exc:
            raise ActionContractError(
                f"cannot inspect pbrun checkout identity: {exc}"
            ) from exc
    if special_paths:
        ignored_specials = set(
            value
            for value in _git(
                "check-ignore",
                "--no-index",
                "-z",
                "--stdin",
                input_text="\0".join(special_paths) + "\0",
                accepted_returncodes=(0, 1),
            ).split("\0")
            if value
        )
        unsupported = sorted(set(special_paths) - ignored_specials)
        if unsupported:
            raise ActionContractError(
                "pbrun checkout identity refuses untracked paths with an "
                "unsupported file type: " + ", ".join(map(repr, unsupported))
            )
    untracked: list[tuple[str, str]] = []
    for relative in untracked_paths:
        member = checkout / relative
        try:
            digest = hashlib.sha256()
            member_stat = member.lstat()
            if stat.S_ISDIR(member_stat.st_mode):
                continue
            if stat.S_ISLNK(member_stat.st_mode):
                digest.update(b"symlink\0")
                digest.update(os.fsencode(os.readlink(member)))
            elif stat.S_ISREG(member_stat.st_mode):
                digest.update(b"file\0")
                with member.open("rb") as handle:
                    for chunk in iter(lambda: handle.read(1 << 20), b""):
                        digest.update(chunk)
            else:
                raise ActionContractError(
                    "pbrun checkout identity refuses an untracked path with "
                    f"an unsupported file type: {relative!r}"
                )
            untracked.append((relative, digest.hexdigest()))
        except OSError as exc:
            raise ActionContractError(
                f"cannot hash untracked path {relative!r}: {exc}"
            ) from exc
    dirty = bytearray(os.fsencode(_git("diff", "--binary", "HEAD")))
    for relative, digest in sorted(untracked, key=lambda item: os.fsencode(item[0])):
        dirty.extend(b"\0untracked\0")
        dirty.extend(os.fsencode(relative))
        dirty.extend(b"\0")
        dirty.extend(digest.encode("ascii"))
    return {
        "head": head,
        "dirty_sha256": hashlib.sha256(dirty).hexdigest(),
    }


def validate_pbrun_snapshot_ref_name(value: object, *, where: str) -> str:
    """One branch name a materializing worker may create from a queue record.

    These names are not constants: they arrive on the queue item and become
    ``refs/heads/<name>`` and a ``git fetch`` refspec inside the claiming
    worker.  A colon splits a refspec, a leading dash becomes an option, and
    the Git revision grammar (``~ ^ @{ ..``) turns a name into a different
    object than the one the record priced.  ``HEAD`` and the snapshot's own
    ref name are refused for a second reason: both would make every later
    revision lookup in the materialized checkout ambiguous, which is the
    failure this whole contract exists to remove.
    """

    name = _text(value, where=where, pattern=_SNAPSHOT_REF_NAME_RE)
    if (
        name == "HEAD"
        or name.startswith(
            ("-", "/", ".", "refs/", PBRUN_CHECKOUT_SNAPSHOT_REF_NAME)
        )
        or name.endswith((".", "/", ".lock"))
        or ".." in name
        or "//" in name
        or "@{" in name
    ):
        _fail(f"{where} is not a usable branch name")
    return name


def validate_pbrun_checkout_snapshot(value: object) -> dict[str, object]:
    """Validate the immutable Git bundle a pbrun action executes from."""

    if not isinstance(value, Mapping):
        _fail("pbrun checkout snapshot must be an object")
    schema = value.get("schema")
    if schema not in {
        PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V1, PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V2
    }:
        _fail(
            "pbrun checkout snapshot.schema must be "
            f"{PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V1!r} or "
            f"{PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V2!r}"
        )
    ancestral = schema == PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V2
    keys = {"schema", "commit", "subdirectory", "input"}
    raw = _exact_mapping(
        value,
        # Each schema owns one exact key set, so a v1 record cannot smuggle
        # ancestry a v1 materializer would silently ignore.
        keys=frozenset(keys | {"parent", "refs"} if ancestral else keys),
        where="pbrun checkout snapshot",
    )
    snapshot_input = validate_input_contract(raw["input"])
    if snapshot_input["id"] != PBRUN_CHECKOUT_SNAPSHOT_INPUT_ID:
        _fail(
            "pbrun checkout snapshot input.id must be "
            f"{PBRUN_CHECKOUT_SNAPSHOT_INPUT_ID!r}"
        )
    validated: dict[str, object] = {
        "schema": schema,
        "commit": _text(
            raw["commit"],
            where="pbrun checkout snapshot.commit",
            pattern=_GIT_OBJECT_ID_RE,
        ),
        "subdirectory": _normalize_relative_path(
            raw["subdirectory"],
            where="pbrun checkout snapshot.subdirectory",
            dot_ok=True,
        ),
        "input": snapshot_input,
    }
    if not ancestral:
        return validated
    parent = raw["parent"]
    validated["parent"] = None if parent is None else _text(
        parent,
        where="pbrun checkout snapshot.parent",
        pattern=_GIT_OBJECT_ID_RE,
    )
    raw_refs = raw["refs"]
    if not isinstance(raw_refs, Mapping):
        _fail("pbrun checkout snapshot.refs must be an object")
    refs: dict[str, str] = {}
    for raw_name in raw_refs:
        name = validate_pbrun_snapshot_ref_name(
            raw_name, where="pbrun checkout snapshot.refs name"
        )
        refs[name] = _text(
            raw_refs[raw_name],
            where=f"pbrun checkout snapshot.refs[{name}]",
            pattern=_GIT_OBJECT_ID_RE,
        )
    validated["refs"] = {name: refs[name] for name in sorted(refs)}
    return validated


def _materialized_git(root: Path, *args: str) -> str:
    """One read of the materialized checkout, refused rather than guessed."""

    try:
        completed = subprocess.run(
            ["git", "-C", str(root), *args],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ActionContractError(
            f"cannot read materialized pbrun checkout: Git "
            f"{' '.join(args)} failed: {exc}"
        ) from exc
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise ActionContractError(
            f"cannot read materialized pbrun checkout: Git "
            f"{' '.join(args)} failed: {detail or completed.returncode}"
        )
    return completed.stdout.strip()


def _verify_pbrun_checkout_ancestry(
    snapshot: Mapping[str, object], root: Path
) -> None:
    """Prove the ancestry the v2 contract sells, in the tree that will run.

    A snapshot whose bundle omitted a parent still checks out clean at the
    sealed commit, so the identity proof above cannot see the difference --
    and the action then fails inside its own diff-derived gate with
    ``fatal: ambiguous argument``, on a worker, after the queue said yes.
    Ancestry is part of what the record promises, so it is part of what the
    preflight proves.
    """

    parent = snapshot["parent"]
    lineage = _materialized_git(
        root, "rev-list", "--max-count=1", "--parents", "HEAD"
    ).split()
    expected = [str(snapshot["commit"])] + ([str(parent)] if parent else [])
    if lineage != expected:
        raise ActionContractError(
            "materialized pbrun checkout does not carry its sealed parent"
        )
    refs = snapshot["refs"]
    assert isinstance(refs, Mapping)
    for name in sorted(refs):
        resolved = _materialized_git(
            root, "rev-parse", "--verify", f"refs/heads/{name}^{{commit}}"
        )
        if resolved != str(refs[name]):
            raise ActionContractError(
                f"materialized pbrun checkout ref {name!r} differs from the "
                "sealed snapshot"
            )


def _verify_pbrun_checkout_identity(
    action: Mapping[str, object], root: Path
) -> None:
    """Verify the live Git semantics claimed by a pbrun closure stamp."""

    task = action["task"]
    assert isinstance(task, Mapping)
    if task["definition_id"] != "fleet/pbrun":
        return
    closure = action["code_closure"]
    assert isinstance(closure, Mapping)
    raw_files = closure["files"]
    assert isinstance(raw_files, list)
    stamps = [
        str(entry["path"])
        for entry in raw_files
        if isinstance(entry, Mapping)
        and Path(str(entry["path"])).name.startswith(PBRUN_STAMP_PREFIX)
    ]
    if len(stamps) != 1:
        raise ActionContractError(
            "fleet/pbrun code closure must contain exactly one pbrun stamp"
        )
    raw = _read_regular_file_nofollow(
        root / stamps[0], where="pbrun checkout identity stamp", max_bytes=4096
    )
    stamp = _exact_mapping(
        _decode_strict_json(raw, where="pbrun checkout identity stamp"),
        keys=frozenset({"cwd", "head", "dirty_sha256"}),
        where="pbrun checkout identity stamp",
    )
    recorded = {
        "head": _text(stamp["head"], where="pbrun checkout identity stamp.head"),
        "dirty_sha256": _sha256(
            stamp["dirty_sha256"],
            where="pbrun checkout identity stamp.dirty_sha256",
        ),
    }
    stamped_cwd = _text(
        stamp["cwd"], where="pbrun checkout identity stamp.cwd"
    )
    params = action["params"]
    assert isinstance(params, Mapping)
    source_cwd = _text(params.get("cwd"), where="fleet/pbrun params.cwd")
    if stamped_cwd != source_cwd:
        raise ActionContractError(
            "pbrun checkout identity stamp cwd differs from action params"
        )
    raw_snapshot = params.get("checkout_snapshot")
    if raw_snapshot is not None:
        snapshot = validate_pbrun_checkout_snapshot(raw_snapshot)
        if source_cwd != snapshot["subdirectory"]:
            raise ActionContractError(
                "pbrun checkout stamp cwd differs from snapshot subdirectory"
            )
        inputs = action["inputs"]
        assert isinstance(inputs, list)
        if snapshot["input"] not in inputs:
            raise ActionContractError(
                "pbrun checkout snapshot is absent from action.inputs"
            )
        live = git_checkout_identity(root)
        clean = hashlib.sha256(b"").hexdigest()
        if live != {"head": snapshot["commit"], "dirty_sha256": clean}:
            raise ActionContractError(
                "materialized pbrun checkout differs from its sealed commit"
            )
        if snapshot["schema"] == PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V2:
            _verify_pbrun_checkout_ancestry(snapshot, root)
        return
    if git_checkout_identity(root) != recorded:
        raise ActionContractError(
            "live pbrun checkout identity differs from its sealed stamp"
        )


def _normalize_inputs(value: object) -> list[dict[str, object]]:
    if type(value) is not list:
        _fail("action.inputs must be an array")
    inputs: list[dict[str, object]] = []
    seen: set[str] = set()
    for index, raw_entry in enumerate(value):
        entry = _exact_mapping(
            raw_entry, keys=_INPUT_KEYS, where=f"action.inputs[{index}]"
        )
        identity = _text(
            entry["id"], where=f"action.inputs[{index}].id", pattern=_ID_RE
        )
        if identity in seen:
            _fail("action.inputs ids must be unique")
        seen.add(identity)
        inputs.append(
            {
                "id": identity,
                "sha256": _sha256(
                    entry["sha256"], where=f"action.inputs[{index}].sha256"
                ),
                "bytes": _nonnegative_integer(
                    entry["bytes"], where=f"action.inputs[{index}].bytes"
                ),
            }
        )
    return sorted(inputs, key=lambda entry: str(entry["id"]))


def validate_input_contract(value: object) -> dict[str, object]:
    """Normalize one content-addressed input row used by an action.

    The returned object is exactly the ``{id, sha256, bytes}`` shape accepted
    in ``action.inputs``.  Keeping this validation public lets input ingestion
    and later lookup share the action schema instead of inventing a second
    descriptor vocabulary.
    """

    return _normalize_inputs([value])[0]


def _normalize_task(value: object) -> dict[str, object]:
    task = _exact_mapping(value, keys=_TASK_KEYS, where="action.task")
    task_class = _text(task["task_class"], where="action.task.task_class")
    if task_class not in _TASK_CLASSES:
        _fail(f"action.task.task_class must be one of {sorted(_TASK_CLASSES)}")
    determinism = _text(task["determinism"], where="action.task.determinism")
    if determinism not in _DETERMINISM:
        _fail(f"action.task.determinism must be one of {sorted(_DETERMINISM)}")
    artifact_family = _text(
        task["artifact_family"], where="action.task.artifact_family"
    )
    if artifact_family not in _ARTIFACT_FAMILIES:
        _fail(
            "action.task.artifact_family must be one of "
            f"{sorted(_ARTIFACT_FAMILIES)}"
        )
    return {
        "definition_id": _text(
            task["definition_id"],
            where="action.task.definition_id",
            pattern=_ID_RE,
        ),
        "definition_version": _text(
            task["definition_version"],
            where="action.task.definition_version",
            pattern=_VERSION_RE,
        ),
        "task_class": task_class,
        "determinism": determinism,
        "artifact_family": artifact_family,
        "artifact_kind": _text(
            task["artifact_kind"],
            where="action.task.artifact_kind",
            pattern=_ID_RE,
        ),
        "argv": _normalize_argv(task["argv"]),
        "working_directory": _normalize_relative_path(
            task["working_directory"],
            where="action.task.working_directory",
            dot_ok=True,
        ),
        "result_path": _normalize_relative_path(
            task["result_path"], where="action.task.result_path", dot_ok=False
        ),
    }


def _normalize_environment(value: object) -> dict[str, object]:
    environment = _exact_mapping(
        value, keys=_ENVIRONMENT_KEYS, where="action.environment"
    )
    return {
        "variables": _normalize_string_mapping(
            environment["variables"],
            where="action.environment.variables",
            key_pattern=_ENV_RE,
        ),
        "toolchain": _normalize_string_mapping(
            environment["toolchain"], where="action.environment.toolchain"
        ),
    }


def _normalize_scope(value: object) -> dict[str, object]:
    scope = _exact_mapping(value, keys=_SCOPE_KEYS, where="action.execution_scope")
    portability = _text(
        scope["portability"], where="action.execution_scope.portability"
    )
    if portability not in _PORTABILITY:
        _fail(
            "action.execution_scope.portability must be one of "
            f"{sorted(_PORTABILITY)}"
        )
    platform_key = _optional_token(
        scope["platform_key"], where="action.execution_scope.platform_key"
    )
    host_class = _optional_token(
        scope["host_class"], where="action.execution_scope.host_class"
    )
    if portability == "portable" and (platform_key is not None or host_class is not None):
        _fail("portable actions must set platform_key and host_class to null")
    if portability == "platform_keyed" and (
        platform_key is None or host_class is not None
    ):
        _fail("platform_keyed actions require platform_key and null host_class")
    if portability == "host_class_keyed" and (
        host_class is None or platform_key is not None
    ):
        _fail("host_class_keyed actions require host_class and null platform_key")
    return {
        "portability": portability,
        "platform_key": platform_key,
        "host_class": host_class,
    }


def _normalize_action_body(value: object) -> dict[str, object]:
    body = _exact_mapping(value, keys=_ACTION_BODY_KEYS, where="action body")
    if body["schema"] == ACTION_SCHEMA_V1:
        _fail(
            "v1 actions must be redeclared and resealed with an explicit "
            f"artifact_family under {ACTION_SCHEMA_V2!r}"
        )
    if body["schema"] != ACTION_SCHEMA_V2:
        _fail(f"action.schema must be {ACTION_SCHEMA_V2!r}")
    task = _normalize_task(body["task"])
    scope = _normalize_scope(body["execution_scope"])
    if task["task_class"] == "measurement" and scope["portability"] == "portable":
        _fail("measurement actions must be platform_keyed or host_class_keyed")
    if (
        task["artifact_family"] == "codebook"
        and scope["portability"] == "portable"
    ):
        _fail(
            "codebook actions cannot be portable: D29 records cross-architecture "
            "row-scale byte drift"
        )
    params = body["params"]
    if not isinstance(params, Mapping):
        _fail("action.params must be an object with string keys")
    normalized_params = _normalize_json_value(params, where="action.params")
    assert isinstance(normalized_params, Mapping)
    # Detach the sealed value from caller-owned mutable containers and replay
    # the strict decoder used for persisted manifests.
    normalized_params = _decode_strict_json(
        _canonical_bytes(normalized_params), where="action.params"
    )
    environment = _normalize_environment(body["environment"])
    toolchain = environment["toolchain"]
    assert isinstance(toolchain, Mapping)
    if "argv0.sha256" in toolchain:
        _sha256(
            toolchain["argv0.sha256"],
            where="action.environment.toolchain.argv0.sha256",
        )
    if "argv0.bytes" in toolchain:
        raw_bytes = toolchain["argv0.bytes"]
        if (
            re.fullmatch(r"0|[1-9][0-9]*", str(raw_bytes)) is None
            or str(int(str(raw_bytes))) != raw_bytes
        ):
            _fail(
                "action.environment.toolchain.argv0.bytes must be a canonical "
                "integer string"
            )
    if "cuda_compute_capability" in toolchain and re.fullmatch(
        r"[0-9]+\.[0-9]+", str(toolchain["cuda_compute_capability"])
    ) is None:
        _fail("action.environment.toolchain.cuda_compute_capability is malformed")
    if scope["portability"] != "portable":
        unknown = set(toolchain) - _ATTESTABLE_TOOLCHAIN_KEYS
        if unknown:
            _fail(
                "nonportable action toolchain contains fields with no worker "
                f"preflight: {sorted(unknown)}"
            )
        required = {
            "argv0.sha256",
            "argv0.bytes",
            "system",
            "machine",
            "libc",
        }
        if not required <= set(toolchain):
            _fail(
                "nonportable actions must bind argv[0] and platform ABI with "
                "toolchain fields argv0.sha256, argv0.bytes, system, machine, "
                "and libc"
            )
    return {
        "schema": ACTION_SCHEMA_V2,
        "task": task,
        "inputs": _normalize_inputs(body["inputs"]),
        "code_closure": validate_code_closure(body["code_closure"]),
        "params": normalized_params,
        "environment": environment,
        "execution_scope": scope,
    }


def seal_action(value: object) -> dict[str, object]:
    """Normalize an action body and attach its action key."""

    body = _normalize_action_body(value)
    return {**body, "action_key": canonical_sha256(body)}


def validate_action(value: object) -> dict[str, object]:
    action = _exact_mapping(value, keys=_ACTION_KEYS, where="action")
    raw_body = {key: action[key] for key in _ACTION_BODY_KEYS}
    body = _normalize_action_body(raw_body)
    if raw_body != body:
        _fail("action body is valid but not in normalized contract form")
    recorded = _sha256(action["action_key"], where="action.action_key")
    expected = canonical_sha256(body)
    if recorded != expected:
        _fail("action.action_key does not match the canonical action body")
    return {**body, "action_key": recorded}


def validate_worker_scope(
    action: object,
    *,
    attestation: object,
) -> None:
    """Validate scope from live-derived, self-hashed worker evidence.

    Scheduler placement strings are intentionally not accepted here: they are
    intent, not evidence that the allocated machine has the requested scope.
    """

    validate_worker_attestation(attestation, action=action)


def _validate_scope_labels(
    action: Mapping[str, object], *, platform_key: str | None, host_class: str | None
) -> None:
    actual_platform = _optional_token(platform_key, where="worker platform_key")
    actual_host = _optional_token(host_class, where="worker host_class")
    normalized = validate_action(action)
    scope = normalized["execution_scope"]
    assert isinstance(scope, Mapping)
    if (
        scope["portability"] == "platform_keyed"
        and actual_platform != scope["platform_key"]
    ):
        raise ActionContractError(
            "worker platform_key does not match the action execution scope"
        )
    if (
        scope["portability"] == "host_class_keyed"
        and actual_host != scope["host_class"]
    ):
        raise ActionContractError(
            "worker host_class does not match the action execution scope"
        )


def _atomic_publish(
    path: Path,
    raw: bytes,
    *,
    prelink_verify: Callable[[], None] | None = None,
) -> bool:
    """Publish immutable bytes relative to a held no-follow parent FD."""

    path = _absolute_nofollow_path(path, where="publication path")
    directory_fd = _open_directory_nofollow(
        path.parent, where="publication directory", create=True
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
        _assert_directory_identity(
            directory_fd, path.parent, where="publication directory"
        )
        return won
    finally:
        if temporary_name is not None:
            try:
                os.unlink(temporary_name, dir_fd=directory_fd)
            except FileNotFoundError:
                pass
        os.close(directory_fd)


def _absolute_nofollow_path(path: Path, *, where: str) -> Path:
    """Return an absolute lexical path that cannot walk through ``..``."""

    candidate = path if path.is_absolute() else Path.cwd() / path
    if ".." in candidate.parts:
        raise ActionContractError(f"{where} must not contain parent traversal")
    return candidate


def _open_directory_nofollow(
    path: Path,
    *,
    where: str,
    create: bool = False,
    mode: int = 0o755,
) -> int:
    """Open/create an absolute directory using only mkdirat/openat operations."""

    path = _absolute_nofollow_path(path, where=where)
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


def _assert_directory_identity(descriptor: int, path: Path, *, where: str) -> None:
    """Fail if the held directory is no longer the configured pathname."""

    try:
        current = _open_directory_nofollow(path, where=where)
    except FileNotFoundError as exc:
        raise CASTamperError(f"{where} disappeared during operation: {path}") from exc
    try:
        held = os.fstat(descriptor)
        observed = os.fstat(current)
        if (held.st_dev, held.st_ino) != (observed.st_dev, observed.st_ino):
            raise CASTamperError(f"{where} changed during operation: {path}")
    finally:
        os.close(current)


def _open_regular_nofollow(path: Path, *, where: str) -> tuple[int, int]:
    """Open a regular-file candidate relative to its held no-follow parent."""

    path = _absolute_nofollow_path(path, where=where)
    parent_fd = _open_directory_nofollow(path.parent, where=f"{where} parent")
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
        descriptor = _open_retrying_would_block(
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


def _assert_regular_identity(
    descriptor: int, parent_fd: int, path: Path, *, where: str
) -> None:
    """Fail if a held regular inode is no longer its canonical leaf name."""

    try:
        observed = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
    except OSError as exc:
        raise CASTamperError(f"{where} changed during operation: {path}") from exc
    held = os.fstat(descriptor)
    if (
        not stat.S_ISREG(observed.st_mode)
        or (held.st_dev, held.st_ino) != (observed.st_dev, observed.st_ino)
    ):
        raise CASTamperError(f"{where} changed during operation: {path}")


def _stable_file_read_identity(info: os.stat_result) -> tuple[int, ...]:
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


def _substantive_file_read_identity(info: os.stat_result) -> tuple[int, ...]:
    """Metadata changes that are tamper, not an NFS link-visibility retry."""

    identity = _stable_file_read_identity(info)
    return identity[:7]


def _read_regular_file_nofollow(
    path: Path,
    *,
    where: str,
    require_readonly: bool = False,
    require_single_link: bool = False,
    max_bytes: int | None = None,
) -> bytes:
    """Read one stable inode without following any pathname component.

    A ctime/nlink-only mismatch is discarded and retried through a fresh path
    resolution and FD.  No bytes from an unstable attempt are returned. When
    ``max_bytes`` is set, both the opening size and streamed byte count are
    bounded so a dishonest or racing size cannot cause unbounded allocation.
    """

    if max_bytes is not None and (type(max_bytes) is not int or max_bytes < 0):
        raise ActionContractError("stable regular-file read max_bytes is invalid")
    for attempt in range(_STABLE_FILE_READ_ATTEMPTS):
        descriptor, parent_fd = _open_regular_nofollow(path, where=where)
        try:
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode):
                raise CASTamperError(f"{where} is not a regular file: {path}")
            if require_readonly and before.st_mode & 0o222:
                raise CASTamperError(f"{where} is writable: {path}")
            if require_single_link and before.st_nlink != 1:
                raise _FileLinkCountError(
                    f"{where} must have exactly one link: {path}"
                )
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
            if require_single_link and after.st_nlink != 1:
                raise _FileLinkCountError(
                    f"{where} must have exactly one link: {path}"
                )
            if _stable_file_read_identity(before) != _stable_file_read_identity(after):
                if _substantive_file_read_identity(
                    before
                ) != _substantive_file_read_identity(after):
                    raise CASTamperError(
                        f"{where} changed substantively while it was read: {path}"
                    )
                _assert_regular_identity(descriptor, parent_fd, path, where=where)
                _assert_directory_identity(
                    parent_fd, path.parent, where=f"{where} parent"
                )
                if attempt + 1 < _STABLE_FILE_READ_ATTEMPTS:
                    continue
                raise CASTamperError(
                    f"{where} did not stabilize after "
                    f"{_STABLE_FILE_READ_ATTEMPTS} fresh reads: {path}"
                )
            _assert_regular_identity(descriptor, parent_fd, path, where=where)
            _assert_directory_identity(
                parent_fd, path.parent, where=f"{where} parent"
            )
            return b"".join(chunks)
        finally:
            os.close(descriptor)
            os.close(parent_fd)
    raise AssertionError("stable file read retry loop did not return or raise")


def _file_identity_nofollow(
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
        descriptor, parent_fd = _open_regular_nofollow(path, where=where)
        digest = hashlib.sha256()
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
            if _stable_file_read_identity(before) != _stable_file_read_identity(after):
                if _substantive_file_read_identity(
                    before
                ) != _substantive_file_read_identity(after):
                    raise CASTamperError(
                        f"{where} changed substantively while it was read: {path}"
                    )
                _assert_regular_identity(descriptor, parent_fd, path, where=where)
                _assert_directory_identity(
                    parent_fd, path.parent, where=f"{where} parent"
                )
                if attempt + 1 < _STABLE_FILE_READ_ATTEMPTS:
                    continue
                raise CASTamperError(
                    f"{where} did not stabilize after "
                    f"{_STABLE_FILE_READ_ATTEMPTS} fresh reads: {path}"
                )
            _assert_regular_identity(descriptor, parent_fd, path, where=where)
            _assert_directory_identity(
                parent_fd, path.parent, where=f"{where} parent"
            )
            return observed_digest, size, before.st_mode
        finally:
            os.close(descriptor)
            os.close(parent_fd)
    raise AssertionError("stable file identity retry loop did not return or raise")


def _unlink_nofollow(path: Path, *, where: str) -> None:
    """Unlink a leaf only through its current no-follow parent directory."""

    try:
        directory_fd = _open_directory_nofollow(path.parent, where=f"{where} parent")
    except FileNotFoundError:
        return
    try:
        try:
            os.unlink(path.name, dir_fd=directory_fd)
        except FileNotFoundError:
            return
        os.fsync(directory_fd)
        _assert_directory_identity(
            directory_fd, path.parent, where=f"{where} parent"
        )
    finally:
        os.close(directory_fd)


def _copy_to_staging(source: Path, staging_directory: Path) -> tuple[Path, str, int]:
    """Take a stable regular-file snapshot into the CAS filesystem."""

    staging_directory = _absolute_nofollow_path(
        staging_directory, where="CAS staging directory"
    )
    staging_fd = _open_directory_nofollow(
        staging_directory, where="CAS staging directory", create=True
    )
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        source_fd = _open_retrying_would_block(source, flags)
    except OSError as exc:
        os.close(staging_fd)
        raise LocalActionError(f"result is not a readable regular file: {source}") from exc
    try:
        descriptor, temporary_raw = tempfile.mkstemp(
            prefix=".payload.", suffix=".tmp", dir=f"/proc/self/fd/{staging_fd}"
        )
    except BaseException:
        os.close(source_fd)
        os.close(staging_fd)
        raise
    temporary_name = Path(temporary_raw).name
    temporary = staging_directory / temporary_name
    digest = hashlib.sha256()
    size = 0
    try:
        before = os.fstat(source_fd)
        if not stat.S_ISREG(before.st_mode):
            raise LocalActionError(f"result is not a regular file: {source}")
        handle = os.fdopen(descriptor, "wb")
        # ``os.fdopen`` owns the descriptor from here; the ``finally`` below
        # must not close it a second time.
        descriptor = -1
        with handle as destination:
            while True:
                chunk = os.read(source_fd, 4 * 1024 * 1024)
                if not chunk:
                    break
                destination.write(chunk)
                digest.update(chunk)
                size += len(chunk)
            destination.flush()
            os.fchmod(destination.fileno(), 0o444)
            os.fsync(destination.fileno())
        after = os.fstat(source_fd)
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
            raise LocalActionError(f"result changed while it was copied: {source}")
        _assert_directory_identity(
            staging_fd, staging_directory, where="CAS staging directory"
        )
        return temporary, digest.hexdigest(), size
    except BaseException:
        try:
            os.unlink(temporary_name, dir_fd=staging_fd)
        except FileNotFoundError:
            pass
        raise
    finally:
        # Every rejection between ``mkstemp`` and ``os.fdopen`` used to leave
        # this descriptor open on an unlinked staging file.  A directory or a
        # FIFO source is refused there, so a caller that kept refusing invalid
        # inputs ran out of descriptors and could no longer do valid work.
        if descriptor >= 0:
            with suppress(OSError):
                os.close(descriptor)
        os.close(source_fd)
        os.close(staging_fd)


def _normalize_controller_evidence(value: object) -> dict[str, object]:
    controller = _exact_mapping(
        value,
        keys=_SLURM_CONTROLLER_EVIDENCE_KEYS,
        where="worker evidence.slurm.controller",
    )

    def features(key: str) -> list[str]:
        raw = controller[key]
        if type(raw) is not list:
            _fail(f"worker evidence.slurm.controller.{key} must be an array")
        items = [
            _text(
                item,
                where=f"worker evidence.slurm.controller.{key}[{index}]",
                pattern=_SCOPE_TOKEN_RE,
            )
            for index, item in enumerate(raw)
        ]
        if items != sorted(set(items)):
            _fail(
                f"worker evidence.slurm.controller.{key} must be unique and sorted"
            )
        return items

    return {
        "partition": _text(
            controller["partition"],
            where="worker evidence.slurm.controller.partition",
            pattern=_SCOPE_TOKEN_RE,
        ),
        "batch_host": _text(
            controller["batch_host"],
            where="worker evidence.slurm.controller.batch_host",
            pattern=_ID_RE,
        ),
        "job_features": features("job_features"),
        "node_active_features": features("node_active_features"),
    }


def _normalize_worker_evidence(value: object) -> dict[str, object]:
    evidence = _exact_mapping(
        value, keys=_EVIDENCE_KEYS, where="worker attestation.evidence"
    )
    source = _text(evidence["source"], where="worker evidence.source")
    if source not in {"local", "slurm"}:
        _fail("worker evidence.source must be 'local' or 'slurm'")
    accelerators_raw = evidence["accelerators"]
    if type(accelerators_raw) is not list:
        _fail("worker evidence.accelerators must be an array")
    accelerators: list[dict[str, str]] = []
    for index, raw in enumerate(accelerators_raw):
        accelerator = _exact_mapping(
            raw,
            keys=_ACCELERATOR_KEYS,
            where=f"worker evidence.accelerators[{index}]",
        )
        kind = _text(
            accelerator["kind"],
            where=f"worker evidence.accelerators[{index}].kind",
            pattern=_SCOPE_TOKEN_RE,
        )
        if kind != "nvidia":
            _fail("worker evidence contains an unsupported accelerator kind")
        capability = _text(
            accelerator["compute_capability"],
            where=f"worker evidence.accelerators[{index}].compute_capability",
        )
        if re.fullmatch(r"[0-9]+\.[0-9]+", capability) is None:
            _fail("worker evidence compute capability is malformed")
        accelerators.append(
            {
                "kind": kind,
                "compute_capability": capability,
                "driver_version": _text(
                    accelerator["driver_version"],
                    where=f"worker evidence.accelerators[{index}].driver_version",
                    pattern=_VERSION_RE,
                ),
            }
        )
    accelerators.sort(
        key=lambda row: (row["kind"], row["compute_capability"], row["driver_version"])
    )
    if len({_canonical_bytes(row) for row in accelerators}) != len(accelerators):
        _fail("worker evidence accelerator rows must be unique")
    raw_slurm = evidence["slurm"]
    slurm: dict[str, object] | None
    if raw_slurm is None:
        slurm = None
    else:
        has_controller = isinstance(raw_slurm, Mapping) and "controller" in raw_slurm
        slurm_mapping = _exact_mapping(
            raw_slurm,
            keys=_SLURM_EVIDENCE_KEYS | ({"controller"} if has_controller else set()),
            where="worker evidence.slurm",
        )
        constraints_raw = slurm_mapping["constraints"]
        if type(constraints_raw) is not list:
            _fail("worker evidence.slurm.constraints must be an array")
        constraints = [
            _text(
                item,
                where=f"worker evidence.slurm.constraints[{index}]",
                pattern=_SCOPE_TOKEN_RE,
            )
            for index, item in enumerate(constraints_raw)
        ]
        if constraints != sorted(set(constraints)):
            _fail("worker evidence.slurm.constraints must be unique and sorted")
        cgroup = _text(
            slurm_mapping["cgroup"], where="worker evidence.slurm.cgroup"
        )
        if not cgroup.startswith("/") or ".." in PurePosixPath(cgroup).parts:
            _fail("worker evidence.slurm.cgroup must be an absolute cgroup path")
        job_id = _text(
            slurm_mapping["job_id"],
            where="worker evidence.slurm.job_id",
            pattern=_SCOPE_TOKEN_RE,
        )
        if re.fullmatch(r"[1-9][0-9]*", job_id) is None:
            _fail("worker evidence.slurm.job_id must be a positive numeric job id")
        slurm = {
            "job_id": job_id,
            "node_name": _text(
                slurm_mapping["node_name"],
                where="worker evidence.slurm.node_name",
                pattern=_ID_RE,
            ),
            "partition": _text(
                slurm_mapping["partition"],
                where="worker evidence.slurm.partition",
                pattern=_SCOPE_TOKEN_RE,
            ),
            "constraints": constraints,
            "cgroup": cgroup,
        }
        if has_controller:
            controller = _normalize_controller_evidence(
                slurm_mapping["controller"]
            )
            if controller["batch_host"] != slurm["node_name"]:
                _fail(
                    "worker evidence.slurm.controller.batch_host differs from "
                    "the node the worker reported"
                )
            slurm["controller"] = controller
        job_pattern = re.compile(
            rf"(?:^|/)job_{re.escape(str(slurm['job_id']))}(?:[./]|$)"
        )
        if job_pattern.search(str(slurm["cgroup"])) is None:
            _fail("worker evidence SLURM cgroup does not bind its job id")
    if (source == "slurm") != (slurm is not None):
        _fail("worker evidence source and SLURM evidence disagree")
    return {
        "source": source,
        "hostname": _text(
            evidence["hostname"], where="worker evidence.hostname", pattern=_ID_RE
        ),
        "system": _text(
            evidence["system"], where="worker evidence.system", pattern=_SCOPE_TOKEN_RE
        ),
        "machine": _text(
            evidence["machine"], where="worker evidence.machine", pattern=_SCOPE_TOKEN_RE
        ),
        "libc": _text(
            evidence["libc"], where="worker evidence.libc", pattern=_SCOPE_TOKEN_RE
        ),
        "accelerators": accelerators,
        "slurm": slurm,
    }


def validate_worker_attestation(
    value: object, *, action: object
) -> dict[str, object]:
    """Validate a persisted preflight against the exact sealed action."""

    normalized_action = validate_action(action)
    raw = _exact_mapping(value, keys=_PRODUCER_KEYS, where="worker attestation")
    if raw["schema"] != WORKER_ATTESTATION_SCHEMA_V2:
        _fail(
            f"worker attestation.schema must be {WORKER_ATTESTATION_SCHEMA_V2!r}"
        )
    action_key = _sha256(raw["action_key"], where="worker attestation.action_key")
    if action_key != normalized_action["action_key"]:
        _fail("worker attestation is bound to a different action")
    evidence = _normalize_worker_evidence(raw["evidence"])
    worker_id = _text(
        raw["worker_id"], where="worker attestation.worker_id", pattern=_ID_RE
    )
    if worker_id != _worker_identity_from_evidence(evidence):
        _fail("worker_id is not derived from the worker evidence")
    platform_key = _optional_token(
        raw["platform_key"], where="worker attestation.platform_key"
    )
    derived_platform = _platform_key_from_evidence(evidence)
    if platform_key != derived_platform:
        _fail("platform_key is not derived from the worker evidence")
    host_class = _optional_token(
        raw["host_class"], where="worker attestation.host_class"
    )
    scope = normalized_action["execution_scope"]
    assert isinstance(scope, Mapping)
    expected_host = (
        str(scope["host_class"])
        if scope["portability"] == "host_class_keyed"
        else None
    )
    derived_host = _host_class_from_evidence(evidence, expected=expected_host)
    if host_class != derived_host:
        _fail("host_class is not derived from SLURM worker evidence")
    runtime = _validate_worker_runtime(raw["runtime"])

    executable_raw = _exact_mapping(
        raw["executable"], keys=_EXECUTABLE_KEYS, where="worker attestation.executable"
    )
    task = normalized_action["task"]
    assert isinstance(task, Mapping)
    argv = task["argv"]
    assert isinstance(argv, list)
    executable = {
        "path": _text(
            executable_raw["path"], where="worker attestation.executable.path"
        ),
        "resolved_path": _text(
            executable_raw["resolved_path"],
            where="worker attestation.executable.resolved_path",
        ),
        "sha256": _sha256(
            executable_raw["sha256"], where="worker attestation.executable.sha256"
        ),
        "bytes": _nonnegative_integer(
            executable_raw["bytes"], where="worker attestation.executable.bytes"
        ),
    }
    if executable["path"] != argv[0] or not Path(
        str(executable["resolved_path"])
    ).is_absolute():
        _fail("worker attestation executable differs from action.task.argv[0]")

    toolchain_raw = _exact_mapping(
        raw["toolchain"],
        keys=_TOOLCHAIN_ATTESTATION_KEYS,
        where="worker attestation.toolchain",
    )
    declared = _normalize_string_mapping(
        toolchain_raw["declared"], where="worker attestation.toolchain.declared"
    )
    environment = normalized_action["environment"]
    assert isinstance(environment, Mapping)
    if declared != environment["toolchain"]:
        _fail("worker attestation toolchain differs from the action")
    verified = _normalize_string_mapping(
        toolchain_raw["verified"], where="worker attestation.toolchain.verified"
    )
    if not set(verified) <= set(declared):
        _fail("worker attestation verifies undeclared toolchain fields")
    for key, observed in verified.items():
        if not _toolchain_value_matches(declared[key], observed):
            _fail(f"worker toolchain field {key!r} differs from the action")
    if (
        declared.get("argv0.sha256") is not None
        and declared["argv0.sha256"] != executable["sha256"]
    ):
        _fail("worker executable digest differs from toolchain argv0.sha256")
    if (
        declared.get("argv0.bytes") is not None
        and declared["argv0.bytes"] != str(executable["bytes"])
    ):
        _fail("worker executable size differs from toolchain argv0.bytes")

    inputs = _normalize_inputs(raw["inputs"])
    expected_inputs = {
        str(entry["id"]): entry
        for entry in normalized_action["inputs"]  # type: ignore[union-attr]
    }
    if any(expected_inputs.get(str(entry["id"])) != entry for entry in inputs):
        _fail("worker attestation contains an input not bound by the action")

    nonportable = scope["portability"] != "portable"
    if nonportable:
        if evidence["libc"] == "unknown":
            _fail("nonportable action cannot attest an unknown libc ABI")
        if set(verified) != set(declared):
            _fail("nonportable action has unverified toolchain fields")
        if len(inputs) != len(expected_inputs):
            _fail("nonportable action has unresolved input artifacts")
        accelerators = evidence["accelerators"]
        assert isinstance(accelerators, list)
        if accelerators and not {
            "cuda_compute_capability",
            "nvidia_driver",
        } <= set(verified):
            _fail(
                "nonportable NVIDIA action must bind cuda_compute_capability "
                "and nvidia_driver in its toolchain"
            )

    _validate_scope_labels(
        normalized_action, platform_key=platform_key, host_class=host_class
    )
    body = {
        "schema": WORKER_ATTESTATION_SCHEMA_V2,
        "action_key": action_key,
        "worker_id": worker_id,
        "platform_key": platform_key,
        "host_class": host_class,
        "evidence": evidence,
        "runtime": runtime,
        "executable": executable,
        "toolchain": {"declared": declared, "verified": verified},
        "inputs": inputs,
    }
    recorded = _sha256(
        raw["attestation_sha256"], where="worker attestation.attestation_sha256"
    )
    if recorded != canonical_sha256(body):
        _fail("worker attestation digest does not match its body")
    return {**body, "attestation_sha256": recorded}


def _verified_toolchain(
    declared: Mapping[str, str],
    *,
    executable: Mapping[str, object],
    evidence: Mapping[str, object],
) -> dict[str, str]:
    observed: dict[str, str] = {
        "argv0.sha256": str(executable["sha256"]),
        "argv0.bytes": str(executable["bytes"]),
        "system": str(evidence["system"]),
        "machine": str(evidence["machine"]),
        "libc": str(evidence["libc"]),
    }
    accelerators = evidence["accelerators"]
    assert isinstance(accelerators, list)
    if accelerators:
        capabilities = {str(row["compute_capability"]) for row in accelerators}
        drivers = {str(row["driver_version"]) for row in accelerators}
        if len(capabilities) == 1:
            observed["cuda_compute_capability"] = next(iter(capabilities))
        if len(drivers) == 1:
            observed["nvidia_driver"] = next(iter(drivers))
    python_keys = set(declared) & ({"python"} | _PYTHON_DISTRIBUTIONS)
    if python_keys:
        observed.update(
            _probe_python_toolchain(Path(str(executable["path"])))
        )
    verified: dict[str, str] = {}
    for key, expected in declared.items():
        actual = observed.get(key)
        if actual is None:
            continue
        if not _toolchain_value_matches(expected, actual):
            raise ActionContractError(
                f"worker toolchain field {key!r} differs: "
                f"declared={expected!r}, observed={actual!r}"
            )
        verified[key] = actual
    return dict(sorted(verified.items()))


def _verified_input_contracts(
    action: Mapping[str, object], cas: "PrismaBuildCAS"
) -> list[dict[str, object]]:
    scope = action["execution_scope"]
    assert isinstance(scope, Mapping)
    required = scope["portability"] != "portable"
    verified: list[dict[str, object]] = []
    for entry in action["inputs"]:  # type: ignore[union-attr]
        assert isinstance(entry, Mapping)
        path = cas._blob_path(str(entry["sha256"]))
        try:
            path.lstat()
        except FileNotFoundError:
            if required:
                raise ActionContractError(
                    f"nonportable action input is absent from the CAS: {entry['id']}"
                )
            continue
        except OSError as exc:
            raise CASUnavailableError(
                f"cannot inspect action input in the CAS: {entry['id']}: {exc}"
            ) from exc
        cas.input_path(entry)
        verified.append(dict(entry))
    return sorted(verified, key=lambda entry: str(entry["id"]))


def preflight_action(
    action: object,
    *,
    cas_root: str | Path,
    checkout_root: str | Path,
    worker_launcher_identity: object | None = None,
) -> dict[str, object]:
    """Derive and verify the execution facts required before launching argv."""

    normalized = validate_action(action)
    root = Path(checkout_root)
    if not root.is_absolute():
        _fail("checkout_root must be absolute")
    verify_code_closure(normalized["code_closure"], root)
    _verify_pbrun_checkout_identity(normalized, root)
    scope = normalized["execution_scope"]
    assert isinstance(scope, Mapping)
    expected_host = (
        str(scope["host_class"])
        if scope["portability"] == "host_class_keyed"
        else None
    )
    evidence = _collect_worker_evidence(attest_host_class=expected_host)
    host_class = _host_class_from_evidence(evidence, expected=expected_host)
    executable = identify_executable(
        normalized["task"]["argv"][0]  # type: ignore[index]
    )
    environment = normalized["environment"]
    assert isinstance(environment, Mapping)
    declared = environment["toolchain"]
    assert isinstance(declared, Mapping)
    verified = _verified_toolchain(
        declared, executable=executable, evidence=evidence  # type: ignore[arg-type]
    )
    inputs = _verified_input_contracts(normalized, PrismaBuildCAS(cas_root))
    body: dict[str, object] = {
        "schema": WORKER_ATTESTATION_SCHEMA_V2,
        "action_key": normalized["action_key"],
        "worker_id": _worker_identity_from_evidence(evidence),
        "platform_key": _platform_key_from_evidence(evidence),
        "host_class": host_class,
        "evidence": evidence,
        "runtime": _worker_runtime_identity(worker_launcher_identity),
        "executable": executable,
        "toolchain": {"declared": dict(declared), "verified": verified},
        "inputs": inputs,
    }
    attestation = {**body, "attestation_sha256": canonical_sha256(body)}
    return validate_worker_attestation(attestation, action=normalized)


def _verify_attested_executable_unchanged(
    attestation: Mapping[str, object], action: Mapping[str, object]
) -> None:
    validated = validate_worker_attestation(attestation, action=action)
    expected = validated["executable"]
    assert isinstance(expected, Mapping)
    observed = identify_executable(str(expected["path"]))
    if observed != expected:
        raise LocalActionError("action executable changed after worker preflight")


def _verify_attested_worker_runtime_unchanged(
    attestation: Mapping[str, object], action: Mapping[str, object]
) -> None:
    validated = validate_worker_attestation(attestation, action=action)
    _verify_worker_runtime_unchanged(validated["runtime"])


class PrismaBuildCAS:
    """Immutable file-result CAS with verified first-result-wins receipts."""

    def __init__(self, root: str | Path):
        self.root = Path(root)
        if not self.root.is_absolute() or self.root == Path("/"):
            raise ActionContractError("CAS root must be a non-root absolute path")
        if ".." in self.root.parts:
            raise ActionContractError("CAS root must not contain parent traversal")

    def _receipt_path(self, action_key: str) -> Path:
        # Receipt schemas are immutable interpretation domains.  v2 occupied
        # the unversioned path below ``actions/``; v3 must coexist with it and
        # must never parse, overwrite, or delete those historical bytes.
        return (
            self.root
            / "actions"
            / _CAS_RECEIPT_NAMESPACE
            / action_key[:2]
            / f"{action_key}.json"
        )

    def _legacy_v2_receipt_path(self, action_key: str) -> Path:
        return self.root / "actions" / action_key[:2] / f"{action_key}.json"

    def _blob_path(self, digest: str) -> Path:
        return self.root / "blobs" / digest[:2] / digest

    def _open_input_blob_shard(self, digest: str) -> tuple[Path, int]:
        """Open a real CAS shard directory without following CAS symlinks."""

        shard = self.root / "blobs" / digest[:2]
        descriptor = _open_directory_nofollow(
            shard,
            where="CAS input directory",
            create=True,
        )
        return shard / digest, descriptor

    def _publish_staged_input_blob(
        self, staging: Path, contract: Mapping[str, object]
    ) -> tuple[Path, bool]:
        """Link a verified staging inode into the CAS and verify its name.

        ``staging`` is the private, read-only inode just returned by
        ``_copy_to_staging`` together with ``contract``.  If this process wins
        the hard-link publication, identity of the canonical name to that
        already-hashed inode proves the content without rereading a potentially
        huge payload.  A pre-existing race winner remains untrusted and is
        always reopened and hashed in full.
        """

        digest = str(contract["sha256"])
        blob_path, directory_fd = self._open_input_blob_shard(digest)
        try:
            source_fd = _open_directory_nofollow(
                staging.parent, where="CAS staging directory"
            )
        except BaseException:
            os.close(directory_fd)
            raise
        try:
            try:
                staged_identity = os.stat(
                    staging.name, dir_fd=source_fd, follow_symlinks=False
                )
            except OSError as exc:
                raise CASTamperError(
                    f"CAS staging file changed before publication: {staging}"
                ) from exc
            if not stat.S_ISREG(staged_identity.st_mode):
                raise CASTamperError(
                    f"CAS staging file is not regular: {staging}"
                )
            if staged_identity.st_mode & 0o222:
                raise CASTamperError(
                    f"CAS staging file is writable: {staging}"
                )
            if staged_identity.st_size != int(contract["bytes"]):
                raise CASTamperError(
                    f"CAS staging file size differs from its contract: {staging}"
                )
            try:
                os.link(
                    staging.name,
                    digest,
                    src_dir_fd=source_fd,
                    dst_dir_fd=directory_fd,
                    follow_symlinks=False,
                )
                won = True
            except FileExistsError:
                won = False
            if won:
                os.fsync(directory_fd)
                flags = (
                    os.O_RDONLY
                    | os.O_CLOEXEC
                    | os.O_NOFOLLOW
                    | getattr(os, "O_NONBLOCK", 0)
                )
                try:
                    published_fd = _open_retrying_would_block(
                        digest, flags, dir_fd=directory_fd
                    )
                except OSError as exc:
                    raise CASTamperError(
                        f"published CAS blob changed before readback: {blob_path}"
                    ) from exc
                try:
                    published_identity = os.fstat(published_fd)
                    if _substantive_file_read_identity(
                        published_identity
                    ) != _substantive_file_read_identity(staged_identity):
                        raise CASTamperError(
                            "published CAS blob differs from the verified staging "
                            f"inode: {blob_path}"
                        )
                    _assert_regular_identity(
                        published_fd,
                        directory_fd,
                        blob_path,
                        where="published CAS blob",
                    )
                finally:
                    os.close(published_fd)
            _assert_directory_identity(
                source_fd, staging.parent, where="CAS staging directory"
            )
            _assert_directory_identity(
                directory_fd, blob_path.parent, where="CAS blob directory"
            )
        finally:
            os.close(source_fd)
            os.close(directory_fd)
        if won:
            return blob_path, True
        # A different inode won the content address.  Its name is only a claim;
        # reopen and hash every byte before it can satisfy this publication.
        return self._verify_input_blob(contract), False

    def ingest_input(
        self,
        source_path: str | Path,
        *,
        input_id: str,
        expected_sha256: str | None = None,
        expected_bytes: int | None = None,
    ) -> tuple[dict[str, object], bool]:
        """Publish one stable file snapshot as an immutable action input.

        The digest and byte count are derived from the staged bytes.  Optional
        expectations fail before publication when the source differs.  The
        returned row can be inserted directly into ``action.inputs``; ``won``
        is false only when an independently verified identical blob already
        occupied the content address.
        """

        identity = _text(input_id, where="input id", pattern=_ID_RE)
        expected_digest = (
            _sha256(expected_sha256, where="expected input sha256")
            if expected_sha256 is not None
            else None
        )
        expected_size = (
            _nonnegative_integer(expected_bytes, where="expected input bytes")
            if expected_bytes is not None
            else None
        )
        staging, digest, size = _copy_to_staging(
            Path(source_path), self.root / ".staging"
        )
        try:
            if expected_digest is not None and digest != expected_digest:
                raise ActionContractError(
                    "ingested input sha256 differs from the expected digest"
                )
            if expected_size is not None and size != expected_size:
                raise ActionContractError(
                    "ingested input byte count differs from the expected size"
                )
            entry = validate_input_contract(
                {"id": identity, "sha256": digest, "bytes": size}
            )
            contract = {"sha256": digest, "bytes": size}
            _, won = self._publish_staged_input_blob(staging, contract)
            return entry, won
        finally:
            _unlink_nofollow(staging, where="CAS staging file")

    def input_path(self, input_contract: object) -> Path:
        """Return an action input's CAS path after full content verification."""

        entry = validate_input_contract(input_contract)
        return self._verify_input_blob(
            {"sha256": entry["sha256"], "bytes": entry["bytes"]}
        )

    def publish_action_request(self, action: object) -> Path:
        """Publish the canonical immutable request consumed by remote workers."""

        normalized = validate_action(action)
        key = str(normalized["action_key"])
        path = self.root / "requests" / key[:2] / f"{key}.json"
        raw = _canonical_file_bytes(normalized)
        won = _atomic_publish(path, raw)
        if not won:
            observed = _read_regular_file_nofollow(
                path,
                where="PrismaBuild action request",
                require_readonly=True,
            )
            if observed != raw:
                raise CASTamperError(
                    "existing PrismaBuild action request differs from its action key"
                )
        # Re-read after either side of a publication race.  A scheduler must
        # never send a path whose bytes the submitter merely assumes.
        verified = _read_regular_file_nofollow(
            path,
            where="PrismaBuild action request",
            require_readonly=True,
        )
        if verified != raw:
            raise CASTamperError(
                "published PrismaBuild action request failed canonical readback"
            )
        return path

    def _load_receipt_bytes(self, path: Path) -> bytes:
        try:
            return _read_regular_file_nofollow(
                path, where="CAS receipt", require_readonly=True
            )
        except FileNotFoundError as exc:
            raise FileNotFoundError(path) from exc

    def _validate_receipt(
        self, value: object, *, action: Mapping[str, object]
    ) -> dict[str, object]:
        try:
            receipt = _exact_mapping(value, keys=_RECEIPT_KEYS, where="CAS receipt")
            if receipt["schema"] != CAS_RECEIPT_SCHEMA_V3:
                _fail(f"CAS receipt schema must be {CAS_RECEIPT_SCHEMA_V3!r}")
            action_key = _sha256(receipt["action_key"], where="CAS receipt.action_key")
            if action_key != action["action_key"]:
                _fail("CAS receipt action_key differs from the requested action")
            manifest_sha = _sha256(
                receipt["action_manifest_sha256"],
                where="CAS receipt.action_manifest_sha256",
            )
            if manifest_sha != canonical_sha256(action):
                _fail("CAS receipt action manifest digest differs from the action")
            raw_result = _exact_mapping(
                receipt["result"], keys=_RESULT_KEYS, where="CAS receipt.result"
            )
            result = {
                "sha256": _sha256(
                    raw_result["sha256"], where="CAS receipt.result.sha256"
                ),
                "bytes": _nonnegative_integer(
                    raw_result["bytes"], where="CAS receipt.result.bytes"
                ),
            }
            raw_producer = _exact_mapping(
                receipt["producer"],
                keys=_PRODUCER_KEYS,
                where="CAS receipt.producer",
            )
            # Publication runs this preflight live.  Lookup independently
            # replays its derivations and action binding from persisted data.
            producer = validate_worker_attestation(raw_producer, action=action)
            body = {
                "schema": CAS_RECEIPT_SCHEMA_V3,
                "action_key": action_key,
                "action_manifest_sha256": manifest_sha,
                "result": result,
                "producer": producer,
            }
            recorded = _sha256(
                receipt["receipt_sha256"], where="CAS receipt.receipt_sha256"
            )
            if recorded != canonical_sha256(body):
                _fail("CAS receipt.receipt_sha256 does not match its body")
            return {**body, "receipt_sha256": recorded}
        except ActionContractError as exc:
            raise CASTamperError(str(exc)) from exc

    def _verify_blob(
        self,
        result: Mapping[str, object],
        *,
        writable_label: str = "CAS payload",
    ) -> Path:
        digest = str(result["sha256"])
        path = self._blob_path(digest)
        try:
            observed_digest, observed_size, observed_mode = _file_identity_nofollow(
                path,
                where="CAS payload",
                expected_sha256=digest,
                expected_bytes=int(result["bytes"]),
            )
        except FileNotFoundError as exc:
            raise CASTamperError(f"CAS payload is missing: {path}") from exc
        if observed_size != result["bytes"]:
            raise CASTamperError(
                f"CAS payload content differs from receipt (size mismatch): {path}"
            )
        if observed_digest != digest or observed_size != result["bytes"]:
            raise CASTamperError(
                f"CAS payload content differs from receipt: {path}"
            )
        if observed_mode & 0o222:
            raise CASTamperError(f"{writable_label} is writable: {path}")
        return path

    def _verify_input_blob(self, contract: Mapping[str, object]) -> Path:
        return self._verify_blob(contract, writable_label="CAS input payload")

    def _lookup_receipt_only(
        self, action: Mapping[str, object]
    ) -> dict[str, object] | None:
        """Validate canonical receipt bytes without consuming its blob."""

        path = self._receipt_path(str(action["action_key"]))
        try:
            raw = self._load_receipt_bytes(path)
        except FileNotFoundError:
            return None
        try:
            value = _decode_strict_json(raw, where="CAS receipt")
            receipt = self._validate_receipt(value, action=action)
        except ActionContractError as exc:
            raise CASTamperError(str(exc)) from exc
        if raw != _canonical_file_bytes(receipt):
            raise CASTamperError("CAS receipt bytes are not canonical JSON")
        return receipt

    def lookup(self, action: object) -> dict[str, object] | None:
        """Return a content-verified v3 receipt, or ``None`` on a v3 miss.

        An unversioned legacy-v2 receipt is deliberately neither migrated nor
        interpreted here.  It remains immutable history while v3 recomputes
        into its disjoint namespace.
        """

        normalized = validate_action(action)
        receipt = self._lookup_receipt_only(normalized)
        if receipt is None:
            return None
        self._verify_blob(receipt["result"])  # type: ignore[arg-type]
        return receipt

    def _verified_receipt_result_path(self, receipt: Mapping[str, object]) -> Path:
        """Return the blob path after ``lookup`` has already verified it."""

        result = receipt["result"]
        assert isinstance(result, Mapping)
        return self._blob_path(str(result["sha256"]))

    def result_path(self, receipt: object, action: object) -> Path:
        normalized = validate_action(action)
        validated = self._validate_receipt(receipt, action=normalized)
        return self._verify_blob(validated["result"])  # type: ignore[arg-type]

    def publish_result(
        self,
        action: object,
        result_path: str | Path,
        *,
        attestation: object,
        precommit_verify: Callable[[], None] | None = None,
        staging_namespace: str | None = None,
    ) -> tuple[dict[str, object], bool]:
        """Publish a result and return ``(canonical_receipt, won_publication)``.

        A losing deterministic producer must reproduce the winner byte-for-byte;
        a stochastic producer accepts the already-published canonical result.
        ``precommit_verify`` runs after the payload copy and temporary receipt
        fsync. Local workers use it for their action-specific closure checks;
        the worker core and optional script launcher are rechecked after that
        callback as the final userspace operation before the no-clobber link.
        This cannot make mutable source an immutable snapshot; the remaining
        verification-to-link syscall interval is deliberately small.
        """

        normalized = validate_action(action)
        producer = validate_worker_attestation(attestation, action=normalized)
        if staging_namespace is None:
            staging_directory = self.root / ".staging"
        else:
            namespace = _sha256(
                staging_namespace, where="result staging namespace"
            )
            staging_directory = (
                self.root / ".staging" / "local-results" / namespace
            )
        staging, digest, size = _copy_to_staging(
            Path(result_path), staging_directory
        )
        result = {"sha256": digest, "bytes": size}
        try:
            _, blob_won = self._publish_staged_input_blob(staging, result)
            body: dict[str, object] = {
                "schema": CAS_RECEIPT_SCHEMA_V3,
                "action_key": normalized["action_key"],
                "action_manifest_sha256": canonical_sha256(normalized),
                "result": result,
                "producer": producer,
            }
            candidate = {**body, "receipt_sha256": canonical_sha256(body)}
            receipt_path = self._receipt_path(str(normalized["action_key"]))

            def verify_publication_provenance() -> None:
                # Action-specific closure/executable checks may be relatively
                # slow.  Run them first so the runtime recheck is the final
                # userspace operation before _atomic_publish calls os.link.
                if precommit_verify is not None:
                    precommit_verify()
                _verify_worker_runtime_unchanged(producer["runtime"])

            won = _atomic_publish(
                receipt_path,
                _canonical_file_bytes(candidate),
                prelink_verify=verify_publication_provenance,
            )
            # The canonical blob was already consumed above: a winning link
            # was proven identical to our private hashed staging inode, while
            # a losing link was hashed in full.  Re-read and validate the
            # receipt, but do not perform a redundant full blob pass when it
            # names that same result.
            canonical = self._lookup_receipt_only(normalized)
            if canonical is None:
                raise CASTamperError("CAS receipt vanished after publication race")
            if won:
                if canonical != candidate:
                    raise CASTamperError(
                        "published CAS receipt failed canonical readback"
                    )
                return canonical, True
            task = normalized["task"]
            assert isinstance(task, Mapping)
            canonical_result = canonical["result"]
            assert isinstance(canonical_result, Mapping)
            if canonical_result != result:
                # A stochastic receipt race may select a different blob.  It
                # has not been consumed by this publication and must retain
                # the ordinary full-content verification contract.
                self._verify_blob(canonical_result)
            if task["determinism"] == "deterministic" and canonical_result != result:
                raise CASConflictError(
                    "deterministic recomputation differs from the canonical CAS result"
                )
            return canonical, False
        finally:
            _unlink_nofollow(staging, where="CAS staging file")


def _validate_initial_miss_rendezvous_manifest(
    value: object, *, action: Mapping[str, object], cas: PrismaBuildCAS
) -> dict[str, object]:
    raw = _exact_mapping(
        value,
        keys=_INITIAL_MISS_RENDEZVOUS_MANIFEST_KEYS,
        where="initial-miss rendezvous manifest",
    )
    if raw["schema"] != INITIAL_MISS_RENDEZVOUS_MANIFEST_SCHEMA_V1:
        _fail(
            "initial-miss rendezvous manifest.schema must be "
            f"{INITIAL_MISS_RENDEZVOUS_MANIFEST_SCHEMA_V1!r}"
        )
    namespace_text = _text(
        raw["rendezvous_namespace"],
        where="initial-miss rendezvous manifest.rendezvous_namespace",
    )
    namespace = Path(namespace_text)
    if (
        not namespace.is_absolute()
        or namespace == Path("/")
        or ".." in namespace.parts
        or str(namespace) != namespace_text
    ):
        _fail(
            "initial-miss rendezvous namespace must be a normalized, "
            "non-root absolute path"
        )
    cas_root_text = _text(
        raw["cas_root"],
        where="initial-miss rendezvous manifest.cas_root",
    )
    cas_root = Path(cas_root_text)
    if (
        not cas_root.is_absolute()
        or cas_root == Path("/")
        or ".." in cas_root.parts
        or str(cas_root) != cas_root_text
    ):
        _fail(
            "initial-miss rendezvous CAS root must be a normalized, "
            "non-root absolute path"
        )
    if cas_root != cas.root:
        _fail("initial-miss rendezvous CAS root differs from the worker CAS")
    run_nonce = _text(
        raw["run_nonce"],
        where="initial-miss rendezvous manifest.run_nonce",
        pattern=_RUN_NONCE_RE,
    )
    action_key = _sha256(
        raw["action_key"],
        where="initial-miss rendezvous manifest.action_key",
    )
    if action_key != action["action_key"]:
        _fail("initial-miss rendezvous manifest action_key differs from the action")
    participants_raw = raw["participants"]
    if type(participants_raw) is not list or len(participants_raw) != 2:
        _fail(
            "initial-miss rendezvous manifest.participants must contain "
            "exactly two hostnames"
        )
    participants = [
        _text(
            participant,
            where=f"initial-miss rendezvous manifest.participants[{index}]",
            pattern=_HOSTNAME_RE,
        )
        for index, participant in enumerate(participants_raw)
    ]
    if participants != sorted(participants) or len(set(participants)) != 2:
        _fail(
            "initial-miss rendezvous participants must be exact, sorted, "
            "unique lowercase hostnames"
        )
    timeout_raw = raw["timeout_seconds"]
    if (
        type(timeout_raw) not in {int, float}
        or not math.isfinite(float(timeout_raw))
        or float(timeout_raw) <= 0.0
        or float(timeout_raw) > _MAX_INITIAL_MISS_RENDEZVOUS_TIMEOUT_SECONDS
    ):
        _fail(
            "initial-miss rendezvous timeout_seconds must be a positive finite "
            f"number no greater than {_MAX_INITIAL_MISS_RENDEZVOUS_TIMEOUT_SECONDS}"
        )
    body: dict[str, object] = {
        "schema": INITIAL_MISS_RENDEZVOUS_MANIFEST_SCHEMA_V1,
        "rendezvous_namespace": namespace_text,
        "cas_root": cas_root_text,
        "run_nonce": run_nonce,
        "action_key": action_key,
        "participants": participants,
        "timeout_seconds": timeout_raw,
    }
    manifest_sha256 = _sha256(
        raw["manifest_sha256"],
        where="initial-miss rendezvous manifest.manifest_sha256",
    )
    if manifest_sha256 != canonical_sha256(body):
        _fail("initial-miss rendezvous manifest digest does not match its body")
    return {**body, "manifest_sha256": manifest_sha256}


def _load_initial_miss_rendezvous_manifest(
    path: str | Path, *, action: Mapping[str, object], cas: PrismaBuildCAS
) -> dict[str, object]:
    manifest_path = Path(path)
    if (
        not manifest_path.is_absolute()
        or manifest_path == Path("/")
        or ".." in manifest_path.parts
    ):
        raise ActionContractError(
            "initial-miss rendezvous manifest path must be a non-root absolute path"
        )
    raw = _read_regular_file_nofollow(
        manifest_path,
        where="initial-miss rendezvous manifest",
        require_readonly=True,
        require_single_link=True,
        max_bytes=_MAX_INITIAL_MISS_RENDEZVOUS_BYTES,
    )
    value = _decode_strict_json(raw, where="initial-miss rendezvous manifest")
    manifest = _validate_initial_miss_rendezvous_manifest(
        value, action=action, cas=cas
    )
    if raw != _canonical_file_bytes(manifest):
        raise ActionContractError(
            "initial-miss rendezvous manifest bytes are not canonical JSON"
        )
    return manifest


def _initial_miss_hostname() -> str:
    """Derive the exact lowercase host participant; never trust environment."""

    hostname = socket.gethostname().lower()
    return _text(
        hostname,
        where="initial-miss rendezvous local hostname",
        pattern=_HOSTNAME_RE,
    )


def _process_start_ticks() -> int:
    """Return this process's Linux start tick, disambiguating PID reuse."""

    pid = os.getpid()
    raw = _read_regular_file(
        Path(f"/proc/{pid}/stat"), where="initial-miss rendezvous process stat"
    )
    try:
        text = raw.decode("ascii")
    except UnicodeDecodeError as exc:
        raise InitialMissRendezvousError(
            "initial-miss rendezvous process stat is not ASCII"
        ) from exc
    close = text.rfind(")")
    if close < 0 or text[:close].split("(", 1)[0].strip() != str(pid):
        raise InitialMissRendezvousError(
            "initial-miss rendezvous process stat has a mismatched PID"
        )
    fields = text[close + 1 :].split()
    if len(fields) <= 19 or re.fullmatch(r"[1-9][0-9]*", fields[19]) is None:
        raise InitialMissRendezvousError(
            "initial-miss rendezvous process stat has no canonical start tick"
        )
    return int(fields[19])


def _initial_miss_process_identity(
    *, hostname: str, worker_launcher_identity: object | None
) -> dict[str, object]:
    runtime = _worker_runtime_identity(worker_launcher_identity)
    body: dict[str, object] = {
        "schema": INITIAL_MISS_RENDEZVOUS_PROCESS_SCHEMA_V1,
        "hostname": hostname,
        "pid": os.getpid(),
        "proc_start_ticks": _process_start_ticks(),
        "invocation_nonce": os.urandom(16).hex(),
        "runtime": runtime,
    }
    return {**body, "process_identity_sha256": canonical_sha256(body)}


def _validate_initial_miss_process_identity(
    value: object,
    *,
    participant: str,
    runtime: Mapping[str, object],
) -> dict[str, object]:
    raw = _exact_mapping(
        value,
        keys=_INITIAL_MISS_RENDEZVOUS_PROCESS_KEYS,
        where="initial-miss rendezvous process identity",
    )
    if raw["schema"] != INITIAL_MISS_RENDEZVOUS_PROCESS_SCHEMA_V1:
        _fail("initial-miss rendezvous process identity schema is unsupported")
    hostname = _text(
        raw["hostname"],
        where="initial-miss rendezvous process identity.hostname",
        pattern=_HOSTNAME_RE,
    )
    if hostname != participant:
        _fail("initial-miss rendezvous process hostname differs from participant")
    pid = raw["pid"]
    start_ticks = raw["proc_start_ticks"]
    if type(pid) is not int or pid <= 0:
        _fail("initial-miss rendezvous process pid must be positive")
    if type(start_ticks) is not int or start_ticks <= 0:
        _fail("initial-miss rendezvous process start tick must be positive")
    invocation_nonce = _text(
        raw["invocation_nonce"],
        where="initial-miss rendezvous process identity.invocation_nonce",
        pattern=_INVOCATION_NONCE_RE,
    )
    observed_runtime = _validate_worker_runtime(raw["runtime"])
    if observed_runtime != runtime:
        _fail(
            "initial-miss rendezvous participant runtime differs from the "
            "loaded core and launcher identity"
        )
    body: dict[str, object] = {
        "schema": INITIAL_MISS_RENDEZVOUS_PROCESS_SCHEMA_V1,
        "hostname": hostname,
        "pid": pid,
        "proc_start_ticks": start_ticks,
        "invocation_nonce": invocation_nonce,
        "runtime": observed_runtime,
    }
    digest = _sha256(
        raw["process_identity_sha256"],
        where="initial-miss rendezvous process identity digest",
    )
    if digest != canonical_sha256(body):
        _fail("initial-miss rendezvous process identity digest does not match")
    return {**body, "process_identity_sha256": digest}


def _initial_miss_arrival(
    *,
    manifest: Mapping[str, object],
    participant: str,
    process: Mapping[str, object],
) -> dict[str, object]:
    body: dict[str, object] = {
        "schema": INITIAL_MISS_RENDEZVOUS_ARRIVAL_SCHEMA_V1,
        "manifest_sha256": manifest["manifest_sha256"],
        "run_nonce": manifest["run_nonce"],
        "action_key": manifest["action_key"],
        "participant": participant,
        "process": process,
        "observation": "initial_cas_lookup_absent",
    }
    return {**body, "arrival_sha256": canonical_sha256(body)}


def _validate_initial_miss_arrival(
    value: object,
    *,
    manifest: Mapping[str, object],
    participant: str,
    runtime: Mapping[str, object],
) -> dict[str, object]:
    raw = _exact_mapping(
        value,
        keys=_INITIAL_MISS_RENDEZVOUS_ARRIVAL_KEYS,
        where="initial-miss rendezvous arrival",
    )
    if raw["schema"] != INITIAL_MISS_RENDEZVOUS_ARRIVAL_SCHEMA_V1:
        _fail("initial-miss rendezvous arrival schema is unsupported")
    for key in ("manifest_sha256", "run_nonce", "action_key"):
        if raw[key] != manifest[key]:
            _fail(f"initial-miss rendezvous arrival {key} differs from manifest")
    observed_participant = _text(
        raw["participant"],
        where="initial-miss rendezvous arrival.participant",
        pattern=_HOSTNAME_RE,
    )
    if observed_participant != participant:
        _fail("initial-miss rendezvous arrival participant differs from filename")
    if raw["observation"] != "initial_cas_lookup_absent":
        _fail("initial-miss rendezvous arrival observation is unsupported")
    process = _validate_initial_miss_process_identity(
        raw["process"], participant=participant, runtime=runtime
    )
    body: dict[str, object] = {
        "schema": INITIAL_MISS_RENDEZVOUS_ARRIVAL_SCHEMA_V1,
        "manifest_sha256": manifest["manifest_sha256"],
        "run_nonce": manifest["run_nonce"],
        "action_key": manifest["action_key"],
        "participant": participant,
        "process": process,
        "observation": "initial_cas_lookup_absent",
    }
    digest = _sha256(
        raw["arrival_sha256"], where="initial-miss rendezvous arrival digest"
    )
    if digest != canonical_sha256(body):
        _fail("initial-miss rendezvous arrival digest does not match")
    return {**body, "arrival_sha256": digest}


def _initial_miss_ready(
    *,
    manifest: Mapping[str, object],
    participant: str,
    process: Mapping[str, object],
    arrival: Mapping[str, object],
    arrival_set_sha256: str,
) -> dict[str, object]:
    body: dict[str, object] = {
        "schema": INITIAL_MISS_RENDEZVOUS_READY_SCHEMA_V1,
        "manifest_sha256": manifest["manifest_sha256"],
        "run_nonce": manifest["run_nonce"],
        "action_key": manifest["action_key"],
        "participant": participant,
        "process_identity_sha256": process["process_identity_sha256"],
        "arrival_sha256": arrival["arrival_sha256"],
        "arrival_set_sha256": arrival_set_sha256,
        "observation": "cas_absent_after_complete_arrival_set",
    }
    return {**body, "ready_sha256": canonical_sha256(body)}


def _validate_initial_miss_ready(
    value: object,
    *,
    manifest: Mapping[str, object],
    participant: str,
    arrivals: Mapping[str, Mapping[str, object]],
    arrival_set_sha256: str,
) -> dict[str, object]:
    raw = _exact_mapping(
        value,
        keys=_INITIAL_MISS_RENDEZVOUS_READY_KEYS,
        where="initial-miss rendezvous ready record",
    )
    if raw["schema"] != INITIAL_MISS_RENDEZVOUS_READY_SCHEMA_V1:
        _fail("initial-miss rendezvous ready schema is unsupported")
    for key in ("manifest_sha256", "run_nonce", "action_key"):
        if raw[key] != manifest[key]:
            _fail(f"initial-miss rendezvous ready {key} differs from manifest")
    observed_participant = _text(
        raw["participant"],
        where="initial-miss rendezvous ready.participant",
        pattern=_HOSTNAME_RE,
    )
    if observed_participant != participant:
        _fail("initial-miss rendezvous ready participant differs from filename")
    arrival = arrivals[participant]
    process = arrival["process"]
    if not isinstance(process, Mapping):
        _fail("initial-miss rendezvous arrival process is not an object")
    if raw["process_identity_sha256"] != process["process_identity_sha256"]:
        _fail("initial-miss rendezvous ready process differs from its arrival")
    if raw["arrival_sha256"] != arrival["arrival_sha256"]:
        _fail("initial-miss rendezvous ready record differs from its arrival")
    if raw["arrival_set_sha256"] != arrival_set_sha256:
        _fail("initial-miss rendezvous ready record binds the wrong arrival set")
    if raw["observation"] != "cas_absent_after_complete_arrival_set":
        _fail("initial-miss rendezvous ready observation is unsupported")
    body: dict[str, object] = {
        "schema": INITIAL_MISS_RENDEZVOUS_READY_SCHEMA_V1,
        "manifest_sha256": manifest["manifest_sha256"],
        "run_nonce": manifest["run_nonce"],
        "action_key": manifest["action_key"],
        "participant": participant,
        "process_identity_sha256": process["process_identity_sha256"],
        "arrival_sha256": arrival["arrival_sha256"],
        "arrival_set_sha256": arrival_set_sha256,
        "observation": "cas_absent_after_complete_arrival_set",
    }
    digest = _sha256(
        raw["ready_sha256"], where="initial-miss rendezvous ready digest"
    )
    if digest != canonical_sha256(body):
        _fail("initial-miss rendezvous ready digest does not match")
    return {**body, "ready_sha256": digest}


def _rendezvous_directory_entries(path: Path, *, where: str) -> set[str]:
    descriptor = _open_directory_nofollow(path, where=where)
    try:
        try:
            names: list[str] = []
            with os.scandir(descriptor) as entries:
                for entry in entries:
                    names.append(entry.name)
                    if (
                        len(names)
                        > _MAX_INITIAL_MISS_RENDEZVOUS_DIRECTORY_ENTRIES
                    ):
                        raise InitialMissRendezvousError(
                            f"{where} exceeds the bounded entry count"
                        )
        except InitialMissRendezvousError:
            raise
        except OSError as exc:
            raise InitialMissRendezvousError(
                f"cannot list {where}: {path}: {exc}"
            ) from exc
        if any(type(name) is not str for name in names):
            raise InitialMissRendezvousError(
                f"{where} returned a non-text directory entry"
            )
        _assert_directory_identity(descriptor, path, where=where)
        return set(names)
    finally:
        os.close(descriptor)


def _initial_miss_phase_temp_name(name: str, expected: set[str]) -> bool:
    return any(
        name.startswith(f".{leaf}.") and name.endswith(".tmp")
        for leaf in expected
    )


def _scan_initial_miss_phase(
    directory: Path,
    *,
    phase: str,
    participants: Sequence[str],
    validate: Callable[[object, str], dict[str, object]],
) -> dict[str, dict[str, object]] | None:
    expected = {f"{participant}.json" for participant in participants}
    observed = _rendezvous_directory_entries(
        directory, where=f"initial-miss rendezvous {phase} directory"
    )
    unexpected = {
        name
        for name in observed - expected
        if not _initial_miss_phase_temp_name(name, expected)
    }
    if unexpected:
        raise InitialMissRendezvousError(
            f"initial-miss rendezvous {phase} directory has unexpected entries: "
            f"{sorted(unexpected)}"
        )
    incomplete = observed != expected
    records: dict[str, dict[str, object]] = {}
    for participant in participants:
        leaf = f"{participant}.json"
        if leaf not in observed:
            continue
        path = directory / leaf
        try:
            raw = _read_regular_file_nofollow(
                path,
                where=f"initial-miss rendezvous {phase} record",
                require_readonly=True,
                require_single_link=True,
                max_bytes=_MAX_INITIAL_MISS_RENDEZVOUS_BYTES,
            )
        except FileNotFoundError:
            incomplete = True
            continue
        except _FileLinkCountError:
            # A valid hard-link publication briefly has two names until its
            # private temporary name is removed.  It may become acceptable,
            # but a persistent or hostile hard link can only reach timeout.
            incomplete = True
            continue
        value = _decode_strict_json(
            raw, where=f"initial-miss rendezvous {phase} record"
        )
        record = validate(value, participant)
        if raw != _canonical_file_bytes(record):
            raise InitialMissRendezvousError(
                f"initial-miss rendezvous {phase} record is not canonical JSON"
            )
        records[participant] = record
    after = _rendezvous_directory_entries(
        directory, where=f"initial-miss rendezvous {phase} directory"
    )
    after_unexpected = {
        name
        for name in after - expected
        if not _initial_miss_phase_temp_name(name, expected)
    }
    if after_unexpected:
        raise InitialMissRendezvousError(
            f"initial-miss rendezvous {phase} directory has unexpected entries: "
            f"{sorted(after_unexpected)}"
        )
    if after != expected or len(records) != len(expected):
        incomplete = True
    return None if incomplete else records


def _publish_initial_miss_record(
    path: Path,
    record: Mapping[str, object],
    *,
    phase: str,
    cas: PrismaBuildCAS,
    action: Mapping[str, object],
    runtime: Mapping[str, object],
) -> None:
    def verify_absence_and_runtime() -> None:
        if cas.lookup(action) is not None:
            raise InitialMissRendezvousError(
                "CAS receipt appeared before initial-miss rendezvous release"
            )
        _verify_worker_runtime_unchanged(runtime)

    if not _atomic_publish(
        path,
        _canonical_file_bytes(record),
        prelink_verify=verify_absence_and_runtime,
    ):
        raise InitialMissRendezvousError(
            f"duplicate or replayed initial-miss rendezvous {phase} participant: "
            f"{path.stem}"
        )


def _wait_initial_miss_arrivals(
    *,
    directory: Path,
    manifest: Mapping[str, object],
    runtime: Mapping[str, object],
    cas: PrismaBuildCAS,
    action: Mapping[str, object],
    deadline: float,
) -> dict[str, dict[str, object]]:
    participants = manifest["participants"]
    if not isinstance(participants, list):
        raise InitialMissRendezvousError(
            "validated rendezvous participants are not an array"
        )
    while True:
        records = _scan_initial_miss_phase(
            directory,
            phase="arrival",
            participants=participants,
            validate=lambda value, participant: _validate_initial_miss_arrival(
                value,
                manifest=manifest,
                participant=participant,
                runtime=runtime,
            ),
        )
        # Complete arrivals are not a release. Every participant must make a
        # fresh absence observation before publishing its second-phase ready.
        if cas.lookup(action) is not None:
            raise InitialMissRendezvousError(
                "CAS receipt appeared before initial-miss arrivals completed"
            )
        if records is not None:
            return records
        remaining = deadline - time.monotonic()
        if remaining <= 0.0:
            raise InitialMissRendezvousError(
                "initial-miss rendezvous timed out waiting for exact arrivals"
            )
        time.sleep(min(_INITIAL_MISS_RENDEZVOUS_POLL_SECONDS, remaining))


def _wait_initial_miss_ready(
    *,
    directory: Path,
    manifest: Mapping[str, object],
    arrivals: Mapping[str, Mapping[str, object]],
    arrival_set_sha256: str,
    cas: PrismaBuildCAS,
    action: Mapping[str, object],
    deadline: float,
) -> dict[str, dict[str, object]]:
    participants = manifest["participants"]
    if not isinstance(participants, list):
        raise InitialMissRendezvousError(
            "validated rendezvous participants are not an array"
        )
    receipt_observed = False
    while True:
        records = _scan_initial_miss_phase(
            directory,
            phase="ready",
            participants=participants,
            validate=lambda value, participant: _validate_initial_miss_ready(
                value,
                manifest=manifest,
                participant=participant,
                arrivals=arrivals,
                arrival_set_sha256=arrival_set_sha256,
            ),
        )
        # The exact second ready link is the logical release event. A fast
        # peer may legitimately acquire the output lock and publish after it;
        # do not turn that post-release receipt into a false refusal here.
        if records is not None:
            return records
        if not receipt_observed and cas.lookup(action) is not None:
            # Release may already have happened while NFS still returns a
            # stale ready-directory view. Once a receipt is visible, continue
            # bounded exact scans through the original monotonic deadline.
            # An actually missing in-protocol ready cannot newly link after
            # this observation because its pre-link CAS check fails closed.
            receipt_observed = True
        remaining = deadline - time.monotonic()
        if remaining <= 0.0:
            if receipt_observed:
                raise InitialMissRendezvousError(
                    "CAS receipt appeared before initial-miss ready release"
                )
            raise InitialMissRendezvousError(
                "initial-miss rendezvous timed out waiting for exact ready records"
            )
        time.sleep(min(_INITIAL_MISS_RENDEZVOUS_POLL_SECONDS, remaining))


def _run_initial_miss_rendezvous(
    *,
    manifest_path: str | Path,
    cas: PrismaBuildCAS,
    action: Mapping[str, object],
    worker_launcher_identity: object | None,
) -> dict[str, object]:
    """Prove exact peers observed one initial miss before either may publish."""

    manifest = _load_initial_miss_rendezvous_manifest(
        manifest_path, action=action, cas=cas
    )
    deadline = time.monotonic() + float(manifest["timeout_seconds"])
    participant = _initial_miss_hostname()
    participants = manifest["participants"]
    if not isinstance(participants, list) or participant not in participants:
        raise InitialMissRendezvousError(
            "local lowercase hostname is not an exact rendezvous participant"
        )
    process = _initial_miss_process_identity(
        hostname=participant,
        worker_launcher_identity=worker_launcher_identity,
    )
    runtime = process["runtime"]
    if not isinstance(runtime, Mapping):
        raise InitialMissRendezvousError(
            "initial-miss rendezvous runtime identity is not an object"
        )
    namespace = Path(str(manifest["rendezvous_namespace"]))
    for directory, where in (
        (namespace, "initial-miss rendezvous namespace"),
        (namespace / "arrivals", "initial-miss rendezvous arrival directory"),
        (namespace / "ready", "initial-miss rendezvous ready directory"),
    ):
        descriptor = _open_directory_nofollow(directory, where=where, create=True)
        os.close(descriptor)
    if _rendezvous_directory_entries(
        namespace, where="initial-miss rendezvous namespace"
    ) != {"arrivals", "ready"}:
        raise InitialMissRendezvousError(
            "initial-miss rendezvous namespace has missing or extra entries"
        )
    if time.monotonic() >= deadline:
        raise InitialMissRendezvousError(
            "initial-miss rendezvous timed out during manifest validation"
        )
    if cas.lookup(action) is not None:
        raise InitialMissRendezvousError(
            "CAS receipt appeared after the worker's initial miss"
        )
    arrival = _initial_miss_arrival(
        manifest=manifest,
        participant=participant,
        process=process,
    )
    _publish_initial_miss_record(
        namespace / "arrivals" / f"{participant}.json",
        arrival,
        phase="arrival",
        cas=cas,
        action=action,
        runtime=runtime,
    )
    arrivals = _wait_initial_miss_arrivals(
        directory=namespace / "arrivals",
        manifest=manifest,
        runtime=runtime,
        cas=cas,
        action=action,
        deadline=deadline,
    )
    arrival_set_sha256 = canonical_sha256(
        [arrivals[name] for name in participants]
    )
    # This is the second-phase observation required from every participant.
    # The same check runs again as the final userspace operation before link.
    if cas.lookup(action) is not None:
        raise InitialMissRendezvousError(
            "CAS receipt appeared before initial-miss ready publication"
        )
    ready = _initial_miss_ready(
        manifest=manifest,
        participant=participant,
        process=process,
        arrival=arrivals[participant],
        arrival_set_sha256=arrival_set_sha256,
    )
    _publish_initial_miss_record(
        namespace / "ready" / f"{participant}.json",
        ready,
        phase="ready",
        cas=cas,
        action=action,
        runtime=runtime,
    )
    ready_records = _wait_initial_miss_ready(
        directory=namespace / "ready",
        manifest=manifest,
        arrivals=arrivals,
        arrival_set_sha256=arrival_set_sha256,
        cas=cas,
        action=action,
        deadline=deadline,
    )
    _verify_worker_runtime_unchanged(runtime)
    ready_set_sha256 = canonical_sha256(
        [ready_records[name] for name in participants]
    )
    body: dict[str, object] = {
        "schema": INITIAL_MISS_RENDEZVOUS_RECEIPT_SCHEMA_V1,
        "manifest_sha256": manifest["manifest_sha256"],
        "rendezvous_namespace": manifest["rendezvous_namespace"],
        "cas_root": manifest["cas_root"],
        "run_nonce": manifest["run_nonce"],
        "action_key": manifest["action_key"],
        "participants": participants,
        "participant": participant,
        "process_identity_sha256": process["process_identity_sha256"],
        "arrival_sha256": arrival["arrival_sha256"],
        "ready_sha256": ready["ready_sha256"],
        "arrival_set_sha256": arrival_set_sha256,
        "ready_set_sha256": ready_set_sha256,
    }
    receipt = {**body, "receipt_sha256": canonical_sha256(body)}
    if set(receipt) != set(_INITIAL_MISS_RENDEZVOUS_RECEIPT_KEYS):
        raise InitialMissRendezvousError(
            "initial-miss rendezvous receipt schema drifted"
        )
    return receipt


def _validate_execution_paths(
    checkout_root: Path, working_directory: str, result_path: str
) -> tuple[Path, Path]:
    """Resolve an action cwd and refuse traversal out of the checkout."""

    if checkout_root.is_symlink():
        raise LocalActionError(f"checkout root is a symlink: {checkout_root}")
    try:
        resolved_root = checkout_root.resolve(strict=True)
    except OSError as exc:
        raise LocalActionError(f"checkout root is unavailable: {checkout_root}") from exc
    cwd = (
        checkout_root
        if working_directory == "."
        else checkout_root / working_directory
    )
    try:
        resolved_cwd = cwd.resolve(strict=True)
        resolved_cwd.relative_to(resolved_root)
    except (OSError, ValueError) as exc:
        raise LocalActionError(
            f"declared working directory escapes or is unavailable: {cwd}"
        ) from exc
    if cwd.is_symlink() or not resolved_cwd.is_dir():
        raise LocalActionError(
            f"declared working directory is not a real directory: {cwd}"
        )
    return resolved_cwd, resolved_cwd / result_path


def _validate_result_containment(output: Path, cwd: Path) -> None:
    """Refuse result files reached through a symlinked parent or final link."""

    try:
        resolved = output.resolve(strict=True)
        resolved.relative_to(cwd)
    except (OSError, ValueError) as exc:
        raise LocalActionError(
            f"declared result escapes or is unavailable: {output}"
        ) from exc
    relative = output.relative_to(cwd)
    cursor = cwd
    for component in relative.parts:
        cursor = cursor / component
        try:
            mode = cursor.lstat().st_mode
        except OSError as exc:
            raise LocalActionError(
                f"cannot inspect declared result path: {cursor}"
            ) from exc
        if stat.S_ISLNK(mode):
            raise LocalActionError(
                f"declared result path traverses a symlink: {cursor}"
            )
    if not stat.S_ISREG(output.lstat().st_mode):
        raise LocalActionError(f"declared result is not a regular file: {output}")


def _refuse_existing_result_symlink_prefix(output: Path, cwd: Path) -> None:
    """Reject a pre-existing symlink in the declared output path."""

    relative = output.relative_to(cwd)
    cursor = cwd
    for component in relative.parts:
        cursor = cursor / component
        try:
            mode = cursor.lstat().st_mode
        except FileNotFoundError:
            return
        except OSError as exc:
            raise LocalActionError(
                f"cannot inspect declared result path: {cursor}"
            ) from exc
        if stat.S_ISLNK(mode):
            raise LocalActionError(
                f"declared result path traverses a symlink: {cursor}"
            )


def _local_result_claim_body(
    action: Mapping[str, object], checkout: Path
) -> dict[str, object]:
    """Describe exclusive ownership of one action's live-checkout result path."""

    task = action["task"]
    if not isinstance(task, Mapping):
        raise ActionContractError("validated action task is not an object")
    return {
        "schema": LOCAL_RESULT_CLAIM_SCHEMA_V1,
        "action_key": action["action_key"],
        "action_manifest_sha256": canonical_sha256(action),
        "checkout_root": str(checkout.resolve(strict=True)),
        "working_directory": task["working_directory"],
        "result_path": task["result_path"],
    }


def _local_result_claim_path(
    cas: PrismaBuildCAS, action: Mapping[str, object], checkout: Path
) -> Path:
    body = _local_result_claim_body(action, checkout)
    digest = canonical_sha256(body)
    return (
        cas.root
        / "local-results"
        / "v1"
        / digest[:2]
        / f"{digest}.json"
    )


def _validate_local_result_claim(
    value: object,
    *,
    action: Mapping[str, object],
    checkout: Path,
) -> dict[str, object]:
    try:
        raw = _exact_mapping(
            value,
            keys=_LOCAL_RESULT_CLAIM_KEYS,
            where="local result claim",
        )
        expected = _local_result_claim_body(action, checkout)
        body = {key: raw[key] for key in _LOCAL_RESULT_CLAIM_BODY_KEYS}
        if body != expected:
            _fail("local result claim differs from the action and checkout")
        digest = _sha256(
            raw["claim_sha256"], where="local result claim.claim_sha256"
        )
        if digest != canonical_sha256(body):
            _fail("local result claim digest does not match its body")
        return {**body, "claim_sha256": digest}
    except ActionContractError as exc:
        raise CASTamperError(str(exc)) from exc


def _load_local_result_claim(
    cas: PrismaBuildCAS,
    action: Mapping[str, object],
    checkout: Path,
) -> dict[str, object] | None:
    path = _local_result_claim_path(cas, action, checkout)
    try:
        raw = _read_regular_file_nofollow(
            path, where="local result claim", require_readonly=True
        )
    except FileNotFoundError:
        return None
    value = _decode_strict_json(raw, where="local result claim")
    claim = _validate_local_result_claim(
        value, action=action, checkout=checkout
    )
    if raw != _canonical_file_bytes(claim):
        raise CASTamperError("local result claim is not canonical JSON")
    return claim


def _ensure_local_result_claim(
    cas: PrismaBuildCAS,
    action: Mapping[str, object],
    checkout: Path,
) -> dict[str, object]:
    body = _local_result_claim_body(action, checkout)
    claim = {**body, "claim_sha256": canonical_sha256(body)}
    path = _local_result_claim_path(cas, action, checkout)
    won = _atomic_publish(path, _canonical_file_bytes(claim))
    observed = _load_local_result_claim(cas, action, checkout)
    if observed is None:
        raise CASTamperError("local result claim vanished after publication")
    if observed != claim:
        raise CASTamperError(
            "local result claim publication raced with conflicting state"
        )
    if won and path.name != f"{claim['claim_sha256']}.json":
        raise AssertionError("local result claim address differs from its digest")
    return observed


def _reap_local_result_staging(
    cas: PrismaBuildCAS, claim: Mapping[str, object]
) -> int:
    """Remove bounded private staging leaves left by a killed claimed run."""

    namespace = _sha256(
        claim["claim_sha256"], where="local result claim.claim_sha256"
    )
    directory = cas.root / ".staging" / "local-results" / namespace
    try:
        directory_fd = _open_directory_nofollow(
            directory, where="local result staging directory"
        )
    except FileNotFoundError:
        return 0
    names: list[str] = []
    try:
        try:
            with os.scandir(directory_fd) as entries:
                for entry in entries:
                    name = entry.name
                    if not (name.startswith(".payload.") and name.endswith(".tmp")):
                        raise CASTamperError(
                            f"unexpected local result staging entry: {directory / name}"
                        )
                    names.append(name)
                    if len(names) > _MAX_LOCAL_RESULT_STAGING_FILES:
                        raise CASTamperError(
                            "local result staging file count exceeds the recovery bound"
                        )
        except OSError as exc:
            raise CASUnavailableError(
                f"cannot list local result staging directory: {directory}: {exc}"
            ) from exc
        for name in sorted(names):
            try:
                info = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            except OSError as exc:
                raise CASTamperError(
                    f"local result staging entry changed during recovery: {directory / name}"
                ) from exc
            if not stat.S_ISREG(info.st_mode):
                raise CASTamperError(
                    f"local result staging entry is not regular: {directory / name}"
                )
            if info.st_uid != os.geteuid():
                raise CASTamperError(
                    f"local result staging entry has a foreign owner: {directory / name}"
                )
            try:
                os.unlink(name, dir_fd=directory_fd)
            except OSError as exc:
                raise CASUnavailableError(
                    f"cannot remove local result staging entry: {directory / name}: {exc}"
                ) from exc
        if names:
            os.fsync(directory_fd)
        _assert_directory_identity(
            directory_fd,
            directory,
            where="local result staging directory",
        )
        return len(names)
    finally:
        os.close(directory_fd)


def _remove_claimed_local_result(output: Path, cwd: Path) -> None:
    """Remove only a regular, contained result owned by a validated claim."""

    _validate_result_containment(output, cwd)
    _unlink_nofollow(output, where="claimed local result")
    if output.exists() or output.is_symlink():
        raise LocalActionError(
            f"claimed local result remained after deterministic recovery: {output}"
        )


def repair_local_result(
    action: object,
    *,
    cas_root: str | Path,
    checkout_root: str | Path,
) -> dict[str, object]:
    """Remove a crash-left declared result only under its immutable claim.

    This never removes an unclaimed path, a symlink, or a result whose action
    already has a canonical receipt.  The same output lock used by execution
    serializes repair with a live local producer.
    """

    normalized = validate_action(action)
    root = Path(checkout_root)
    if not root.is_absolute():
        raise ActionContractError("checkout_root must be absolute")
    cas = PrismaBuildCAS(cas_root)
    if cas.lookup(normalized) is not None:
        raise LocalActionError(
            "cannot repair a declared result after its action has a CAS receipt"
        )
    task = normalized["task"]
    if not isinstance(task, Mapping):
        raise ActionContractError("validated action task is not an object")
    cwd, output = _validate_execution_paths(
        root, str(task["working_directory"]), str(task["result_path"])
    )
    with _local_output_lock(cas, root, output):
        # The pre-lock lookup is only a fast refusal.  A producer can publish
        # while this repair waits for the shared output lock, so the decision
        # to unlink must be made from a fresh receipt lookup while exclusion
        # is held.
        if cas.lookup(normalized) is not None:
            raise LocalActionError(
                "cannot repair a declared result after its action has a CAS receipt"
            )
        claim = _load_local_result_claim(cas, normalized, root)
        if claim is None:
            raise LocalActionError(
                "declared result has no matching immutable recovery claim"
            )
        reaped_staging_files = _reap_local_result_staging(cas, claim)
        if output.exists() or output.is_symlink():
            _remove_claimed_local_result(output, cwd)
            status = "removed"
        else:
            _refuse_existing_result_symlink_prefix(output, cwd)
            status = "already_absent"
    return {
        "status": status,
        "action_key": normalized["action_key"],
        "claim_sha256": claim["claim_sha256"],
        "result_path": str(output),
        "reaped_staging_files": reaped_staging_files,
    }


@contextmanager
def _local_output_lock(cas: PrismaBuildCAS, checkout: Path, output: Path):
    """Serialize actions sharing one live-checkout result path.

    The yielded descriptor is the locked open-file description.  Local task
    processes inherit it explicitly so abrupt worker death cannot release the
    lock while the task (or an inherited descendant) can still write the
    declared output.

    The identity is the canonical physical output path and nothing else.  It
    used to hash the resolved checkout root as well, which made one file two
    locks: ``checkout_root=/repo`` with ``working_directory=sub`` and
    ``checkout_root=/repo/sub`` with ``working_directory=.`` both resolve to
    ``/repo/sub/result.bin``, so two concurrent actions each passed the
    absent-result check and one published the other's bytes under its own
    deterministic key.  ``checkout`` is still taken, because the caller's root
    is what names the output, but it is deliberately not part of the
    exclusion identity: what has to be exclusive is the file.
    """

    identity = hashlib.sha256(
        os.path.normpath(str(output)).encode("utf-8")
    ).hexdigest()
    directory = cas.root / ".worker-locks"
    path = directory / f"{identity}.lock"
    directory_fd = _open_directory_nofollow(
        directory, where="local action lock directory", create=True
    )
    flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW
    try:
        descriptor = os.open(path.name, flags, 0o600, dir_fd=directory_fd)
    except OSError as exc:
        os.close(directory_fd)
        raise LocalActionError(f"cannot open local action lock: {path}") from exc
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise LocalActionError(
                f"local action lock is not a regular file: {path}"
            )
        _assert_directory_identity(
            directory_fd, directory, where="local action lock directory"
        )
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield descriptor
    finally:
        # Closing releases the lock only after the final duplicate of this
        # open-file description closes.  Do not issue LOCK_UN: a task that
        # inherited the descriptor must retain exclusion if the worker
        # unwinds before that task can be reaped.
        os.close(descriptor)
        os.close(directory_fd)


def _process_group_has_live_members(pgid: int) -> bool:
    """Whether any process that has not exited still belongs to ``pgid``.

    ``os.killpg(pgid, 0)`` answers "is this group non-empty", and a zombie
    counts: a process that has exited stays a member of its group until its
    parent reaps it.  A worker that happens to be the nearest subreaper
    inherits an orphaned descendant's zombie and would then see a group that
    never empties.  So ``killpg`` is used only for the cheap "definitely
    empty" answer, and ``/proc`` decides the rest, skipping the exited states.
    """

    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except OSError:
        return True
    try:
        entries = os.listdir("/proc")
    except OSError:
        # Without a process table to read, the non-empty answer above stands.
        return True
    for entry in entries:
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat", "rb") as handle:
                raw = handle.read()
        except OSError:
            continue
        # The command name is parenthesized and may itself contain spaces and
        # parentheses, so the fields are read from after its final ``)``:
        # state, then ppid, then the process group id.
        _, _, tail = raw.rpartition(b")")
        fields = tail.split()
        if len(fields) < 3 or fields[0] in {b"Z", b"X", b"x"}:
            continue
        try:
            if int(fields[2]) == pgid:
                return True
        except ValueError:
            continue
    return False


def _process_group_settled(
    process: subprocess.Popen[bytes] | subprocess.Popen[str], pgid: int
) -> bool:
    """Whether the leader is reaped and nothing else in its group is left."""

    if process.poll() is None:
        return False
    return not _process_group_has_live_members(pgid)


def _await_process_group_exit(
    process: subprocess.Popen[bytes] | subprocess.Popen[str],
    pgid: int,
    grace_s: float,
) -> bool:
    """Wait up to ``grace_s`` for the whole group to go, leader included."""

    deadline = time.monotonic() + grace_s
    while True:
        if _process_group_settled(process, pgid):
            return True
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        interval = min(remaining, _PROCESS_GROUP_POLL_SECONDS)
        if process.poll() is None:
            try:
                process.wait(timeout=interval)
            except subprocess.TimeoutExpired:
                pass
        else:
            time.sleep(interval)


def _terminate_process_group(
    process: subprocess.Popen[bytes] | subprocess.Popen[str],
    *,
    grace_s: float = _PROCESS_GROUP_GRACE_SECONDS,
) -> None:
    """Signal a whole process group down: TERM, ``grace_s``, KILL, ``grace_s``.

    ``grace_s`` is a parameter because the leader of the group is not always
    the last thing that has to die.  A worker launcher relays the TERM into an
    action of its own before exiting, and a caller reaping *that* group has to
    outlast the relay or it SIGKILLs the launcher mid-reap and orphans the
    action -- see ``pool.PoolQueue.execute``.

    The protocol runs against the live group, not against the leader.  Leader
    exit is not proof the group is gone: a descendant only has to ignore
    SIGTERM while the leader accepts it, and the old early return then skipped
    SIGKILL and handed the caller a timeout while that descendant kept the
    compute and its inherited copy of the output lock.  Every branch, the
    leader-already-exited one included, now drives TERM, grace and KILL until
    the group has no live member or the bounded grace runs out.
    """

    pgid = process.pid
    if _process_group_settled(process, pgid):
        return
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        # Nothing in the group is signallable; the leader may still need
        # reaping so it does not linger as a zombie of this worker.
        process.poll()
        return
    if _await_process_group_exit(process, pgid, grace_s):
        return
    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        process.poll()
        return
    _await_process_group_exit(process, pgid, grace_s)


@contextmanager
def _sigterm_unwinds_this_process():
    """Make SIGTERM and SIGINT unwind this worker so its action is reaped too.

    The action below runs in its own session on purpose, which is exactly what
    keeps it alive through a signal aimed at this worker -- and exactly what
    makes the default SIGTERM disposition wrong here.  Terminating where we
    stand skips the ``except BaseException`` reap, and the action keeps the
    GPU, the memory and the caller's ledger token with nothing left watching
    it.  Handling the signal turns termination into the unwind that already
    knows how to reap the action group.

    SIGINT is owned here for the same reason, and explicitly: Python installs
    its ``KeyboardInterrupt`` handler only when the interpreter starts with
    SIGINT at its default disposition.  A worker launched by a non-interactive
    shell's ``&``, by ``nohup``, or by any launcher that ignores SIGINT
    inherits ``SIG_IGN`` and is then not interruptible at all -- the fleet's
    dl380g10 loops ran that way, which is how the interruption gate could not
    certify its own property there (issue #25).  Whether an interruption
    reaps the action must be a fact about this worker, not about who exec'd
    it.

    The handler disarms itself before raising: a second signal landing while
    ``_terminate_process_group`` waits out its grace would raise straight
    through the reap and abandon it half-finished.
    """

    def _unwind(signum, frame):                       # noqa: ARG001
        signal.signal(signum, signal.SIG_IGN)
        raise SystemExit(128 + signum)

    def _interrupt(signum, frame):                    # noqa: ARG001
        signal.signal(signum, signal.SIG_IGN)
        raise KeyboardInterrupt

    try:
        previous_term = signal.signal(signal.SIGTERM, _unwind)
    except ValueError:
        # Not the main thread.  A library caller's signal disposition is not
        # this function's to set, and an in-process run has a live parent that
        # owns it; leave it alone rather than fail the action over it.
        yield
        return
    previous_int = signal.signal(signal.SIGINT, _interrupt)
    try:
        yield
    finally:
        signal.signal(signal.SIGINT, previous_int)
        signal.signal(signal.SIGTERM, previous_term)


def run_local_action(
    action: object,
    *,
    cas_root: str | Path,
    checkout_root: str | Path,
    timeout_seconds: float | None = None,
    recompute: bool = False,
    worker_launcher_identity: object | None = None,
    initial_miss_rendezvous: str | Path | None = None,
) -> dict[str, object]:
    """Execute one action locally and publish its declared file result."""

    normalized = validate_action(action)
    root = Path(checkout_root)
    if not root.is_absolute():
        raise ActionContractError("checkout_root must be absolute")
    cas = PrismaBuildCAS(cas_root)
    if recompute and initial_miss_rendezvous is not None:
        raise ActionContractError(
            "initial-miss rendezvous is incompatible with recompute"
        )
    initial_miss_receipt: dict[str, object] | None = None
    if not recompute:
        cached = cas.lookup(normalized)
        if cached is not None:
            verify_code_closure(normalized["code_closure"], root)
            return {
                "status": "cache_hit",
                "receipt": cached,
                "payload_path": str(cas._verified_receipt_result_path(cached)),
            }
        if initial_miss_rendezvous is not None:
            # This call is deliberately adjacent to the first authoritative
            # miss and precedes checkout/output resolution and its local lock.
            initial_miss_receipt = _run_initial_miss_rendezvous(
                manifest_path=initial_miss_rendezvous,
                cas=cas,
                action=normalized,
                worker_launcher_identity=worker_launcher_identity,
            )
    task = normalized["task"]
    environment = normalized["environment"]
    assert isinstance(task, Mapping)
    assert isinstance(environment, Mapping)
    cwd, output = _validate_execution_paths(
        root, str(task["working_directory"]), str(task["result_path"])
    )
    variables = environment["variables"]
    assert isinstance(variables, Mapping)
    recovered_declared_result = False
    reaped_staging_files = 0
    with _local_output_lock(cas, root, output) as output_lock_descriptor:
        # A concurrent producer may have filled the cache while this worker
        # waited for the checkout/output lock.
        if not recompute:
            cached = cas.lookup(normalized)
            if cached is not None:
                verify_code_closure(normalized["code_closure"], root)
                result: dict[str, object] = {
                    "status": "cache_hit",
                    "receipt": cached,
                    "payload_path": str(cas._verified_receipt_result_path(cached)),
                }
                if initial_miss_receipt is not None:
                    result["initial_miss_rendezvous"] = initial_miss_receipt
                return result
        attestation = preflight_action(
            normalized,
            cas_root=cas_root,
            checkout_root=root,
            worker_launcher_identity=worker_launcher_identity,
        )
        claim = _load_local_result_claim(cas, normalized, root)
        if claim is not None:
            reaped_staging_files = _reap_local_result_staging(cas, claim)
        if output.exists() or output.is_symlink():
            if claim is None:
                raise LocalActionError(
                    "declared result path must be absent before execution and "
                    f"has no matching recovery claim: {output}"
                )
            _remove_claimed_local_result(output, cwd)
            recovered_declared_result = True
        _refuse_existing_result_symlink_prefix(output, cwd)
        if claim is None:
            claim = _ensure_local_result_claim(cas, normalized, root)
        # The claim is durable before task argv. Recheck the owned path after
        # publication so an out-of-protocol checkout writer cannot silently
        # pre-populate the result in the claim-publication interval.
        if output.exists() or output.is_symlink():
            raise LocalActionError(
                f"declared result appeared before action execution: {output}"
            )
        _refuse_existing_result_symlink_prefix(output, cwd)
        process: subprocess.Popen[bytes] | None = None
        with _sigterm_unwinds_this_process():
            try:
                process = subprocess.Popen(
                    list(task["argv"]),
                    cwd=cwd,
                    env={
                        str(key): str(value) for key, value in variables.items()
                    },
                    shell=False,
                    stdin=subprocess.DEVNULL,
                    start_new_session=True,
                    # The action keeps exclusion alive if this worker is killed.
                    # ``pass_fds`` clears close-on-exec in the child while the
                    # parent's descriptor remains CLOEXEC for unrelated execs.
                    pass_fds=(output_lock_descriptor,),
                )
                returncode = process.wait(timeout=timeout_seconds)
            except subprocess.TimeoutExpired as exc:
                if process is not None:
                    _terminate_process_group(process)
                raise LocalActionError(
                    f"action execution timed out: {exc}"
                ) from exc
            except OSError as exc:
                if process is not None:
                    _terminate_process_group(process)
                raise LocalActionError(f"action execution failed: {exc}") from exc
            except BaseException:
                # A handled signal (SIGINT/KeyboardInterrupt, and SIGTERM by
                # the context manager above) unwinds this process while its
                # new-session child keeps running.  Reap the entire action
                # group before the context manager closes its copy of the
                # descriptor shared with that child.  If the child cannot be
                # reaped, its duplicate continues to hold the lock.
                if process is not None:
                    _terminate_process_group(process)
                raise
        if returncode != 0:
            raise LocalActionError(
                f"action argv exited with status {returncode}",
                returncode=returncode,
                signal=-returncode if returncode < 0 else None,
            )
        if not output.exists() and not output.is_symlink():
            raise LocalActionError(
                f"action succeeded without its declared result file: {output}"
            )
        # Preflight alone is insufficient: an action (or another checkout
        # writer) can change a closure member while argv is running.  Check
        # once before an expensive result copy, then again at the CAS receipt
        # commit point so copying a large artifact cannot reopen that gap.
        verify_code_closure(normalized["code_closure"], root)
        _verify_attested_executable_unchanged(attestation, normalized)
        _verify_attested_worker_runtime_unchanged(attestation, normalized)
        _validate_result_containment(output, cwd)

        def verify_publication_provenance() -> None:
            verify_code_closure(normalized["code_closure"], root)
            _verify_attested_executable_unchanged(attestation, normalized)

        receipt, won = cas.publish_result(
            normalized,
            output,
            attestation=attestation,
            precommit_verify=verify_publication_provenance,
            staging_namespace=str(claim["claim_sha256"]),
        )
    result = {
        "status": "published" if won else "canonical_result_reused",
        "receipt": receipt,
        # ``publish_result`` has already consumed or identity-proved the exact
        # canonical blob.  Returning its name must not trigger a fourth full
        # read of a large result; public lookups remain content-verifying.
        "payload_path": str(cas._verified_receipt_result_path(receipt)),
        "recovered_declared_result": recovered_declared_result,
        "reaped_staging_files": reaped_staging_files,
        "local_result_claim_sha256": claim["claim_sha256"],
    }
    if initial_miss_receipt is not None:
        result["initial_miss_rendezvous"] = initial_miss_receipt
    return result


def _read_json_mapping(path: Path, *, where: str) -> Mapping[str, object]:
    raw = _read_regular_file(path, where=where)
    value = _decode_strict_json(raw, where=where)
    if not isinstance(value, Mapping):
        raise ActionContractError(f"{where} root must be an object")
    return value


def _atomic_write_new_json(path: Path, value: Mapping[str, object]) -> None:
    if path.exists() or path.is_symlink():
        raise ActionContractError(f"output already exists: {path}")
    if not _atomic_publish(path, json.dumps(
        value,
        indent=2,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8") + b"\n"):
        raise ActionContractError(f"output appeared concurrently: {path}")


def _require_slurm_initial_start(
    environment: Mapping[str, str] | None = None,
) -> None:
    """Refuse a requeued/admin-restarted batch process before task argv runs.

    PrismaBuild's Slurm adapter currently seals ``max_requeues=0`` and submits
    with ``--no-requeue``. Slurm nevertheless exposes the actual restart count
    to the launched process; this worker-side gate is the final defense if an
    administrator or site policy restarts the allocation anyway.
    """

    env = os.environ if environment is None else environment
    job_id = env.get("SLURM_JOB_ID")
    if type(job_id) is not str or re.fullmatch(r"[1-9][0-9]{0,19}", job_id) is None:
        raise ActionContractError(
            "Slurm worker requires a positive numeric SLURM_JOB_ID"
        )
    restart_count = env.get("SLURM_RESTART_COUNT", "0")
    if type(restart_count) is not str or re.fullmatch(
        r"(?:0|[1-9][0-9]{0,8})", restart_count
    ) is None:
        raise ActionContractError("SLURM_RESTART_COUNT is malformed")
    if int(restart_count) != 0:
        raise ActionContractError(
            "restarted Slurm allocation is not authorized to execute task argv"
        )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    closure = commands.add_parser("seal-closure")
    closure.add_argument("--root", required=True, type=Path)
    closure.add_argument("--file", action="append", required=True)
    closure.add_argument("--output", required=True, type=Path)
    seal = commands.add_parser("seal-action")
    seal.add_argument("--body", required=True, type=Path)
    seal.add_argument("--output", required=True, type=Path)
    key = commands.add_parser("key")
    key.add_argument("--action", required=True, type=Path)
    verify = commands.add_parser("verify")
    verify.add_argument("--action", required=True, type=Path)
    verify.add_argument("--cas-root", required=True, type=Path)
    ingest = commands.add_parser("ingest-input")
    ingest.add_argument("--source", required=True, type=Path)
    ingest.add_argument("--cas-root", required=True, type=Path)
    ingest.add_argument("--input-id", required=True)
    ingest.add_argument("--expected-sha256")
    ingest.add_argument("--expected-bytes", type=int)
    verify_input = commands.add_parser("verify-input")
    verify_input.add_argument("--input-contract", required=True, type=Path)
    verify_input.add_argument("--cas-root", required=True, type=Path)
    run = commands.add_parser("run-local")
    run.add_argument("--action", required=True, type=Path)
    run.add_argument("--cas-root", required=True, type=Path)
    run.add_argument("--checkout-root", required=True, type=Path)
    run.add_argument("--timeout-seconds", type=float)
    run.add_argument("--recompute", action="store_true")
    run.add_argument("--require-slurm-initial-start", action="store_true")
    run.add_argument("--initial-miss-rendezvous", type=Path)
    repair = commands.add_parser("repair-local-result")
    repair.add_argument("--action", required=True, type=Path)
    repair.add_argument("--cas-root", required=True, type=Path)
    repair.add_argument("--checkout-root", required=True, type=Path)
    preflight = commands.add_parser("preflight")
    preflight.add_argument("--action", required=True, type=Path)
    preflight.add_argument("--cas-root", required=True, type=Path)
    preflight.add_argument("--checkout-root", required=True, type=Path)
    return parser


def _record_action_status(error: LocalActionError) -> None:
    """Leave the action's own ending where the transport that asked can read it.

    This process exits 1 whatever the action did, so its exit status cannot
    carry the action's -- and it must not: the launcher's status is what every
    fleet reader means by ``detail.returncode``. The number goes beside the
    job's logs instead, and the caller decides what to do with it.

    Written only for an action that ran and ended by itself. A worker verdict
    -- a missing result, a timeout -- leaves no file, so that a transport
    reading one knows it is reading the action's ending and not a default.
    A file this cannot write is a diagnostic lost, never an ending changed, so
    every failure here is swallowed and the original error is raised on.
    """

    destination = os.environ.get(ACTION_STATUS_PATH_ENV) or ""
    if not destination or error.returncode is None:
        return
    body: dict[str, object] = {"action_returncode": int(error.returncode)}
    if error.signal is not None:
        body["action_signal"] = int(error.signal)
    path = Path(destination)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.parent / f".{path.name}.{os.getpid()}.tmp"
        tmp.write_text(
            json.dumps(body, sort_keys=True) + "\n", encoding="utf-8"
        )
        os.replace(tmp, path)
    except OSError:
        pass


def main(
    argv: Sequence[str] | None = None,
    *,
    worker_launcher_identity: object | None = None,
) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "run-local" and args.require_slurm_initial_start:
        if args.recompute:
            raise ActionContractError(
                "Slurm worker protocol does not permit recompute"
            )
        # This is deliberately before reading even the immutable action
        # request: a forced/admin-restarted allocation must reach no worker or
        # task-side input processing beyond argument parsing.
        _require_slurm_initial_start()
    if args.command == "seal-closure":
        closure = build_code_closure(args.root, args.file)
        _atomic_write_new_json(args.output, closure)
        print(args.output)
        return 0
    if args.command == "ingest-input":
        cas = PrismaBuildCAS(args.cas_root)
        entry, won = cas.ingest_input(
            args.source,
            input_id=args.input_id,
            expected_sha256=args.expected_sha256,
            expected_bytes=args.expected_bytes,
        )
        print(
            json.dumps(
                {
                    "status": "published" if won else "already_present",
                    "input": entry,
                    "payload_path": str(cas.input_path(entry)),
                },
                sort_keys=True,
            )
        )
        return 0
    if args.command == "verify-input":
        contract = _read_json_mapping(args.input_contract, where="input contract")
        cas = PrismaBuildCAS(args.cas_root)
        normalized = validate_input_contract(contract)
        print(
            json.dumps(
                {
                    "input": normalized,
                    "payload_path": str(cas.input_path(normalized)),
                },
                sort_keys=True,
            )
        )
        return 0
    action_path = args.action if hasattr(args, "action") else args.body
    if args.command == "run-local" and args.require_slurm_initial_start:
        raw_action = _read_regular_file_nofollow(
            action_path, where="action request", require_readonly=True
        )
        action_data_value = _decode_strict_json(raw_action, where="action request")
        if not isinstance(action_data_value, Mapping):
            raise ActionContractError("action request root must be an object")
        action_data = action_data_value
    else:
        action_data = _read_json_mapping(action_path, where="action")
    if args.command == "seal-action":
        sealed = seal_action(action_data)
        _atomic_write_new_json(args.output, sealed)
        print(args.output)
        return 0
    action = validate_action(action_data)
    if args.command == "run-local" and args.require_slurm_initial_start:
        cas = PrismaBuildCAS(args.cas_root)
        expected_request = (
            cas.root
            / "requests"
            / str(action["action_key"])[:2]
            / f"{action['action_key']}.json"
        )
        if action_path != expected_request:
            raise ActionContractError(
                "Slurm worker action path differs from its canonical CAS request"
            )
    if args.command == "key":
        print(action["action_key"])
        return 0
    if args.command == "preflight":
        result = preflight_action(
            action,
            cas_root=args.cas_root,
            checkout_root=args.checkout_root,
            worker_launcher_identity=worker_launcher_identity,
        )
        print(json.dumps(result, sort_keys=True))
        return 0
    if args.command == "repair-local-result":
        result = repair_local_result(
            action,
            cas_root=args.cas_root,
            checkout_root=args.checkout_root,
        )
        print(json.dumps(result, sort_keys=True))
        return 0
    cas = PrismaBuildCAS(args.cas_root)
    if args.command == "verify":
        receipt = cas.lookup(action)
        if receipt is None:
            raise PrismaBuildError("action is not present in the CAS")
        print(json.dumps(receipt, sort_keys=True))
        return 0
    try:
        result = run_local_action(
            action,
            cas_root=args.cas_root,
            checkout_root=args.checkout_root,
            timeout_seconds=args.timeout_seconds,
            recompute=args.recompute,
            worker_launcher_identity=worker_launcher_identity,
            initial_miss_rendezvous=args.initial_miss_rendezvous,
        )
    except LocalActionError as error:
        _record_action_status(error)
        raise
    print(json.dumps(result, sort_keys=True))
    return 0


__all__ = [
    "ACTION_SCHEMA_V1",
    "ACTION_SCHEMA_V2",
    "ACTION_STATUS_PATH_ENV",
    "CAS_RECEIPT_SCHEMA_V3",
    "CODE_CLOSURE_SCHEMA_V1",
    "INITIAL_MISS_RENDEZVOUS_ARRIVAL_SCHEMA_V1",
    "INITIAL_MISS_RENDEZVOUS_MANIFEST_SCHEMA_V1",
    "INITIAL_MISS_RENDEZVOUS_PROCESS_SCHEMA_V1",
    "INITIAL_MISS_RENDEZVOUS_READY_SCHEMA_V1",
    "INITIAL_MISS_RENDEZVOUS_RECEIPT_SCHEMA_V1",
    "LOCAL_RESULT_CLAIM_SCHEMA_V1",
    "PBRUN_GENERATED_FINGERPRINT_HEX_LENGTH",
    "PBRUN_CHECKOUT_SNAPSHOT_INPUT_ID",
    "PBRUN_CHECKOUT_SNAPSHOT_REF_NAME",
    "PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V1",
    "PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V2",
    "PBRUN_RESULT_PREFIX",
    "PBRUN_STAMP_PREFIX",
    "WORKER_ATTESTATION_SCHEMA_V2",
    "SCONTROL_RETRY_DELAYS_S",
    "live_platform_toolchain_contract",
    "WORKER_RUNTIME_SCHEMA_V1",
    "ActionContractError",
    "CASConflictError",
    "CASTamperError",
    "CASUnavailableError",
    "InitialMissRendezvousError",
    "LocalActionError",
    "PrismaBuildCAS",
    "PrismaBuildError",
    "build_code_closure",
    "executable_toolchain_contract",
    "find_git_worktree_marker",
    "git_checkout_identity",
    "identify_executable",
    "is_pbrun_generated_path",
    "main",
    "preflight_action",
    "pbrun_git_exclude_patterns",
    "repair_local_result",
    "run_local_action",
    "seal_action",
    "validate_action",
    "validate_code_closure",
    "validate_input_contract",
    "validate_pbrun_checkout_snapshot",
    "validate_pbrun_snapshot_ref_name",
    "validate_worker_scope",
    "validate_worker_attestation",
    "verify_code_closure",
]


if __name__ == "__main__":
    raise SystemExit(
        main(worker_launcher_identity=dict(_LOADED_WORKER_CORE_IDENTITY))
    )
