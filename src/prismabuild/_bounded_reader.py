"""Private abandonable-reader infrastructure shared with fleet diagnostics.

Extracted from pbstatus without changing its deadline, IPC or child ownership
contract. Callers supply the read and retain the abandoned PID/starttime records;
this boundary does not certify a census as complete or serialize queue writes.
In particular, an advisory ready_items snapshot is not an authoritative census.
"""
from __future__ import annotations

import contextvars
import json
import math
import os
import select
import signal
import socket
import sys
import time
import uuid
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

from prismabuild.core import _sigterm_unwinds_this_process

#: How long to wait for a killed child before treating it as retained. Same
#: reasoning as mount_latency.KILL_GRACE_S: SIGKILL is delivered at the child's
#: next scheduling point, so immediate WNOHANG also sees children already dying.
KILL_GRACE_S = 0.25
_LIVE_PROCESS_STATES = (b"R", b"S", b"D", b"T", b"t", b"I")
_EXITED_PROCESS_STATES = (b"Z", b"X", b"x")


class ReaderOwnershipUnavailable(RuntimeError):
    """The parent cannot authorize an identity-bound reader to start."""


@dataclass(frozen=True)
class ReaderOwnership:
    """Exact local process and caller-supplied pool/read scope, not a lease.

    The parent records this before releasing the read handshake. A durable
    fence consumer must use reader_liveness, never ECHILD as proof of death.
    """

    pid: int
    starttime_ticks: int
    host: str
    boot_id: str
    pool_identity: str
    section: str

    def __post_init__(self) -> None:
        if (type(self.pid) is not int or self.pid <= 0
                or type(self.starttime_ticks) is not int or self.starttime_ticks < 0):
            raise ValueError("reader ownership requires an exact PID/starttime")
        for value in (self.host, self.boot_id, self.pool_identity, self.section):
            if not isinstance(value, str) or not value or value != value.strip():
                raise ValueError("reader ownership requires exact nonblank scope fields")
        if str(uuid.UUID(self.boot_id)) != self.boot_id:
            raise ValueError("reader ownership requires a canonical boot identity")

    def to_record(self) -> dict:
        return {"schema": "prismabuild.reader_ownership.v1", **asdict(self)}

    @classmethod
    def from_record(cls, record: Mapping[str, object]) -> ReaderOwnership:
        values = dict(record)
        if values.pop("schema", None) != "prismabuild.reader_ownership.v1":
            raise ValueError("unknown reader ownership schema")
        if set(values) != {"pid", "starttime_ticks", "host", "boot_id", "pool_identity", "section"}:
            raise ValueError("incomplete or foreign reader ownership fields")
        pid, ticks = values["pid"], values["starttime_ticks"]
        host, boot = values["host"], values["boot_id"]
        pool, section = values["pool_identity"], values["section"]
        if (not isinstance(pid, int) or not isinstance(ticks, int)
                or not isinstance(host, str) or not isinstance(boot, str)
                or not isinstance(pool, str) or not isinstance(section, str)):
            raise ValueError("reader ownership field types are invalid")
        return cls(pid=pid, starttime_ticks=ticks, host=host, boot_id=boot,
                   pool_identity=pool, section=section)


class Deadline:
    """The whole run's budget, so that one slow section cannot spend another's.

    Held in ``time.monotonic`` because a status screen must not change its
    mind about how long it has waited when NTP steps the clock. A budget of
    zero or less means no deadline at all, which is what this command did
    before issue #350 and what ``--timeout-s 0`` still asks for.

    Only zero asks for that, so a non-finite budget is refused rather than
    quietly granted one. ``NaN`` compares false against everything, so it
    would leave ``bounded`` false and select the unbounded path without ever
    saying so; positive infinity would reach ``select`` as an infinite timeout,
    which is the same waiting-forever this class exists to end.
    """

    def __init__(self, timeout_s: float) -> None:
        self.timeout_s = float(timeout_s)
        if not math.isfinite(self.timeout_s):
            raise ValueError(
                f"a deadline must be a finite number of seconds, not {timeout_s!r}")
        self.bounded = self.timeout_s > 0
        self._expires = time.monotonic() + self.timeout_s if self.bounded else None

    def remaining(self) -> float | None:
        if self._expires is None:
            return None
        return self._expires - time.monotonic()


def _proc_stat_fields(pid: int, proc: Path) -> list[bytes] | None:
    """The fields of ``/proc/<pid>/stat`` after ``comm``, or ``None``.

    Split on the last ``)`` because ``comm`` is the only field that can hold a
    space or a parenthesis. ``fields[0]`` is the state and ``fields[19]`` is
    ``starttime`` -- fields 3 and 22 of ``proc(5)``.
    """
    try:
        raw = (proc / str(pid) / "stat").read_bytes()
    except OSError:
        return None
    return _parse_proc_stat_fields(raw)


def _parse_proc_stat_fields(raw: bytes) -> list[bytes] | None:
    close = raw.rfind(b")")
    if close < 0:
        return None
    fields = raw[close + 2:].split()
    return fields if len(fields) > 19 else None


def _local_identity_bytes(path: Path, limit: int) -> bytes:
    # These are local /proc observations, not shared queue/CAS reads. Bound
    # allocation even when a caller supplies a private fixture proc tree.
    descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        chunks = []
        size = 0
        while size <= limit:
            chunk = os.read(descriptor, limit + 1 - size)
            if not chunk:
                return b"".join(chunks)
            chunks.append(chunk)
            size += len(chunk)
        raise ReaderOwnershipUnavailable("local reader identity exceeds its byte limit")
    finally:
        os.close(descriptor)


def _boot_id(proc: Path) -> str:
    raw = _local_identity_bytes(proc / "sys/kernel/random/boot_id", 64)
    value = raw.decode("ascii").strip()
    if str(uuid.UUID(value)) != value:
        raise ReaderOwnershipUnavailable("local boot identity is not canonical")
    return value


def _capture_ownership(pid: int, *, pool_identity: str, section: str,
                       allow_exited: bool = False) -> ReaderOwnership:
    try:
        fields = _parse_proc_stat_fields(
            _local_identity_bytes(Path("/proc") / str(pid) / "stat", 4096))
        states = _LIVE_PROCESS_STATES + _EXITED_PROCESS_STATES if allow_exited else _LIVE_PROCESS_STATES
        if fields is None or fields[0] not in states:
            raise ReaderOwnershipUnavailable("reader process identity is not provable")
        return ReaderOwnership(pid=pid, starttime_ticks=int(fields[19]),
                               host=socket.gethostname(), boot_id=_boot_id(Path("/proc")),
                               pool_identity=pool_identity, section=section)
    except (OSError, ValueError, UnicodeError) as exc:
        raise ReaderOwnershipUnavailable("reader process/boot identity is unavailable") from exc


def reader_liveness(ownership: ReaderOwnership, *, pool_identity: str, section: str,
                    proc: Path = Path("/proc")) -> Literal["running", "settled", "unknown"]:
    """Read exact liveness without signalling or reaping an unrelated process.

    Restarted consumers cannot waitpid an orphan. ECHILD therefore supplies
    no evidence here: only local host/boot and PID/starttime observations can
    settle the fence. Missing/invalid observations stay unknown, not empty.
    """
    if ownership.pool_identity != pool_identity or ownership.section != section:
        return "unknown"
    try:
        if socket.gethostname() != ownership.host:
            return "unknown"
        boot = _boot_id(proc)
        if boot != ownership.boot_id:
            return "settled"  # same host, a different kernel boot
        try:
            raw = _local_identity_bytes(proc / str(ownership.pid) / "stat", 4096)
        except FileNotFoundError:
            return "settled"
        fields = _parse_proc_stat_fields(raw)
        if fields is None:
            return "unknown"
        ticks = int(fields[19])
        if ticks < 0 or fields[0] not in _LIVE_PROCESS_STATES + _EXITED_PROCESS_STATES:
            return "unknown"
        if ticks != ownership.starttime_ticks:
            return "settled"  # this PID now belongs to a different process
        if fields[0] in _EXITED_PROCESS_STATES:
            return "settled"  # exited processes hold no reader descriptors
        return "running"
    except (OSError, ValueError, UnicodeError, ReaderOwnershipUnavailable):
        return "unknown"


def _require_pidfd_support() -> None:
    if (not hasattr(os, "pidfd_open") or not hasattr(os, "P_PIDFD")
            or not hasattr(os, "waitid") or not hasattr(os, "WNOWAIT")
            or not hasattr(signal, "pidfd_send_signal")):
        raise ReaderOwnershipUnavailable("owned readers require Linux pidfd/waitid support")


def _check_pidfd_pid(descriptor: int, pid: int) -> None:
    raw = _local_identity_bytes(Path("/proc/self/fdinfo") / str(descriptor), 4096)
    values = [line.split(b":", 1)[1].strip() for line in raw.splitlines()
              if line.startswith(b"Pid:")]
    if len(values) != 1 or int(values[0]) != pid:
        raise ReaderOwnershipUnavailable("pidfd does not bind the expected reader PID")


def _pin_reader(ownership: ReaderOwnership, *, allow_exited: bool = False) -> int:
    """Bind the exact still-owned child, never merely whoever has its PID now."""
    _require_pidfd_support()
    descriptor = None
    pinned = False
    try:
        before = _capture_ownership(ownership.pid, pool_identity=ownership.pool_identity,
                                    section=ownership.section, allow_exited=allow_exited)
        if before != ownership:
            raise ReaderOwnershipUnavailable("reader identity changed before pidfd open")
        descriptor = os.pidfd_open(ownership.pid, 0)
        _check_pidfd_pid(descriptor, ownership.pid)
        after = _capture_ownership(ownership.pid, pool_identity=ownership.pool_identity,
                                   section=ownership.section, allow_exited=allow_exited)
        if after != ownership:
            raise ReaderOwnershipUnavailable("reader identity changed around pidfd open")
        result = os.waitid(os.P_PIDFD, descriptor, os.WEXITED | os.WNOHANG | os.WNOWAIT)
        if result is not None and (result.si_pid != ownership.pid or not allow_exited):
            raise ReaderOwnershipUnavailable("pidfd does not prove a live owned reader")
        pinned = True
        return descriptor
    except (OSError, ValueError) as exc:
        raise ReaderOwnershipUnavailable("reader pidfd/parent relationship is unavailable") from exc
    finally:
        if descriptor is not None and not pinned:
            os.close(descriptor)


def _reap_pidfd_within(descriptor: int, pid: int, grace_s: float) -> tuple[bool, int | None]:
    deadline = time.monotonic() + grace_s
    while True:
        try:
            _check_pidfd_pid(descriptor, pid)
            observed = os.waitid(os.P_PIDFD, descriptor, os.WEXITED | os.WNOHANG | os.WNOWAIT)
            if observed is not None:
                if observed.si_pid != pid:
                    return False, None
                reaped = os.waitid(os.P_PIDFD, descriptor, os.WEXITED | os.WNOHANG)
                if reaped is None or reaped.si_pid != pid:
                    return False, None
                status = (reaped.si_status << 8 if reaped.si_code == os.CLD_EXITED
                          else reaped.si_status | (128 if reaped.si_code == os.CLD_DUMPED else 0))
                return True, status
        except (OSError, ValueError, ReaderOwnershipUnavailable):
            # ECHILD is NOT proof of death for persistent ownership, including
            # another reaper winning. The exact identity remains reconcilable.
            return False, None
        if time.monotonic() >= deadline:
            return False, None
        time.sleep(0.01)


def _retain_owned_reader(pid: int, section: str, started: float, abandoned: list,
                         ownership: ReaderOwnership | None, pool_identity: str | None) -> None:
    child = {"section": section, "pid": pid,
             "starttime_ticks": ownership.starttime_ticks if ownership is not None else None,
             "since_unix": round(time.time() - (time.monotonic() - started), 3),
             "pool_identity": pool_identity, "ownership_unsettled": True}
    if ownership is not None:
        child["ownership"] = ownership.to_record()
    abandoned.append(child)
    if _ANNOUNCE_RETAINED.get() or sys.exc_info()[1] is not None:
        print(f"pbstatus: retained reader {json.dumps(child, sort_keys=True)}", file=sys.stderr)


def _stop_owned_reader(pid: int, descriptor: int | None, ownership: ReaderOwnership | None,
                       section: str, started: float, abandoned: list,
                       pool_identity: str | None) -> None:
    recovered = None
    try:
        if descriptor is None and ownership is not None:
            # Closing the IPC descriptors first can recover an EMFILE slot.
            # This is another exact pidfd/parent proof, never a PID-only reap.
            with suppress(ReaderOwnershipUnavailable):
                recovered = _pin_reader(ownership, allow_exited=True)
            descriptor = recovered
        if descriptor is not None:
            try:
                _check_pidfd_pid(descriptor, pid)
                observed = os.waitid(os.P_PIDFD, descriptor, os.WEXITED | os.WNOHANG | os.WNOWAIT)
                if observed is not None and observed.si_pid != pid:
                    raise ReaderOwnershipUnavailable("pidfd no longer proves this reader")
                if observed is None:
                    signal.pidfd_send_signal(descriptor, signal.SIGKILL)
                settled, _ = _reap_pidfd_within(descriptor, pid, KILL_GRACE_S)
                if settled:
                    return
            except (OSError, ValueError, ReaderOwnershipUnavailable):
                # Retain uncertainty; never fall back to PID-only operations.
                _retain_owned_reader(pid, section, started, abandoned, ownership, pool_identity)
                return
        _retain_owned_reader(pid, section, started, abandoned, ownership, pool_identity)
    finally:
        if recovered is not None:
            os.close(recovered)


def _reap_status_within(pid: int, grace_s: float) -> tuple[bool, int | None]:
    """Bounded reap, preserving the kernel status for failure diagnostics.

    ``None`` means unknown, not exit 0: another reaper may already have taken
    the status, or the reader may still be alive. Never join a child in ``D``.
    """
    deadline = time.monotonic() + grace_s
    while True:
        try:
            done, status = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            return True, None
        except OSError:
            return False, None
        if done == pid:
            return True, status
        if time.monotonic() >= deadline:
            return False, None
        time.sleep(0.01)


def _reap_within(pid: int, grace_s: float) -> bool:
    """Compatibility wrapper for cleanup callers that only need ownership."""
    return _reap_status_within(pid, grace_s)[0]


def _reader_exit_detail(pid: int, status: int | None) -> str:
    if status is None:
        return f"reader pid={pid} exit status unavailable"
    if os.WIFSIGNALED(status):
        signum = os.WTERMSIG(status)
        try:
            name = signal.Signals(signum).name
        except ValueError:                          # pragma: no cover - kernel
            name = "unknown"
        return f"reader pid={pid} signal {signum} ({name})"
    if os.WIFEXITED(status):
        return f"reader pid={pid} exit code {os.WEXITSTATUS(status)}"
    return f"reader pid={pid} unexpected wait status {status}"


def _write_reader_payload(write_fd: int, payload: bytes) -> None:
    """Complete a pipe write even after a short write or interrupted syscall."""
    pending = memoryview(payload)
    while pending:
        try:
            written = os.write(write_fd, pending)
        except InterruptedError:
            continue
        if written <= 0:
            raise OSError("reader pipe write made no progress")
        pending = pending[written:]


def _isolate_child_fds(write_fd: int, *, keep_fds: tuple[int, ...] = ()) -> int:
    """Leave the forked reader owning its own pipe and nothing else.

    A child this process may have to abandon inherits every descriptor the
    caller held, and a descriptor is not a private copy: the open file
    description behind it is shared, so an abandoned reader keeps the caller's
    resources alive after the caller has let go of them. Two consequences were
    reproduced by the review of #358, and they are one defect seen twice.

    A wrapper that captures ``pbstatus``'s output waits for EOF on the pipe, and
    EOF arrives when the last writer closes. The parent exiting ``3`` is not
    that: the abandoned child still holds the write end, so the wrapper blocks
    on a command that has already returned. And an ``flock`` lives on the open
    file description rather than on the process, so an inherited lock fd left
    open in the child holds the caller's lock after the caller closed it --
    against a lock the child was never told about and cannot release.

    So the child keeps exactly two things: the IPC writer it must answer on,
    and ``/dev/null`` on the three standard streams so that a write from
    anything it calls has somewhere to go. Everything else is closed here,
    before the section runs. ``/proc/self/fd`` is the list the kernel already
    keeps; the ``SC_OPEN_MAX`` sweep is for a box without ``/proc`` mounted,
    where a status screen still has to answer.

    Returns the descriptor the caller must write on, which is ``write_fd``
    moved out of the way first when the pipe landed on 0, 1 or 2. It is moved
    in a loop, not once: ``os.dup`` hands back the lowest free descriptor, and
    when the caller closed its own standard streams the lowest free descriptor
    is another one below 3 -- the read end this child has just closed. Each
    turn of the loop spends one of the three low slots, so it ends after at
    most three, and the copies left behind are closed by the ``dup2`` below.
    """
    while write_fd < 3:
        write_fd = os.dup(write_fd)
    null = os.open(os.devnull, os.O_RDWR)
    for target in (0, 1, 2):                       # write_fd is above these now
        with suppress(OSError):
            os.dup2(null, target)
    if null > 2:
        with suppress(OSError):
            os.close(null)
    try:
        # Materialised before any closing: the listing's own directory
        # descriptor is gone by the time this list is walked, and closing a
        # closed descriptor is the OSError swallowed below.
        open_fds = [int(name) for name in os.listdir("/proc/self/fd")
                    if name.isdigit()]
    except OSError:                                # pragma: no cover - no /proc
        try:
            limit = int(os.sysconf("SC_OPEN_MAX"))
        except (ValueError, OSError):
            limit = 4096
        open_fds = list(range(3, min(limit, 65536)))
    for fd in open_fds:
        if fd <= 2 or fd == write_fd or fd in keep_fds:
            continue
        with suppress(OSError):
            os.close(fd)
    return write_fd


def bounded(section: str, read, *, deadline: Deadline, abandoned: list,
            cap_s: float | None = None, announce_retained: bool = True,
            on_spawn: Callable[[ReaderOwnership], None] | None = None,
            pool_identity: str | None = None) -> dict:
    """Run one read of the shared mount in a child this process can abandon.

    A stat on a hard NFS mount need not return at the caller's deadline, even
    after a signal; no in-process timeout can bound it. The parent stops
    reading at the deadline and never joins a child that may be in the kernel.

    The whole-run deadline means an expiry in one section leaves nothing for
    the next. Abandoned children are recorded by PID and starttime in the
    caller-owned ``abandoned`` list, not forgotten or implicitly retried.

    Returns the existing ``ok``/``error``/``timed_out`` status dictionaries.
    ``announce_retained=False`` leaves the notice to the caller, except while
    cancellation unwinds: the caller cannot report ownership then. The CLI
    notice prefix remains pbstatus for compatibility with existing consumers.

    Optional on_spawn runs ONLY in the parent, with a proved process/boot and
    caller-supplied pool/read identity. It must return only after that identity
    is durably fenced in parent-owned local storage. The child waits on a
    separate read-release pipe after FD isolation; failed callback, missing
    identity, expired budget or a dead parent cannot authorize its read.
    The callback does not extend the deadline. It must use a supported bounded
    LOCAL persistence operation: these checks cannot preempt an arbitrary
    callback or an uninterruptible parent filesystem call. Do not put an NFS
    or CAS operation here and advertise it as bounded. Persistence exceptions
    unwind exact-child cleanup; the caller retains any partially written fence.
    """
    if on_spawn is not None:
        if (not deadline.bounded or not isinstance(pool_identity, str)
                or not pool_identity or pool_identity != pool_identity.strip()
                or not section or section != section.strip()):
            raise ValueError("owned readers require a finite budget and exact pool/read scope")
        _require_pidfd_support()
    elif pool_identity is not None:
        raise ValueError("pool identity requires a parent ownership callback")
    if not deadline.bounded:
        # --timeout-s 0: the pre-#350 path, in-process and unchanged.
        try:
            return {"status": "ok", "value": read()}
        except Exception as exc:                   # diagnostic boundary
            return {"status": "error", "type": type(exc).__name__, "error": str(exc)}

    remaining = deadline.remaining() or 0.0
    if cap_s is not None:
        remaining = min(remaining, cap_s)
    if remaining <= 0:
        return {"status": "timed_out", "elapsed_s": 0.0, "started": False}

    # A signal aimed only at the parent still unwinds through exact-child
    # cleanup, using the worker's established signal contract.
    with _sigterm_unwinds_this_process():
        token = _ANNOUNCE_RETAINED.set(announce_retained)
        try:
            if on_spawn is None:
                return _bounded_reader(section, read, deadline=deadline,
                                       abandoned=abandoned, cap_s=cap_s)
            return _bounded_reader(section, read, deadline=deadline,
                                   abandoned=abandoned, cap_s=cap_s,
                                   on_spawn=on_spawn, pool_identity=pool_identity)
        finally:
            _ANNOUNCE_RETAINED.reset(token)


_ANNOUNCE_RETAINED: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "pbstatus_announce_retained", default=True)


def _stop_reader(pid: int, section: str, started: float, abandoned: list) -> None:
    with suppress(OSError):
        os.kill(pid, signal.SIGKILL)
    if not _reap_within(pid, KILL_GRACE_S):
        child = {"section": section, "pid": pid,
                 "starttime_ticks": _starttime_ticks(pid),
                 "since_unix": round(time.time() - (time.monotonic() - started), 3)}
        abandoned.append(child)
        if _ANNOUNCE_RETAINED.get() or sys.exc_info()[1] is not None:
            print(f"pbstatus: retained reader {json.dumps(child, sort_keys=True)}",
                  file=sys.stderr)


def _bounded_reader(section: str, read, *, deadline: Deadline, abandoned: list,
                    cap_s: float | None,
                    on_spawn: Callable[[ReaderOwnership], None] | None = None,
                    pool_identity: str | None = None) -> dict:
    read_fd, write_fd = os.pipe()
    release_read = release_write = None
    started = time.monotonic()
    try:
        if on_spawn is not None:
            release_read, release_write = os.pipe()
        pid = os.fork()
    except BaseException:
        os.close(read_fd)
        os.close(write_fd)
        for descriptor in (release_read, release_write):
            if descriptor is not None:
                os.close(descriptor)
        raise
    if pid == 0:                                   # child
        code = 1
        stage = "close_read_fd"
        try:
            os.close(read_fd)
            if release_write is not None:
                os.close(release_write)
            # Isolate BEFORE the section runs or any shared read can block.
            stage = "isolate_fds"
            if release_read is None:
                write_fd = _isolate_child_fds(write_fd)
            else:
                # Relocate a low release descriptor before nulling stdio,
                # just as the existing answer writer is relocated there.
                while release_read < 3:
                    release_read = os.dup(release_read)
                write_fd = _isolate_child_fds(write_fd, keep_fds=(release_read,))
                stage = "await_release"
                while True:
                    try:
                        grant = os.read(release_read, 1)
                        break
                    except InterruptedError:
                        continue
                if grant != b"R":
                    raise ReaderOwnershipUnavailable("parent did not release reader ownership")
                os.close(release_read)
                release_read = None
            stage = "read"
            value = read()
            stage = "serialize"
            payload = json.dumps({"status": "ok", "value": value}).encode("utf-8")
            stage = "write"
            _write_reader_payload(write_fd, payload)
            code = 0
        except BaseException as exc:               # child boundary
            # A broken IPC channel cannot carry its own diagnosis; keep the
            # nonzero exit for the parent's exact-child diagnostic instead.
            with suppress(BaseException):
                try:
                    detail = str(exc)[:4096]
                except BaseException:
                    detail = "exception message unavailable"
                payload = json.dumps({"status": "error",
                                      "type": type(exc).__name__,
                                      "error": f"stage={stage}: {detail}"})
                _write_reader_payload(write_fd, payload.encode("utf-8"))
        finally:
            with suppress(OSError):
                os.close(write_fd)
            if release_read is not None:
                with suppress(OSError):
                    os.close(release_read)
            # Never flush inherited buffers or run the parent's finalisers.
            os._exit(code)

    reaped = False
    ownership = None
    pidfd = None
    try:
        os.close(write_fd)
        if release_read is not None:
            os.close(release_read)
            release_read = None
        if on_spawn is not None:
            left = deadline.remaining() or 0.0
            if cap_s is not None:
                left = min(left, cap_s - (time.monotonic() - started))
            if left <= 0:
                return {"status": "timed_out", "elapsed_s": round(time.monotonic() - started, 3),
                        "started": True}
            if pool_identity is None:
                raise ReaderOwnershipUnavailable("owned reader lacks its pool identity")
            ownership = _capture_ownership(pid, pool_identity=pool_identity, section=section)
            pidfd = _pin_reader(ownership)
            left = deadline.remaining() or 0.0
            if cap_s is not None:
                left = min(left, cap_s - (time.monotonic() - started))
            if left <= 0:
                return {"status": "timed_out", "elapsed_s": round(time.monotonic() - started, 3),
                        "started": True}
            on_spawn(ownership)
            left = deadline.remaining() or 0.0
            if cap_s is not None:
                left = min(left, cap_s - (time.monotonic() - started))
            if left <= 0:
                return {"status": "timed_out", "elapsed_s": round(time.monotonic() - started, 3),
                        "started": True}
            if reader_liveness(ownership, pool_identity=pool_identity, section=section) != "running":
                raise ReaderOwnershipUnavailable("reader identity changed before read release")
            _check_pidfd_pid(pidfd, pid)
            relation = os.waitid(os.P_PIDFD, pidfd, os.WEXITED | os.WNOHANG | os.WNOWAIT)
            if relation is not None:
                raise ReaderOwnershipUnavailable("reader exited before read release")
            if release_write is None:
                raise ReaderOwnershipUnavailable("owned reader lacks its release channel")
            _write_reader_payload(release_write, b"R")
            os.close(release_write)
            release_write = None
        chunks: list[bytes] = []
        saw_eof = False
        while True:
            left = deadline.remaining() or 0.0
            if cap_s is not None:
                left = min(left, cap_s - (time.monotonic() - started))
            if left <= 0:
                break
            try:
                ready, _, _ = select.select([read_fd], [], [], left)
            except OSError:
                break
            if not ready:
                break
            try:
                chunk = os.read(read_fd, 65536)
            except OSError:
                break
            if not chunk:
                saw_eof = True
                break
            chunks.append(chunk)
        elapsed = time.monotonic() - started

        # Only EOF proves the whole payload; a partial reply is a timeout,
        # not an error parsing an incomplete census.
        if saw_eof:
            if chunks:
                try:
                    result = json.loads(b"".join(chunks).decode("utf-8"))
                except ValueError as exc:
                    result = {"status": "error", "type": "ValueError",
                              "error": f"unreadable {section} payload: {exc}"}
            else:
                result = {"status": "error", "type": "RuntimeError",
                          "error": f"the {section} reader exited without a payload"}
            if isinstance(result, dict) and result.get("status") == "error":
                reaped, status = (_reap_status_within(pid, KILL_GRACE_S) if on_spawn is None
                                  else _reap_pidfd_within(pidfd, pid, KILL_GRACE_S)
                                  if pidfd is not None else (False, None))
                result["error"] = (f"{result.get('error', '')}; "
                                   f"{_reader_exit_detail(pid, status)}")
            else:
                # Complete snapshots remain valid even if the writer has not
                # exited yet (#906); cleanup is still exact-child-owned.
                reaped = (_reap_within(pid, KILL_GRACE_S) if on_spawn is None
                          else _reap_pidfd_within(pidfd, pid, KILL_GRACE_S)[0]
                          if pidfd is not None else False)
            return result

        return {"status": "timed_out", "elapsed_s": round(elapsed, 3), "started": True}
    finally:
        try:
            os.close(read_fd)
        finally:
            # Closing the grant pipe wakes a not-yet-authorized child on
            # cancellation/callback failure, including the EOF/dead-parent case.
            for descriptor in (release_read, release_write):
                if descriptor is not None:
                    with suppress(OSError):
                        os.close(descriptor)
            try:
                if not reaped:
                    if on_spawn is None:
                        _stop_reader(pid, section, started, abandoned)
                    else:
                        _stop_owned_reader(pid, pidfd, ownership, section, started,
                                           abandoned, pool_identity)
            finally:
                if pidfd is not None:
                    os.close(pidfd)


def _starttime_ticks(pid: int) -> int | None:
    """Kernel starttime distinguishes a retained child from a recycled PID."""
    fields = _proc_stat_fields(pid, Path("/proc"))
    if fields is None:
        return None
    try:
        return int(fields[19])
    except ValueError:
        return None
