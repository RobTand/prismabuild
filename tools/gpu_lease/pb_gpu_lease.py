#!/usr/bin/env python3
"""Single-file, stdlib-only Linux client for prismabuild.gpu_lease.v1.

CLI (Python >= 3.8)::

    python3 pb_gpu_lease.py run [--holder H] [--mem-gb N] [--cpus N] \
        [--optional] [--max-wait-s S] -- CMD [ARGS...]

Python API::

    with lease(holder=None, mem_gb=16, cpus=4, directory=None,
               optional=False, max_wait_s=None) as grant:
        # grant.lease_id, grant.waited_s, grant.pb_state
        run_gpu_test()

The directory defaults to /run/prismabuild-gpu-lease; PB_GPU_LEASE_DIR
or the API's directory argument overrides it. A missing directory raises
FileNotFoundError (CLI exit 3), unless optional=True, which yields a grant
with lease_id and pb_state None. WaitExceeded indicates a max-wait timeout
(CLI exit 75). No max wait is imposed by default. Resources must be
non-negative integers. The holder defaults to TEST_TARGET or hostname:pid
and is truncated to 200 characters.

A lock fd lives until release; records are atomically replaced and never
removed by this client. A daemon renews while waiting and while held.
Main-thread API contexts release on SIGTERM/SIGINT/SIGHUP and raise
SystemExit(128 + signal). Contexts used in other threads still release on
normal/exceptional exit, but signal policy belongs to the calling main thread.
The CLI forwards those signals to its child and returns its exit status.
The child uses Linux PR_SET_PDEATHSIG=SIGKILL so killing the CLI cannot leave
it running. This protects the direct child, not arbitrary detached descendants.
"""

import argparse
from collections import namedtuple
from contextlib import contextmanager
import ctypes
import fcntl
import json
import math
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time


SCHEMA = "prismabuild.gpu_lease.v1"
STATE_SCHEMA = "prismabuild.gpu_lease_state.v1"
DEFAULT_DIRECTORY = "/run/prismabuild-gpu-lease"
TTL_S = 30
RENEW_S = 5
MAX_LIFETIME_S = 3900
POLL_S = 0.5
WAIT_MESSAGE_S = 30
_SIGNALS = (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)
Grant = namedtuple("Grant", "lease_id waited_s pb_state")

# Separate clock and wait functions make protocol timing testable without
# changing global time/threading behavior or adding deployment environment knobs.
_time = time.time
_monotonic = time.monotonic
_sleep = time.sleep


def _renew_wait(stop, seconds):
    return stop.wait(seconds)


def _message(text):
    print("gpu-lease: " + text, file=sys.stderr, flush=True)


class WaitExceeded(Exception):
    """The lease did not become ready within max_wait_s seconds."""


class _SignalExit(SystemExit):
    def __init__(self, signum):
        super().__init__(128 + signum)


def _atomic_write(path, value):
    """Publish a complete JSON snapshot by a same-directory atomic replace."""
    fd, temporary = tempfile.mkstemp(prefix="." + path.name + ".", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            # Action containers run as root; the host reader need not. Set an
            # explicit read mode independent of either process's umask.
            os.fchmod(stream.fileno(), 0o644)
            json.dump(value, stream, sort_keys=True, allow_nan=False)
            stream.write("\n")
        os.replace(temporary, path)
    finally:
        # Only our unpublished temporary can be removed, never a lease record.
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _pb_state(directory, lease_id):
    """Return (grant reason or None, PB token count). Invalid state fails closed."""
    try:
        with (directory / "pb-state.json").open(encoding="utf-8") as stream:
            state = json.load(stream)
    except FileNotFoundError:
        return "absent", 0
    except (OSError, ValueError) as exc:
        _message("cannot read pb-state.json: {}".format(exc))
        return None, 0
    if not isinstance(state, dict) or state.get("schema") != STATE_SCHEMA:
        return None, 0
    updated = state.get("updated_unix")
    if (isinstance(updated, bool) or not isinstance(updated, (int, float))
            or not math.isfinite(updated)):
        return None, 0
    tokens = state.get("pb_gpu_tokens_held", 0)
    if isinstance(tokens, bool) or not isinstance(tokens, int) or tokens < 0:
        tokens = 0
    if _time() - updated > TTL_S:
        return "stale", tokens
    ready = state.get("ready")
    if isinstance(ready, list) and lease_id in ready:
        return "ready", tokens
    return None, tokens


class _Lease:
    """Own the fd, serialized record updates, and renewal thread as one lifetime."""

    def __init__(self, directory, holder, mem_gb, cpus):
        self.directory = directory
        self.lease_id = secrets.token_hex(16)
        self.path = directory / (self.lease_id + ".json")
        self.fd = None
        self.thread = None
        self.stop = threading.Event()
        self.mutex = threading.RLock()
        self.closed = False
        now = _time()
        self.value = {
            "schema": SCHEMA,
            "lease_id": self.lease_id,
            "host": socket.gethostname(),
            "holder": holder[:200],
            "mem_gb": mem_gb,
            "cpus": cpus,
            "requested_unix": now,
            "granted_unix": None,
            "renewed_unix": now,
            "pb_state_at_grant": None,
            "released_unix": None,
            "release_reason": None,
        }

    def acquire(self):
        self.fd = os.open(self.directory / (self.lease_id + ".lock"),
                          os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
        os.set_inheritable(self.fd, False)
        os.fchmod(self.fd, 0o644)
        fcntl.flock(self.fd, fcntl.LOCK_EX)
        _atomic_write(self.path, self.value)
        self.thread = threading.Thread(target=self._renew, name="gpu-lease-renew", daemon=True)
        self.thread.start()

    def _renew(self):
        while not _renew_wait(self.stop, RENEW_S):
            with self.mutex:
                if self.closed:
                    return
                self.value["renewed_unix"] = _time()
                try:
                    _atomic_write(self.path, self.value)
                except OSError as exc:
                    _message("renewal write failed: {}".format(exc))

    def wait(self, max_wait_s):
        start = _monotonic()
        next_message = start + WAIT_MESSAGE_S
        first_probe = True
        while True:
            reason, tokens = _pb_state(self.directory, self.lease_id)
            now = _monotonic()
            waited = now - start
            # Expiry wins over a late grant; max_wait_s=0 can still grant on
            # the initial probe if it is immediately ready/absent/stale.
            immediate = first_probe and max_wait_s == 0 and reason is not None
            if max_wait_s is not None and waited >= max_wait_s and not immediate:
                _message("wait exceeded {:g}s (pb_gpu_tokens_held={})".format(max_wait_s, tokens))
                raise WaitExceeded("GPU lease wait exceeded {:g}s".format(max_wait_s))
            if reason is not None:
                with self.mutex:
                    self.value["granted_unix"] = _time()
                    self.value["pb_state_at_grant"] = reason
                    _atomic_write(self.path, self.value)
                _message("granted after {:.1f}s (pb_state={})".format(waited, reason))
                return Grant(self.lease_id, waited, reason)
            if now >= next_message:
                _message("waiting on {}: {} PrismaBuild GPU token(s) held".format(
                    self.value["host"], tokens))
                next_message = now + WAIT_MESSAGE_S
            delay = POLL_S
            if max_wait_s is not None:
                delay = min(delay, max_wait_s - waited)
            first_probe = False
            _sleep(delay)

    def release(self, reason):
        self.stop.set()
        try:
            with self.mutex:
                if self.closed:
                    return
                self.closed = True
                if self.fd is not None:
                    self.value["released_unix"] = _time()
                    self.value["release_reason"] = reason
                    try:
                        _atomic_write(self.path, self.value)
                    except OSError as exc:
                        # Even a full filesystem must not retain our flock.
                        _message("release write failed: {}".format(exc))
        finally:
            if self.fd is not None:
                os.close(self.fd)
                self.fd = None
            if self.thread is not None and self.thread.ident is not None:
                self.thread.join()


@contextmanager
def _signal_handlers(handler, enabled=True):
    previous = {}
    try:
        if enabled and threading.current_thread() is threading.main_thread():
            for sig in _SIGNALS:
                previous[sig] = signal.signal(sig, handler)
        yield
    finally:
        for sig, old_handler in previous.items():
            signal.signal(sig, old_handler)


def _api_signal(signum, frame):
    raise _SignalExit(signum)


def _validate(mem_gb, cpus, max_wait_s):
    for name, value in (("mem_gb", mem_gb), ("cpus", cpus)):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(name + " must be a non-negative integer")
    if max_wait_s is not None:
        if (isinstance(max_wait_s, bool) or not isinstance(max_wait_s, (int, float))
                or not math.isfinite(max_wait_s) or max_wait_s < 0):
            raise ValueError("max_wait_s must be a finite non-negative number")


@contextmanager
def _lease(holder, mem_gb, cpus, directory, optional, max_wait_s, manage_signals):
    _validate(mem_gb, cpus, max_wait_s)
    directory = Path(directory if directory is not None else os.environ.get(
        "PB_GPU_LEASE_DIR", DEFAULT_DIRECTORY))
    with _signal_handlers(_api_signal, enabled=manage_signals):
        if not directory.is_dir():
            message = "{} missing; host not provisioned for gate GPU leases".format(directory)
            _message(message)
            if not optional:
                raise FileNotFoundError(message)
            _message("optional: running without a lease")
            yield Grant(None, 0.0, None)
            return
        if holder is None:
            holder = os.environ.get("TEST_TARGET") or "{}:{}".format(socket.gethostname(), os.getpid())
        if not isinstance(holder, str):
            raise ValueError("holder must be a string")
        active = _Lease(directory, holder, mem_gb, cpus)
        reason = "released"
        try:
            active.acquire()
            try:
                grant = active.wait(max_wait_s)
            except WaitExceeded:
                reason = "wait_exceeded"
                raise
            yield grant
        finally:
            active.release(reason)


def lease(holder=None, *, mem_gb=16, cpus=4, directory=None, optional=False, max_wait_s=None):
    """Acquire, wait for a grant, yield it, and release even if the body raises."""
    return _lease(holder, mem_gb, cpus, directory, optional, max_wait_s, True)


class _Child:
    """Signal forwarding is installed before acquisition and before Popen."""

    def __init__(self):
        self.process = None
        self.spawning = False
        self.pending = []

    def signal(self, signum, frame):
        if self.process is not None:
            self._forward(signum)
        elif self.spawning:
            # Popen can be interrupted after fork but before returning the PID.
            # Do not unwind that window and lose track of a running GPU child.
            self.pending.append(signum)
        else:
            raise _SignalExit(signum)

    def _forward(self, signum):
        try:
            self.process.send_signal(signum)
        except ProcessLookupError:
            pass

    def run(self, command):
        parent_pid = os.getpid()
        libc = ctypes.CDLL(None, use_errno=True)
        prctl = libc.prctl

        def child_setup():
            if prctl(1, signal.SIGKILL, 0, 0, 0) != 0:  # PR_SET_PDEATHSIG
                err = ctypes.get_errno()
                raise OSError(err, os.strerror(err))
            # Cover death between fork and prctl: Linux does not deliver an
            # already-past parent death retroactively.
            if os.getppid() != parent_pid:
                os.kill(os.getpid(), signal.SIGKILL)

        self.spawning = True
        try:
            self.process = subprocess.Popen(command, preexec_fn=child_setup, close_fds=True)
        finally:
            self.spawning = False
        try:
            for sig in self.pending:
                self._forward(sig)
            status = self.process.wait()
            return status if status >= 0 else 128 - status
        finally:
            # Unexpected Python exceptions must not leave a GPU user behind
            # after the context manager drops admission.
            if self.process.poll() is None:
                self.process.kill()
                self.process.wait()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="action", required=True)
    run = commands.add_parser("run", help="lease the GPU for a child command")
    run.add_argument("--holder")
    run.add_argument("--mem-gb", type=int, default=16)
    run.add_argument("--cpus", type=int, default=4)
    run.add_argument("--optional", action="store_true")
    run.add_argument("--max-wait-s", type=float)
    run.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.command
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        parser.error("run requires -- CMD [ARGS...]")
    try:
        _validate(args.mem_gb, args.cpus, args.max_wait_s)
    except ValueError as exc:
        parser.error(str(exc))
    child = _Child()
    try:
        with _signal_handlers(child.signal):
            with _lease(args.holder, args.mem_gb, args.cpus, None, args.optional,
                        args.max_wait_s, False):
                return child.run(command)
    except WaitExceeded:
        return 75
    except FileNotFoundError as exc:
        # Missing provisioning was already reported by _lease. A missing
        # command has a filename and is a different (spawn) failure.
        if exc.filename is None:
            return 3
        _message("cannot run command: {}".format(exc))
        return 127
    except OSError as exc:
        _message(str(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())
