#!/usr/bin/env python3
"""Gate GPU lease client (protocol ``prismabuild.gpu_lease.v1``).

One stdlib-only file, Python >= 3.8, Linux. Bazel test actions vendor it as a
data input and run it as root inside a BuildBuddy executor's action container,
where the PrismaBuild SDK is not installed. It takes a lease on the host's GPU,
waits until PrismaBuild lets it start, keeps the lease alive while the GPU
work runs, and releases it on the way out.

The lease directory is ``/run/prismabuild-gpu-lease`` (override it with the
``PB_GPU_LEASE_DIR`` environment variable or the ``directory`` argument). If
the directory is absent the host is not provisioned for gate GPU leases.

Protocol, as this client speaks it
----------------------------------
1. Pick ``lease_id = secrets.token_hex(16)``.
2. Create ``<lease_id>.lock`` with ``O_CREAT|O_EXCL``, take a blocking
   ``flock(LOCK_EX)`` and keep that fd open for the life of the lease. The
   kernel drops the lock when the process dies, which is how PrismaBuild sees
   a crashed holder.
3. Only then write ``<lease_id>.json`` (always atomically: temp file in the
   same directory, then rename) with ``granted_unix`` null.
4. Poll ``pb-state.json`` every 0.5 s. The lease is granted when the state is
   at most 30 s old and lists the lease in ``ready`` (``pb_state=ready``),
   when the state file is absent (``absent``), or when it is older than 30 s
   (``stale``). Being listed in ``fencing`` only keeps the lease waiting.
   The JSON is rewritten with ``granted_unix`` and ``pb_state_at_grant``.
5. A daemon thread rewrites ``renewed_unix`` every ``RENEW_S`` (5) seconds,
   while waiting and while held. A failed write is reported on stderr and
   retried at the next interval.
6. A clean release writes ``released_unix`` and ``release_reason``
   (``released``, or ``wait_exceeded``), then closes the lock fd. Neither file
   is deleted: PrismaBuild records the lease and removes the files.

Command line
------------
::

    python3 pb_gpu_lease.py run [--holder H] [--mem-gb N] [--cpus N]
                                [--optional] [--max-wait-s S] -- CMD [ARGS...]

``run`` acquires the lease, runs CMD as a child, and exits with the child's
status (128+signal if the child was killed by a signal). SIGTERM, SIGINT and
SIGHUP are forwarded to the child and the lease is released afterwards (a
signal while still waiting releases the lease and exits 128+signal). The child
asks the kernel for ``PR_SET_PDEATHSIG=SIGKILL``, so a SIGKILLed client cannot
leave GPU work running without a lease.

* ``--holder`` names the holder (at most 200 characters). Default:
  ``$TEST_TARGET``, else ``<hostname>:<pid>``.
* ``--mem-gb`` (default 16) and ``--cpus`` (default 4) are the resources the
  lease claims; they are non-negative integers.
* ``--max-wait-s S`` (no default) gives up after S seconds of waiting: the
  lease is released with ``release_reason`` ``wait_exceeded``, the client
  prints ``gpu-lease: wait exceeded <S>s (pb_gpu_tokens_held=<n>)`` and exits
  75 (EX_TEMPFAIL).
* If the lease directory is missing the client prints
  ``gpu-lease: <dir> missing; host not provisioned for gate GPU leases`` and
  exits 3. With ``--optional`` it says so and runs CMD without a lease.

Python API
----------
::

    import pb_gpu_lease

    with pb_gpu_lease.lease(holder=None, *, mem_gb=16, cpus=4, directory=None,
                            optional=False, max_wait_s=None) as grant:
        grant.lease_id, grant.waited_s, grant.pb_state

``lease`` blocks until the lease is granted and releases it when the block
exits, by normal exit or by an exception. It raises ``WaitExceeded`` when
``max_wait_s`` passes first and ``LeaseDirMissing`` when the directory is
absent. With ``optional=True`` and no directory it yields a grant whose
``lease_id`` and ``pb_state`` are None. The API installs no signal handlers: a
process that is killed without running ``finally`` blocks still frees the lock
(the kernel closes the fd), it just records no ``released_unix``.
"""
import argparse
import collections
import contextlib
import ctypes
import fcntl
import json
import os
import secrets
import signal
import socket
import subprocess
import sys
import threading
import time

SCHEMA = "prismabuild.gpu_lease.v1"
DEFAULT_DIR = "/run/prismabuild-gpu-lease"
DIR_ENV = "PB_GPU_LEASE_DIR"
STATE_NAME = "pb-state.json"
RENEW_S = 5
POLL_S = 0.5
STATE_TTL_S = 30
WAIT_MSG_S = 30
MAX_HOLDER_CHARS = 200
EX_NOT_PROVISIONED = 3
EX_TEMPFAIL = 75
PR_SET_PDEATHSIG = 1
FORWARDED_SIGNALS = (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)

# Clock hooks: tests replace these to drive waits without real time passing.
_now = time.time
_mono = time.monotonic
_sleep = time.sleep

Grant = collections.namedtuple("Grant", "lease_id waited_s pb_state")


class LeaseDirMissing(RuntimeError):
    """The lease directory does not exist: the host is not provisioned."""

    def __init__(self, directory):
        super().__init__(
            "%s missing; host not provisioned for gate GPU leases" % directory
        )
        self.directory = directory


class WaitExceeded(Exception):
    """``max_wait_s`` passed before PrismaBuild let the lease start."""

    def __init__(self, max_wait_s, tokens_held):
        super().__init__(
            "wait exceeded %gs (pb_gpu_tokens_held=%d)" % (max_wait_s, tokens_held)
        )
        self.max_wait_s = max_wait_s
        self.tokens_held = tokens_held


class _Terminated(BaseException):
    """A termination signal arrived before the child started."""

    def __init__(self, signum):
        super().__init__(signum)
        self.signum = signum


def _log(message):
    print("gpu-lease: " + message, file=sys.stderr, flush=True)


def lease_dir(directory=None):
    return directory or os.environ.get(DIR_ENV) or DEFAULT_DIR


def _nonneg_int(value):
    number = int(value) if isinstance(value, (int, str)) else -1
    if number < 0:
        raise ValueError("expected a non-negative integer, got %r" % (value,))
    return number


def _write_atomic(path, doc):
    tmp = path + ".tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            os.fchmod(handle.fileno(), 0o644)
            json.dump(doc, handle, sort_keys=True)
            handle.write("\n")
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


class _Lease:
    """One lease: the held lock fd, its JSON document and the renew thread."""

    def __init__(self, directory, holder, mem_gb, cpus):
        self.host = socket.gethostname()
        holder = holder or os.environ.get("TEST_TARGET") or "%s:%d" % (self.host, os.getpid())
        self.lease_id = secrets.token_hex(16)
        self._state_path = os.path.join(directory, STATE_NAME)
        self._lock_path = os.path.join(directory, self.lease_id + ".lock")
        self._json_path = os.path.join(directory, self.lease_id + ".json")
        requested = _now()
        self._doc = {
            "schema": SCHEMA,
            "lease_id": self.lease_id,
            "host": self.host,
            "holder": holder[:MAX_HOLDER_CHARS],
            "mem_gb": _nonneg_int(mem_gb),
            "cpus": _nonneg_int(cpus),
            "requested_unix": requested,
            "granted_unix": None,
            "renewed_unix": requested,
            "pb_state_at_grant": None,
            "released_unix": None,
            "release_reason": None,
        }
        self._mutex = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self._fd = None
        self._written = False
        self._reason = "released"
        self._tokens_held = 0

    def _update(self, **fields):
        with self._mutex:
            self._doc.update(fields)
            _write_atomic(self._json_path, self._doc)
            self._written = True

    def _create(self):
        """Take the lock, then (and only then) publish the JSON."""
        self._fd = os.open(self._lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o644)
        os.fchmod(self._fd, 0o644)
        fcntl.flock(self._fd, fcntl.LOCK_EX)
        self._update()
        self._thread = threading.Thread(target=self._renew_loop, daemon=True)
        self._thread.start()

    def _renew_loop(self):
        while not self._stop.wait(RENEW_S):
            try:
                self._update(renewed_unix=_now())
            except OSError as exc:
                _log("renewing %s failed (%s); will retry" % (self.lease_id, exc))

    def _poll(self):
        """Return ``(pb_state, tokens_held)``; ``pb_state`` None means keep waiting."""
        try:
            with open(self._state_path, encoding="utf-8") as handle:
                state = json.load(handle)
            age = _now() - float(state["updated_unix"])
            ready = state.get("ready")
            held = int(state.get("pb_gpu_tokens_held") or 0)
        except FileNotFoundError:
            return "absent", self._tokens_held
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            return None, self._tokens_held  # unreadable or half-written: look again
        self._tokens_held = held
        if age > STATE_TTL_S:
            return "stale", held
        if isinstance(ready, list) and self.lease_id in ready:
            return "ready", held
        return None, held

    def acquire(self, max_wait_s):
        self._create()
        start = _mono()
        next_message = start + WAIT_MSG_S
        while True:
            pb_state, held = self._poll()
            if pb_state:
                break
            now = _mono()
            if max_wait_s is not None and now - start >= max_wait_s:
                self._reason = "wait_exceeded"
                raise WaitExceeded(max_wait_s, held)
            if now >= next_message:
                _log("waiting on %s: %d PrismaBuild GPU token(s) held" % (self.host, held))
                next_message += WAIT_MSG_S
            _sleep(POLL_S)
        waited = _mono() - start
        self._update(granted_unix=_now(), pb_state_at_grant=pb_state)
        _log("granted after %.1fs (pb_state=%s)" % (waited, pb_state))
        return Grant(self.lease_id, waited, pb_state)

    def release(self):
        fd, self._fd = self._fd, None
        if fd is None:
            return
        try:
            self._stop.set()
            if self._thread is not None:
                self._thread.join()
            if self._written:
                try:
                    self._update(released_unix=_now(), release_reason=self._reason)
                except OSError as exc:
                    _log("recording the release of %s failed (%s)" % (self.lease_id, exc))
        finally:
            os.close(fd)


@contextlib.contextmanager
def lease(holder=None, *, mem_gb=16, cpus=4, directory=None, optional=False, max_wait_s=None):
    """Hold a gate GPU lease for the duration of the ``with`` block."""
    directory = lease_dir(directory)
    if not os.path.isdir(directory):
        if not optional:
            raise LeaseDirMissing(directory)
        _log("%s missing; running without a lease (--optional)" % directory)
        yield Grant(None, 0.0, None)
        return
    held = _Lease(directory, holder, mem_gb, cpus)
    try:
        yield held.acquire(max_wait_s)
    finally:
        held.release()


def _pdeathsig_preexec():
    libc = ctypes.CDLL(None, use_errno=True)
    parent = os.getpid()

    def preexec():
        if libc.prctl(PR_SET_PDEATHSIG, int(signal.SIGKILL), 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno(), "prctl(PR_SET_PDEATHSIG) failed")
        if os.getppid() != parent:  # the client died before the prctl took effect
            os._exit(1)

    return preexec


class _Forwarder:
    """Signal handler: end the wait before the child exists, forward after."""

    def __init__(self):
        self.running = False
        self.child = None
        self.early = []

    def __call__(self, signum, frame):
        if not self.running:
            raise _Terminated(signum)
        if self.child is None:
            self.early.append(signum)
        else:
            self._send(signum)

    def _send(self, signum):
        with contextlib.suppress(OSError):
            self.child.send_signal(signum)

    def run(self, cmd):
        self.running = True
        try:
            child = subprocess.Popen(cmd, preexec_fn=_pdeathsig_preexec())
        except OSError as exc:
            _log("cannot run %s: %s" % (cmd[0], exc))
            return 127
        self.child = child
        while self.early:
            self._send(self.early.pop(0))
        status = child.wait()
        return 128 - status if status < 0 else status


def _run(args, cmd):
    forwarder = _Forwarder()
    for signum in FORWARDED_SIGNALS:
        signal.signal(signum, forwarder)
    try:
        with lease(
            args.holder,
            mem_gb=args.mem_gb,
            cpus=args.cpus,
            optional=args.optional,
            max_wait_s=args.max_wait_s,
        ):
            return forwarder.run(cmd)
    except LeaseDirMissing as exc:
        _log(str(exc))
        return EX_NOT_PROVISIONED
    except WaitExceeded as exc:
        _log(str(exc))
        return EX_TEMPFAIL
    except _Terminated as exc:
        return 128 + exc.signum


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    cmd = []
    if "--" in argv:
        split = argv.index("--")
        argv, cmd = argv[:split], argv[split + 1:]
    parser = argparse.ArgumentParser(prog="pb_gpu_lease.py", description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="hold a lease while CMD runs: run [options] -- CMD [ARGS...]")
    run.add_argument("--holder", help="holder name (default: $TEST_TARGET, else <hostname>:<pid>)")
    run.add_argument("--mem-gb", type=_nonneg_int, default=16)
    run.add_argument("--cpus", type=_nonneg_int, default=4)
    run.add_argument("--optional", action="store_true", help="run CMD without a lease if the host has none")
    run.add_argument("--max-wait-s", type=float, default=None, help="give up waiting after S seconds (exit 75)")
    args = parser.parse_args(argv)
    if not cmd:
        run.error("a command is required after --")
    return _run(args, cmd)


if __name__ == "__main__":
    sys.exit(main())
