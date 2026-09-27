"""Bounded sealed-action handoff to the lock-owning publisher (#1213).

Not a public submission API. A standalone invocation has no publisher socket
and cannot mint anything. The child uses the published pbrun's ordinary main
pipeline; the parent alone authorizes the exact sealed payload before READY.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import runpy
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time

MAX_BYTES = 4 * 1024 * 1024


def _receive(sock, count, deadline):
    data = bytearray()
    while len(data) < count:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("publication canary handoff deadline expired")
        sock.settimeout(remaining)
        part = sock.recv(count - len(data))
        if not part:
            raise EOFError("publication canary handoff closed before authorization")
        data.extend(part)
    return bytes(data)


def _send_action(sock, action, deadline):
    body = json.dumps(action, allow_nan=False).encode()
    if len(body) > MAX_BYTES:
        raise ValueError("publication canary sealed handoff exceeds size bound")
    sock.settimeout(max(0.001, deadline - time.monotonic()))
    sock.sendall(struct.pack("!I", len(body)) + body)
    if _receive(sock, 1, deadline) != b"Y":
        raise ValueError("publication canary publisher refused authorization")


def submit(argv, *, intent, authorize, timeout_s=120.0):
    """One child, one bounded private handoff; callback runs in the parent.

    The parent callback receives the same absolute monotonic deadline and
    must bound its store operation; the publisher uses the existing bounded
    helper rather than doing shared-filesystem I/O in this process.
    Temporary stdout/stderr files avoid a pipe deadlock while pbrun prepares
    the request. They obey TMPDIR. A timeout kills only our submission child
    group, never an admitted action, campaign row or worker loop.
    """
    parent, child = socket.socketpair()
    deadline = time.monotonic() + timeout_s
    command = [sys.executable, str(Path(__file__).resolve()), str(child.fileno()),
               json.dumps(intent), str(timeout_s), *argv[1:]]
    process = None
    try:
        with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
            process = subprocess.Popen(command, stdin=subprocess.DEVNULL,
                                       stdout=stdout, stderr=stderr,
                                       pass_fds=(child.fileno(),), start_new_session=True)
            child.close()
            authorized = False
            try:
                size = struct.unpack("!I", _receive(parent, 4, deadline))[0]
                if size > MAX_BYTES:
                    raise ValueError("publication canary sealed handoff exceeds size bound")
                action = json.loads(_receive(parent, size, deadline))
                authorize(action, deadline=deadline)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("publication canary authorization deadline expired")
                parent.settimeout(remaining)
                parent.sendall(b"Y")
                authorized = True
            except EOFError:
                # A preparation refusal (e.g. unsupported capability) is the
                # normal pbrun stderr/returncode, not a fabricated test result.
                pass
            except Exception:
                parent.close()  # the child cannot continue without the ACK
                raise
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(argv, timeout_s)
            code = process.wait(timeout=remaining)
            stdout.seek(0)
            stderr.seek(0)
            out, err = stdout.read(MAX_BYTES + 1), stderr.read(MAX_BYTES + 1)
            if len(out) > MAX_BYTES or len(err) > MAX_BYTES:
                raise ValueError("publication canary submit output exceeds size bound")
            if not authorized and code == 0:
                raise ValueError("publication canary child returned without authorization")
            return subprocess.CompletedProcess(argv, code, out.decode(errors="replace"),
                                               err.decode(errors="replace"))
    except (TimeoutError, socket.timeout) as exc:
        # The caller records a bounded refusal, not this exception's traceback.
        # Preserve a mint helper's retained identity across that boundary.
        raise subprocess.TimeoutExpired(
            argv, timeout_s, stderr=str(exc)[:4096]) from exc
    finally:
        parent.close()
        child.close()
        if process is not None and process.poll() is None:
            # Includes grandchildren of this submission process, not queue
            # workers. Never signal the publisher's process group.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                raise RuntimeError(f"publication canary submitter pid={process.pid} retained after kill")


def main():
    fd, value, timeout_s, pbrun_path, *arguments = sys.argv[1:]
    intent = json.loads(value)
    deadline = time.monotonic() + float(timeout_s)
    sys.argv = [pbrun_path, *arguments]
    module = runpy.run_path(pbrun_path, run_name="_publication_canary_pbrun")
    with socket.socket(fileno=int(fd)) as sock:
        return module["main"](
            publication_canary_intent=intent,
            authorize_canary=lambda action: _send_action(sock, action, deadline))


if __name__ == "__main__":
    raise SystemExit(main())
