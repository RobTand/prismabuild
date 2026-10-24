"""Real action processes, socket milestones and an execution-only virtual clock.

The action stops at a semantic boundary before Popen returns to the watchdog.
Only communicate advances execution time. Filesystem work, imports and host
scheduling therefore spend none of the progress allowance being tested.
"""
from contextlib import contextmanager
import os
from pathlib import Path
import socket
import subprocess
import threading
from types import SimpleNamespace

from prismabuild import pool


class ControlledAction:
    def __init__(self, directory, *, steps=0, step_s=0, before_tick=None):
        directory.mkdir()
        self.directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        # The directory is private tmp_path scratch; the alias avoids AF_UNIX's
        # 107-byte limit without depending on a host scratch path.
        self.address = f"/proc/{os.getpid()}/fd/{self.directory_fd}/gate"
        self.server = socket.socket(socket.AF_UNIX)
        self.server.bind(self.address)
        self.server.listen(1)
        self.server.settimeout(60)
        self.connection = None
        self.process = None
        self.ready = threading.Event()
        self.now = 0.0
        self.steps = steps
        self.step_s = step_s
        self.next_step = step_s
        self.before_tick = before_tick
        self.clock = pool.ExecutionClock(lambda: self.now,
                                         lambda: 1_800_000_000 + self.now)

    @property
    def source(self):
        return f'''
import socket as _pb_socket
_pb_connection = None
def _pb_gate():
    global _pb_connection
    if _pb_connection is None:
        _pb_connection = _pb_socket.socket(_pb_socket.AF_UNIX)
        _pb_connection.connect({self.address!r})
    _pb_connection.sendall(b"G")
    if _pb_connection.recv(1) != b"R":
        raise RuntimeError("test controller closed the gate")
'''

    def launch(self, native, argv, **kwargs):
        self.process = native(argv, **kwargs)
        try:
            self.connection, _ = self.server.accept()
            self.connection.settimeout(60)
            self._milestone()
            self.ready.set()
        except BaseException:
            self.process.kill()
            self.process.communicate()
            raise
        return self

    def _milestone(self):
        if self.connection.recv(1) != b"G":
            raise RuntimeError("action ended before its milestone")

    @property
    def pid(self):
        return self.process.pid

    @property
    def returncode(self):
        return self.process.returncode

    def poll(self):
        return self.process.poll()

    def __getattr__(self, name):
        return getattr(self.process, name)

    def communicate(self, timeout):
        if self.process.poll() is not None:
            return self.process.communicate(timeout=60)
        if self.before_tick is not None:
            self.before_tick()
        self.now += timeout
        if self.steps and self.now + 1e-9 >= self.next_step:
            self.connection.sendall(b"R")
            self.steps -= 1
            self.next_step += self.step_s
            if not self.steps:
                return self.process.communicate(timeout=60)
            self._milestone()
        raise subprocess.TimeoutExpired("controlled action", timeout)

    def close(self):
        if self.process is not None and self.process.poll() is None:
            os.killpg(self.process.pid, 9)
            self.process.communicate(timeout=60)
        if self.connection is not None:
            self.connection.close()
        self.server.close()
        Path(self.address).unlink(missing_ok=True)
        os.close(self.directory_fd)


@contextmanager
def controlled_actions(monkeypatch, actions):
    """Route only named action channels; leave unrelated subprocesses native."""
    native = pool.subprocess.Popen

    def launch(argv, **kwargs):
        path = kwargs.get("env", {}).get(pool.pb.ACTION_PROGRESS_PATH_ENV)
        action = actions.get(path)
        return (native(argv, **kwargs) if action is None
                else action.launch(native, argv, **kwargs))

    with monkeypatch.context() as patches:
        patches.setattr(pool, "subprocess", SimpleNamespace(
            **(vars(pool.subprocess) | {"Popen": launch})))
        try:
            yield
        finally:
            for action in actions.values():
                action.close()
