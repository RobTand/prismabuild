"""Process harness for the L2 review probes (not collected)."""
import json
from pathlib import Path
import queue as queue_mod
import signal
import subprocess
import sys
import threading
import time
import warnings

ACTOR = Path(__file__).with_name("heartbeat_review_actor_2230.py")


class Actor:
    def __init__(self, role, **args):
        self.role = role
        self.proc = subprocess.Popen([sys.executable, str(ACTOR), role, json.dumps(args)],
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE, text=True, bufsize=1)
        self.events, self._inbox = [], queue_mod.Queue()
        self.stderr = []
        self._readers = [threading.Thread(target=self._read, daemon=True),
                         threading.Thread(target=lambda: self.stderr.extend(self.proc.stderr), daemon=True)]
        for reader in self._readers:
            reader.start()

    def _read(self):
        for line in self.proc.stdout:
            try:
                event = json.loads(line)
            except ValueError:
                event = {"event": "noise", "line": line}
            event["recv"] = time.time()
            self.events.append(event)
            self._inbox.put(event)
        self._inbox.put(None)

    def expect(self, name, timeout, where=lambda event: True):
        deadline = time.time() + timeout
        while True:
            remaining = deadline - time.time()
            assert remaining > 0, f"{self.role}: no {name} within {timeout}s; stderr={''.join(self.stderr)[-2000:]}"
            try:
                event = self._inbox.get(timeout=remaining)
            except queue_mod.Empty as exc:
                raise AssertionError(f"{self.role}: no {name} within {timeout}s") from exc
            assert event is not None, f"{self.role} exited before {name}; stderr={''.join(self.stderr)[-2000:]}"
            if event.get("event") == name and where(event):
                return event

    def stop(self, timeout=120):
        if self.proc.poll() is None:
            try:
                self.proc.stdin.write("STOP\n")
                self.proc.stdin.flush()
            except (BrokenPipeError, OSError):
                pass
        try:
            self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=10)
        for reader in self._readers:
            reader.join(timeout)
            assert not reader.is_alive(), "actor pipe did not close"
        return self.proc.returncode

    def kill(self):
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait(timeout=30)
        return time.time()

    def of(self, name):
        return [event for event in self.events if event.get("event") == name]


def summary(label, **fields):
    line = "L2REVIEW " + label + " " + json.dumps(fields, sort_keys=True, default=repr)
    print(line)
    # pbtest forwards no -rA/-s; the warnings summary is printed for passing tests too.
    warnings.warn(line, UserWarning, stacklevel=2)
