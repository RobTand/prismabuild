"""Queue change hints. Records and their existing readers remain authoritative."""
from __future__ import annotations

from dataclasses import dataclass
import http.client
import json
import math
from pathlib import Path
import re
import socket
import time
from urllib.parse import urlencode

from .queue_bell_http import DeadlineResponseSocket

DEFAULT_PORT = 9470
MAX_WAIT_S = 300.0
CONNECT_TIMEOUT_S = 1.0
STATE_DIRS = ("ready", "claimed", "done", "failed", "withdrawn", "deferred", "intent")
REQUIRED_STATE_DIRS = STATE_DIRS
STATE_DIRS += ("attempts", "publication-endings")
# All captured outcome inputs, including handoff and cleanup updates.
TERMINAL_DIRS = STATE_DIRS


class Unavailable(Exception):
    """The bell cannot call back; callers must recheck records with backoff."""


@dataclass(frozen=True)
class Cursor:
    epoch: str
    seq: int

    def __str__(self) -> str:
        return f"{self.epoch}:{self.seq}"

    @classmethod
    def parse(cls, value: str) -> Cursor:
        epoch, seq = value.split(":")
        if not re.fullmatch(r"[0-9a-f]{32}", epoch) or not seq.isdecimal():
            raise ValueError("invalid bell cursor")
        return cls(epoch, int(seq))


def bell_host(queue_root: Path, *, mountinfo: Path = Path("/proc/self/mountinfo")) -> str:
    """Use the longest covering mount's NFS source, or loopback for local storage.

    Do not resolve the queue path: that would turn discovery into an NFS read.
    mountinfo is a local kernel view, including mounts nested below /mnt/shared.
    """
    root = str(queue_root.absolute())
    covering = []
    for line in mountinfo.read_text().splitlines():
        before, after = line.split(" - ", 1)
        fields, source = before.split(), after.split()
        mount = re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), fields[4])
        if root == mount or root.startswith(mount.rstrip("/") + "/"):
            covering.append((len(mount), source[0], source[1]))
    if not covering:
        raise Unavailable("queue mount not found")
    _, filesystem, source = max(covering)
    if filesystem not in ("nfs", "nfs4"):
        return "127.0.0.1"
    host, _ = source.rsplit(":", 1)
    return host.strip("[]")


class Client:
    def __init__(self, queue_root: Path, *, host: str | None = None,
                 port: int = DEFAULT_PORT):
        self.queue_root, self.host, self.port = queue_root, host, port

    def _request(self, path: str, *, deadline: float) -> dict:
        connection = None
        try:
            if deadline <= time.monotonic():
                raise TimeoutError("bell HTTP deadline exhausted")
            host = self.host if self.host is not None else bell_host(self.queue_root)
            # Numeric endpoints avoid unbounded DNS and per-address retries.
            family = socket.AF_INET6 if ":" in host else socket.AF_INET
            socket.inet_pton(family, host)
            connection = http.client.HTTPConnection(host, self.port)
            connection.sock = socket.socket(family, socket.SOCK_STREAM)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("bell HTTP deadline exhausted")
            connection.sock.settimeout(min(CONNECT_TIMEOUT_S, remaining))
            connection.sock.connect((host, self.port))
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("bell HTTP deadline exhausted")
            connection.sock.settimeout(remaining)
            connection.request("GET", path, headers={
                "X-PrismaBuild-Queue": str(self.queue_root.absolute())})
            response = http.client.HTTPResponse(DeadlineResponseSocket(connection.sock, deadline))
            response.begin()
            if response.status != 200:
                raise Unavailable(f"bell HTTP {response.status}")
            with response:
                raw = response.read(1024 * 1024 + 1)
            if len(raw) > 1024 * 1024 or time.monotonic() >= deadline:
                raise ValueError("bell reply exceeds its byte or time budget")
            body = json.loads(raw)
            if not isinstance(body, dict):
                raise ValueError("invalid bell reply")
            # Both endpoints carry a cursor, including empty and reset replies.
            Cursor.parse(f"{body['epoch']}:{body['seq']}")
            return body
        except (OSError, http.client.HTTPException, ValueError, KeyError) as exc:
            raise Unavailable(str(exc)) from exc
        finally:
            if connection is not None:
                connection.close()

    def now(self, *, budget_s: float = CONNECT_TIMEOUT_S) -> Cursor:
        deadline = time.monotonic() + min(CONNECT_TIMEOUT_S, budget_s)
        body = self._request("/v1/now", deadline=deadline)
        return Cursor.parse(f"{body['epoch']}:{body['seq']}")

    def wait(self, since: Cursor, dirs: tuple[str, ...], keys: tuple[str, ...],
             timeout: float) -> dict:
        if not math.isfinite(timeout) or timeout < 0:
            raise ValueError("bell timeout must be finite and nonnegative")
        timeout = min(timeout, MAX_WAIT_S)
        deadline = time.monotonic() + timeout
        # Reserve reply delivery inside the same caller budget, never after it.
        server_wait = max(0.0, timeout - min(0.05, timeout / 10))
        query = urlencode({"since": str(since), "dirs": ",".join(dirs),
                           "keys": ",".join(keys), "timeout": server_wait})
        body = self._request("/v1/wait?" + query, deadline=deadline)
        if (body.get("reset") is not True
                and not isinstance(body.get("events"), list)):
            raise Unavailable("invalid bell wait reply")
        return body
