"""Queue change hints. Records and their existing readers remain authoritative."""
from __future__ import annotations

from dataclasses import dataclass
import http.client
import json
import math
from pathlib import Path
import re
from urllib.parse import urlencode

DEFAULT_PORT = 9470
MAX_WAIT_S = 300.0
CONNECT_TIMEOUT_S = 1.0
REPLY_GRACE_S = 1.0
STATE_DIRS = ("ready", "claimed", "done", "failed", "withdrawn", "deferred", "intent")
TERMINAL_DIRS = ("done", "failed", "withdrawn")


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

    def _request(self, path: str, *, timeout: float, budget_s: float) -> dict:
        connection = None
        try:
            host = self.host if self.host is not None else bell_host(self.queue_root)
            connection = http.client.HTTPConnection(
                host, self.port, timeout=min(CONNECT_TIMEOUT_S, budget_s))
            connection.connect()
            connection.sock.settimeout(timeout)
            connection.request("GET", path)
            response = connection.getresponse()
            if response.status != 200:
                raise Unavailable(f"bell HTTP {response.status}")
            body = json.loads(response.read(1024 * 1024))
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
        body = self._request("/v1/now", timeout=min(CONNECT_TIMEOUT_S, budget_s),
                             budget_s=budget_s)
        return Cursor(body["epoch"], body["seq"])

    def wait(self, since: Cursor, dirs: tuple[str, ...], keys: tuple[str, ...],
             timeout: float) -> dict:
        if not math.isfinite(timeout) or timeout < 0:
            raise ValueError("bell timeout must be finite and nonnegative")
        timeout = min(timeout, MAX_WAIT_S)
        query = urlencode({"since": str(since), "dirs": ",".join(dirs),
                           "keys": ",".join(keys), "timeout": timeout})
        body = self._request("/v1/wait?" + query, timeout=timeout + REPLY_GRACE_S,
                             budget_s=max(0.001, timeout))
        if (body.get("reset") is not True
                and not isinstance(body.get("events"), list)):
            raise Unavailable("invalid bell wait reply")
        return body
