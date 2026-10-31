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

from .queue_bell_http import DeadlineResponseSocket, RequestCancellation
from .queue_bell_topics import OUTCOME_TOPICS, TOPICS, TopicFilters

DEFAULT_PORT = 9470
MAX_WAIT_S = 300.0
CONNECT_TIMEOUT_S = 1.0
STATE_DIRS = tuple(topic.directory for topic in OUTCOME_TOPICS)
REQUIRED_STATE_DIRS = tuple(topic.directory for topic in OUTCOME_TOPICS if topic.required)
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
                 port: int = DEFAULT_PORT, cancellation: RequestCancellation | None = None):
        self.queue_root, self.host, self.port = queue_root, host, port
        self._cancellation = cancellation

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
            if self._cancellation is not None:
                self._cancellation.attach(connection.sock)
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
            if self._cancellation is not None:
                self._cancellation.detach()
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

    def now_v2(self, topics: tuple[str, ...], *, budget_s: float = CONNECT_TIMEOUT_S) -> dict:
        """Capture before observing records; require positive topic coverage."""
        TopicFilters.parse(",".join(topics), "")
        body = self._request("/v2/now", deadline=time.monotonic() + min(CONNECT_TIMEOUT_S, budget_s))
        self._coverage_v2(body, topics)
        return body

    @staticmethod
    def _coverage_v2(body: dict, topics: tuple[str, ...]) -> None:
        coverage = body.get("coverage")
        if (type(body.get("seq")) is not int
                or not isinstance(coverage, dict) or type(coverage.get("version")) is not int
                or coverage["version"] < 1 or not isinstance(coverage.get("topics"), list)
                or any(not isinstance(topic, str) for topic in coverage["topics"])
                or not set(topics or TOPICS).issubset(coverage["topics"])):
            raise Unavailable("bell topic coverage is incomplete or invalid")

    def wait_v2(self, since: Cursor, coverage_version: int, topics: tuple[str, ...],
                ids: tuple[str, ...], timeout: float) -> dict:
        """One bounded transport renewal, never a record observation or retry loop.

        Reset invalidates the caller's derived state. A healthy empty reply only
        renews transport; the consumer owns deadlines and unavailable backoff.
        """
        TopicFilters.parse(",".join(topics), ",".join(ids))
        if type(coverage_version) is not int or coverage_version < 1:
            raise ValueError("invalid bell coverage version")
        if not math.isfinite(timeout) or timeout < 0:
            raise ValueError("bell timeout must be finite and nonnegative")
        timeout = min(timeout, MAX_WAIT_S)
        deadline = time.monotonic() + timeout
        query = urlencode({"since": str(since), "coverage_version": coverage_version,
            "topics": ",".join(topics), "ids": ",".join(ids),
            "timeout": max(0.0, timeout - min(0.05, timeout / 10))})
        body = self._request("/v2/wait?" + query, deadline=deadline)
        if body.get("reset") is True:
            return body
        self._coverage_v2(body, topics)
        if (body["coverage"]["version"] != coverage_version
                or body["epoch"] != since.epoch or body["seq"] < since.seq):
            raise Unavailable("bell coverage or cursor changed without reset")
        events = body.get("events")
        if not isinstance(events, list) or len(events) > 4096:
            raise Unavailable("invalid typed bell hints")
        for event in events:
            if (not isinstance(event, dict) or type(event.get("seq")) is not int
                    or not since.seq < event["seq"] <= body["seq"]
                    or not isinstance(event.get("topic"), str)
                    or event["topic"] not in TOPICS
                    or (topics and event["topic"] not in topics)
                    or not TOPICS[event["topic"]].accepts_id(event.get("id"))
                    or (ids and event["id"] not in ids)):
                raise Unavailable("invalid typed bell hint")
        return body
