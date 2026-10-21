"""Local inotify and HTTP long-poll service for queue hints (#1894)."""
from __future__ import annotations

from collections import deque
import ctypes
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
import os
from pathlib import Path
import selectors
import socket
import struct
import threading
import time
from urllib.parse import parse_qs, urlsplit
import uuid

from .queue_bell import Cursor, MAX_WAIT_S, STATE_DIRS

IN_CLOSE_WRITE = 0x00000008
IN_MOVED_TO = 0x00000080
IN_DELETE_SELF = 0x00000400
IN_MOVE_SELF = 0x00000800
IN_Q_OVERFLOW = 0x00004000
IN_IGNORED = 0x00008000
IN_ISDIR = 0x40000000
IN_ONLYDIR = 0x01000000
EVENT_HEADER = struct.Struct("iIII")
DEFAULT_RING_SIZE = 4096
REQUEST_TIMEOUT_S = 5.0


class EventRing:
    def __init__(self, capacity: int = DEFAULT_RING_SIZE):
        if capacity < 1:
            raise ValueError("ring capacity must be positive")
        self.epoch, self.seq, self.floor = uuid.uuid4().hex, 0, 0
        self.events: deque[dict] = deque(maxlen=capacity)
        self.condition = threading.Condition()
        self.closed = False

    def now(self) -> dict:
        with self.condition:
            return {"epoch": self.epoch, "seq": self.seq}

    def append(self, directory: str, key: str) -> None:
        with self.condition:
            self.seq += 1
            if len(self.events) == self.events.maxlen:
                self.floor = self.events[0]["seq"]
            self.events.append({"seq": self.seq, "dir": directory, "key": key})
            self.condition.notify_all()

    def reset(self) -> None:
        with self.condition:
            self.seq += 1
            self.floor = self.seq
            self.events.clear()
            self.condition.notify_all()

    def close(self) -> None:
        with self.condition:
            self.closed = True
            self.condition.notify_all()

    def wait(self, since: Cursor, dirs: set[str], keys: set[str], timeout: float) -> dict:
        deadline = time.monotonic() + min(timeout, MAX_WAIT_S)
        with self.condition:
            while True:
                cursor = {"epoch": self.epoch, "seq": self.seq}
                if (self.closed or since.epoch != self.epoch
                        or not self.floor <= since.seq <= self.seq):
                    return {**cursor, "reset": True}
                events = [event for event in self.events
                          if event["seq"] > since.seq
                          and (not dirs or event["dir"] in dirs)
                          and (not keys or event["key"] in keys)]
                remaining = deadline - time.monotonic()
                if events or remaining <= 0:
                    return {**cursor, "events": events}
                self.condition.wait(remaining)


class InotifyQueue:
    def __init__(self, root: Path):
        libc = ctypes.CDLL(None, use_errno=True)
        libc.inotify_init1.argtypes = [ctypes.c_int]
        libc.inotify_init1.restype = ctypes.c_int
        libc.inotify_add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
        libc.inotify_add_watch.restype = ctypes.c_int
        self.fd = libc.inotify_init1(os.O_NONBLOCK | os.O_CLOEXEC)
        if self.fd < 0:
            raise OSError(ctypes.get_errno(), "inotify_init1")
        self.directories = {}
        try:
            for directory in STATE_DIRS:
                mask = (IN_CLOSE_WRITE | IN_MOVED_TO | IN_DELETE_SELF
                        | IN_MOVE_SELF | IN_ONLYDIR)
                wd = libc.inotify_add_watch(self.fd, os.fsencode(root / directory), mask)
                if wd < 0:
                    raise OSError(ctypes.get_errno(), str(root / directory))
                self.directories[wd] = directory
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        os.close(self.fd)

    def consume(self, data: bytes, ring: EventRing) -> None:
        offset = 0
        while offset < len(data):
            wd, mask, _, length = EVENT_HEADER.unpack_from(data, offset)
            name = os.fsdecode(data[offset + EVENT_HEADER.size:
                                    offset + EVENT_HEADER.size + length].split(b"\0", 1)[0])
            offset += EVENT_HEADER.size + length
            if mask & IN_Q_OVERFLOW:
                ring.reset()
            elif mask & (IN_DELETE_SELF | IN_MOVE_SELF | IN_IGNORED):
                # A replaced state directory is no longer watched. Refuse service
                # and let the supervisor restart, rather than promise false quiet.
                raise OSError("queue bell lost a state directory watch")
            elif (mask & (IN_CLOSE_WRITE | IN_MOVED_TO) and not mask & IN_ISDIR
                  and name.endswith(".json") and not name.startswith(".")):
                ring.append(self.directories[wd], name[:-5])


class BellHTTPServer(ThreadingHTTPServer):
    def __init__(self, address, ring: EventRing, root: Path):
        self.ring = ring
        self.queue_root = str(root.absolute())
        super().__init__(address, BellRequest)

    def get_request(self):
        connection, address = super().get_request()
        connection.settimeout(REQUEST_TIMEOUT_S)
        return connection, address


class BellRequest(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        # A scratch queue may share the production queue's mount source. It
        # cannot use this stream to wait for its own records. This is routing,
        # not authorization; unqualified read-only health queries still work.
        queue = self.headers.get("X-PrismaBuild-Queue")
        if queue is not None and queue != self.server.queue_root:
            self.send_error(503, "bell serves a different queue")
            return
        target = urlsplit(self.path)
        try:
            if target.path == "/v1/now":
                reply = self.server.ring.now()
            elif target.path == "/v1/wait":
                query = parse_qs(target.query, keep_blank_values=True, max_num_fields=4)
                since = Cursor.parse(query["since"][0])
                timeout = float(query["timeout"][0])
                if not math.isfinite(timeout) or timeout < 0:
                    raise ValueError("invalid timeout")
                dirs = set(filter(None, query.get("dirs", [""])[0].split(",")))
                keys = set(filter(None, query.get("keys", [""])[0].split(",")))
                if not dirs <= set(STATE_DIRS):
                    raise ValueError("unknown queue directory")
                reply = self.server.ring.wait(since, dirs, keys, timeout)
            else:
                self.send_error(404)
                return
        except (ValueError, KeyError):
            self.send_error(400)
            return
        body = json.dumps(reply).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, format: str, *args: object) -> None:
        pass


class BellService:
    """Bind and install every watch before publishing a usable cursor.

    One selector blocks on listeners, inotify and a shutdown socket. Requests
    block on the ring condition. Shutdown wakes requests before joining them.
    """
    def __init__(self, root: Path, addresses: list[tuple[str, int]], *,
                 ring_size: int = DEFAULT_RING_SIZE, on_exit=lambda: None):
        self.ring = EventRing(ring_size)
        self.watch = InotifyQueue(root)
        self.servers = []
        self.error: BaseException | None = None
        self.on_exit = on_exit
        try:
            for address in addresses:
                self.servers.append(BellHTTPServer(address, self.ring, root))
            self.reader, self.writer = socket.socketpair()
        except BaseException:
            for server in self.servers:
                server.server_close()
            self.watch.close()
            raise
        self.thread = threading.Thread(target=self._listen, name="queue-bell")

    def __enter__(self):
        self.thread.start()
        return self

    def _listen(self) -> None:
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(self.reader, selectors.EVENT_READ, "stop")
                selector.register(self.watch.fd, selectors.EVENT_READ, "inotify")
                for server in self.servers:
                    selector.register(server, selectors.EVENT_READ, server)
                while True:
                    for selected, _ in selector.select():
                        if selected.data == "stop":
                            return
                        if selected.data == "inotify":
                            self.watch.consume(os.read(self.watch.fd, 65536), self.ring)
                        else:
                            selected.data._handle_request_noblock()
        except BaseException as exc:
            self.error = exc
        finally:
            self.ring.close()
            self.on_exit()

    def __exit__(self, *exc):
        self.writer.send(b"x")
        self.thread.join()
        for server in self.servers:
            server.server_close()
        self.watch.close()
        self.reader.close()
        self.writer.close()
        if self.error is not None:
            raise self.error
