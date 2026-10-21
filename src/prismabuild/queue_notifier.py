"""Local inotify and HTTP long-poll service for queue hints (#1894)."""
from __future__ import annotations

from http.server import BaseHTTPRequestHandler, HTTPServer
import io
import json
import math
import os
from pathlib import Path
import selectors
import socket
import threading
import time
from urllib.parse import parse_qs, urlsplit

from .queue_bell import Cursor, MAX_WAIT_S, STATE_DIRS
from .queue_bell_events import (
    EventRing, InotifyQueue, DEFAULT_RING_SIZE, EVENT_HEADER, IN_Q_OVERFLOW,
)
from .queue_bell_http import DeadlineInput

REQUEST_TIMEOUT_S = 5.0
MAX_CONNECTIONS = 128


class RequestWorkers:
    """One aggregate connection cap across all listeners; own and join children."""

    def __init__(self, capacity):
        if capacity < 1:
            raise ValueError("connection capacity must be positive")
        self.capacity = capacity
        self.lock = threading.Condition()
        self.active = set()
        self.threads = set()

    def start(self, server, request, address):
        with self.lock:
            for ended in tuple(self.threads):
                if not ended.is_alive():
                    ended.join()
                    self.threads.remove(ended)
            if len(self.active) >= self.capacity:
                # Refusal runs on the selector thread and must never block it.
                request.setblocking(False)
                try:
                    request.send(b"HTTP/1.1 503 Service Unavailable\r\n"
                                 b"Content-Length: 0\r\nConnection: close\r\n\r\n")
                except OSError:
                    pass
                server.shutdown_request(request)
                return
            thread = threading.Thread(target=self._serve,
                args=(server, request, address), name="bell-request", daemon=False)
            self.active.add(thread)
            self.threads.add(thread)
            self.lock.notify_all()
            try:
                thread.start()
            except BaseException:
                self.active.remove(thread)
                self.threads.remove(thread)
                server.shutdown_request(request)
                raise

    def _serve(self, server, request, address):
        try:
            server.finish_request(request, address)
        except (OSError, ValueError):
            pass
        finally:
            server.shutdown_request(request)
            with self.lock:
                self.active.remove(threading.current_thread())
                self.lock.notify_all()

    def join(self):
        with self.lock:
            threads = list(self.threads)
        for thread in threads:
            thread.join()


class BellHTTPServer(HTTPServer):
    def __init__(self, address, ring: EventRing, root: Path, workers: RequestWorkers):
        self.workers = workers
        self.ring = ring
        self.queue_root = str(root.absolute())
        super().__init__(address, BellRequest)

    def get_request(self):
        connection, address = super().get_request()
        connection.settimeout(REQUEST_TIMEOUT_S)
        return connection, address

    def process_request(self, request, address):
        self.workers.start(self, request, address)


class BellRequest(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def setup(self):
        super().setup()
        self.rfile.close()
        self.rfile = io.BufferedReader(DeadlineInput(
            self.connection, time.monotonic() + REQUEST_TIMEOUT_S))

    def do_GET(self) -> None:
        # A scratch queue may share the production queue's mount source. It
        # cannot use this stream to wait for its own records. This is routing,
        # not authorization; unqualified read-only health queries still work.
        self.close_connection = True
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
                reply = self.server.ring.wait(since, dirs, keys, min(timeout, MAX_WAIT_S), self.connection)
                if reply is None:
                    return
            else:
                self.send_error(404)
                return
        except (ValueError, KeyError):
            self.send_error(400)
            return
        self.connection.settimeout(REQUEST_TIMEOUT_S)
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
    block on filtered notifications and client disconnects. Shutdown joins them.
    """
    def __init__(self, root: Path, addresses: list[tuple[str, int]], *,
                 ring_size: int = DEFAULT_RING_SIZE, on_exit=lambda: None,
                 max_connections: int = MAX_CONNECTIONS, generation: Path | None = None):
        self.ring = EventRing(ring_size)
        self.workers = RequestWorkers(max_connections)
        self.watch = InotifyQueue(root, generation=generation)
        self.servers = []
        self.error: BaseException | None = None
        self.on_exit = on_exit
        try:
            for address in addresses:
                self.servers.append(BellHTTPServer(address, self.ring, root, self.workers))
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
                while not self.watch.retired():
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
        self.workers.join()
        self.watch.close()
        self.reader.close()
        self.writer.close()
        if self.error is not None:
            raise self.error
