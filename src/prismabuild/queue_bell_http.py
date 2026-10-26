"""HTTP reads with one absolute budget, including dribbling peers (#1894)."""
from __future__ import annotations

import io
import socket
import threading
import time


class RequestCancellation:
    """Own cancellation of one request, including attachment after cancellation."""

    def __init__(self):
        self._lock = threading.Lock()
        self._cancelled = False
        self._socket = None

    def attach(self, connection):
        with self._lock:
            if self._cancelled:
                connection.close()
                raise OSError("bell request cancelled")
            self._socket = connection

    def detach(self):
        with self._lock:
            self._socket = None

    @property
    def cancelled(self):
        with self._lock:
            return self._cancelled

    def cancel(self):
        with self._lock:
            self._cancelled = True
            if self._socket is not None:
                try:
                    self._socket.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                self._socket.close()


class DeadlineInput(io.RawIOBase):
    """Re-arm before every recv, even inside HTTP's buffered header/body reads."""

    def __init__(self, connection, deadline: float):
        self.connection = connection
        self.deadline = deadline

    def readable(self):
        return True

    def readinto(self, buffer):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("bell HTTP deadline exhausted")
        self.connection.settimeout(remaining)
        return self.connection.recv_into(buffer)


class DeadlineResponseSocket:
    """The makefile contract HTTPResponse needs; the caller owns the socket."""

    def __init__(self, connection, deadline: float):
        self.connection = connection
        self.deadline = deadline

    def makefile(self, mode):
        return io.BufferedReader(DeadlineInput(self.connection, self.deadline))
