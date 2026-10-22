"""HTTP reads with one absolute budget, including dribbling peers (#1894)."""
from __future__ import annotations

import io
import time


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
