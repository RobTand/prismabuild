"""Logical census time; real owned children wake the parent with pipe readiness."""
import select

from prismabuild import _bounded_reader as reader
from prismabuild._measurement_reservation import CensusScheduler


class FixtureCensusScheduler(CensusScheduler):
    def __init__(self) -> None:
        self.now = 0.

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds

    def deadline(self, seconds: float) -> reader.Deadline:
        return reader.Deadline(seconds, clock=lambda: self.now)

    @staticmethod
    def wait_readable(descriptor: int, remaining: float) -> bool:
        # Logical time advances only at explicit test boundaries. Kernel pipe
        # readiness schedules the real child; the Bazel timeout is a watchdog.
        return bool(select.select([descriptor], [], [])[0])

    @staticmethod
    def wait_for_exit(descriptor: int) -> None:
        # EOF can precede os._exit. Wait for the real pidfd readiness before
        # the production reaper verifies and consumes that child's status.
        select.select([descriptor], [], [])
