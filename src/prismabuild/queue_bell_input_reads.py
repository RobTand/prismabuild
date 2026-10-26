"""Retain input invalidations until authoritative reads confirm NFS visibility.

Consumers call this outside admission and transition exclusion. A reader must
use its existing bounded/confirmed read with the supplied absolute deadline,
and return True only on confirmed evidence, including confirmed removal.
Missing, unreadable or unchanged unconfirmed evidence returns False. This
helper grants no capacity and does not replace the existing authoritative readers.
"""
from __future__ import annotations

from dataclasses import dataclass
import math
import threading
import time

from .queue_bell import Unavailable
from .queue_bell_topics import TOPICS


@dataclass
class _Visibility:
    deadline: float
    retry_at: float
    delay: float = 0.05
    generation: int = 0


class DirtyInputs:
    """One reader's pending input IDs, independent of the transport cursor."""

    def __init__(self):
        self._pending: dict[tuple[str, str], _Visibility] = {}
        self._lock = threading.Lock()
        self.read_attempts = 0
        self.visibility_retries = 0

    @property
    def dirty(self):
        with self._lock:
            return set(self._pending)

    def invalidate(self, topic: str, identity: str, *, deadline: float):
        if topic not in ("workers", "reservations", "adaptive", "claim-denials") or not TOPICS[topic].accepts_id(identity):
            raise ValueError("invalid input hint")
        if not math.isfinite(deadline):
            raise ValueError("input visibility needs a finite deadline")
        key = topic, identity
        with self._lock:
            if key in self._pending:
                pending = self._pending[key]
                pending.deadline = min(pending.deadline, deadline)
                pending.generation += 1
            else:
                self._pending[key] = _Visibility(deadline, time.monotonic())

    def confirm(self, read, *, stop):
        """Retry unknown visibility without advancing the original read budget.

        The timed wait is NFS visibility recovery: 50 ms doubling to one second,
        clipped to the original deadline, cancelled by confirmation or stop.
        Expiry keeps the ID dirty and reports degraded source availability.
        """
        while True:
            with self._lock:
                keys = tuple(self._pending)
            if not keys:
                return
            if stop.is_set():
                raise Unavailable("input visibility stopped")
            for key in keys:
                with self._lock:
                    pending = self._pending[key]
                    now = time.monotonic()
                    if now >= pending.deadline:
                        raise Unavailable("input visibility budget exhausted")
                    if now < pending.retry_at:
                        continue
                    generation, deadline = pending.generation, pending.deadline
                    self.read_attempts += 1
                try:
                    confirmed = read(*key, deadline)
                except OSError:
                    confirmed = False
                with self._lock:
                    now = time.monotonic()
                    if now >= pending.deadline:
                        raise Unavailable("input visibility budget exhausted")
                    if confirmed is True and pending.generation == generation:
                        self._pending.pop(key)
                    elif confirmed is True:
                        # A newer hint needs its own read within the same budget.
                        pending.retry_at = now
                    else:
                        self.visibility_retries += 1
                        pending.retry_at = now + pending.delay
                        pending.delay = min(1.0, pending.delay * 2)
            with self._lock:
                if not self._pending:
                    return
                wake = min(min(item.retry_at, item.deadline) for item in self._pending.values())
            if stop.wait(max(0.0, wake - time.monotonic())):
                raise Unavailable("input visibility stopped")
