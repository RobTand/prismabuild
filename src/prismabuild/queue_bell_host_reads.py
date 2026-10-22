"""Retain host invalidations until authoritative reads confirm NFS visibility.

Consumers call this outside admission and transition exclusion. A reader must
use its existing bounded/confirmed read with the supplied absolute deadline,
and return True only on confirmed evidence, including confirmed removal.
Missing, unreadable or unchanged unconfirmed evidence returns False. This
helper grants no capacity and does not replace the existing offer/ledger reads.
"""
from __future__ import annotations

from dataclasses import dataclass
import math
import time

from .queue_bell import Unavailable
from .queue_bell_topics import TOPICS


@dataclass
class _HostVisibility:
    deadline: float
    retry_at: float
    delay: float = 0.05


class DirtyHostInputs:
    """One reader's pending host IDs, independent of the transport cursor."""

    def __init__(self):
        self._pending: dict[tuple[str, str], _HostVisibility] = {}
        self.read_attempts = 0
        self.visibility_retries = 0

    @property
    def dirty(self):
        return set(self._pending)

    def invalidate(self, topic: str, host: str, *, deadline: float):
        if topic not in ("workers", "reservations") or not TOPICS[topic].accepts_id(host):
            raise ValueError("invalid host input hint")
        if not math.isfinite(deadline):
            raise ValueError("host input visibility needs a finite deadline")
        key = topic, host
        if key in self._pending:
            self._pending[key].deadline = min(self._pending[key].deadline, deadline)
        else:
            self._pending[key] = _HostVisibility(deadline, time.monotonic())

    def confirm(self, read, *, stop):
        """Retry unknown visibility without advancing the original read budget.

        The timed wait is NFS visibility recovery: 50 ms doubling to one second,
        clipped to the original deadline, cancelled by confirmation or stop.
        Expiry keeps the ID dirty and reports degraded source availability.
        """
        while self._pending:
            if stop.is_set():
                raise Unavailable("host input visibility stopped")
            for key, pending in tuple(self._pending.items()):
                now = time.monotonic()
                if now >= pending.deadline:
                    raise Unavailable("host input visibility budget exhausted")
                if now < pending.retry_at:
                    continue
                self.read_attempts += 1
                try:
                    confirmed = read(*key, pending.deadline)
                except OSError:
                    confirmed = False
                now = time.monotonic()
                if now >= pending.deadline:
                    raise Unavailable("host input visibility budget exhausted")
                if confirmed is True:
                    self._pending.pop(key)
                else:
                    self.visibility_retries += 1
                    pending.retry_at = now + pending.delay
                    pending.delay = min(1.0, pending.delay * 2)
            if self._pending:
                wake = min(min(item.retry_at, item.deadline) for item in self._pending.values())
                if stop.wait(max(0.0, wake - time.monotonic())):
                    raise Unavailable("host input visibility stopped")
