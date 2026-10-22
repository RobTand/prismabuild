"""Per-invocation submission diagnostics, deliberately outside action identity."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import time

from .core import _SUBMISSION_TIMINGS, submission_phase, submission_phase_snapshot

SUBMISSION_PHASE_NAMES = frozenset({
    "checkout_identity", "bundle_pack", "cas_stage", "cas_write", "offer_read",
    "d38_gate", "queue_publication", "transition_lock_wait", "lock_wait",
    "sealed_request_read", "cas_input_verify", "cas_read",
})
SLOW_SUBMISSION_PHASE_S = 1.0


@dataclass(frozen=True)
class SubmissionPhaseEvent:
    phase: str
    state: str
    seconds: float | None = None


class SubmissionTimings:
    def __init__(self, *, on_phase=None, on_slow=None):
        self.durations = {}
        self.stack = []
        self.on_phase = on_phase
        self.on_slow = on_slow

    def snapshot(self):
        result = dict(self.durations)
        if self.stack:
            name, started = self.stack[-1]
            result[name] = result.get(name, 0.0) + time.monotonic() - started
        return result

    @contextmanager
    def activate(self):
        token = _SUBMISSION_TIMINGS.set(self)
        try:
            yield self
        finally:
            _SUBMISSION_TIMINGS.reset(token)

    @contextmanager
    def span(self, name):
        if name not in SUBMISSION_PHASE_NAMES:
            raise ValueError("unknown submission phase")
        if self.stack and self.stack[-1][0] == name:
            yield
            return
        now = time.monotonic()
        if self.stack:
            parent, started = self.stack[-1]
            self.durations[parent] += now - started
        previous = self.durations.setdefault(name, 0.0)
        self.stack.append([name, now])
        if self.on_phase is not None:
            self.on_phase(SubmissionPhaseEvent(name, "started"))
        try:
            yield
        finally:
            now = time.monotonic()
            _, started = self.stack.pop()
            self.durations[name] += now - started
            if self.stack:
                self.stack[-1][1] = now
            seconds = self.durations[name] - previous
            if self.on_phase is not None:
                self.on_phase(SubmissionPhaseEvent(name, "finished", seconds))
            if seconds >= SLOW_SUBMISSION_PHASE_S and self.on_slow is not None:
                self.on_slow(f"pbrun: submission phase {name} took {seconds:.3f}s")
