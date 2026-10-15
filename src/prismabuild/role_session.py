"""Shared role operations; serving loops own their completion policies."""
from contextlib import contextmanager
import signal
import threading


@contextmanager
def stop_requests(*signals: int):
    """Request cooperative stop and restore the caller's signal handlers."""
    stop = threading.Event()
    previous = {}
    try:
        for signum in signals:
            previous[signum] = signal.signal(signum, lambda *_: stop.set())
        yield stop
    finally:
        for signum, handler in reversed(tuple(previous.items())):
            signal.signal(signum, handler)
