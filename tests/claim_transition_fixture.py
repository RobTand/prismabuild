"""A sibling claimant holds a real transition lock until the history releases it."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import threading


@contextmanager
def held_transition(queue, key):
    acquired, release = threading.Event(), threading.Event()

    def sibling():
        with queue._transition_locked(key, blocking=False) as held:
            assert held, "sibling failed to acquire the real candidate transition"
            acquired.set()
            assert release.wait(60), "history failed to release its sibling claimant"

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(sibling)
        try:
            assert acquired.wait(10), "sibling transition did not become observable"
            yield
        finally:
            release.set()
            future.result(timeout=10)
