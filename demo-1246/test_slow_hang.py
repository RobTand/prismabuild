"""Synthetic stand-in for #1246's 1513 s file: hangs past any bound."""
import signal
import time


def test_hang_holds_the_shard():
    # A futex-hung test never sees the SIGALRM the per-test bound arms
    # (the bound module's own docstring case); blocking the signal
    # reproduces that shape deterministically in a synthetic file.
    signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})
    time.sleep(150)
