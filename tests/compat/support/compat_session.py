"""Retain verified, read-only source closures for one pytest configuration."""
from contextlib import ExitStack

import pytest

from record_compat import retained


_SOURCES = pytest.StashKey()


def retained_for_session(config, directory, *, history_control=False):
    shared = config.stash.get(_SOURCES, None)
    if shared is None:
        lifetime = ExitStack()
        shared = lifetime, {}
        config.stash[_SOURCES] = shared
        config.add_cleanup(lifetime.close)
    lifetime, sources = shared
    key = directory.resolve(), history_control
    if key not in sources:
        sources[key] = tuple(lifetime.enter_context(retained(directory, history_control=history_control)))
    return sources[key]
