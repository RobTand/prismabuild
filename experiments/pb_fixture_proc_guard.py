"""Optional pytest guard for fixture runs that forbid peer proc descriptor reads.

Load through PYTEST_PLUGINS=pb_fixture_proc_guard in an admitted PB test job.
Diagnostic callers receive unknown through their existing OSError handling.
"""
import builtins
import io
import os

import pytest


def pytest_sessionstart(session):
    # Nested pytest fixtures run from their own scratch checkouts. Do not make
    # this optional outer-session plugin an undeclared child import dependency.
    plugins = [name for name in os.environ.get("PYTEST_PLUGINS", "").split(",")
               if name and name != __name__]
    if plugins:
        os.environ["PYTEST_PLUGINS"] = ",".join(plugins)
    else:
        os.environ.pop("PYTEST_PLUGINS", None)
    def forbidden(path):
        try:
            value = os.fsdecode(path)
        except TypeError:
            return False
        parts = os.path.normpath(value).split("/")
        return (value == "/proc/locks" or (
            len(parts) >= 4 and parts[1] == "proc" and parts[2].isdigit()
            and int(parts[2]) != os.getpid() and parts[3] in {"fd", "fdinfo"}))

    def protect(native):
        def guarded(path=".", *args, **kwargs):
            if forbidden(path):
                raise PermissionError("PB fixture policy forbids peer proc descriptor reads")
            return native(path, *args, **kwargs)
        return guarded

    for module, name in ((builtins, "open"), (io, "open"), (os, "open"),
                         (os, "stat"), (os, "lstat"), (os, "readlink"),
                         (os, "listdir"), (os, "scandir")):
        setattr(module, name, protect(getattr(module, name)))


def pytest_collection_modifyitems(items):
    for item in items:
        if item.get_closest_marker("live_store") is not None:
            item.add_marker(pytest.mark.skip(reason="fixture-only run forbids live-store access"))
        elif item.nodeid.endswith(
                "test_pbstatus_deadline.py::test_an_abandoned_child_owns_no_inherited_descriptor"):
            item.add_marker(pytest.mark.skip(reason="fixture policy forbids inspecting a peer fd table"))
