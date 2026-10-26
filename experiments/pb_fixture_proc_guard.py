"""Optional pytest guard for fixture runs that forbid peer proc descriptor reads.

Load through PYTEST_PLUGINS=pb_fixture_proc_guard in an admitted PB test job.
Diagnostic callers receive unknown through their existing OSError handling.
"""
import builtins
import io
import os


def pytest_sessionstart(session):
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
