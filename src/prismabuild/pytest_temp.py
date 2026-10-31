"""Private scratch for pytest launched inside an admitted action.

Keep TMPDIR intact: it is also the sealed compiler/cache placement. Callers
pass the yielded leaf as pytest's --basetemp, never the shared root itself.
"""
from contextlib import contextmanager
import os
from pathlib import Path
import re
import signal
import sys
import tempfile

from . import owned_tree, pbtest_scratch_roots


@contextmanager
def pytest_basetemp(root=None):
    """Own a short external pytest base for one invocation, including failures.

    SIGKILL cannot unwind Python. The registered owner lock lets the existing
    supervisor sweep recover such attempts after its normal retention window.
    Explicit roots are useful for admitted callers with their own placement.
    """
    root = Path(root if root is not None else
                os.environ.get("TMPDIR") or tempfile.gettempdir()).resolve(strict=True)
    checkout = Path.cwd().resolve()
    if (root == checkout or checkout in root.parents
            or any((parent / ".git").exists() for parent in (root, *root.parents))):
        raise ValueError("pytest scratch must be outside the checkout; choose a short TMPDIR")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    parent = os.open(root, flags)
    try:
        pbtest_scratch_roots.register_pbtest_root(parent)
        owned_tree.reap_dead_trees(parent, re.compile("pb-[a-z0-9_]{8}"))
        attempt = Path(tempfile.mkdtemp(prefix="pb-", dir=root))
        directory = os.open(attempt.name, flags, dir_fd=parent)
        try:
            lock = owned_tree.take_owner_lock(directory, owned_tree.LOCK_NAME)
        finally:
            os.close(directory)
        previous = signal.signal(signal.SIGTERM, _terminated)
        try:
            yield attempt / "pytest"
        finally:
            signal.signal(signal.SIGTERM, previous)
            owned_tree.release_owned_tree(parent, attempt.name, lock, remove=True)
    finally:
        os.close(parent)


def _terminated(signum, frame):
    raise SystemExit(128 + signum)


def main():
    """Run inside an admitted action; do not submit another PB action."""
    import pytest

    with pytest_basetemp() as base:
        return int(pytest.main([*sys.argv[1:], "--basetemp", str(base)]))


if __name__ == "__main__":
    raise SystemExit(main())
