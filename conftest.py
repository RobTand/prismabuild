"""Make a bare checkout run its own suite with a plain `pytest`.

Not `pythonpath = ["src"]` in pyproject: that fixes the pytest process only,
and several tests spawn a *subprocess* that imports prismabuild (the worker
entry point, the "importing dagster.py pulls in no dagster" check). Those
children inherit the environment, not pytest's sys.path, so the import has to
be exported too. Without this the suite passes only for someone who already
knows to set PYTHONPATH -- which is exactly the knowledge a fresh clone lacks.
"""

import os
import pathlib
import sys

_SRC = str((pathlib.Path(__file__).parent / "src").resolve())

if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

_existing = os.environ.get("PYTHONPATH", "")
if _SRC not in _existing.split(os.pathsep):
    os.environ["PYTHONPATH"] = os.pathsep.join(p for p in (_SRC, _existing) if p)

# The per-test bound, wired here rather than in ``tests/conftest.py`` because
# ``pytest_plugins`` is honoured only in the rootdir conftest.  It is inert
# until ``PRISMABUILD_TEST_TIMEOUT_S`` names a bound; ``pbtest.py`` sets that
# for every shard it builds, so a hung test fails as itself instead of holding
# a fleet slot to the execution ceiling with nothing naming it (#600).
pytest_plugins = ("prismabuild.pytest_test_bound",)
