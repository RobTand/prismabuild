"""Explicit pytest plugin: one assigned CPU and four acknowledged Python loads.

Enable with ``-p queue_contention_2669``. Never expands assigned affinity.
"""
from contextlib import contextmanager
import os
import subprocess
import sys

import pytest


@contextmanager
def cpu_contention():
    affinity = os.sched_getaffinity(0)
    children = []
    names = ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
             "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS")
    previous = {name: os.environ.get(name) for name in names}
    try:
        os.sched_setaffinity(0, {min(affinity)})
        for name in names:
            os.environ[name] = "1"
        environment = {name: value for name, value in os.environ.items()
                       if name in ("PATH", "HOME", "LANG", "TMPDIR", *names)}
        for _ in range(4):
            child = subprocess.Popen(
                [sys.executable, "-I", "-S", "-c",
                 "import os\nos.write(1, b'R')\nwhile True:\n    value = 37 * 41\n"],
                stdout=subprocess.PIPE, env=environment)
            children.append(child)
            assert child.stdout.read(1) == b"R", "load child did not enter its loop"
        print("contention: one assigned CPU, four acknowledged pure-Python loops")
        yield
    finally:
        for child in children:
            child.terminate()
        for child in children:
            try:
                child.wait(timeout=60)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=60)
            finally:
                child.stdout.close()
        os.sched_setaffinity(0, affinity)
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


@pytest.fixture(scope="session", autouse=True)
def contention():
    with cpu_contention():
        yield
