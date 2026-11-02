"""Explicit pytest plugin: one assigned CPU and four acknowledged Python loads.

Enable with ``-p tests.queue_contention_2669``. Never expands assigned affinity.
"""
from contextlib import contextmanager, suppress
import os
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest


def pytest_addoption(parser):
    parser.addoption("--census-deschedule", action="store_true",
                     help="advance the host deadline clock at reader ownership")
    parser.addoption("--sample-deschedule", action="store_true",
                     help="advance host Unix time after CPU sample publication")


@pytest.fixture(autouse=True)
def census_descheduling(request, monkeypatch):
    if not request.config.getoption("--census-deschedule"):
        return
    from prismabuild import _bounded_reader as reader
    wall = [time.monotonic()]
    monkeypatch.setattr(reader, "time", SimpleNamespace(
        **{**vars(time), "monotonic": lambda: wall[0]}))
    capture = reader._capture_ownership

    def descheduled(*args, **kwargs):
        identity = capture(*args, **kwargs)
        wall[0] += 6.
        return identity

    monkeypatch.setattr(reader, "_capture_ownership", descheduled)


@pytest.fixture(autouse=True)
def sample_descheduling(request, monkeypatch):
    if not request.config.getoption("--sample-deschedule"):
        return
    from prismabuild import adaptive_cpu
    wall_time = time.time
    elapsed = [0.]
    monkeypatch.setattr(time, "time", lambda: wall_time() + elapsed[0])
    sample = adaptive_cpu.Controller.sample

    def descheduled(controller):
        observed = sample(controller)
        elapsed[0] += 6.
        if observed:
            print("CPU sample age after descheduling:",
                  time.time() - observed["sampled_unix"])
        return observed

    monkeypatch.setattr(adaptive_cpu.Controller, "sample", descheduled)


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
                 "import os\nentered = False\nwhile True:\n"
                 "    if not entered:\n        os.write(1, b'R')\n        entered = True\n"
                 "    value = 37 * 41\n"],
                stdout=subprocess.PIPE, env=environment)
            children.append(child)
            assert child.stdout.read(1) == b"R", "load child did not enter its loop"
        print("contention: one assigned CPU, four acknowledged pure-Python loops")
        yield
    finally:
        for child in children:
            with suppress(ProcessLookupError):
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
