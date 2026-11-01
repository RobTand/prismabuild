"""Run the suite against a repeatable low-space host: pytest -p queue_space_host.

Set PB_TEST_HOST_SPACE to warning, critical or inodes. This replaces only the
filesystem-capacity module's OS view; explicit samples in capacity tests still
win. Client bootstrap programs get the same simulated host in their children.
"""
import os
from pathlib import Path
import types

import pytest


def install_host_space(state):
    from prismabuild import filesystem_capacity

    available = {"warning": 13, "critical": 3, "inodes": 40}[state] << 30
    sample = os.statvfs_result((4096, 4096, (100 << 30) // 4096,
                               (100 << 30) // 4096, available // 4096,
                               1000, 900, 1 if state == "inodes" else 900, 0, 255))
    filesystem_capacity.os = types.SimpleNamespace(**{**vars(os), "statvfs": lambda path: sample})


def pytest_configure(config):
    install_host_space(os.environ.get("PB_TEST_HOST_SPACE", "warning"))


def pytest_collection_modifyitems(items):
    state = os.environ.get("PB_TEST_HOST_SPACE", "warning")
    prefix = (f"import sys; sys.path.insert(0, {str(Path(__file__).parent)!r}); "
              f"from queue_space_host import install_host_space; install_host_space({state!r})\n")
    modules = {item.module for item in items}
    for module in modules:
        for name in ("BOOTSTRAP", "PBRUN_CHILD"):
            program = getattr(module, name, None)
            if isinstance(program, str) and "pbrun.main()" in program:
                setattr(module, name, prefix + program)
