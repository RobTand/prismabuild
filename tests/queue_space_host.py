"""Run the suite against a repeatable low-space host: pytest -p queue_space_host.

Set PB_TEST_HOST_SPACE to warning, critical, inodes or full (no bytes). This replaces only the
filesystem-capacity module's OS view; explicit samples in capacity tests still
win. Client bootstrap programs get the same simulated host in their children.
"""
import os
from pathlib import Path


def host_sample(state):
    available = {"warning": 13 << 30, "critical": 3 << 30, "inodes": 40 << 30, "full": 0}[state]
    return os.statvfs_result((4096, 4096, (100 << 30) // 4096,
                              (100 << 30) // 4096, available // 4096,
                              1000, 900, 1 if state == "inodes" else 900, 0, 255))


def install_host_space(state):
    from prismabuild import filesystem_capacity
    import queue_space_support

    sample = host_sample(state)
    # Isolated record-compatibility interpreters see the same simulated host.
    queue_space_support.HOST_PRELUDE = queue_space_support.statvfs_prelude(sample)
    read_room = filesystem_capacity.local_disk_room
    host_statvfs = filesystem_capacity.os.statvfs

    def room(path, floor_percent, *, statvfs=None):
        # Only default zero-floor observations use the simulated host. Explicit
        # samples, including module/global OS patches, keep their own meaning.
        if (statvfs is None and floor_percent == 0
                and filesystem_capacity.os.statvfs is host_statvfs):
            statvfs = lambda path: sample
        return read_room(path, floor_percent, statvfs=statvfs)

    filesystem_capacity.local_disk_room = room


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
