"""Deterministic filesystem inputs for queue diagnostics, including CLI children.

Tests of pressure override ``filesystem_capacity.os.statvfs`` as usual. A test
that patches the real ``os.statvfs`` also takes precedence. No global OS module
or production threshold is changed.
"""
import os
from pathlib import Path
import types


AMPLE_SPACE = os.statvfs_result((4096, 4096, (1 << 40) // 4096,
                                (1 << 40) // 4096, (1 << 40) // 4096,
                                1_000_000, 900_000, 900_000, 0, 255))


def queue_space_os():
    native_statvfs = os.statvfs

    def statvfs(path):
        # A test's explicit filesystem fault must override the default sample.
        if os.statvfs is not native_statvfs:
            return os.statvfs(path)
        return AMPLE_SPACE

    return types.SimpleNamespace(**{**vars(os), "statvfs": statvfs})


def pin_queue_space():
    """Install the same input in a scratch-store client outside pytest."""
    from prismabuild import filesystem_capacity

    filesystem_capacity.os = queue_space_os()


QUEUE_SPACE_BOOTSTRAP = (
    f"import sys; sys.path.insert(0, {str(Path(__file__).parent)!r}); "
    "from queue_space_support import pin_queue_space; pin_queue_space()\n"
)
