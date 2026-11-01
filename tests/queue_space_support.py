"""Deterministic filesystem inputs for queue diagnostics, including CLI children.

Tests of pressure override ``filesystem_capacity.os.statvfs`` as usual. A test
that patches the real ``os.statvfs`` also takes precedence. No global OS module
or production threshold is changed in the pytest process.

Isolated record-compatibility interpreters (``python -I -S compat_isolate.py``)
load none of this, so ``record_compat.invoke`` runs ``isolated_prelude()`` in
them before the sealed driver. ``HOST_PRELUDE`` is the simulated host of
``queue_space_host``; ``ISOLATED_PIN`` runs after it and wins, as the shared
fixture wins over the simulated host in the pytest process.
"""
import os
from pathlib import Path
import types


AMPLE_SPACE = os.statvfs_result((4096, 4096, (1 << 40) // 4096,
                                (1 << 40) // 4096, (1 << 40) // 4096,
                                1_000_000, 900_000, 900_000, 0, 255))


def statvfs_prelude(sample):
    """Source that makes every ``os.statvfs`` read in a child return ``sample``."""
    return ("import os\n"
            f"os.statvfs = lambda path, _sample=os.statvfs_result({tuple(sample)!r}): _sample\n")


HOST_PRELUDE = ""
ISOLATED_PIN = statvfs_prelude(AMPLE_SPACE)


def isolated_prelude():
    return HOST_PRELUDE + ISOLATED_PIN


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
