"""A deterministic roomy disk for the local resident tests (#1506).

``local_tier.require_write_space`` refuses a write unless the filesystem has
20 GiB free beyond the Docker allowance plus 1.5 times the bytes written, and it
reads the real disk.  The x86 pool runs these tests on dl380g10, whose root had
14 GiB free, so 29 tests failed there and passed on a roomy host.  A test of the
copy, lease or eviction logic must not depend on the host's free space.

The fixture swaps the ``os`` that ``local_resident`` and ``local_tier`` see for
a proxy whose ``statvfs`` reports at least a terabyte free.  It does not patch
the global ``os.statvfs``, and the guard itself is unchanged: its refusal is
still tested directly, with an explicit sample, in
``test_local_resident_space_isolation.py``.
"""
import os
import types

import pytest

from prismabuild import local_resident, local_tier

ROOMY_BYTES = 1 << 40


class _RoomyOs:
    """``os`` with a ``statvfs`` that reports a large, empty filesystem."""

    def __getattr__(self, name):
        return getattr(os, name)

    @staticmethod
    def statvfs(path):
        real = os.statvfs(path)
        frsize = real.f_frsize or 4096
        roomy = ROOMY_BYTES // frsize
        return types.SimpleNamespace(
            f_bsize=real.f_bsize, f_frsize=frsize,
            f_blocks=max(real.f_blocks, roomy),
            f_bfree=max(real.f_bfree, roomy), f_bavail=max(real.f_bavail, roomy))


@pytest.fixture
def roomy_disk(monkeypatch):
    for module in (local_resident, local_tier):
        monkeypatch.setattr(module, "os", _RoomyOs())
