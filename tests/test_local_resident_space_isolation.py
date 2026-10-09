"""The local resident tests do not depend on the host's free disk (#1506).

Deterministic on any host: the real ``statvfs`` is replaced by a nearly full
disk for the duration of each test.
"""
import os
import types

import pytest

from prismabuild import local_resident, local_tier
from local_resident_space import roomy_disk  # noqa: F401

SPEC = {"floor_fraction": .05, "docker_allowance_gib": 0}


def _nearly_full(path):
    """4 KiB blocks, 100 GiB in all, 14 GiB free: dl380g10's root on 2026-10-07."""
    block = 4096
    return types.SimpleNamespace(
        f_bsize=block, f_frsize=block, f_blocks=(100 << 30) // block,
        f_bfree=(14 << 30) // block, f_bavail=(14 << 30) // block)


def test_the_guard_still_refuses_a_nearly_full_disk():
    """The 20 GiB rule is unchanged: 14 GiB free refuses even a tiny write."""
    with pytest.raises(ValueError, match="D1 floor"):
        local_tier.require_write_space(SPEC, _nearly_full("/"), 1024)


def test_the_roomy_disk_hides_a_nearly_full_host(roomy_disk, monkeypatch, tmp_path):
    """With the fixture the modules sample a roomy disk, whatever the host has."""
    monkeypatch.setattr(os, "statvfs", _nearly_full)
    assert os.statvfs(tmp_path).f_bavail * 4096 == 14 << 30
    for module in (local_resident, local_tier):
        local_tier.require_write_space(SPEC, module.os.statvfs(tmp_path), 1024)
