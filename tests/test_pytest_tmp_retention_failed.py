"""A passing test's tmp_path is gone when the test ends (inode-limited scratch).

On 2026-10-05 the RAM tmpfs that holds the CPU suites' scratch ran out of
inodes, not bytes: six concurrent full suites left 750,000 inodes in pytest's
retained run directories and every action placed on the host failed in preflight
with OSError 28 while the filesystem reported free space. pytest keeps a
passing test's directory by default. ``tmp_path_retention_policy = "failed"`` in
``pyproject.toml`` removes it at once; ``pbtest`` refuses a command-line ``-o``,
so the setting has to live in the sealed checkout.

Two ordered tests show the effect: the first records its directory, the second
asserts it no longer exists. The ordering holds in one process, which is how a
``pbtest`` shard runs by default; a distributed run skips the second check.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_FIRST: dict[str, Path] = {}


def test_the_configuration_asks_for_the_failed_only_policy(request):
    assert request.config.getini("tmp_path_retention_policy") == "failed"


def test_a_passing_tests_directory_is_recorded(tmp_path):
    (tmp_path / "marker").write_text("x")
    _FIRST["path"] = tmp_path
    assert tmp_path.exists()


def test_the_previous_passing_tests_directory_is_already_gone(request):
    if request.config.pluginmanager.has_plugin("dsession"):
        pytest.skip("distributed run: the previous test may live in another process")
    previous = _FIRST.get("path")
    assert previous is not None, "the recording test must run first in this process"
    assert not previous.exists(), f"{previous} survived its passing test"
