"""Isolated record-compatibility interpreters see a controlled capacity (#2551)."""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests/compat/support"))
import queue_space_support as support
from queue_space_host import host_sample
from record_compat import accepted, invoke, with_prelude


@pytest.mark.parametrize("state", ["inodes", "full"])
def test_a_spool_case_ignores_the_hosts_inodes_and_bytes(state, tmp_path, monkeypatch):
    monkeypatch.setattr(support, "HOST_PRELUDE", support.statvfs_prelude(host_sample(state)))
    result = accepted(invoke(ROOT, "spool-record/reservation", tmp_path / "store"))
    assert result["raw"]


DRIVER = "/support/compat_isolate.py"
ARGS = ["--root", "/generation", "--adapters", "/support/compat_adapters.py", "--store", "/store"]


def test_a_plain_launch_gets_the_prelude_before_an_unedited_driver():
    command = ["/py", "-I", "-S", DRIVER, *ARGS]
    wrapped = with_prelude(command, "PRELUDE\n")
    assert wrapped[:4] == ["/py", "-I", "-S", "-c"] and wrapped[4].startswith("PRELUDE\n")
    assert wrapped[5:] == [DRIVER, *ARGS]


def test_an_archival_launch_gets_the_prelude_ahead_of_its_bootstrap():
    command = ["/old/py", "-I", "-S", "-c", "BOOTSTRAP", "{expected}", DRIVER, *ARGS]
    assert with_prelude(command, "PRELUDE\n") == [
        "/old/py", "-I", "-S", "-c", "PRELUDE\nBOOTSTRAP", "{expected}", DRIVER, *ARGS]


def test_a_namespaced_launch_keeps_its_wrapper_arguments():
    command = ["bwrap", "--bind", "/a", "/b", "--", "/py", "-I", "-S", DRIVER, *ARGS]
    wrapped = with_prelude(command, "PRELUDE\n")
    assert wrapped[:6] == command[:6] and wrapped[-len(ARGS) - 1:] == [DRIVER, *ARGS]


def test_no_prelude_leaves_the_command_alone():
    command = ["/py", "-I", "-S", DRIVER, *ARGS]
    assert with_prelude(command, "") is command
