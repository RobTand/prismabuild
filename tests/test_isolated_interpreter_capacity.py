"""How the controlled capacity prelude reaches an isolated interpreter (#2551).

The end-to-end spool case needs bwrap, so it lives with the other sandbox
cases in test_current_receipt_compat_composition.py.
"""
from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests/compat/support"))
import queue_space_support as support
from record_compat import _ARCHIVAL_BOOTSTRAP, invoke, isolated_prelude, with_prelude


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


def test_invoke_runs_the_prelude_in_the_child_before_the_driver(tmp_path, monkeypatch):
    # Delivery, not content: a prelude that exits proves the launch ran it.
    monkeypatch.setattr(support, "HOST_PRELUDE", "raise SystemExit(97)\n")
    result = invoke(ROOT, "shape-table/unhashed", tmp_path / "store")
    assert result["process_returncode"] == 97
    assert "driver_sha256" not in result


def test_the_real_preludes_compile_in_both_launch_shapes():
    plain = ["/py", "-I", "-S", DRIVER, *ARGS]
    archival = ["/old/py", "-I", "-S", "-c", _ARCHIVAL_BOOTSTRAP, "{expected}", DRIVER, *ARGS]
    prelude = isolated_prelude()
    assert prelude
    for command in (plain, archival):
        wrapped = with_prelude(command, prelude)
        assert wrapped[3] == "-c"
        compile(wrapped[4], "<bootstrap>", "exec")
