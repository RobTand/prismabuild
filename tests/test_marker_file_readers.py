"""A test that reads a helper's marker waits for its content, not its name.

``Path.write_text`` creates the file and then writes it, so a reader that waits
on ``exists()`` and immediately parses can win that race and read nothing. The
failure lands on the reader, as ``invalid literal for int() with base 10: ''``
or as an unpack of an empty list, and it looks like the behaviour under test
broke rather than the test's own timing.

It is not hypothetical. ``tests/test_process_group_termination.py`` failed this
way about one run in six on the x86 box while passing on both Sparks, and the
pull-queue withdrawal tests were reported failing intermittently under ``-n 8``
with the same signature. The helpers below wait for a value; these tests hold
them to it by handing each one the empty file the race produces.
"""
from __future__ import annotations

from pathlib import Path
import sys
import threading
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_pool import _await_pid as await_action_pid  # noqa: E402
from test_pool_withdraw import _await_pid as await_pool_pid  # noqa: E402
from test_process_group_termination import _await_marker  # noqa: E402
from test_withdraw_end_to_end import _await_pid as await_worker_pid  # noqa: E402

#: Long enough that a reader which does not wait returns inside it, short
#: enough to cost nothing when the reader does wait.
WRITE_DELAY_S = 0.2


def _write_after(path: Path, text: str) -> threading.Thread:
    """Create the file empty now and fill it shortly, as ``write_text`` does."""

    path.write_text("")

    def later() -> None:
        time.sleep(WRITE_DELAY_S)
        path.write_text(text)

    thread = threading.Thread(target=later, daemon=True)
    thread.start()
    return thread


def test_the_pool_withdrawal_reader_waits_for_a_pid(tmp_path: Path) -> None:
    pidfile = tmp_path / "action.pid"
    thread = _write_after(pidfile, "4242\n")
    try:
        assert pidfile.exists(), "the empty file is what the reader must not accept"
        assert await_pool_pid(pidfile) == 4242
    finally:
        thread.join()


def test_the_timeout_reader_waits_for_a_pid(tmp_path: Path) -> None:
    """``tests/test_pool.py``'s reader, added after it failed on the x86 box.

    The timeout tests there start an action that writes its own pid and then
    outlives the kill, so the test needs that pid to reap it. The old
    ``int(pidfile.read_text())`` had no wait at all, which is a wider window
    than the other three had: it failed
    ``tests/test_pool.py::test_execute_timeout_returns_even_when_the_action_outlives_the_kill``
    on dl380g10 while both Sparks passed.
    """

    pidfile = tmp_path / "action.pid"
    thread = _write_after(pidfile, "4242\n")
    try:
        assert pidfile.exists(), "the empty file is what the reader must not accept"
        assert await_action_pid(pidfile) == 4242
    finally:
        thread.join()


def test_the_worker_withdrawal_reader_waits_for_a_pid(tmp_path: Path) -> None:
    pidfile = tmp_path / "worker.pid"
    thread = _write_after(pidfile, "909")
    try:
        assert await_worker_pid(pidfile) == 909
    finally:
        thread.join()


def test_the_process_group_reader_waits_for_both_fields(tmp_path: Path) -> None:
    """Both fields, because the marker carries a pid and a process group."""

    marker = tmp_path / "child.txt"
    thread = _write_after(marker, "1234 1200")
    try:
        assert _await_marker(marker) == (1234, 1200)
    finally:
        thread.join()


def test_a_half_written_marker_is_not_accepted(tmp_path: Path) -> None:
    """One field is as unusable as none, and arrives the same way."""

    marker = tmp_path / "child.txt"
    marker.write_text("1234")

    def finish() -> None:
        time.sleep(WRITE_DELAY_S)
        marker.write_text("1234 1200")

    thread = threading.Thread(target=finish, daemon=True)
    thread.start()
    try:
        assert _await_marker(marker) == (1234, 1200)
    finally:
        thread.join()


def test_the_pattern_these_helpers_replace_reads_the_empty_file(
    tmp_path: Path,
) -> None:
    """Why the helpers exist, stated as the failure they used to produce.

    The old shape was ``_await(path.exists)`` and then ``int(path.read_text())``.
    Existence becomes true when the file is created, which is before it is
    written, so both halves are reached in order and the parse is handed an
    empty string. This is that sequence, with the window held open.
    """

    pidfile = tmp_path / "action.pid"
    thread = _write_after(pidfile, "4242")
    try:
        assert pidfile.exists(), "the old wait would have returned here"
        with pytest.raises(ValueError):
            int(pidfile.read_text())
    finally:
        thread.join()
