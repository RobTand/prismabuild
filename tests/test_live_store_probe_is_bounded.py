"""The suite must not be hostage to the NFS server being up.

``/mnt/shared`` is a ``hard`` NFS mount with ``timeo=600``. When the server
is down, a read of it does not fail and does not return: it blocks in
uninterruptible sleep, where no timeout, signal or thread cancellation
reaches it. The session guard used to ask ``LIVE_ROOT.is_dir()`` directly,
so every test run on every box stopped at ``pytest_sessionstart`` for as
long as dl380g10 was away. Measured on 2026-09-05: that call had not
returned after 15 s, and xdist workers sat in ``rpc_wait_bit_killable`` for
over 330 s.

The outage itself cannot be staged in a test, so the bound is tested where
it lives: given a probe that does not answer, ``reachable`` must give up on
time and say no. The probe command is injectable for exactly that reason.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

from conftest import LIVE_PROBE_TIMEOUT_S, reachable


def _sleeper(seconds: float):
    def argv(root: Path) -> list[str]:
        return [sys.executable, "-c", f"import time; time.sleep({seconds})"]
    return argv


def test_a_directory_that_answers_is_reachable(tmp_path: Path) -> None:
    assert reachable(tmp_path) is True


def test_a_path_that_is_not_a_directory_is_not_reachable(tmp_path: Path) -> None:
    missing = tmp_path / "nothing-here"
    assert reachable(missing) is False
    plain = tmp_path / "file"
    plain.write_text("")
    assert reachable(plain) is False


def test_a_probe_that_never_answers_is_given_up_on(tmp_path: Path) -> None:
    """The defect: an unanswering probe used to hold the session forever."""

    started = time.monotonic()
    assert reachable(tmp_path, timeout_s=0.5, argv=_sleeper(30)) is False
    elapsed = time.monotonic() - started
    assert elapsed < 10, (
        f"the probe took {elapsed:.1f}s to give up on a 0.5s budget; an "
        "unreachable mount would hold the whole session the same way"
    )


def test_giving_up_does_not_wait_for_the_child_to_die() -> None:
    """A killed child in uninterruptible sleep would never be reaped.

    ``reachable`` must not call ``wait`` after ``kill``. A process blocked in
    an NFS read ignores SIGKILL until the server returns, so waiting for it
    would put the hang back where it was.
    """

    import inspect

    source = inspect.getsource(reachable)
    after_kill = source.split("probe.kill()", 1)[1]
    assert ".wait(" not in after_kill and ".communicate(" not in after_kill, (
        "reachable() waits for the child after killing it, which reintroduces "
        "the hang this function exists to avoid"
    )


def test_the_budget_is_configurable_and_sane() -> None:
    assert 0 < LIVE_PROBE_TIMEOUT_S <= 60
